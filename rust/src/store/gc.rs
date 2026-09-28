use std::collections::{HashMap, HashSet};
use std::io;
use std::ops::AddAssign;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, ensure};
use harmonia_store_path::StorePath;
use tokio::task::JoinHandle;
use tracing::warn;
use walkdir::WalkDir;

use crate::closure;

use super::fs::{Scratch, remove_file};
use super::{Store, list_store_paths};

impl Store {
    /// Forgets the volumes that `is_bound(view, target)` says aren't mounted,
    /// and moves the views no volume uses to the trash. It returns before the
    /// deletion, which after a reboot can take longer than kubelet waits.
    pub async fn prune_views(
        self: &Arc<Self>,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + 'static,
    ) -> anyhow::Result<()> {
        let pruned = |this: &Self, trash: &mut _| this.drop_unmounted(trash, is_bound).map(drop);
        self.exclusively(pruned).await?.0
    }

    /// Prunes the views like [`Store::prune_views`], then deletes the store
    /// paths that no view, ensure or pending sync holds and that weren't used
    /// within `keep_recent`. Then it deletes more such paths, least recently
    /// used first, until their NARs add up to `excess.bytes` and their store
    /// objects to `excess.inodes` inodes. Returns how many paths it deleted.
    pub async fn collect(
        self: &Arc<Self>,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + 'static,
        keep_recent: Duration,
        excess: Excess,
    ) -> anyhow::Result<usize> {
        let started = Instant::now();
        let collected = async {
            // Counting inodes reads every directory of the node store, so it
            // runs before `exclusively` makes publishes wait.
            let this = self.clone();
            let inodes = tokio::task::spawn_blocking(move || this.inode_counts(excess)).await??;
            let (collected, deleting) = self
                .exclusively(move |this, trash| {
                    let mut in_use = this.drop_unmounted(trash, is_bound)?;
                    in_use.extend(this.ensuring.lock().unwrap().keys().cloned());
                    this.drop_unused(trash, &in_use, keep_recent, excess, &inodes)
                })
                .await?;
            // Waits for the deletion, so the next check of free space sees it.
            deleting.await?;
            collected
        }
        .await;
        self.metrics.collection(collected.is_ok(), started);
        collected
    }

    /// Returns how many bytes and inodes to free for the store's filesystem
    /// to have `ensure_free` available.
    pub fn excess(&self, ensure_free: EnsureFree) -> io::Result<Excess> {
        Ok(excess(Space::of(&self.dir)?, ensure_free))
    }

    /// Runs `f` with no publish or unpublish under way. Returns the result of
    /// `f`, and a task that deletes what `f` moved into the trash. It releases
    /// the guard before it starts the task, since deleting big views and store
    /// objects is slow.
    pub(super) async fn exclusively<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&Self, &mut Vec<Scratch>) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<(anyhow::Result<T>, JoinHandle<()>)> {
        let this = self.clone();
        let guard = self.gc.write().await;
        let (result, trash) = tokio::task::spawn_blocking(move || {
            let mut trash = Vec::new();
            (f(&this, &mut trash), trash)
        })
        .await?;
        drop(guard);
        Ok((result, self.discard(trash)))
    }

    /// Deletes `trash` on a blocking thread. The task keeps the store open,
    /// with its lock on the state dir, until the trash is gone.
    pub fn discard(
        self: &Arc<Self>,
        trash: impl IntoIterator<Item = Scratch> + Send + 'static,
    ) -> JoinHandle<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            for trash in trash {
                let path = trash.path().to_owned();
                if let Err(e) = trash.close() {
                    warn!("deleting {}: {e}", path.display());
                }
            }
            drop(this);
        })
    }

    fn drop_unused(
        &self,
        trash: &mut Vec<Scratch>,
        in_use: &HashSet<StorePath>,
        keep_recent: Duration,
        excess: Excess,
        inodes: &HashMap<StorePath, u64>,
    ) -> anyhow::Result<usize> {
        let old = |used: SystemTime| used.elapsed().unwrap_or_default() >= keep_recent;
        // Deleting a path frees every inode in its store object, and one more
        // for its narinfo. Store objects share no inodes, and a view that
        // links one either keeps it in use or is deleted in the same
        // collection. The narinfo has the NAR size, so `cost` reads it before
        // the loop below deletes it.
        let cost = |path: &StorePath| Excess {
            bytes: self.nar_size(path),
            inodes: inodes.get(path).map_or(1, |n| n + 1),
        };
        let mut doomed = Vec::new();
        let mut spared = Vec::new();
        for path in list_store_paths(&self.dir)? {
            // A fetch marks its path unsynced before moving it in.
            if in_use.contains(&path) || self.unsynced.lock().unwrap().contains_key(&path) {
                continue;
            }
            let used = self.last_use(&path);
            if old(used) {
                let cost = cost(&path);
                doomed.push((path, cost));
            } else {
                spared.push((used, path));
            }
        }
        let mut freed = Excess::default();
        for (_, cost) in &doomed {
            freed += *cost;
        }
        spared.sort_unstable();
        for (_, path) in spared {
            if freed.bytes >= excess.bytes && freed.inodes >= excess.inodes {
                break;
            }
            let cost = cost(&path);
            freed += cost;
            doomed.push((path, cost));
        }
        for (path, cost) in &doomed {
            trash.push(self.evict(path, cost.bytes)?);
            self.metrics.deleted(cost.bytes);
            // A fetch may have brought the path back since the check above,
            // with a narinfo of its own.
            let unsynced = self.unsynced.lock().unwrap();
            if !unsynced.contains_key(path) {
                self.remove_narinfo(path)?;
            }
        }
        // A crash can leave a narinfo without its path. A fetch marks its path
        // unsynced before writing the narinfo, so with `unsynced` locked, an
        // unmarked narinfo without its path is safe to delete.
        let narinfos = self.caches.local_dir();
        for entry in std::fs::read_dir(narinfos)
            .with_context(|| format!("reading {}", narinfos.display()))?
        {
            let entry = entry?;
            // Any other name is the temporary file of a narinfo being
            // written. Start-up deletes the ones a crash left.
            let Some((path, _)) = closure::local_name(&entry.file_name()) else {
                continue;
            };
            if in_use.contains(&path) {
                continue;
            }
            let modified = entry.metadata().and_then(|m| m.modified());
            if !old(modified.unwrap_or(SystemTime::UNIX_EPOCH)) {
                continue;
            }
            let unsynced = self.unsynced.lock().unwrap();
            if !unsynced.contains_key(&path) && !self.present(&path) {
                remove_file(&entry.path())?;
            }
        }
        Ok(doomed.len())
    }

    /// Counts the inodes in each store object, if `excess` asks for inodes.
    /// Collection treats a path that appears after the count as holding none.
    fn inode_counts(&self, excess: Excess) -> anyhow::Result<HashMap<StorePath, u64>> {
        if excess.inodes == 0 {
            return Ok(HashMap::new());
        }
        let count = |path: StorePath| {
            let tree = self.unpacked(&path);
            let inodes = count_inodes(&tree).unwrap_or_else(|e| {
                warn!("counting the inodes in {}: {e}", tree.display());
                0
            });
            (path, inodes)
        };
        let paths = list_store_paths(&self.dir)?;
        Ok(paths.into_iter().map(count).collect())
    }
}

/// Counts the files, directories and symlinks in the tree at `path`.
fn count_inodes(path: &Path) -> io::Result<u64> {
    (WalkDir::new(path).follow_root_links(false).into_iter())
        .try_fold(0, |n, entry| entry.map(|_| n + 1))
        .map_err(io::Error::from)
}

/// How much to free on the state dir's filesystem to meet `--ensure-free`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Excess {
    pub bytes: u64,
    pub inodes: u64,
}

impl AddAssign for Excess {
    fn add_assign(&mut self, other: Self) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.inodes = self.inodes.saturating_add(other.inodes);
    }
}

/// What `statvfs` says about a filesystem.
#[derive(Clone, Copy, Debug)]
pub(super) struct Space {
    size: u64,
    free: u64,
    /// 0 on a filesystem with no limit on inodes, like btrfs.
    pub(super) inodes: u64,
    pub(super) free_inodes: u64,
}

impl Space {
    pub(super) fn of(path: &Path) -> io::Result<Self> {
        let stat = rustix::fs::statvfs(path)?;
        Ok(Self {
            size: stat.f_blocks * stat.f_frsize,
            free: stat.f_bavail * stat.f_frsize,
            inodes: stat.f_files,
            free_inodes: stat.f_favail,
        })
    }
}

/// Returns how many bytes and inodes to free on a filesystem with `space` to
/// meet `ensure_free`. The inode target is the same share of the
/// filesystem's inodes as the byte target is of its size.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "a share of a total is within it"
)]
fn excess(space: Space, ensure_free: EnsureFree) -> Excess {
    let percent = |total: u64, p: f64| (total as f64 * p / 100.0) as u64;
    let (bytes, inodes) = match ensure_free {
        EnsureFree::Bytes(bytes) => {
            let share = u128::from(space.inodes) * u128::from(bytes.min(space.size))
                / u128::from(space.size.max(1));
            (bytes, share as u64)
        }
        EnsureFree::Percent(p) => (percent(space.size, p), percent(space.inodes, p)),
    };
    Excess {
        bytes: bytes.saturating_sub(space.free),
        inodes: inodes.saturating_sub(space.free_inodes),
    }
}

/// Free space to keep on the state dir's filesystem, and the same share of
/// its inodes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EnsureFree {
    Bytes(u64),
    /// A percentage of the filesystem's size.
    Percent(f64),
}

/// Parses `--ensure-free` like harmonia-gc does: a percentage like `20%`, or a
/// size like `50G`, where K, M, G and T are powers of 1024.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "the size is checked to fit"
)]
pub fn parse_ensure_free(s: &str) -> anyhow::Result<EnsureFree> {
    let s = s.trim();
    if let Some(p) = s.strip_suffix('%') {
        let p: f64 = (p.trim().parse()).with_context(|| format!("invalid percentage {s:?}"))?;
        ensure!(
            (0.0..=100.0).contains(&p),
            "{s:?} isn't between 0% and 100%"
        );
        return Ok(EnsureFree::Percent(p));
    }
    let (number, shift) = match s.chars().last().map(|c| c.to_ascii_uppercase()) {
        Some('K') => (&s[..s.len() - 1], 10),
        Some('M') => (&s[..s.len() - 1], 20),
        Some('G') => (&s[..s.len() - 1], 30),
        Some('T') => (&s[..s.len() - 1], 40),
        _ => (s, 0),
    };
    let n: f64 = number
        .parse()
        .with_context(|| format!("invalid size {s:?}"))?;
    let bytes = n * (1u64 << shift) as f64;
    ensure!(
        (0.0..=u64::MAX as f64).contains(&bytes),
        "{s:?} is out of range"
    );
    Ok(EnsureFree::Bytes(bytes as u64))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{DirEntryExt, MetadataExt, PermissionsExt};

    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{fixture_root, fixtures};
    use crate::store::test_utils::*;

    const NONE: Excess = Excess {
        bytes: 0,
        inodes: 0,
    };
    const ALL: Excess = Excess {
        bytes: u64::MAX,
        inodes: u64::MAX,
    };

    #[tokio::test]
    async fn collects_unused_paths() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let day = Duration::from_hours(24);

        // v1 is mounted and holds hello alone. v2 isn't mounted, so collection
        // deletes its view, which counts as a use of its paths.
        age(&store, &paths, 2);
        let t1 = tmp.path().join("t1");
        let view = store
            .view("v1", std::slice::from_ref(&hello), &t1, UNBOUND)
            .unwrap();
        store
            .view("v2", &paths, &tmp.path().join("t2"), UNBOUND)
            .unwrap();
        let bound = move |v: &Path, t: &Path| Ok(v == view && t == t1);
        assert_eq!(store.collect(bound.clone(), day, NONE).await.unwrap(), 0);
        assert_eq!(file_names(tmp.path().join("volumes")), ["v1"]);
        assert_eq!(count(tmp.path().join("views")), 1);

        age(&store, &paths, 2);
        assert_eq!(store.collect(bound, day, NONE).await.unwrap(), 4);
        let deleted = sample(
            &crate::metrics::registry(&store),
            "nix_store_csi_gc_deleted_paths_total",
        );
        assert_eq!(deleted, Some(4.0));
        let size = sample(
            &crate::metrics::registry(&store),
            "nix_store_csi_store_size_bytes",
        );
        #[allow(clippy::cast_precision_loss)]
        let hello_size = store.nar_size(&hello) as f64;
        assert_eq!(size, Some(hello_size));
        assert_eq!(count(store.dir()), 1);
        assert_eq!(count(tmp.path().join("narinfo")), 1);
        assert!(store.unpacked(&hello).exists());

        // Dropping a view counts as a use too.
        age(&store, std::slice::from_ref(&hello), 2);
        store.drop_view("v1").unwrap();
        assert_eq!(store.collect(UNBOUND, day, NONE).await.unwrap(), 0);
        age(&store, std::slice::from_ref(&hello), 2);
        assert_eq!(store.collect(UNBOUND, day, NONE).await.unwrap(), 1);

        // Deleted paths come back on the next ensure.
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
        assert_eq!(count(store.dir()), 5);
        assert_eq!(count(tmp.path().join("tmp")), 0);
    }

    #[tokio::test]
    async fn collects_for_space() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let oldest = paths.iter().find(|&p| *p != hello).unwrap();
        age(&store, &paths, 1);
        age(&store, std::slice::from_ref(oldest), 3);
        let target = tmp.path().join("target");
        let view = store
            .view("v1", std::slice::from_ref(&hello), &target, UNBOUND)
            .unwrap();
        let bound = move |v: &Path, t: &Path| Ok(v == view && t == target);

        // Collection deletes the least recently used paths first, whatever
        // `keep_recent` is.
        let collect = |excess| store.collect(bound.clone(), Duration::MAX, excess);
        let bytes = |bytes| Excess { bytes, inodes: 0 };
        assert_eq!(collect(bytes(1)).await.unwrap(), 1);
        assert!(!store.unpacked(oldest).exists());
        assert_eq!(count(store.dir()), 4);

        // Paths in use stay, however much space is asked for.
        assert_eq!(collect(ALL).await.unwrap(), 3);
        assert_eq!(count(store.dir()), 1);
        assert!(store.unpacked(&hello).exists());
    }

    /// Collection deletes the least recently used paths until their store
    /// objects hold the inodes asked for.
    #[tokio::test]
    async fn collects_for_inodes() {
        let tmp = scratch();
        let Some((store, _, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        // The oldest first.
        let lru: Vec<_> = paths.iter().cloned().collect();
        for (days, path) in (1..).zip(lru.iter().rev()) {
            age(&store, std::slice::from_ref(path), days);
        }
        let inodes: Vec<u64> = (lru.iter())
            .map(|path| {
                let out = std::process::Command::new("find")
                    .arg(store.unpacked(path))
                    .output()
                    .unwrap();
                assert!(out.status.success());
                // And one for the narinfo.
                u64::try_from(io::BufRead::lines(&out.stdout[..]).count() + 1).unwrap()
            })
            .collect();
        assert!(inodes.iter().all(|&n| n > 2), "{inodes:?}");
        let excess = |inodes| Excess { bytes: 0, inodes };
        let collect = |inodes| store.collect(UNBOUND, Duration::MAX, excess(inodes));

        // Collection counts inodes before it holds off publishes, so it misses
        // a file added while it waits for a publish to end.
        let publish = store.gc_guard().await;
        let add_file = async {
            // A collection waiting for the lock keeps new publishes out.
            while store.gc.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
            let tree = store.unpacked(&lru[0]);
            std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::write(tree.join("added"), "").unwrap();
            drop(publish);
        };
        let (collected, ()) = tokio::join!(collect(inodes[0] + 1), add_file);
        assert_eq!(collected.unwrap(), 2);
        assert_eq!(collect(inodes[2]).await.unwrap(), 1);
        let left: Vec<_> = lru.iter().filter(|path| store.present(path)).collect();
        assert_eq!(left, [&lru[3], &lru[4]]);
    }

    /// Deletion renames what it deletes straight into tmp/, since a
    /// filesystem with no free inodes couldn't create a directory to hold it.
    #[tokio::test]
    async fn trashes_without_new_directories() {
        fn inodes(dir: &Path) -> Vec<u64> {
            (std::fs::read_dir(dir).unwrap())
                .map(|entry| entry.unwrap().ino())
                .collect()
        }
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let ino = |path: &Path| std::fs::symlink_metadata(path).unwrap().ino();
        let view = (store.view("v1", &paths, &tmp.path().join("t1"), UNBOUND)).unwrap();
        let view_ino = ino(&view);
        let trash = store.drop_view("v1").unwrap();
        assert_eq!(inodes(&tmp.path().join("tmp")), [view_ino]);
        drop(trash);

        let object = ino(&store.unpacked(&hello));
        let moved = store.exclusively(move |this, trash| {
            trash.push(this.evict(&hello, 0)?);
            Ok(inodes(&this.tmp))
        });
        let (moved, deleting) = moved.await.unwrap();
        deleting.await.unwrap();
        assert_eq!(moved.unwrap(), [object]);
    }

    /// Collection spares a path until its sync. Otherwise a new fetch of the
    /// path between the sync's syncfs and renames would get a .narinfo for
    /// files that aren't on disk.
    #[tokio::test]
    async fn spares_paths_until_synced() {
        let tmp = scratch();
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let store =
            Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), tmp.path())).unwrap());
        let collect = || store.collect(UNBOUND, Duration::ZERO, ALL);
        let turn = store.syncing.lock().await;
        let paths = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap();
        assert_eq!(collect().await.unwrap(), 0);
        assert_eq!(count(store.dir()), paths.len());
        assert_eq!(count(tmp.path().join("narinfo")), paths.len());
        drop(turn);
        settled(&store).await;
        assert_eq!(collect().await.unwrap(), paths.len());
    }

    /// Collection spares a closure from its resolution until the caller drops
    /// it, so it can't delete the paths a fetch is about to need, or those a
    /// publish is about to put in a view.
    #[tokio::test]
    async fn spares_ensured_closures() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let collect = || store.collect(UNBOUND, Duration::ZERO, ALL);
        // The store lacks one path, and its fetch waits for a job.
        let missing = paths.iter().find(|&p| *p != hello).unwrap();
        unsync(&store, missing);
        store.forget(vec![missing.clone()]).await.unwrap();
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        resolved(&ensuring).await;
        assert_eq!(collect().await.unwrap(), 0);
        assert_eq!(count(store.dir()), paths.len() - 1);

        drop(jobs);
        ensuring.done().await.unwrap();
        assert_eq!(collect().await.unwrap(), 0);
        drop(ensuring);
        settled(&store).await;
        assert_eq!(collect().await.unwrap(), paths.len());
    }

    /// A path whose sync hasn't run yet is in the store like any other.
    #[tokio::test]
    async fn uses_unsynced_paths() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let narinfos = tmp.path().join("narinfo");
        let unsynced = paths.iter().find(|&p| *p != hello).unwrap().clone();
        unsync(&store, &unsynced);

        // Publishes find it on the node.
        let closure = store.caches.resolve(&[hello]).await.unwrap();
        assert_eq!(closure[&unsynced].cache, None);
        // Collection spares it until its sync.
        let day = Duration::from_hours(24);
        age(&store, &paths, 2);
        assert_eq!(
            store.collect(UNBOUND, day, NONE).await.unwrap(),
            paths.len() - 1
        );
        assert_eq!(file_names(store.dir()), [unsynced.to_string()]);
        assert_eq!(count(&narinfos), 1);
        // A crash leaves no mark. Uses of it count, and collection deletes it
        // with its narinfo.
        store.unsynced.lock().unwrap().remove(&unsynced);
        store.touch([&unsynced]).unwrap();
        assert_eq!(store.collect(UNBOUND, day, NONE).await.unwrap(), 0);
        age(&store, std::slice::from_ref(&unsynced), 2);
        assert_eq!(store.collect(UNBOUND, day, NONE).await.unwrap(), 1);
        assert_eq!(count(store.dir()), 0);
        assert_eq!(count(&narinfos), 0);

        // Collection deletes narinfos without their paths once they're old.
        let ghost = StorePath::from_base_path(&format!("{}-ghost-1", "0".repeat(32))).unwrap();
        for file in store.caches.local_files(&ghost) {
            std::fs::write(file, "").unwrap();
        }
        assert_eq!(store.collect(UNBOUND, day, NONE).await.unwrap(), 0);
        assert_eq!(count(&narinfos), 2);
        for file in store.caches.local_files(&ghost) {
            let old = SystemTime::now() - 2 * day;
            std::fs::File::open(file)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        // Unless a fetch has marked the path on its way in.
        store.unsynced.lock().unwrap().insert(ghost.clone(), 0);
        store.collect(UNBOUND, day, NONE).await.unwrap();
        assert_eq!(count(&narinfos), 2);
        store.unsynced.lock().unwrap().remove(&ghost);
        store.collect(UNBOUND, day, NONE).await.unwrap();
        assert_eq!(count(&narinfos), 0);
    }

    fn space(size: u64, free: u64, inodes: u64, free_inodes: u64) -> Space {
        Space {
            size,
            free,
            inodes,
            free_inodes,
        }
    }

    #[test]
    fn measures_excess() {
        use EnsureFree::{Bytes, Percent};
        let excess = |space, ensure_free| {
            let Excess { bytes, inodes } = excess(space, ensure_free);
            (bytes, inodes)
        };
        // btrfs has no limit on inodes.
        assert_eq!(excess(space(100, 10, 0, 0), Percent(20.0)), (10, 0));
        // On ext4, small files can use up the inodes while bytes are free.
        let ext4 = space(1000, 500, 100, 5);
        assert_eq!(excess(ext4, Percent(20.0)), (0, 15));
        // 200 of 1000 bytes is 20%.
        assert_eq!(excess(ext4, Bytes(200)), (0, 15));
        assert_eq!(excess(ext4, Bytes(5000)), (4500, 95));
        // `0` turns it off, even with no bytes or inodes free.
        assert_eq!(excess(space(100, 0, 100, 0), Bytes(0)), (0, 0));
    }

    /// XFS reports as many inodes as are in use plus what its free space
    /// would hold, up to a cap. Below the cap, inodes are never short while
    /// bytes aren't.
    #[test]
    fn measures_excess_on_xfs() {
        use EnsureFree::{Bytes, Percent};
        // mkfs.xfs defaults to 4 KiB blocks and 512-byte inodes.
        let (blocks, per_block) = (1000, 8);
        let xfs = |used_blocks: u64, used_inodes: u64, cap: u64| {
            let free_blocks = blocks - used_blocks;
            let inodes = (used_inodes + free_blocks * per_block)
                .min(cap)
                .max(used_inodes);
            space(
                blocks * 4096,
                free_blocks * 4096,
                inodes,
                inodes - used_inodes,
            )
        };
        let targets = [
            Percent(20.0),
            Percent(50.0),
            Bytes(100 * 4096),
            Bytes(blocks * 4096),
        ];
        for used_blocks in (0..=blocks).step_by(10) {
            // Inodes take up blocks too.
            for used_inodes in (0..=used_blocks * per_block).step_by(37) {
                let space = xfs(used_blocks, used_inodes, u64::MAX);
                for ensure_free in targets {
                    let excess = excess(space, ensure_free);
                    assert!(
                        excess.bytes > 0 || excess.inodes == 0,
                        "{space:?} {ensure_free:?} {excess:?}"
                    );
                }
            }
        }
        // At the cap, XFS runs out of inodes with space free.
        let capped = xfs(100, 2000, 2000);
        assert_eq!(
            excess(capped, Percent(20.0)),
            Excess {
                bytes: 0,
                inodes: 400
            }
        );
    }

    #[test]
    fn parses_ensure_free() {
        for (s, want) in [
            ("20%", EnsureFree::Percent(20.0)),
            ("12.5 %", EnsureFree::Percent(12.5)),
            ("512", EnsureFree::Bytes(512)),
            ("1K", EnsureFree::Bytes(1 << 10)),
            ("50G", EnsureFree::Bytes(50 << 30)),
            ("1.5g", EnsureFree::Bytes(3 << 29)),
            ("2T", EnsureFree::Bytes(2 << 40)),
        ] {
            assert_eq!(parse_ensure_free(s).unwrap(), want, "{s}");
        }
        for s in ["", "101%", "-1G", "x%", "50GB", "NaN%", "inf"] {
            assert!(parse_ensure_free(s).is_err(), "{s}");
        }
    }
}
