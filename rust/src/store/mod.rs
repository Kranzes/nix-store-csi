//! Verified store objects, unpacked once per node into a directory that every
//! volume shares.

mod fetch;
mod fs;
mod gc;
#[cfg(test)]
mod test_utils;
mod views;

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{Context, bail, ensure};
use harmonia_store_path::{StoreDir, StorePath};
use rustix::fs::{AtFlags, CWD, FlockOperation, Timespec, Timestamps, UTIME_NOW, flock, utimensat};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, Semaphore};
use tracing::{info, warn};

use self::fetch::{Fetch, Installs};
use self::fs::{Scratch, remove_file, remove_tree};
use self::gc::Space;
use crate::closure::{self, Caches};
use crate::metrics::Metrics;
use crate::narinfo::PublicKey;
use crate::store_path;

pub use self::gc::{EnsureFree, Excess, parse_ensure_free};
pub use self::views::check_volume;

/// The kernel's id for the current boot, which a reboot changes.
const BOOT_ID: &str = "/proc/sys/kernel/random/boot_id";

/// The version of the state dir's layout, which `<state>/version` records.
/// Raise it when the layout changes.
const LAYOUT_VERSION: u32 = 1;

pub struct Config {
    pub stores: Vec<String>,
    /// `None` skips the signature check.
    pub trusted_keys: Option<Vec<PublicKey>>,
    pub state_dir: PathBuf,
    /// Narinfo signatures cover it.
    pub store_dir: StoreDir,
    pub jobs: usize,
    /// Credentials for caches whose URLs have none.
    pub netrc_file: Option<PathBuf>,
}

pub struct Store {
    caches: Caches,
    dir: PathBuf,
    views: PathBuf,
    volumes: PathBuf,
    /// Held while a view gains or loses a volume.
    views_lock: Mutex<()>,
    /// Names the views that publishes build now. It goes up when store
    /// objects that views may link are deleted because they may have lost
    /// data. Then new publishes build new views, and pods keep the old ones
    /// until they unmount them.
    generation: AtomicU64,
    /// A turn for each view that publishes are building, by key.
    building: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    tmp: PathBuf,
    jobs: Semaphore,
    max_jobs: usize,
    /// Paths whose narinfos wait for a sync to get their final names, each
    /// with the number of failed syncs when its fetch began writing it. A
    /// fetch adds its path before it writes the narinfo, and collection
    /// spares these paths. So no path is deleted and fetched again between a
    /// sync's syncfs and its renames. If one were, a rename would mark the
    /// path durable although its new files never reached the disk.
    unsynced: Mutex<HashMap<StorePath, u64>>,
    /// Counts failed syncs. A failed syncfs doesn't say which files it lost,
    /// and a later syncfs doesn't report the loss again. So no later sync can
    /// make the paths written before a failed sync durable.
    failed_syncs: AtomicU64,
    /// A failed sync creates this file, so a restart on the same boot deletes
    /// the unsynced paths, as after a reboot.
    failed_record: PathBuf,
    /// One sync holds it at a time, so the paths that come in during a sync
    /// share the next one.
    syncing: tokio::sync::Mutex<()>,
    /// Fetches under way, each with the NAR bytes it has read. Volumes that
    /// need the same path share its fetch. A fetch leaves the map when it
    /// ends, so a publish after a failure starts a new one.
    fetching: Mutex<HashMap<StorePath, (Fetch, Arc<AtomicU64>)>>,
    /// The paths of closures under [`Store::ensure`], each with its number of
    /// holds. Collection spares them however full the disk is.
    ensuring: Mutex<HashMap<StorePath, usize>>,
    /// Closures that [`Store::ensure`] is resolving or fetching, for the
    /// metrics.
    ensures: AtomicUsize,
    /// The store paths that fetches install while ensures run, so each ensure
    /// can check their references. Ensures share it, so a fetch writes one
    /// entry however many ensures run.
    installs: Mutex<Installs>,
    /// The NAR size of the store objects in store/, for the metrics.
    size: AtomicU64,
    /// Publishes and unpublishes hold it shared, so collection and
    /// [`Store::forget`] never run in the middle of one.
    gc: Arc<RwLock<()>>,
    /// store/, opened once. The kernel reports a writeback error to every
    /// file opened before the error, but not to a file opened after another
    /// file has reported it. So a syncfs on a freshly opened store/ could
    /// miss lost data. A file opened after an error that no file has reported
    /// yet still gets it, so start-up syncs once on this one to take any such
    /// error from before the plugin wrote anything.
    dir_fd: std::fs::File,
    metrics: Metrics,
    /// Held for as long as the store is open.
    _lock: std::fs::File,
}

/// Gauges that the metrics read at each scrape. A directory that can't be
/// read leaves its gauge out.
pub struct Gauges {
    pub store_paths: Option<usize>,
    pub store_size: u64,
    pub unsynced_paths: usize,
    pub views: Option<usize>,
    /// Left out on a filesystem with no limit on inodes.
    pub free_inodes: Option<u64>,
    pub fetches_in_flight: usize,
    pub closures_in_flight: usize,
    /// Each cache, and whether it is paused after a failed request.
    pub paused_caches: Vec<(String, bool)>,
}

impl Store {
    /// Call it within a Tokio runtime, since caches open in the background.
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let state = &config.state_dir;
        std::fs::create_dir_all(state).with_context(|| format!("creating {}", state.display()))?;
        let lock = std::fs::File::create(state.join("lock"))
            .with_context(|| format!("creating {}", state.join("lock").display()))?;
        if flock(&lock, FlockOperation::NonBlockingLockExclusive).is_err() {
            bail!("another nix-store-csi is using {}", state.display());
        }
        check_layout(&state.join("version"))?;
        let dir = state.join("store");
        let narinfos = state.join("narinfo");
        let views = state.join("views");
        let volumes = state.join("volumes");
        let tmp = state.join("tmp");
        // Whatever a crash left in tmp/ is incomplete.
        remove_tree(&tmp).with_context(|| format!("clearing {}", tmp.display()))?;
        for d in [&dir, &narinfos, &views, &volumes, &tmp] {
            std::fs::create_dir_all(d).with_context(|| format!("creating {}", d.display()))?;
        }
        let dir_fd =
            std::fs::File::open(&dir).with_context(|| format!("opening {}", dir.display()))?;
        let store = Self {
            caches: Caches::new(
                &config.stores,
                config.store_dir,
                config.trusted_keys,
                narinfos,
                config.netrc_file.as_deref(),
            )?,
            dir,
            views,
            volumes,
            views_lock: Mutex::default(),
            generation: AtomicU64::default(),
            building: Mutex::default(),
            tmp,
            jobs: Semaphore::new(config.jobs),
            max_jobs: config.jobs,
            unsynced: Mutex::default(),
            failed_syncs: AtomicU64::default(),
            failed_record: state.join("sync-failed"),
            syncing: tokio::sync::Mutex::default(),
            fetching: Mutex::default(),
            ensuring: Mutex::default(),
            ensures: AtomicUsize::default(),
            installs: Mutex::default(),
            size: AtomicU64::default(),
            gc: Arc::default(),
            dir_fd,
            metrics: Metrics::default(),
            _lock: lock,
        };
        store
            .settle(&state.join("boot-id"), &state.join("generation"))
            .context("settling the paths that the last run didn't sync")?;
        let size = store.measure().context("measuring the node store")?;
        store.size.store(size, Ordering::Relaxed);
        store.metrics.init(store.caches.names());
        Ok(store)
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn gauges(&self) -> Gauges {
        let entries = |dir: &Path| {
            (std::fs::read_dir(dir).map(Iterator::count))
                .inspect_err(|e| warn!("reading {}: {e}", dir.display()))
                .ok()
        };
        // Inside the struct literal, the guard would hold the lock while a
        // later field reads a directory.
        let unsynced_paths = self.unsynced.lock().unwrap().len();
        Gauges {
            store_paths: entries(&self.dir),
            store_size: self.size.load(Ordering::Relaxed),
            unsynced_paths,
            views: entries(&self.views),
            free_inodes: (Space::of(&self.dir))
                .inspect_err(|e| warn!("measuring the node store's filesystem: {e}"))
                .ok()
                .filter(|space| space.inodes > 0)
                .map(|space| space.free_inodes),
            fetches_in_flight: self.max_jobs - self.jobs.available_permits(),
            closures_in_flight: self.ensures.load(Ordering::Relaxed),
            paused_caches: self.caches.paused(),
        }
    }

    /// Syncs or deletes the paths that the last run left unsynced in store/.
    /// After a restart on the same boot, the page cache still holds all that
    /// the last run wrote. So one sync makes those paths durable, and live
    /// mounts keep using them. After a reboot or a failed sync they may have
    /// lost data, so this deletes them. It also deletes any path in store/
    /// with no narinfo. `record` holds the last run's boot id, and
    /// `generation` the views' generation.
    fn settle(&self, record: &Path, generation: &Path) -> anyhow::Result<()> {
        let boot =
            std::fs::read_to_string(BOOT_ID).with_context(|| format!("reading {BOOT_ID}"))?;
        let same_boot = std::fs::read_to_string(record).is_ok_and(|last| last == boot);
        // This syncs even when the result doesn't matter, so the first sync
        // of a new path can't report an error from before start-up.
        let flushed = rustix::fs::syncfs(&self.dir_fd)
            .inspect_err(|e| warn!("syncing the node store: {e}"))
            .is_ok();
        let synced = same_boot && !self.failed_record.exists() && flushed;
        let narinfos = self.caches.local_dir();
        let mut durable = HashSet::new();
        let mut unsynced = Vec::new();
        for entry in std::fs::read_dir(narinfos)
            .with_context(|| format!("reading {}", narinfos.display()))?
        {
            let entry = entry?;
            match closure::local_name(&entry.file_name()) {
                Some((path, true)) => {
                    durable.insert(path);
                }
                Some((path, false)) => unsynced.push(path),
                // A temporary file that a crash left half written.
                None => remove_file(&entry.path())?,
            }
        }
        let (mut kept, mut dropped) = (0, 0);
        for path in unsynced {
            let file = self.caches.local_unsynced(&path);
            // A fetch writes the narinfo before it moves the path in.
            if synced && self.present(&path) {
                std::fs::rename(&file, self.caches.local_narinfo(&path))
                    .with_context(|| format!("renaming {}", file.display()))?;
                durable.insert(path);
                kept += 1;
            } else {
                self.remove_narinfo(&path)?;
                durable.remove(&path);
            }
        }
        for path in list_store_paths(&self.dir)? {
            if !durable.contains(&path) {
                let tree = self.unpacked(&path);
                remove_tree(&tree).with_context(|| format!("deleting {}", tree.display()))?;
                dropped += 1;
            }
        }
        if kept > 0 {
            info!(
                paths = kept,
                "synced the paths that the last run left unsynced"
            );
        }
        if dropped > 0 {
            warn!(paths = dropped, "deleted paths that may have lost data");
        }
        sync_dir(narinfos)?;
        sync_dir(&self.dir)?;
        let recorded: Option<u64> = std::fs::read_to_string(generation)
            .ok()
            .and_then(|g| g.trim().parse().ok());
        // Views from before a reboot may lack links, and views from before
        // a failed sync may link lost data. A failed sync raised the last
        // run's generation only in memory, so the new generation is higher
        // than every view's.
        let generation = match recorded {
            Some(recorded) if synced && dropped == 0 => recorded,
            _ => {
                let next = recorded.unwrap_or_default().max(self.newest_generation()?) + 1;
                write_synced(generation, next.to_string().as_bytes())?;
                next
            }
        };
        self.generation.store(generation, Ordering::SeqCst);
        remove_file(&self.failed_record)?;
        write_synced(record, boot.as_bytes())
    }

    /// Adds up the NAR sizes of the paths in store/.
    fn measure(&self) -> anyhow::Result<u64> {
        let paths = list_store_paths(&self.dir)?;
        Ok(paths.iter().map(|path| self.nar_size(path)).sum())
    }

    /// Returns the NAR size in the path's narinfo, as an estimate of the
    /// space its store object takes.
    fn nar_size(&self, path: &StorePath) -> u64 {
        match self.caches.local_info(path) {
            Ok(info) => info.map_or(0, |info| info.nar_size()),
            Err(e) => {
                warn!("{e:#}");
                0
            }
        }
    }

    /// Moves `path`, whose NAR has `size` bytes, out of store/ into the trash
    /// it returns. The path leaves store/ before anything deletes its files or
    /// its narinfo, since a name in store/ means a whole store object with a
    /// narinfo.
    fn evict(&self, path: &StorePath, size: u64) -> anyhow::Result<Scratch> {
        let tree = self.unpacked(path);
        let trash = Scratch::move_in(&self.tmp, &tree)
            .with_context(|| format!("moving {} out of the node store", tree.display()))?;
        let _ = (self.size).fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_sub(size))
        });
        Ok(trash)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn unpacked(&self, path: &StorePath) -> PathBuf {
        self.dir.join(path.to_string())
    }

    fn present(&self, path: &StorePath) -> bool {
        self.unpacked(path).symlink_metadata().is_ok()
    }

    /// Parses `paths` into store paths. Each may point inside a store path.
    pub fn roots<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> anyhow::Result<Vec<StorePath>> {
        (paths.into_iter())
            .map(|path| {
                store_path::parse(self.caches.store_dir(), path)
                    .with_context(|| format!("parsing {path}"))
            })
            .collect()
    }

    /// Makes `path`, which a fetch moved into store/, durable. It syncs the
    /// store's filesystem, which costs far less than syncing each file, then
    /// gives the path's narinfo its final name. Paths that come in while a
    /// sync runs share the next one.
    async fn sync(self: &Arc<Self>, path: &StorePath) -> anyhow::Result<()> {
        let _turn = self.syncing.lock().await;
        let batch = {
            let unsynced = self.unsynced.lock().unwrap();
            // An earlier sync made it durable or deleted it.
            if !unsynced.contains_key(path) {
                return Ok(());
            }
            unsynced.clone()
        };
        let this = self.clone();
        let (batch, synced) = tokio::task::spawn_blocking(move || {
            // A fetch marks its path before writing it, so of the marked
            // paths only those in store/ have all their files written.
            let batch: Vec<_> = (batch.into_iter())
                .filter(|(p, _)| this.present(p))
                .collect();
            let synced = rustix::fs::syncfs(&this.dir_fd);
            if synced.is_err() {
                this.failed_syncs.fetch_add(1, Ordering::SeqCst);
                // Only a restart on the same boot reads the record, and it
                // reads it from the page cache. On a failing disk an fsync
                // would fail and leave no record.
                if let Err(e) = write_unsynced(&this.failed_record, b"") {
                    warn!("writing {}: {e}", this.failed_record.display());
                }
            }
            (batch, synced)
        })
        .await?;
        if let Err(e) = synced {
            self.forget(batch.into_iter().map(|(p, _)| p).collect())
                .await?;
            return Err(e).context("syncing the node store, so its new paths were deleted");
        }
        let failed_syncs = self.failed_syncs.load(Ordering::SeqCst);
        let (lost, batch): (Vec<_>, Vec<_>) =
            (batch.into_iter()).partition(|&(_, since)| since < failed_syncs);
        if !lost.is_empty() {
            warn!(
                paths = lost.len(),
                "deleting paths that may have lost data in a failed sync"
            );
            self.forget(lost.into_iter().map(|(p, _)| p).collect())
                .await?;
        }
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            for (path, _) in &batch {
                let file = this.caches.local_unsynced(path);
                let narinfo = this.caches.local_narinfo(path);
                match std::fs::rename(&file, &narinfo) {
                    // An earlier sync renamed it but failed before it
                    // removed the mark.
                    Err(e) if e.kind() == io::ErrorKind::NotFound && narinfo.exists() => {}
                    result => result.with_context(|| format!("renaming {}", file.display()))?,
                }
            }
            sync_dir(this.caches.local_dir())?;
            let mut unsynced = this.unsynced.lock().unwrap();
            for (path, _) in &batch {
                unsynced.remove(path);
            }
            Ok(())
        })
        .await?
    }

    /// Makes the paths that fetches moved into store/ durable, for a caller
    /// that exits next and so drops the syncs that fetches run in the
    /// background. Fails if a sync failed, since that deleted paths.
    pub async fn sync_all(self: &Arc<Self>) -> anyhow::Result<()> {
        let marked: Vec<_> = self.unsynced.lock().unwrap().keys().cloned().collect();
        for path in marked {
            // One sync covers every marked path in store/.
            self.sync(&path).await?;
        }
        ensure!(
            self.failed_syncs.load(Ordering::SeqCst) == 0,
            "syncing the node store failed, so some of its new paths were deleted"
        );
        Ok(())
    }

    /// Deletes marked `paths` and their narinfos, then unmarks the ones that
    /// left the node store. A path still in it stays marked, so the next sync
    /// tries again. Call it with no guard from [`Store::gc_guard`] held,
    /// since it waits for the publishes under way.
    async fn forget(self: &Arc<Self>, paths: Vec<StorePath>) -> anyhow::Result<()> {
        // Publishes hold the guard while they build views, and `exclusively`
        // waits for them. So no publish links a store object as it leaves
        // store/, or builds a view of the new generation before the lost store
        // objects are gone.
        let (forgot, deleting) = self
            .exclusively(move |this, trash| {
                // Views may link the lost data, so the generation goes up even
                // if the deletion below fails. A restart on this boot finds
                // the failed sync's record and raises the generation too.
                this.generation.fetch_add(1, Ordering::SeqCst);
                let deleted = paths.iter().try_for_each(|path| {
                    // `nar_size` reads the narinfo, so it runs before
                    // `remove_narinfo`.
                    trash.push(this.evict(path, this.nar_size(path))?);
                    // An earlier sync may have renamed the narinfo before it
                    // failed.
                    this.remove_narinfo(path)
                });
                let gone: Vec<_> = paths.iter().filter(|path| !this.present(path)).collect();
                let mut unsynced = this.unsynced.lock().unwrap();
                for path in gone {
                    unsynced.remove(path);
                }
                deleted
            })
            .await?;
        deleting.await?;
        forgot
    }

    /// Holds off [`Store::collect`] until the guard drops.
    pub async fn gc_guard(&self) -> OwnedRwLockReadGuard<()> {
        self.gc.clone().read_owned().await
    }

    /// Records a use of `paths`. [`Store::collect`] reads the modification
    /// time of a path's narinfo as its last use.
    fn touch<'a>(&self, paths: impl IntoIterator<Item = &'a StorePath>) -> anyhow::Result<()> {
        let now = Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_NOW,
        };
        let times = Timestamps {
            last_access: now,
            last_modification: now,
        };
        for path in paths {
            for file in self.caches.local_files(path) {
                match utimensat(CWD, &file, &times, AtFlags::empty()) {
                    Ok(()) => break,
                    Err(rustix::io::Errno::NOENT) => {}
                    Err(e) => {
                        return Err(e).with_context(|| format!("touching {}", file.display()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Deletes the narinfo of `path`, under either name.
    fn remove_narinfo(&self, path: &StorePath) -> anyhow::Result<()> {
        (self.caches.local_files(path).iter()).try_for_each(|file| remove_file(file))
    }

    /// Returns when [`Store::touch`] last recorded a use of `path`.
    fn last_use(&self, path: &StorePath) -> SystemTime {
        (self.caches.local_files(path).iter())
            .find_map(|file| std::fs::metadata(file).and_then(|m| m.modified()).ok())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }
}

/// Fails unless `record` holds [`LAYOUT_VERSION`]. A state dir without the
/// record is new or from an unreleased build with the same layout, so this
/// writes it.
fn check_layout(record: &Path) -> anyhow::Result<()> {
    match std::fs::read_to_string(record) {
        Ok(found) => {
            let found = found.trim();
            ensure!(
                found.parse().ok() == Some(LAYOUT_VERSION),
                "{} holds layout version {found}, but this nix-store-csi reads only version {LAYOUT_VERSION}",
                record.display()
            );
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            write_synced(record, LAYOUT_VERSION.to_string().as_bytes())
        }
        Err(e) => Err(e).with_context(|| format!("reading {}", record.display())),
    }
}

/// Parses the names in `dir` with `parse`, skipping those it rejects.
fn list_names<T>(dir: &Path, parse: impl Fn(&OsStr) -> Option<T>) -> anyhow::Result<Vec<T>> {
    (std::fs::read_dir(dir).and_then(|entries| {
        (entries.filter_map(|entry| entry.map(|e| parse(&e.file_name())).transpose())).collect()
    }))
    .with_context(|| format!("reading {}", dir.display()))
}

/// The store paths that `dir` holds, skipping other names.
fn list_store_paths(dir: &Path) -> anyhow::Result<Vec<StorePath>> {
    list_names(dir, |name| StorePath::from_base_path(name.to_str()?).ok())
}

fn sync_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing {}", dir.display()))
}

/// Replaces `dest` atomically, then syncs it and its directory to disk.
fn write_synced(dest: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let dir = dest.parent().expect("a file is in a directory");
    (|| -> io::Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(dir)?;
        file.write_all(contents)?;
        file.as_file().sync_all()?;
        file.persist(dest)?;
        std::fs::File::open(dir)?.sync_all()
    })()
    .with_context(|| format!("writing {}", dest.display()))
}

/// Replaces `dest` atomically, and leaves the write to the page cache.
fn write_unsynced(dest: &Path, contents: &[u8]) -> io::Result<()> {
    let dir = dest.parent().expect("a file is in a directory");
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    file.write_all(contents)?;
    file.persist(dest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::narinfo::tests::{file_store, fixtures};
    use crate::store::test_utils::*;

    /// Start-up records the layout version in a state dir that has none, and
    /// refuses a state dir with another version before it changes anything.
    #[tokio::test]
    async fn checks_layout_version() {
        let tmp = scratch();
        let config = |state: &Path| Config {
            stores: vec![file_store(tmp.path())],
            trusted_keys: None,
            state_dir: state.to_owned(),
            store_dir: StoreDir::default(),
            jobs: 1,
            netrc_file: None,
        };
        let new = tmp.path().join("new");
        drop(Store::new(config(&new)).unwrap());
        assert_eq!(std::fs::read_to_string(new.join("version")).unwrap(), "1");

        let newer = tmp.path().join("newer");
        std::fs::create_dir(&newer).unwrap();
        std::fs::write(newer.join("version"), "2\n").unwrap();
        let err = Store::new(config(&newer)).err().unwrap();
        assert!(
            format!("{err:#}")
                .contains("layout version 2, but this nix-store-csi reads only version 1"),
            "{err:#}"
        );
        assert_eq!(file_names(&newer), ["lock", "version"]);
    }

    /// A sync renames the narinfos of only those marked paths that are in
    /// store/, since the fetches of the other paths haven't written them yet.
    #[tokio::test]
    async fn syncs_only_present_paths() {
        let tmp = scratch();
        let Some((store, hello, _)) = hello_store(tmp.path()).await else {
            return;
        };
        let ghost = StorePath::from_base_path(&format!("{}-ghost-1", "0".repeat(32))).unwrap();
        let ghost_file = store.caches.local_unsynced(&ghost);
        std::fs::write(&ghost_file, "").unwrap();
        store.unsynced.lock().unwrap().insert(ghost.clone(), 0);
        unsync(&store, &hello);
        store.sync(&hello).await.unwrap();
        assert!(store.caches.local_narinfo(&hello).exists());
        assert!(ghost_file.exists());
        let marked = store.unsynced.lock().unwrap().clone();
        assert_eq!(marked, [(ghost, 0)].into());
    }

    /// The kernel reports a lost write only once, so a later sync can't make
    /// a path written before a failed sync durable.
    #[tokio::test]
    async fn forgets_paths_from_before_a_failed_sync() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        unsync(&store, &hello);
        // An earlier sync may have renamed a path's narinfo and failed before
        // it unmarked the path.
        let renamed = paths.iter().find(|&p| *p != hello).unwrap();
        store.unsynced.lock().unwrap().insert(renamed.clone(), 0);
        store.failed_syncs.store(1, Ordering::SeqCst);
        store.sync(&hello).await.unwrap();
        for path in [&hello, renamed] {
            assert!(!store.present(path));
            assert!(store.caches.local_files(path).iter().all(|f| !f.exists()));
        }
        assert!(store.unsynced.lock().unwrap().is_empty());

        // A failed sync's record makes a restart delete what wasn't synced, as
        // after a reboot.
        let other = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap();
        settled(&store).await;
        let path = other.iter().find(|&p| *p != hello).unwrap();
        unsync(&store, path);
        std::fs::write(&store.failed_record, "").unwrap();
        let store = reopen(store, || config(&dir, &dir.join("cache-zstd"), tmp.path())).await;
        assert!(!store.present(path));
        assert!(!store.failed_record.exists());
    }

    /// Publishes build views with the guard held, and the deletion after a
    /// failed sync waits for them. So no view links a store object part way
    /// through its deletion, or gets the new generation before the deletion
    /// ends.
    #[tokio::test]
    async fn forgets_paths_between_publishes() {
        let tmp = scratch();
        let Some((store, hello, _)) = hello_store(tmp.path()).await else {
            return;
        };
        unsync(&store, &hello);
        let generation = store.generation.load(Ordering::SeqCst);
        let guard = store.gc_guard().await;
        let (this, path) = (store.clone(), hello.clone());
        let forgetting = tokio::spawn(async move { this.forget(vec![path]).await });
        // A writer that waits for the lock makes new readers wait too.
        while store.gc.try_read().is_ok() && !forgetting.is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(!forgetting.is_finished());
        assert!(store.present(&hello));
        assert_eq!(store.generation.load(Ordering::SeqCst), generation);
        drop(guard);
        forgetting.await.unwrap().unwrap();
        assert!(!store.present(&hello));
        assert_eq!(store.generation.load(Ordering::SeqCst), generation + 1);
        assert!(store.unsynced.lock().unwrap().is_empty());
    }

    /// `unpack` syncs what the fetches' own syncs haven't yet, since it exits
    /// next. It fails after a failed sync, which deleted paths.
    #[tokio::test]
    async fn syncs_all() {
        let tmp = scratch();
        let Some((store, hello, _)) = hello_store(tmp.path()).await else {
            return;
        };
        unsync(&store, &hello);
        store.sync_all().await.unwrap();
        assert!(store.caches.local_narinfo(&hello).exists());
        assert!(store.unsynced.lock().unwrap().is_empty());
        store.failed_syncs.store(1, Ordering::SeqCst);
        assert!(store.sync_all().await.is_err());
    }

    /// A restart on the same boot keeps the paths that weren't synced, since
    /// mounts may use them. After a reboot, start-up deletes them.
    #[tokio::test]
    async fn settles_unsynced_paths() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let (narinfos, record) = (tmp.path().join("narinfo"), tmp.path().join("boot-id"));
        let others: Vec<_> = paths.iter().filter(|&p| *p != hello).cloned().collect();
        unsync(&store, &others[0]);
        unsync(&store, &others[1]);
        // A crash after writing a narinfo and before moving its path in
        // leaves this.
        let ghost = StorePath::from_base_path(&format!("{}-ghost-1", "0".repeat(32))).unwrap();
        std::fs::write(store.caches.local_unsynced(&ghost), "").unwrap();
        // Start-up deletes a path with no narinfo in any case.
        remove_file(&store.caches.local_narinfo(&others[2])).unwrap();
        // It also deletes a temporary file that a crash left half written.
        std::fs::write(narinfos.join(".tmpAbC123"), "").unwrap();

        let store = reopen(store, || config(&dir, &dir.join("cache-zstd"), tmp.path())).await;
        assert!(store.present(&others[0]) && store.present(&others[1]));
        assert!(!store.present(&others[2]));
        let mut names: Vec<_> = (paths.iter())
            .filter(|&p| *p != others[2])
            .map(|p| format!("{p}.narinfo"))
            .collect();
        names.sort();
        assert_eq!(file_names(&narinfos), names);
        let boot = std::fs::read_to_string(BOOT_ID).unwrap();
        assert_eq!(std::fs::read_to_string(&record).unwrap(), boot);

        unsync(&store, &others[0]);
        std::fs::write(&record, "another boot\n").unwrap();
        let store = reopen(store, || config(&dir, &dir.join("cache-zstd"), tmp.path())).await;
        assert!(!store.present(&others[0]));
        assert_eq!(count(store.dir()), paths.len() - 2);
        assert_eq!(count(&narinfos), paths.len() - 2);
        assert_eq!(std::fs::read_to_string(&record).unwrap(), boot);

        // Start-up also deletes them after a run that left no boot id, such
        // as an older plugin's.
        unsync(&store, &others[1]);
        std::fs::remove_file(&record).unwrap();
        let store = reopen(store, || config(&dir, &dir.join("cache-zstd"), tmp.path())).await;
        assert!(!store.present(&others[1]));
        assert_eq!(count(store.dir()), paths.len() - 3);
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
        assert_eq!(count(store.dir()), paths.len());
        assert_eq!(count(&narinfos), paths.len());
    }
}
