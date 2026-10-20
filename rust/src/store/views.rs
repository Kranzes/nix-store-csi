use std::collections::{BTreeMap, HashSet};
use std::ffi::{CStr, CString, OsStr};
use std::fs::Permissions;
use std::io;
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use anyhow::{Context, bail, ensure};
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, HashFormat};
use rayon::prelude::*;
use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, fchmod, futimens, linkat, mkdirat, openat, statat,
};
use tracing::info;

use super::fs::{CANONICAL_TIME, Scratch, count_inodes, remove_file, set_canonical_time};
use super::{Store, list_names, list_store_paths, write_synced};

/// The directory in a view that links each root under its name without the
/// hash. Pods reach a root there by the same path on every system.
pub const ROOTS: &str = ".roots";

impl Store {
    /// Returns the view of `paths`, the closure of `roots`, for `volume` to
    /// mount at `target`. Volumes with the same roots and closure share a view
    /// of the current generation. Call it with the guard from
    /// [`Store::gc_guard`] held.
    pub fn view(
        &self,
        volume: &str,
        roots: &[StorePath],
        paths: &[StorePath],
        target: &Path,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
    ) -> anyhow::Result<PathBuf> {
        let record = self.volumes.join(check_volume(volume)?);
        // Kubelet retries publishes that succeeded. Replacing a view that is
        // still mounted wouldn't change what the pod sees, and the old view
        // would lose its record while the pod uses it.
        if let Some(old) = Volume::read(&record)?
            && old.target == target
            && is_bound(&self.views.join(&old.key), target)?
        {
            return Ok(self.views.join(old.key));
        }
        let links = root_links(roots)?;
        let volume = Volume {
            key: view_key(self.generation.load(Ordering::SeqCst), &links, paths),
            target: target.to_owned(),
        };
        let dir = self.views.join(&volume.key);
        // Publishes of one closure take turns, so they build its view once.
        let turn = {
            let mut building = self.building.lock().unwrap();
            // Otherwise the map keeps every view there ever was.
            building.retain(|_, turn| Arc::strong_count(turn) > 1);
            building.entry(volume.key.clone()).or_default().clone()
        };
        let _turn = turn.lock().unwrap_or_else(PoisonError::into_inner);
        {
            let _views = self.views_lock.lock().unwrap();
            if (dir.try_exists()).with_context(|| format!("reading {}", dir.display()))? {
                volume.write(&record)?;
                return Ok(dir);
            }
        }
        // A view gets its final name only once it's whole. This builds it
        // without the lock, so work on other views doesn't wait for it.
        let part = Scratch::new_in(&self.tmp)?;
        let view = part.path().join(&volume.key);
        std::fs::create_dir(&view)?;
        std::fs::set_permissions(&view, Permissions::from_mode(0o755))?;
        let view_dir = std::fs::File::open(&view)?;
        // Uses all cores, since a big closure has tens of thousands of files.
        paths.par_iter().try_for_each(|path| {
            let name = CString::new(path.to_string())?;
            let kind = statat(&self.dir_fd, &name, AtFlags::SYMLINK_NOFOLLOW)
                .with_context(|| format!("reading {}", self.unpacked(path).display()))?;
            let kind = FileType::from_raw_mode(kind.st_mode);
            link_tree(&self.dir_fd, &view_dir, &name, kind)
                .with_context(|| format!("linking {path} into the view"))
        })?;
        link_roots(&view.join(ROOTS), &links)?;
        let _views = self.views_lock.lock().unwrap();
        std::fs::rename(&view, &dir)
            .with_context(|| format!("moving {} into place", dir.display()))?;
        volume.write(&record)?;
        Ok(dir)
    }

    /// Forgets `volume`. If no other volume uses its view, moves the view
    /// into a trash and returns the trash. Call it with the guard from
    /// [`Store::gc_guard`] held. Drop the trash after giving up the guard,
    /// since dropping it deletes the view, and deleting a big view is slow.
    pub fn drop_view(&self, volume: &str) -> anyhow::Result<Option<Scratch>> {
        let record = self.volumes.join(check_volume(volume)?);
        let _views = self.views_lock.lock().unwrap();
        let old = Volume::read(&record)?;
        remove_file(&record)?;
        let Some(Volume { key, .. }) = old else {
            return Ok(None);
        };
        for entry in std::fs::read_dir(&self.volumes)
            .with_context(|| format!("reading {}", self.volumes.display()))?
        {
            if Volume::read(&entry?.path())?.is_some_and(|volume| volume.key == key) {
                return Ok(None);
            }
        }
        self.trash_view(&key).map(Some)
    }

    /// Moves the view into the trash it returns, so views/ only ever holds
    /// whole views. Records a use of its paths.
    fn trash_view(&self, key: &str) -> anyhow::Result<Scratch> {
        let dir = self.views.join(key);
        self.touch(&list_store_paths(&dir)?)?;
        Scratch::move_in(&self.tmp, &dir)
            .with_context(|| format!("moving {} to the trash", dir.display()))
    }

    /// Measures the view of `volume`, or returns `None` if `volume` isn't
    /// published at `target`. Views never change, so this measures each once.
    pub fn usage(&self, volume: &str, target: &Path) -> anyhow::Result<Option<Usage>> {
        let record = self.volumes.join(check_volume(volume)?);
        let Some(Volume { key, .. }) = Volume::read(&record)?.filter(|v| v.target == target) else {
            return Ok(None);
        };
        if let Some(&usage) = self.measured.lock().unwrap().get(&key) {
            return Ok(Some(usage));
        }
        let view = self.views.join(&key);
        let usage = Usage {
            bytes: self.measure(&view)?,
            inodes: count_inodes(&view).with_context(|| format!("reading {}", view.display()))?,
        };
        self.measured.lock().unwrap().insert(key, usage);
        Ok(Some(usage))
    }

    /// Returns the store paths that the remaining views hold.
    pub(super) fn drop_unmounted(
        &self,
        trash: &mut Vec<Scratch>,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
    ) -> anyhow::Result<HashSet<StorePath>> {
        let mut mounted = HashSet::new();
        for entry in std::fs::read_dir(&self.volumes)
            .with_context(|| format!("reading {}", self.volumes.display()))?
        {
            let entry = entry?;
            let record = entry.path();
            if let Some(volume) = Volume::read(&record)?
                && is_bound(&self.views.join(&volume.key), &volume.target)?
            {
                mounted.insert(volume.key);
                continue;
            }
            // A reboot drops the mounts without kubelet unpublishing them.
            let volume = entry.file_name();
            info!(volume = %volume.display(), "forgetting a volume that is not mounted");
            remove_file(&record)?;
        }
        (self.measured.lock().unwrap()).retain(|key, _| mounted.contains(key));
        let mut in_use = HashSet::new();
        for entry in std::fs::read_dir(&self.views)
            .with_context(|| format!("reading {}", self.views.display()))?
        {
            let entry = entry?;
            let key = entry.file_name().to_string_lossy().into_owned();
            if mounted.contains(&key) {
                in_use.extend(list_store_paths(&entry.path())?);
            } else {
                trash.push(self.trash_view(&key)?);
            }
        }
        Ok(in_use)
    }

    /// The newest generation of the views in views/.
    pub(super) fn newest_generation(&self) -> anyhow::Result<u64> {
        let generation = |name: &OsStr| name.to_str()?.split_once('-')?.0.parse().ok();
        let generations = list_names(&self.views, generation)?;
        Ok(generations.into_iter().max().unwrap_or_default())
    }
}

/// Maps the names of `roots` without their hashes to the roots, and fails if
/// two roots share a name.
pub fn root_links(roots: &[StorePath]) -> anyhow::Result<BTreeMap<&str, &StorePath>> {
    let mut links = BTreeMap::new();
    for root in roots {
        if let Some(other) = links.insert(&**root.name(), root)
            && other != root
        {
            bail!("{other} and {root} have the same name");
        }
    }
    Ok(links)
}

/// Names the view of a closure in `generation`, so volumes with the same
/// roots and closure share it.
fn view_key(generation: u64, links: &BTreeMap<&str, &StorePath>, paths: &[StorePath]) -> String {
    let mut names: Vec<String> = paths.iter().map(ToString::to_string).collect();
    names.sort();
    // The dot keeps roots apart from the closure, since no store path starts
    // with one.
    names.extend(links.values().map(|root| format!(".{root}")));
    let hash = Algorithm::SHA256.digest(names.join("\n"));
    format!("{generation}-{}", hash.as_base32().as_bare())
}

/// Makes `dir` with a relative symlink to each root, so the links also
/// resolve outside the pod.
fn link_roots(dir: &Path, links: &BTreeMap<&str, &StorePath>) -> anyhow::Result<()> {
    std::fs::create_dir(dir)?;
    for (name, root) in links {
        std::os::unix::fs::symlink(Path::new("..").join(root.to_string()), dir.join(name))
            .with_context(|| format!("linking {root} into the view"))?;
    }
    std::fs::set_permissions(dir, Permissions::from_mode(0o555))?;
    set_canonical_time(dir)?;
    Ok(())
}

/// What a view holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    /// The NAR size of its store objects.
    pub bytes: u64,
    /// Its files, directories and symlinks, with `.roots/` and the view itself.
    pub inodes: u64,
}

/// The record of a published volume, so [`Store::collect`] can tell whether
/// it's still mounted.
struct Volume {
    /// Names its view.
    key: String,
    target: PathBuf,
}

impl Volume {
    /// Reads `record`, if there is one.
    fn read(record: &Path) -> anyhow::Result<Option<Self>> {
        let bytes = match std::fs::read(record) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", record.display())),
        };
        let newline = (bytes.iter().position(|&b| b == b'\n'))
            .with_context(|| format!("{} has no newline", record.display()))?;
        Ok(Some(Self {
            key: String::from_utf8_lossy(&bytes[..newline]).into_owned(),
            target: std::ffi::OsString::from_vec(bytes[newline + 1..].to_vec()).into(),
        }))
    }

    fn write(&self, record: &Path) -> anyhow::Result<()> {
        let target = self.target.as_os_str().as_encoded_bytes();
        write_synced(record, &[self.key.as_bytes(), b"\n", target].concat())
    }
}

pub fn check_volume(volume: &str) -> anyhow::Result<&str> {
    ensure!(
        !volume.is_empty() && volume != "." && volume != ".." && !volume.contains('/'),
        "volume ID {volume:?} cannot name a directory"
    );
    Ok(volume)
}

/// Copies the tree `name` in `src_dir` to `dst_dir` with hard links for files
/// and symlinks, so the view shares inodes and page cache with the node store.
/// Working in directory fds, each call looks up one name, not a whole path.
fn link_tree(
    src_dir: impl AsFd,
    dst_dir: impl AsFd,
    name: &CStr,
    kind: FileType,
) -> io::Result<()> {
    if kind != FileType::Directory {
        // This links a symlink itself, not its target.
        return Ok(linkat(src_dir, name, dst_dir, name, AtFlags::empty())?);
    }
    mkdirat(&dst_dir, name, Mode::RWXU)?;
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut entries = Dir::new(openat(&src_dir, name, flags, Mode::empty())?)?;
    let dst = openat(&dst_dir, name, flags, Mode::empty())?;
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let child = entry.file_name();
        if child == c"." || child == c".." {
            continue;
        }
        link_tree(entries.fd()?, &dst, child, entry.file_type())?;
    }
    // The mode every sealed directory has.
    fchmod(&dst, Mode::from_raw_mode(0o555))?;
    Ok(futimens(&dst, &CANONICAL_TIME)?)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;
    use crate::narinfo::tests::fixtures;
    use crate::store::test_utils::*;

    #[tokio::test]
    async fn builds_views() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);

        let target = tmp.path().join("target");
        let view = store.view("v1", roots, &paths, &target, UNBOUND).unwrap();
        // The closure and .roots.
        assert_eq!(count(&view), 6);
        let link = view.join(ROOTS).join(&**hello.name());
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("..").join(hello.to_string())
        );
        assert!(link.join("bin/hello").exists());
        let meta = |p: &Path| std::fs::symlink_metadata(p).unwrap();
        let bin = format!("{hello}/bin/hello");
        assert_eq!(
            meta(&view.join(&bin)).ino(),
            meta(&store.dir().join(&bin)).ino()
        );
        let share = format!("{hello}/share");
        assert_eq!(
            meta(&view.join(&share)).mode(),
            meta(&store.dir().join(&share)).mode()
        );
        assert_eq!(meta(&view.join(&share)).mtime(), 1);
        assert_eq!(count(tmp.path().join("tmp")), 0);

        // Volumes with the same closure share a view, and a retried publish
        // gets the same one.
        let other_target = tmp.path().join("other");
        assert_eq!(
            store
                .view("v2", roots, &paths, &other_target, UNBOUND)
                .unwrap(),
            view
        );
        assert_eq!(
            store.view("v1", roots, &paths, &target, UNBOUND).unwrap(),
            view
        );
        let other = store
            .view("v3", &paths[..1], &paths[..1], &other_target, UNBOUND)
            .unwrap();
        assert_ne!(other, view);
        assert_eq!(count(&other), 2);
        assert_eq!(count(tmp.path().join("views")), 2);
        // Other roots get another view of the same closure.
        let more_roots = store
            .view("v4", &paths, &paths, &other_target, UNBOUND)
            .unwrap();
        assert!(more_roots != view && more_roots != other);
        assert_eq!(count(more_roots.join(ROOTS)), paths.len());
        store.drop_view("v3").unwrap();
        store.drop_view("v4").unwrap();

        // Dropping the last volume that uses a view deletes the view.
        store.drop_view("v1").unwrap();
        assert!(view.exists());
        store.drop_view("v2").unwrap();
        assert!(!view.exists());
        store.drop_view("v2").unwrap();
        assert!(store.view("..", roots, &paths, &target, UNBOUND).is_err());

        // A view that can't be read, here a symlink loop, is an error, not a
        // missing view.
        std::os::unix::fs::symlink(view.file_name().unwrap(), &view).unwrap();
        let err = store
            .view("v1", roots, &paths, &target, UNBOUND)
            .unwrap_err();
        assert_eq!(err.to_string(), format!("reading {}", view.display()));
    }

    #[tokio::test]
    async fn measures_views() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = tmp.path().join("target");
        let view = store.view("v1", roots, &paths, &target, UNBOUND).unwrap();
        let usage = store.usage("v1", &target).unwrap().unwrap();
        let nar_size = |path| store.caches.local_info(path).unwrap().unwrap().nar_size();
        assert_eq!(usage.bytes, paths.iter().map(nar_size).sum::<u64>());
        let find = std::process::Command::new("find")
            .arg(&view)
            .args(["-printf", "."])
            .output()
            .unwrap();
        assert_eq!(usage.inodes, u64::try_from(find.stdout.len()).unwrap());
        assert_eq!(store.measured.lock().unwrap().len(), 1);
        assert_eq!(store.usage("v1", &target).unwrap(), Some(usage));
        assert_eq!(store.usage("v1", &tmp.path().join("other")).unwrap(), None);
        assert_eq!(store.usage("v2", &target).unwrap(), None);

        store.prune_views(UNBOUND).await.unwrap();
        assert!(store.measured.lock().unwrap().is_empty());
    }

    #[test]
    fn rejects_roots_with_the_same_name() {
        let path = |hash| StorePath::from_base_path(&format!("{hash}-hello-2.12.3")).unwrap();
        let a = path("xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi");
        let b = path("jxxp01dayz0pv6vp7n39r9042ycsiicx");
        assert_eq!(root_links(&[a.clone(), a.clone()]).unwrap().len(), 1);
        let err = root_links(&[a.clone(), b.clone()]).unwrap_err();
        assert_eq!(err.to_string(), format!("{a} and {b} have the same name"));
    }

    /// Symlinks in store objects often point at store paths the node store
    /// doesn't have, so the view must link the symlink, not follow it.
    #[test]
    fn links_symlinks() {
        let tmp = scratch();
        let (src, dst) = (tmp.path().join("src"), tmp.path().join("dst"));
        std::fs::create_dir_all(src.join("tree")).unwrap();
        std::fs::create_dir(&dst).unwrap();
        std::os::unix::fs::symlink("/nix/store/missing", src.join("tree/link")).unwrap();
        let open = |dir: &Path| std::fs::File::open(dir).unwrap();
        let tree = FileType::Directory;
        link_tree(open(&src), open(&dst), c"tree", tree).unwrap();
        let meta = |p: &Path| std::fs::symlink_metadata(p.join("tree/link")).unwrap();
        assert_eq!(meta(&dst).ino(), meta(&src).ino());
        assert_eq!(
            std::fs::read_link(dst.join("tree/link")).unwrap(),
            Path::new("/nix/store/missing")
        );
    }

    /// Views may link store objects that a failed sync deleted, so later
    /// publishes build new views, also after a restart on the same boot.
    #[tokio::test]
    async fn builds_new_views_after_a_failed_sync() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = |name: &str| tmp.path().join(name);
        let old = store
            .view("v1", roots, &paths, &target("t1"), UNBOUND)
            .unwrap();

        // The sync loses a store object, and the next ensure fetches it again.
        let lost = paths.iter().find(|&p| *p != hello).unwrap();
        unsync(&store, lost);
        store.failed_syncs.store(1, Ordering::SeqCst);
        store.sync(lost).await.unwrap();
        assert!(!store.present(lost));
        store.ensure(roots).done().await.unwrap();
        let new = store
            .view("v2", roots, &paths, &target("t2"), UNBOUND)
            .unwrap();
        assert_ne!(new, old);
        assert_eq!(count(&new), paths.len() + 1);
        assert!(old.exists());

        // A retried publish keeps the view that is mounted at its target.
        let t1 = target("t1");
        let bound = |v: &Path, t: &Path| Ok(v == old && t == t1);
        assert_eq!(store.view("v1", roots, &paths, &t1, bound).unwrap(), old);
        assert_eq!(store.view("v1", roots, &paths, &t1, UNBOUND).unwrap(), new);

        std::fs::write(&store.failed_record, "").unwrap();
        let config = || zstd_config(&dir, tmp.path());
        let store = reopen(store, config).await;
        let after = store
            .view("v3", roots, &paths, &target("t3"), UNBOUND)
            .unwrap();
        assert!(after != new && after != old);
        // A restart with nothing lost keeps the generation.
        let store = reopen(store, config).await;
        assert_eq!(
            store
                .view("v4", roots, &paths, &target("t4"), UNBOUND)
                .unwrap(),
            after
        );
    }
}
