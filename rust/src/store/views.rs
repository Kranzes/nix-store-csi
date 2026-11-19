use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, OsString};
use std::fmt::Write as _;
use std::fs::Permissions;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use anyhow::{Context, bail, ensure};
use foldhash::{HashSet, HashSetExt};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::fmt::Base32;
use harmonia_utils_hash::{Algorithm, Hash, HashFormat};
use rayon::prelude::*;
use rustix::fs::{
    AtFlags, CWD, Dir, FileType, Mode, OFlags, Stat, Timespec, Timestamps, UTIME_OMIT, fchmod,
    fstat, futimens, linkat, mkdirat, openat, statat, utimensat,
};
use tracing::{info, warn};

use super::fs::{Scratch, count_inodes, remove_file};
use super::{GcGuard, Store, list_names, write_synced};
use crate::store_path::Names;

/// The directory in a view that links each root under its name without the
/// hash. Pods reach a root there by the same path on every system.
pub const ROOTS: &str = ".roots";

/// The modification time 1, which Nix gives every file in the store, with the
/// access time left as it is.
const CANONICAL_TIME: Timestamps = Timestamps {
    last_access: Timespec {
        tv_sec: 0,
        tv_nsec: UTIME_OMIT,
    },
    last_modification: Timespec {
        tv_sec: 1,
        tv_nsec: 0,
    },
};

/// An empty file or directory in a view, where the container runtime binds
/// something of the node's. The runtime cannot create it in the read-only
/// volume. A view's name and store paths leave mountpoints out. The plugin
/// adds them to views that exist, and never removes one, since a container
/// may be bound there.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mountpoint {
    pub store_path: StorePath,
    /// The path inside `store_path`, empty for the store path itself.
    pub rest: String,
    pub dir: bool,
}

impl Mountpoint {
    /// Parses a path in `store_dir` like the container runtime does, with
    /// `.`, `..` and repeated slashes resolved. A path that ends in a slash is
    /// a directory. Returns `None` for a path that resolves outside the store
    /// dir, or to the store dir itself.
    pub fn parse(store_dir: &StoreDir, path: &str) -> anyhow::Result<Option<Self>> {
        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop();
                }
                part => parts.push(part),
            }
        }
        let dir: Vec<_> = (store_dir.to_str().split('/'))
            .filter(|part| !part.is_empty())
            .collect();
        let Some([name, rest @ ..]) = parts.strip_prefix(dir.as_slice()) else {
            return Ok(None);
        };
        let store_path = StorePath::from_base_path(name)
            .with_context(|| format!("{path} is not in a store path"))?;
        Ok(Some(Self {
            store_path,
            rest: rest.join("/"),
            dir: path.ends_with('/'),
        }))
    }
}

/// Like `--mountpoints` takes it, without the store dir.
impl std::fmt::Display for Mountpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.store_path)?;
        if !self.rest.is_empty() {
            write!(f, "/{}", self.rest)?;
        }
        if self.dir {
            f.write_str("/")?;
        }
        Ok(())
    }
}

/// Opens a directory in a view without following a symlink.
const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// The type of the mountpoint that `stat` is of, or `None` for a store
/// object. The plugin creates mountpoints writable by their owner, and no
/// store object is.
fn mountpoint_kind(stat: &Stat) -> Option<FileType> {
    let kind = FileType::from_raw_mode(stat.st_mode);
    let writable = stat.st_mode & 0o200 != 0;
    (writable && matches!(kind, FileType::Directory | FileType::RegularFile)).then_some(kind)
}

/// Creates `mountpoint` in the view `view` unless it is there, and returns
/// whether it did. It works in directory fds and never follows a symlink, so
/// it writes only in the view, and only in directories it made. A store path
/// that the view's closure has stays as it is, and the runtime binds the
/// node's copy of it over the closure's, which has the same contents.
fn add_mountpoint(view: &OwnedFd, mountpoint: &Mountpoint) -> anyhow::Result<bool> {
    let name = mountpoint.store_path.to_string();
    match statat(view, &name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if mountpoint_kind(&stat).is_none() => return Ok(false),
        Ok(_) | Err(rustix::io::Errno::NOENT) => {}
        Err(e) => return Err(e.into()),
    }
    let parts: Vec<_> = std::iter::once(name.as_str())
        .chain(mountpoint.rest.split('/').filter(|part| !part.is_empty()))
        .collect();
    let (last, parents) = parts.split_last().expect("a mountpoint has a store path");
    let mut dir = view.try_clone()?;
    for (i, part) in parents.iter().enumerate() {
        match mkdirat(&dir, *part, Mode::from_raw_mode(0o755)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(e) => return Err(e.into()),
        }
        let opened = openat(&dir, *part, DIR_FLAGS, Mode::empty());
        dir = match opened {
            Ok(fd) if mountpoint_kind(&fstat(&fd)?) == Some(FileType::Directory) => fd,
            Ok(_) | Err(rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR) => {
                bail!(
                    "{} is not a directory that the plugin made",
                    parts[..=i].join("/")
                );
            }
            Err(e) => return Err(e.into()),
        };
    }
    let (made, kind) = if mountpoint.dir {
        let made = mkdirat(&dir, *last, Mode::from_raw_mode(0o755));
        (made, FileType::Directory)
    } else {
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC;
        let made = openat(&dir, *last, flags, Mode::from_raw_mode(0o644)).map(drop);
        (made, FileType::RegularFile)
    };
    match made {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::EXIST) => {
            let stat = statat(&dir, *last, AtFlags::SYMLINK_NOFOLLOW)?;
            ensure!(
                mountpoint_kind(&stat) == Some(kind),
                "{} is already there as something else",
                parts.join("/")
            );
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// The store paths whose store objects the view `dir` holds, without its
/// mountpoints.
fn view_store_paths(dir: &Path) -> anyhow::Result<Vec<StorePath>> {
    list_names(dir, |name| {
        let path = StorePath::from_base_path(name.to_str()?).ok()?;
        let stat = statat(CWD, dir.join(name), AtFlags::SYMLINK_NOFOLLOW);
        // A failed stat counts the store path, so garbage collection keeps it.
        (!stat.is_ok_and(|stat| mountpoint_kind(&stat).is_some())).then_some(path)
    })
}

/// Like [`Store::add_mountpoints`] with `mountpoints`. One it cannot add gets
/// a warning.
fn add_to_view(view: &Path, mountpoints: &[Mountpoint]) -> anyhow::Result<Vec<Mountpoint>> {
    let fd = rustix::fs::open(view, DIR_FLAGS, Mode::empty())
        .map_err(io::Error::from)
        .with_context(|| format!("opening {}", view.display()))?;
    let mut created = Vec::new();
    for mountpoint in mountpoints {
        match add_mountpoint(&fd, mountpoint) {
            Ok(true) => created.push(mountpoint.clone()),
            Ok(false) => {}
            Err(e) => warn!(
                "adding the mountpoint {mountpoint} to {}: {e:#}; leaving it out",
                view.display()
            ),
        }
    }
    Ok(created)
}

impl Store {
    /// Makes `mountpoints` those that publishes add, and adds them to every
    /// view. It sets them first, so a publish whose view this misses adds them
    /// itself.
    pub fn set_mountpoints(&self, mountpoints: impl Into<Arc<[Mountpoint]>>) -> anyhow::Result<()> {
        let mountpoints = mountpoints.into();
        *self.mountpoints.lock().unwrap() = mountpoints.clone();
        let mut created = BTreeSet::new();
        let mut updated = 0;
        for view in list_names(&self.views, |name| Some(self.views.join(name)))? {
            match add_to_view(&view, &mountpoints) {
                Ok(new) if new.is_empty() => {}
                Ok(new) => {
                    updated += 1;
                    created.extend(new);
                }
                // An unpublish moved it to the trash meanwhile.
                Err(e)
                    if e.downcast_ref()
                        .is_some_and(|e: &io::Error| e.kind() == io::ErrorKind::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        if updated > 0 {
            let created: Vec<_> = created.into_iter().collect();
            info!(mountpoints = %Names(&created), updated, "added mountpoints to views");
        }
        Ok(())
    }

    /// Adds the mountpoints that the view `view` lacks, and returns those it
    /// created. Call it once the view is in views/, since a
    /// [`Store::set_mountpoints`] that misses it has set its mountpoints by
    /// then.
    pub fn add_mountpoints(&self, view: &Path) -> anyhow::Result<Vec<Mountpoint>> {
        let mountpoints = self.mountpoints.lock().unwrap().clone();
        add_to_view(view, &mountpoints)
    }

    /// Returns the view of `paths`, the closure of `roots`, for `volume` to
    /// mount at `target`. `paths` must be in order, as [`Store::ensure`]
    /// returns them. Volumes with the same roots and closure share a view of
    /// the current generation.
    pub fn view(
        &self,
        _gc: &GcGuard,
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
        // A failed sync misses the paths that fetches were still unpacking,
        // and they stay in store/ until their own syncs delete them.
        let lost = {
            let failed_syncs = self.failed_syncs.load(Ordering::SeqCst);
            let unsynced = self.unsynced.lock().unwrap();
            (unsynced.iter())
                .find(|&(p, &since)| since < failed_syncs && paths.binary_search(p).is_ok())
                .map(|(p, _)| p.clone())
        };
        if let Some(path) = lost {
            bail!(
                "{} may have lost data in a failed sync of the node store; a later try fetches it again",
                self.caches.store_dir().display(&path)
            );
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
    /// into a trash and returns the trash. Drop the trash after giving up the
    /// guard, since dropping it deletes the view, and deleting a big view is
    /// slow.
    pub fn drop_view(&self, _gc: &GcGuard, volume: &str) -> anyhow::Result<Option<Scratch>> {
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

    /// The NAR size of each store path in the view `key`. A view's store paths
    /// never change, so this reads their narinfos once.
    pub(super) fn view_nar_sizes(&self, key: &str) -> anyhow::Result<NarSizes> {
        if let Some(sizes) = self.view_sizes.lock().unwrap().get(key) {
            return Ok(sizes.clone());
        }
        let paths = view_store_paths(&self.views.join(key))?;
        let sizes: NarSizes = (paths.iter())
            .map(|path| (path.clone(), self.nar_size(path)))
            .collect();
        (self.view_sizes.lock().unwrap()).insert(key.to_owned(), sizes.clone());
        Ok(sizes)
    }

    /// Moves the view into the trash it returns, so views/ only ever holds
    /// whole views. Records a use of its paths.
    fn trash_view(&self, key: &str) -> anyhow::Result<Scratch> {
        let dir = self.views.join(key);
        self.touch(&view_store_paths(&dir)?)?;
        Scratch::move_in(&self.tmp, &dir)
            .with_context(|| format!("moving {} to the trash", dir.display()))
    }

    /// Measures the view of `volume`, or returns `None` if `volume` isn't
    /// published at `target`. A view changes only by gaining mountpoints, so
    /// this measures each view once, and mountpoints added later don't count.
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
            bytes: self
                .view_nar_sizes(&key)?
                .iter()
                .map(|(_, size)| size)
                .sum(),
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
        let (mounted, unmounted) = self.mounted_views(is_bound)?;
        for volume in unmounted {
            // A reboot drops the mounts without kubelet unpublishing them.
            info!(volume = %volume.display(), "forgetting a volume that is not mounted");
            remove_file(&self.volumes.join(volume))?;
        }
        (self.measured.lock().unwrap()).retain(|key, _| mounted.contains(key));
        (self.view_sizes.lock().unwrap()).retain(|key, _| mounted.contains(key));
        for entry in std::fs::read_dir(&self.views)
            .with_context(|| format!("reading {}", self.views.display()))?
        {
            let key = entry?.file_name().to_string_lossy().into_owned();
            if !mounted.contains(&key) {
                trash.push(self.trash_view(&key)?);
            }
        }
        self.view_paths(&mounted)
    }

    /// Returns the store paths that the views named by `keys` hold. It skips
    /// a view that is gone, like one an unpublish moved to the trash.
    pub(super) fn view_paths(&self, keys: &HashSet<String>) -> anyhow::Result<HashSet<StorePath>> {
        let mut paths = HashSet::new();
        for key in keys {
            match view_store_paths(&self.views.join(key)) {
                Ok(linked) => paths.extend(linked),
                Err(e)
                    if e.downcast_ref()
                        .is_some_and(|e: &io::Error| e.kind() == io::ErrorKind::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(paths)
    }

    /// Reads the volumes' records. Returns the keys of the views that mounted
    /// volumes use, and the other volumes. It changes nothing, so it can run
    /// alongside publishes.
    pub(super) fn mounted_views(
        &self,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
    ) -> anyhow::Result<(HashSet<String>, Vec<OsString>)> {
        let (mut mounted, mut unmounted) = (HashSet::new(), Vec::new());
        for entry in std::fs::read_dir(&self.volumes)
            .with_context(|| format!("reading {}", self.volumes.display()))?
        {
            let entry = entry?;
            match Volume::read(&entry.path())? {
                Some(volume) if is_bound(&self.views.join(&volume.key), &volume.target)? => {
                    mounted.insert(volume.key);
                }
                _ => unmounted.push(entry.file_name()),
            }
        }
        Ok((mounted, unmounted))
    }

    /// The newest generation of the views in views/.
    pub(super) fn newest_generation(&self) -> anyhow::Result<u64> {
        let generations = list_names(&self.views, |name| view_generation(name.to_str()?))?;
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
/// roots and closure share it. Store paths sort like their names, so it
/// hashes the names in sorted order.
fn view_key(generation: u64, links: &BTreeMap<&str, &StorePath>, paths: &[StorePath]) -> String {
    debug_assert!(paths.is_sorted());
    let mut text = String::new();
    for path in paths {
        writeln!(text, "{path}").unwrap();
    }
    // The dot keeps roots apart from the closure, since no store path starts
    // with one.
    for root in links.values() {
        writeln!(text, ".{root}").unwrap();
    }
    text.pop();
    let hash = Algorithm::SHA256.digest(text);
    format!("{generation}-{}", hash.as_base32().as_bare())
}

/// The generation of a view named `name`, if [`view_key`] could make that
/// name.
pub fn view_generation(name: &str) -> Option<u64> {
    let (generation, hash) = name.split_once('-')?;
    Base32::<Hash>::parse(Algorithm::SHA256, hash).ok()?;
    generation.parse().ok()
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
    Ok(utimensat(CWD, dir, &CANONICAL_TIME, AtFlags::empty())?)
}

pub(super) type NarSizes = Arc<[(StorePath, u64)]>;

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
        // Only a temporary file that a kill cut short has no newline.
        let Some(newline) = bytes.iter().position(|&b| b == b'\n') else {
            return Ok(None);
        };
        Ok(Some(Self {
            key: String::from_utf8_lossy(&bytes[..newline]).into_owned(),
            target: OsString::from_vec(bytes[newline + 1..].to_vec()).into(),
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
    let mut entries = Dir::new(openat(&src_dir, name, DIR_FLAGS, Mode::empty())?)?;
    let dst = openat(&dst_dir, name, DIR_FLAGS, Mode::empty())?;
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let child = entry.file_name();
        if child == c"." || child == c".." {
            continue;
        }
        link_tree(entries.fd()?, &dst, child, entry.file_type())?;
    }
    // The mode of every directory in the node store.
    fchmod(&dst, Mode::from_raw_mode(0o555))?;
    Ok(futimens(&dst, &CANONICAL_TIME)?)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use tracing_test::traced_test;

    use super::*;
    use crate::narinfo::tests::fixtures;
    use crate::store::test_utils::*;

    const DRIVER: &str = "00000000000000000000000000000000-nvidia-x11";
    const GLIBC: &str = "11111111111111111111111111111111-glibc";

    fn mountpoint(path: &str) -> Mountpoint {
        Mountpoint::parse(&StoreDir::default(), path)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn parses_mountpoints() {
        let parse = |path: &str| -> Option<String> {
            let parsed = Mountpoint::parse(&StoreDir::default(), path).unwrap();
            parsed.map(|m| m.to_string())
        };
        for (path, parsed) in [
            (format!("/nix/store/{GLIBC}"), Some(GLIBC.to_owned())),
            (
                format!("//nix//store/./{GLIBC}//lib/./x/"),
                Some(format!("{GLIBC}/lib/x/")),
            ),
            (
                format!("/nix/store/{DRIVER}/../{GLIBC}/lib"),
                Some(format!("{GLIBC}/lib")),
            ),
            (format!("/nix/store/{GLIBC}/lib/../../../etc/passwd"), None),
            (format!("/nix/store/{GLIBC}/.."), None),
            ("/nix/store/".to_owned(), None),
            ("/usr/lib/libcuda.so.1".to_owned(), None),
        ] {
            assert_eq!(parse(&path), parsed, "{path}");
        }
        for path in ["/nix/store/.roots/hello", "/nix/store/not-a-store-path/lib"] {
            assert!(
                Mountpoint::parse(&StoreDir::default(), path).is_err(),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn adds_mountpoints() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = tmp.path().join("target");
        let view = (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap();
        let mountpoints = [
            format!("/nix/store/{DRIVER}/"),
            format!("/nix/store/{DRIVER}/lib/libcuda.so.1"),
            // Like glibc's in NixOS's CDI spec, below a store path that the
            // closure doesn't have.
            format!("/nix/store/{GLIBC}/lib/"),
            format!("/nix/store/{GLIBC}/lib64/"),
            // The closure has hello, so the view's copy stays as it is.
            format!("/nix/store/{hello}/lib/"),
            format!("/nix/store/{hello}/bin/"),
        ]
        .map(|path| mountpoint(&path));
        store.set_mountpoints(mountpoints).unwrap();
        let libcuda = std::fs::metadata(view.join(DRIVER).join("lib/libcuda.so.1")).unwrap();
        assert!(libcuda.is_file() && libcuda.len() == 0);
        for dir in ["lib", "lib64"] {
            assert!(view.join(GLIBC).join(dir).is_dir());
        }
        let hello_dir = view.join(hello.to_string());
        assert!(!hello_dir.join("lib").exists());
        let mode = std::fs::metadata(&hello_dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o555);
        assert_eq!(count(&view), paths.len() + 3);
        assert_eq!(store.add_mountpoints(&view).unwrap(), []);
    }

    /// The node store has the store path of the mountpoint, but the view
    /// doesn't hold it.
    #[tokio::test]
    async fn leaves_mountpoints_out_of_the_closure() {
        let tmp = scratch();
        let Some((store, _, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let (closure, other) = (&paths[..1], &paths[1]);
        let target = tmp.path().join("target");
        let view = (store.guarded_view("v1", closure, closure, &target, UNBOUND)).unwrap();
        let key = view.file_name().unwrap().to_str().unwrap().to_owned();
        let other_lib = mountpoint(&format!("/nix/store/{other}/lib/"));
        store.set_mountpoints([other_lib]).unwrap();
        assert!(view.join(other.to_string()).join("lib").is_dir());
        let held = store.view_paths(&HashSet::from_iter([key])).unwrap();
        assert_eq!(held, HashSet::from_iter(closure.iter().cloned()));
        let bytes = store.usage("v1", &target).unwrap().unwrap().bytes;
        assert_eq!(bytes, store.nar_size(&closure[0]));
    }

    #[tokio::test]
    #[traced_test]
    async fn adds_mountpoints_only_in_directories_it_made() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = tmp.path().join("target");
        let view = (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap();
        let file = mountpoint(&format!("/nix/store/{DRIVER}/lib/libcuda.so.1"));
        let lib = mountpoint(&format!("/nix/store/{GLIBC}/lib/"));
        store.set_mountpoints([file, lib]).unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, view.join(GLIBC).join("link")).unwrap();
        // A store object that is a symlink.
        let symlinked = "22222222222222222222222222222222-link";
        std::os::unix::fs::symlink(&outside, view.join(symlinked)).unwrap();
        let refused = [
            format!("/nix/store/{GLIBC}/link/x/"),
            format!("/nix/store/{symlinked}/x/"),
            format!("/nix/store/{DRIVER}/lib/libcuda.so.1/x"),
            format!("/nix/store/{GLIBC}/lib"),
        ]
        .map(|path| mountpoint(&path));
        store.set_mountpoints(refused).unwrap();
        assert_eq!(store.add_mountpoints(&view).unwrap(), []);
        assert_eq!(count(&outside), 0);
        for warning in [
            format!("{GLIBC}/link is not a directory that the plugin made"),
            format!("{DRIVER}/lib/libcuda.so.1 is not a directory that the plugin made"),
            format!("{GLIBC}/lib is already there as something else"),
        ] {
            assert!(logs_contain(&warning), "{warning}");
        }
    }

    #[tokio::test]
    async fn builds_views() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);

        let target = tmp.path().join("target");
        let view = (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap();
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
                .guarded_view("v2", roots, &paths, &other_target, UNBOUND)
                .unwrap(),
            view
        );
        assert_eq!(
            (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap(),
            view
        );
        let other = store
            .guarded_view("v3", &paths[..1], &paths[..1], &other_target, UNBOUND)
            .unwrap();
        assert_ne!(other, view);
        assert_eq!(count(&other), 2);
        assert_eq!(count(tmp.path().join("views")), 2);
        // Other roots get another view of the same closure.
        let more_roots = store
            .guarded_view("v4", &paths, &paths, &other_target, UNBOUND)
            .unwrap();
        assert!(more_roots != view && more_roots != other);
        assert_eq!(count(more_roots.join(ROOTS)), paths.len());
        store.guarded_drop_view("v3").unwrap();
        store.guarded_drop_view("v4").unwrap();

        // Dropping the last volume that uses a view deletes the view.
        store.guarded_drop_view("v1").unwrap();
        assert!(view.exists());
        store.guarded_drop_view("v2").unwrap();
        assert!(!view.exists());
        store.guarded_drop_view("v2").unwrap();
        assert!((store.guarded_view("..", roots, &paths, &target, UNBOUND)).is_err());

        // A view that can't be read, here a symlink loop, is an error, not a
        // missing view.
        std::os::unix::fs::symlink(view.file_name().unwrap(), &view).unwrap();
        let err = store
            .guarded_view("v1", roots, &paths, &target, UNBOUND)
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
        let view = (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap();
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
        assert!(store.view_sizes.lock().unwrap().is_empty());
    }

    /// A kill between creating a record and renaming it into place leaves an
    /// empty temporary file in volumes/. Pruning deletes it.
    #[tokio::test]
    async fn prunes_cut_short_records() {
        let tmp = scratch();
        let state = tmp.path().join("state");
        let store = Arc::new(Store::new(unsigned_config(tmp.path(), &state)).unwrap());
        let leftover = state.join("volumes/.tmpAbC123");
        std::fs::write(&leftover, "").unwrap();
        store.prune_views(UNBOUND).await.unwrap();
        assert!(!leftover.exists());
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

    /// Views in views/ have these names, so the key must not change. The
    /// first two hashes are in text order but not in byte order.
    #[test]
    fn keeps_view_keys() {
        let paths = [
            "00000000000000000000000000000001-b-1",
            "10000000000000000000000000000000-a-1",
            "1000000000000000000000000000000z-c-1",
        ]
        .map(|p| StorePath::from_base_path(p).unwrap());
        let links = root_links(&paths[..2]).unwrap();
        let key = view_key(3, &links, &paths);
        assert_eq!(
            key,
            "3-04d1rq8d54408hyaxvaadn844xwai6hzs4j6ymnfk68x4dnqnysj"
        );
        assert_eq!(view_generation(&key), Some(3));
        for name in [&key[2..], &key.replace('3', "x"), &key.replace('4', "e")] {
            assert_eq!(view_generation(name), None, "{name}");
        }
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
            .guarded_view("v1", roots, &paths, &target("t1"), UNBOUND)
            .unwrap();

        // The sync loses a store object, and the next ensure fetches it again.
        let lost = paths.iter().find(|&p| *p != hello).unwrap();
        unsync(&store, lost);
        store.failed_syncs.store(1, Ordering::SeqCst);
        store.sync(lost).await.unwrap();
        assert!(!store.present(lost));
        store.ensure(roots).done().await.unwrap();
        let new = store
            .guarded_view("v2", roots, &paths, &target("t2"), UNBOUND)
            .unwrap();
        assert_ne!(new, old);
        assert_eq!(count(&new), paths.len() + 1);
        assert!(old.exists());

        // A retried publish keeps the view that is mounted at its target.
        let t1 = target("t1");
        let bound = |v: &Path, t: &Path| Ok(v == old && t == t1);
        assert_eq!(
            (store.guarded_view("v1", roots, &paths, &t1, bound)).unwrap(),
            old
        );
        assert_eq!(
            (store.guarded_view("v1", roots, &paths, &t1, UNBOUND)).unwrap(),
            new
        );

        std::fs::write(&store.failed_record, "").unwrap();
        let config = || zstd_config(&dir, tmp.path());
        let store = reopen(store, config).await;
        let after = store
            .guarded_view("v3", roots, &paths, &target("t3"), UNBOUND)
            .unwrap();
        assert!(after != new && after != old);
        // A restart with nothing lost keeps the generation.
        let store = reopen(store, config).await;
        assert_eq!(
            store
                .guarded_view("v4", roots, &paths, &target("t4"), UNBOUND)
                .unwrap(),
            after
        );
    }

    #[tokio::test]
    async fn refuses_paths_a_failed_sync_missed() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = tmp.path().join("target");
        unsync(&store, &hello);
        store.failed_syncs.store(1, Ordering::SeqCst);
        let err = (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap_err();
        assert!(
            format!("{err:#}").contains("may have lost data in a failed sync"),
            "{err:#}"
        );
        assert_eq!(count(tmp.path().join("views")), 0);
        store.sync(&hello).await.unwrap();
        let paths = store.ensure(roots).done().await.unwrap();
        (store.guarded_view("v1", roots, &paths, &target, UNBOUND)).unwrap();
    }
}
