use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::{FileType, Permissions};
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use anyhow::{Context, ensure};
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, HashFormat};
use rayon::prelude::*;
use tracing::info;

use super::fs::{Scratch, remove_file, set_canonical_time};
use super::{Store, list_names, list_store_paths, write_synced};

impl Store {
    /// Returns the view of `paths` for `volume` to mount at `target`. Volumes
    /// with the same closure share a view of the current generation. Call it
    /// with the guard from [`Store::gc_guard`] held.
    pub fn view(
        &self,
        volume: &str,
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
        let volume = Volume {
            key: view_key(self.generation.load(Ordering::SeqCst), paths),
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
        // Uses all cores, since a big closure has tens of thousands of files.
        paths.par_iter().try_for_each(|path| {
            let src = self.unpacked(path);
            let kind = std::fs::symlink_metadata(&src)
                .with_context(|| format!("reading {}", src.display()))?
                .file_type();
            link_tree(&src, &view.join(path.to_string()), kind)
                .with_context(|| format!("linking {path} into the view"))
        })?;
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
        self.trash_view(&key)
    }

    /// Moves the view into the trash it returns, so views/ only ever holds
    /// whole views. Records a use of its paths.
    fn trash_view(&self, key: &str) -> anyhow::Result<Option<Scratch>> {
        let dir = self.views.join(key);
        self.touch(&list_store_paths(&dir).unwrap_or_default())?;
        match Scratch::move_in(&self.tmp, &dir) {
            Ok(trash) => Ok(Some(trash)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("moving {} to the trash", dir.display())),
        }
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
            let record = entry?.path();
            if let Some(volume) = Volume::read(&record)?
                && is_bound(&self.views.join(&volume.key), &volume.target)?
            {
                mounted.insert(volume.key);
                continue;
            }
            // A reboot drops the mounts without kubelet unpublishing them.
            info!(volume = %record.display(), "forgetting a volume that isn't mounted");
            remove_file(&record)?;
        }
        let mut in_use = HashSet::new();
        for entry in std::fs::read_dir(&self.views)
            .with_context(|| format!("reading {}", self.views.display()))?
        {
            let entry = entry?;
            let key = entry.file_name().to_string_lossy().into_owned();
            if mounted.contains(&key) {
                in_use.extend(list_store_paths(&entry.path())?);
            } else {
                trash.extend(self.trash_view(&key)?);
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

/// Names the view of a closure in `generation`, so volumes with the same
/// closure share it.
fn view_key(generation: u64, paths: &[StorePath]) -> String {
    let mut names: Vec<String> = paths.iter().map(ToString::to_string).collect();
    names.sort();
    let hash = Algorithm::SHA256.digest(names.join("\n"));
    format!("{generation}-{}", hash.as_base32().as_bare())
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
        let Some(newline) = bytes.iter().position(|&b| b == b'\n') else {
            return Ok(None);
        };
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
        "volume ID {volume:?} can't name a directory"
    );
    Ok(volume)
}

/// Copies the tree at `src` to `dst` with hard links for files and symlinks,
/// so the view shares inodes and page cache with the node store.
fn link_tree(src: &Path, dst: &Path, kind: FileType) -> anyhow::Result<()> {
    if kind.is_dir() {
        std::fs::create_dir(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            link_tree(
                &entry.path(),
                &dst.join(entry.file_name()),
                entry.file_type()?,
            )?;
        }
        // The mode every sealed directory has.
        std::fs::set_permissions(dst, Permissions::from_mode(0o555))?;
        set_canonical_time(dst)?;
    } else {
        // On Linux this links a symlink itself, not its target.
        std::fs::hard_link(src, dst)
            .with_context(|| format!("linking {} to {}", dst.display(), src.display()))?;
    }
    Ok(())
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

        let target = tmp.path().join("target");
        let view = store.view("v1", &paths, &target, UNBOUND).unwrap();
        assert_eq!(count(&view), 5);
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
            store.view("v2", &paths, &other_target, UNBOUND).unwrap(),
            view
        );
        assert_eq!(store.view("v1", &paths, &target, UNBOUND).unwrap(), view);
        let other = store
            .view("v3", &paths[..1], &other_target, UNBOUND)
            .unwrap();
        assert_ne!(other, view);
        assert_eq!(count(&other), 1);
        assert_eq!(count(tmp.path().join("views")), 2);

        // Dropping the last volume that uses a view deletes the view.
        store.drop_view("v1").unwrap();
        assert!(view.exists());
        store.drop_view("v2").unwrap();
        assert!(!view.exists());
        store.drop_view("v2").unwrap();
        assert!(store.view("..", &paths, &target, UNBOUND).is_err());

        // A view that can't be read, here a symlink loop, is an error, not a
        // missing view.
        std::os::unix::fs::symlink(view.file_name().unwrap(), &view).unwrap();
        let err = store.view("v1", &paths, &target, UNBOUND).unwrap_err();
        assert_eq!(err.to_string(), format!("reading {}", view.display()));
    }

    /// Symlinks in store objects often point at store paths the node store
    /// doesn't have, so the view must link the symlink, not follow it.
    #[test]
    fn links_symlinks() {
        let tmp = scratch();
        let src = tmp.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::os::unix::fs::symlink("/nix/store/missing", src.join("link")).unwrap();
        let dst = tmp.path().join("dst");
        link_tree(&src, &dst, std::fs::metadata(&src).unwrap().file_type()).unwrap();
        let meta = |p: &Path| std::fs::symlink_metadata(p.join("link")).unwrap();
        assert_eq!(meta(&dst).ino(), meta(&src).ino());
        assert_eq!(
            std::fs::read_link(dst.join("link")).unwrap(),
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
        let target = |name: &str| tmp.path().join(name);
        let old = store.view("v1", &paths, &target("t1"), UNBOUND).unwrap();

        // The sync loses a store object, and the next ensure fetches it again.
        let lost = paths.iter().find(|&p| *p != hello).unwrap();
        unsync(&store, lost);
        store.failed_syncs.store(1, Ordering::SeqCst);
        store.sync(lost).await.unwrap();
        assert!(!store.present(lost));
        store.ensure(&[hello]).done().await.unwrap();
        let new = store.view("v2", &paths, &target("t2"), UNBOUND).unwrap();
        assert_ne!(new, old);
        assert_eq!(count(&new), paths.len());
        assert!(old.exists());

        // A retried publish keeps the view that is mounted at its target.
        let t1 = target("t1");
        let bound = |v: &Path, t: &Path| Ok(v == old && t == t1);
        assert_eq!(store.view("v1", &paths, &t1, bound).unwrap(), old);
        assert_eq!(store.view("v1", &paths, &t1, UNBOUND).unwrap(), new);

        std::fs::write(&store.failed_record, "").unwrap();
        let config = || config(&dir, &dir.join("cache-zstd"), tmp.path());
        let store = reopen(store, config).await;
        let after = store.view("v3", &paths, &target("t3"), UNBOUND).unwrap();
        assert!(after != new && after != old);
        // A restart with nothing lost keeps the generation.
        let store = reopen(store, config).await;
        assert_eq!(
            store.view("v4", &paths, &target("t4"), UNBOUND).unwrap(),
            after
        );
    }
}
