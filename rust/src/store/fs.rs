use std::fs::Permissions;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use rustix::fs::{AtFlags, CWD, Timespec, Timestamps, UTIME_OMIT, utimensat};

/// A directory that's deleted when dropped. Unlike a `TempDir`, it can hold
/// sealed, read-only trees.
pub struct Scratch(PathBuf);

impl Scratch {
    pub(super) fn new_in(dir: &Path) -> io::Result<Self> {
        Ok(Self(tempfile::tempdir_in(dir)?.keep()))
    }

    /// Renames `from` into `tmp` under a new name. Start-up empties tmp/, so
    /// a counter in the process is enough to keep the names unique. Unlike
    /// creating a directory, a rename needs no free inode, so deletion still
    /// works when the filesystem is out of inodes.
    pub(super) fn move_in(tmp: &Path, from: &Path) -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let trash = Self(tmp.join(format!("trash-{}", NEXT.fetch_add(1, Ordering::Relaxed))));
        // Like `rename_tree`, but the trash stays writable.
        let meta = std::fs::symlink_metadata(from)?;
        if meta.is_dir() && meta.permissions().readonly() {
            std::fs::set_permissions(from, Permissions::from_mode(0o755))?;
        }
        std::fs::rename(from, &trash.0)?;
        Ok(trash)
    }

    pub(super) fn path(&self) -> &Path {
        &self.0
    }

    /// Deletes it and returns any error.
    pub(super) fn close(mut self) -> io::Result<()> {
        remove_tree(&std::mem::take(&mut self.0))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = remove_tree(&self.0);
        }
    }
}

/// Deletes the tree at `path`. Sealed directories aren't writable, so like
/// Nix it makes each one writable first.
pub(super) fn remove_tree(path: &Path) -> io::Result<()> {
    fn remove_dir(dir: &Path) -> io::Result<()> {
        std::fs::set_permissions(dir, Permissions::from_mode(0o755))?;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                remove_dir(&entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
        std::fs::remove_dir(dir)
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => remove_dir(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Renames `from` to `to`, and makes a directory writable for the move.
/// Moving a directory into another needs write access to it, to update its
/// `..`, and a sealed directory is read-only.
pub(super) fn rename_tree(from: &Path, to: &Path) -> io::Result<()> {
    if !std::fs::symlink_metadata(from)?.is_dir() {
        return std::fs::rename(from, to);
    }
    std::fs::set_permissions(from, Permissions::from_mode(0o755))?;
    std::fs::rename(from, to)?;
    std::fs::set_permissions(to, Permissions::from_mode(0o555))
}

/// Deletes the file at `path`, if there is one.
pub(super) fn remove_file(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => {
            Err(e).with_context(|| format!("deleting {}", path.display()))
        }
        _ => Ok(()),
    }
}

/// Sets the modification time to 1, which Nix gives every file in the store.
pub(super) fn set_canonical_time(path: &Path) -> io::Result<()> {
    let times = Timestamps {
        last_access: Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_OMIT,
        },
        last_modification: Timespec {
            tv_sec: 1,
            tv_nsec: 0,
        },
    };
    Ok(utimensat(CWD, path, &times, AtFlags::SYMLINK_NOFOLLOW)?)
}
