use std::fs::Permissions;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use rayon::prelude::*;
use walkdir::WalkDir;

/// A directory that's deleted when dropped. Unlike a `TempDir`, it can hold
/// read-only trees.
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
        rename_writable(from, &trash.0)?;
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

/// Deletes the tree at `path`. Restored directories aren't writable, so like
/// Nix it makes each one writable first. It deletes subdirectories in
/// parallel, since a big view or store object has tens of thousands of
/// files.
pub(super) fn remove_tree(path: &Path) -> io::Result<()> {
    fn remove_dir(dir: &Path) -> io::Result<()> {
        std::fs::set_permissions(dir, Permissions::from_mode(0o755))?;
        let mut subdirs = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                subdirs.push(entry.path());
            } else {
                // Deletes in one directory would wait on its lock, so they
                // stay in order.
                std::fs::remove_file(entry.path())?;
            }
        }
        subdirs.par_iter().try_for_each(|dir| remove_dir(dir))?;
        std::fs::remove_dir(dir)
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => deletes().install(|| remove_dir(path)),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// A thread pool for deletes, so a big one doesn't hold up the global pool,
/// which builds views for publishes.
pub(super) fn deletes() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        (rayon::ThreadPoolBuilder::new())
            .thread_name(|i| format!("delete-{i}"))
            // A thread waiting on a subdirectory runs other subdirectories'
            // deletes on its own stack, so deep trees need more than 2 MiB.
            .stack_size(16 << 20)
            .build()
            .expect("starting the delete threads")
    })
}

/// Renames `from` to `to`, and returns whether it moved a directory. Moving
/// a directory into another needs write access to it, to update its `..`.
/// A restored directory is read-only, so this makes a directory writable
/// first.
pub(super) fn rename_writable(from: &Path, to: &Path) -> io::Result<bool> {
    let dir = std::fs::symlink_metadata(from)?.is_dir();
    if dir {
        std::fs::set_permissions(from, Permissions::from_mode(0o755))?;
    }
    std::fs::rename(from, to)?;
    Ok(dir)
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

/// Counts the files, directories and symlinks in the tree at `path`.
pub(super) fn count_inodes(path: &Path) -> io::Result<u64> {
    (WalkDir::new(path).follow_root_links(false).into_iter())
        .try_fold(0, |n, entry| entry.map(|_| n + 1))
        .map_err(io::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// It deletes read-only trees, and a symlink without what it points at.
    #[test]
    fn removes_read_only_trees() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("kept"), "").unwrap();
        let tree = tmp.path().join("tree");
        for dir in ["a/b/c", "a/d", "e"] {
            std::fs::create_dir_all(tree.join(dir)).unwrap();
            std::fs::write(tree.join(dir).join("file"), "").unwrap();
        }
        std::os::unix::fs::symlink(&outside, tree.join("a/link")).unwrap();
        for dir in ["a/b/c", "a/b", "a/d", "a", "e", ""] {
            std::fs::set_permissions(tree.join(dir), Permissions::from_mode(0o555)).unwrap();
        }
        remove_tree(&tree).unwrap();
        assert!(!tree.exists());
        assert!(outside.join("kept").exists());
        // A symlink at the top is deleted, not followed.
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        remove_tree(&link).unwrap();
        assert!(link.symlink_metadata().is_err());
        assert!(outside.join("kept").exists());
        remove_tree(&link).unwrap();
    }
}
