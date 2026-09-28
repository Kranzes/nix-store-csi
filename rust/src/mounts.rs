//! Each volume is one read-only bind of its view. A view is a directory of
//! hard links into the node store, so a pod costs one mount however big its
//! closure is.

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Context;
use rustix::fs::{AtFlags, CWD, StatVfsMountFlags, StatxAttributes, StatxFlags, statvfs, statx};
use rustix::mount::{MountFlags, UnmountFlags, mount_bind, mount_remount, unmount};

/// Volumes share views, so only one publish at a time binds a view onto itself.
static SELF_BIND: Mutex<()> = Mutex::new(());

/// Kubelet retries calls that succeeded, so a read-only bind of `view` that is
/// already at `target` stays. Anything else at `target` is replaced.
pub fn publish(view: &Path, target: &Path) -> anyhow::Result<()> {
    if is_mount_point(target)? {
        if same_file(view, target) && read_only(target)? {
            return Ok(());
        }
        unmount(target, UnmountFlags::DETACH)
            .with_context(|| format!("unmounting what was at {}", target.display()))?;
    }
    std::fs::create_dir_all(target).with_context(|| format!("creating {}", target.display()))?;
    // Propagation copies the bind at `target` into kubelet's mount namespace
    // with the flags it has when it is made. A later remount changes only the
    // plugin's copy. So this binds the view onto itself, makes that bind
    // read-only, and binds `target` from it.
    let _self_bind = SELF_BIND.lock().unwrap();
    mount_bind(view, view).with_context(|| format!("binding {} onto itself", view.display()))?;
    let flags = MountFlags::BIND | MountFlags::NOSUID | MountFlags::NODEV | MountFlags::RDONLY;
    let result = mount_remount(view, flags, "")
        .and_then(|()| mount_bind(view, target))
        .with_context(|| format!("binding {} at {}", view.display(), target.display()));
    unmount(view, UnmountFlags::DETACH)
        .with_context(|| format!("unmounting {}", view.display()))?;
    result
}

pub fn unpublish(target: &Path) -> anyhow::Result<()> {
    if is_mount_point(target)? {
        unmount(target, UnmountFlags::DETACH)
            .with_context(|| format!("unmounting {}", target.display()))?;
    }
    match std::fs::remove_dir(target) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result.with_context(|| format!("removing {}", target.display())),
    }
}

pub fn is_bound(view: &Path, target: &Path) -> anyhow::Result<bool> {
    Ok(is_mount_point(target)? && same_file(view, target))
}

fn read_only(path: &Path) -> anyhow::Result<bool> {
    let stat = statvfs(path).with_context(|| format!("statvfs {}", path.display()))?;
    Ok(stat.f_flag.contains(StatVfsMountFlags::RDONLY))
}

fn same_file(a: &Path, b: &Path) -> bool {
    let (Ok(a), Ok(b)) = (std::fs::metadata(a), std::fs::metadata(b)) else {
        return false;
    };
    (a.dev(), a.ino()) == (b.dev(), b.ino())
}

/// Comparing the device with the parent's does not work, because the view and
/// kubelet's directory are often on one filesystem.
fn is_mount_point(path: &Path) -> anyhow::Result<bool> {
    let stat = match statx(CWD, path, AtFlags::SYMLINK_NOFOLLOW, StatxFlags::empty()) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(e) => return Err(e).with_context(|| format!("statx {}", path.display())),
    };
    Ok(stat.stx_attributes.contains(StatxAttributes::MOUNT_ROOT))
}
