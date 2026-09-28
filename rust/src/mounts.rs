//! Each volume is one read-only bind of its view, a directory of hard links
//! into the node store, so a pod costs one mount however big its closure is.

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Context;
use rustix::fs::{AtFlags, CWD, StatVfsMountFlags, StatxAttributes, StatxFlags, statvfs, statx};
use rustix::mount::{MountFlags, UnmountFlags, mount_bind, mount_remount, unmount};

/// Volumes share views, so two publishes mustn't bind one onto itself at once.
static SELF_BIND: Mutex<()> = Mutex::new(());

/// Kubelet retries calls that succeeded, so a read-only bind of `view` that's
/// already at `target` stays, and anything else there is replaced.
pub fn publish(view: &Path, target: &Path) -> anyhow::Result<()> {
    if is_mount_point(target)? {
        if is_bound(view, target)? && statvfs(target)?.f_flag.contains(StatVfsMountFlags::RDONLY) {
            return Ok(());
        }
        unmount(target, UnmountFlags::DETACH)
            .with_context(|| format!("unmounting what was at {}", target.display()))?;
    }
    std::fs::create_dir_all(target).with_context(|| format!("creating {}", target.display()))?;
    // Propagation copies the bind at target to kubelet's side as it is when
    // made, and a later remount changes only this side. So target is bound
    // from a bind of the view that's read-only already.
    let _self_bind = SELF_BIND.lock().unwrap();
    mount_bind(view, view).with_context(|| format!("binding {}", view.display()))?;
    let flags = MountFlags::BIND | MountFlags::NOSUID | MountFlags::NODEV | MountFlags::RDONLY;
    let result = mount_remount(view, flags, "")
        .and_then(|()| mount_bind(view, target))
        .with_context(|| format!("binding {} at {}", view.display(), target.display()));
    unmount(view, UnmountFlags::DETACH)
        .with_context(|| format!("unmounting {}", view.display()))?;
    result
}

pub fn unpublish(target: &Path) -> anyhow::Result<()> {
    if !target.exists() {
        return Ok(());
    }
    if is_mount_point(target)? {
        unmount(target, UnmountFlags::DETACH)
            .with_context(|| format!("unmounting {}", target.display()))?;
    }
    std::fs::remove_dir(target).with_context(|| format!("removing {}", target.display()))
}

pub fn is_bound(view: &Path, target: &Path) -> anyhow::Result<bool> {
    if !is_mount_point(target)? {
        return Ok(false);
    }
    let (Ok(v), Ok(t)) = (std::fs::metadata(view), std::fs::metadata(target)) else {
        return Ok(false);
    };
    Ok((v.dev(), v.ino()) == (t.dev(), t.ino()))
}

/// Comparing devices with the parent won't do, since the view and kubelet's
/// directory are often on one filesystem.
fn is_mount_point(path: &Path) -> anyhow::Result<bool> {
    let stat = match statx(CWD, path, AtFlags::SYMLINK_NOFOLLOW, StatxFlags::empty()) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(false),
        Err(e) => return Err(e).with_context(|| format!("statx {}", path.display())),
    };
    anyhow::ensure!(
        stat.stx_attributes_mask
            .contains(StatxAttributes::MOUNT_ROOT),
        "the kernel doesn't report mount roots; it needs Linux 5.8 or later"
    );
    Ok(stat.stx_attributes.contains(StatxAttributes::MOUNT_ROOT))
}
