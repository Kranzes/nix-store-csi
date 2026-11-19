//! Each volume is one read-only bind of its view. A view is a directory of
//! hard links into the node store, so a pod costs one mount however big its
//! closure is.

use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context;
use rustix::fs::{AtFlags, CWD, StatVfsMountFlags, StatxAttributes, StatxFlags, statvfs, statx};
use rustix::mount::{MountFlags, UnmountFlags, mount_bind, mount_remount, unmount};

use crate::store::view_generation;

/// Volumes share views, so only one publish at a time binds a view onto itself.
static SELF_BIND: Mutex<()> = Mutex::new(());

const MOUNTINFO: &str = "/proc/self/mountinfo";

/// A state dir other than the plugin's, with views that volumes mount.
#[derive(Debug, PartialEq, Eq)]
pub struct OtherStateDir {
    /// As a path in its filesystem.
    pub path: PathBuf,
    /// The filesystem's source, like a device.
    pub source: String,
    pub volumes: usize,
}

/// Returns the state dirs, other than the one of `views`, with views that
/// volumes mount, like the dir a plugin used before a change of `--state-dir`.
pub fn other_state_dirs(views: &Path) -> anyhow::Result<Vec<OtherStateDir>> {
    let table = std::fs::read(MOUNTINFO).with_context(|| format!("reading {MOUNTINFO}"))?;
    Ok(other_in(&String::from_utf8_lossy(&table), views))
}

/// The plugin can see a volume more than once, like through the node's root
/// at `/host`. Each copy of a volume's mount is in its peer group, or a slave
/// of it, so this counts those groups.
fn other_in(table: &str, views: &Path) -> Vec<OtherStateDir> {
    let mut dirs = BTreeMap::<_, BTreeSet<_>>::new();
    for (path, source, peers) in table.lines().filter_map(|line| other_view(line, views)) {
        dirs.entry((path, source)).or_default().insert(peers);
    }
    (dirs.into_iter())
        .map(|((path, source), volumes)| OtherStateDir {
            path,
            source,
            volumes: volumes.len(),
        })
        .collect()
}

/// For a line of `/proc/self/mountinfo` that binds a view outside `views`,
/// returns the view's state dir, the filesystem's source, and the peer group
/// that the mount is a slave of, or else the one it is in, or else the line.
fn other_view<'a>(line: &'a str, views: &Path) -> Option<(PathBuf, String, &'a str)> {
    let mut fields = line.split(' ');
    let root = Path::new(fields.nth(3)?);
    let key = root.file_name()?.to_str()?;
    view_generation(key)?;
    let dir = root.parent()?;
    if dir.file_name()? != "views" {
        return None;
    }
    let target = PathBuf::from(unescape(fields.next()?));
    if same_file(&views.join(key), &target) {
        return None;
    }
    let (mut shared, mut master) = (None, None);
    for field in fields.by_ref().skip(1).take_while(|&field| field != "-") {
        shared = shared.or(field.strip_prefix("shared:"));
        master = master.or(field.strip_prefix("master:"));
    }
    let source = unescape(fields.nth(1)?);
    let path = PathBuf::from(unescape(dir.parent()?.to_str()?));
    Some((path, source, master.or(shared).unwrap_or(line)))
}

/// Undoes the kernel's escapes of spaces, tabs, newlines and backslashes. The
/// backslashes go last, so an escaped one does not start another escape.
fn unescape(field: &str) -> String {
    let field = field.replace("\\040", " ").replace("\\011", "\t");
    field.replace("\\012", "\n").replace("\\134", "\\")
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// A view is the state dir's if it is the same directory, whatever path
    /// its filesystem gives it. The mounts in a peer group and its slaves are
    /// one volume.
    #[test]
    fn counts_volumes_of_other_state_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let (views, target) = (tmp.path().join("views"), tmp.path().join("target"));
        let key = "3-04d1rq8d54408hyaxvaadn844xwai6hzs4j6ymnfk68x4dnqnysj";
        std::fs::create_dir_all(views.join(key)).unwrap();
        std::fs::create_dir(&target).unwrap();
        let line = |root: &str, target: &Path, peers: &str| {
            let target = target.display();
            format!("512 24 259:2 {root} {target} ro,nosuid {peers} - ext4 /dev/nvme0n1p2 rw")
        };
        let mut table = vec![
            line(
                "/var/lib/nix-store-csi",
                Path::new("/var/lib/nix-store-csi"),
                "",
            ),
            line("/srv/app/views/index", &target, "shared:6"),
            line(
                &format!("/var/lib/nix-store-csi/views/{key}"),
                &views.join(key),
                "shared:7",
            ),
        ];
        assert_eq!(other_in(&table.join("\n"), &views), []);

        let other = format!("/my\\040disk/views/{key}");
        table.push(line(&other, &target, "shared:12 master:8"));
        table.push(line(&other, &target, "master:8"));
        table.push(line(&other, &target, "shared:9"));
        let dir = OtherStateDir {
            path: "/my disk".into(),
            source: "/dev/nvme0n1p2".to_owned(),
            volumes: 2,
        };
        assert_eq!(other_in(&table.join("\n"), &views), [dir]);
    }
}
