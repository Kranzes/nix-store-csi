//! Where views need mountpoints: the paths in `--mountpoints`, and what the
//! node's CDI specs bind in the store dir, like the driver and glibc that
//! nvidia-container-toolkit binds at their own store paths on NixOS.

use std::io::{self, Read};
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use harmonia_store_path::StoreDir;
use rustix::fs::{Dir, FileType, Mode, OFlags, ResolveFlags, fstat, openat2};
use serde::Deserialize;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use crate::store::{Mountpoint, Store};

/// Where container runtimes read CDI specs from by default.
const SPEC_DIRS: [&str; 2] = ["/etc/cdi", "/var/run/cdi"];

/// How often the plugin reads the CDI specs again. A watch would miss specs
/// that change through a symlink, like NixOS's `/etc`, spec dirs that systemd
/// deletes and creates again, like `/run/cdi`, and host paths that appear
/// later. A container that starts before the next read fails, and kubelet
/// tries again after at least 10 seconds.
const REREAD: Duration = Duration::from_secs(10);

pub struct Mountpoints {
    paths: Vec<Mountpoint>,
    cdi: Option<Cdi>,
    /// The warnings of the last read of the specs. The next read logs only
    /// new ones.
    warned: Mutex<Vec<String>>,
}

impl Mountpoints {
    /// `paths` are separated by whitespace. With `host_root`, the node's root
    /// directory, this also reads the node's CDI specs.
    pub fn new(paths: &str, host_root: Option<&Path>) -> anyhow::Result<Self> {
        let store_dir = StoreDir::default();
        let paths = (paths.split_whitespace())
            .map(|path| {
                (Mountpoint::parse(&store_dir, path)?)
                    .with_context(|| format!("{path} is not in a store path"))
            })
            .collect::<anyhow::Result<_>>()
            .context("parsing --mountpoints")?;
        let cdi = host_root
            .map(|root| Cdi::new(root, store_dir))
            .transpose()?;
        Ok(Self {
            paths,
            cdi,
            warned: Mutex::default(),
        })
    }

    /// Hands the mountpoints to `store`, which adds them to every view. With
    /// CDI specs, it then reads them every [`REREAD`], and hands them over
    /// again when they change.
    pub async fn run(self, store: Arc<Store>) {
        let this = Arc::new(self);
        let mut interval = tokio::time::interval(REREAD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut added: Arc<[Mountpoint]> = Arc::new([]);
        loop {
            interval.tick().await;
            let (reader, store, last) = (this.clone(), store.clone(), added.clone());
            let refresh = move || {
                let read: Arc<[Mountpoint]> = reader.read().into();
                if read != last {
                    store.set_mountpoints(read.clone())?;
                }
                anyhow::Ok(read)
            };
            match tokio::task::spawn_blocking(refresh).await {
                Ok(Ok(read)) => {
                    added = read;
                    if this.cdi.is_none() {
                        return;
                    }
                }
                Ok(Err(e)) => warn!("{e:#}; trying again in {}s", REREAD.as_secs()),
                Err(e) => warn!("{e}; trying again in {}s", REREAD.as_secs()),
            }
        }
    }

    /// The runtime binds a device's mounts only for containers that get the
    /// device, and this returns those of every device.
    fn read(&self) -> Vec<Mountpoint> {
        let mut mountpoints = self.paths.clone();
        if let Some(cdi) = &self.cdi {
            let mut warnings = Vec::new();
            mountpoints.extend(cdi.mountpoints(&mut warnings));
            let mut warned = self.warned.lock().unwrap();
            for warning in warnings.iter().filter(|w| !warned.contains(w)) {
                warn!("{warning}");
            }
            *warned = warnings;
        }
        mountpoints.sort();
        mountpoints.dedup_by(|a, b| (&a.store_path, &a.rest) == (&b.store_path, &b.rest));
        mountpoints
    }
}

struct Cdi {
    /// The node's root directory. Paths resolve in it as on the node, also
    /// through absolute symlinks, like those in NixOS's `/etc`.
    root: OwnedFd,
    store_dir: StoreDir,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    #[serde(default)]
    container_edits: Edits,
    #[serde(default)]
    devices: Vec<Device>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Device {
    #[serde(default)]
    container_edits: Edits,
}

#[derive(Default, Deserialize)]
struct Edits {
    #[serde(default)]
    mounts: Vec<Mount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Mount {
    #[serde(default)]
    host_path: String,
    container_path: String,
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    options: Vec<String>,
}

impl Spec {
    fn mounts(&self) -> impl Iterator<Item = &Mount> {
        let devices = self.devices.iter().map(|d| &d.container_edits);
        (std::iter::once(&self.container_edits).chain(devices)).flat_map(|edits| &edits.mounts)
    }
}

impl Mount {
    /// runc's test for a bind.
    fn is_bind(&self) -> bool {
        self.kind == "bind" || (self.options.iter()).any(|o| o == "bind" || o == "rbind")
    }
}

impl Cdi {
    fn new(root: &Path, store_dir: StoreDir) -> anyhow::Result<Self> {
        let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC;
        let root = rustix::fs::open(root, flags, Mode::empty())
            .with_context(|| format!("opening {}", root.display()))?;
        Ok(Self { root, store_dir })
    }

    /// Adds to `warnings` what it cannot read, and skips it.
    fn mountpoints(&self, warnings: &mut Vec<String>) -> Vec<Mountpoint> {
        let mut mountpoints = Vec::new();
        for file in self.spec_files(warnings) {
            let spec = match self.read_spec(&file) {
                Ok(spec) => spec,
                Err(e) => {
                    warnings.push(format!("{e:#}; skipping the CDI spec"));
                    continue;
                }
            };
            for mount in spec.mounts() {
                match self.mountpoint(mount) {
                    Ok(Some(mountpoint)) => mountpoints.push(mountpoint),
                    Ok(None) => {}
                    Err(e) => warnings.push(format!("{e:#}; skipping the mount in {file}")),
                }
            }
        }
        mountpoints
    }

    /// The specs in [`SPEC_DIRS`], which may not exist.
    fn spec_files(&self, warnings: &mut Vec<String>) -> Vec<String> {
        let mut files = Vec::new();
        for dir in SPEC_DIRS {
            match self.spec_names(dir) {
                Ok(names) => files.extend(names.iter().map(|name| format!("{dir}/{name}"))),
                Err(rustix::io::Errno::NOENT) => {}
                Err(e) => warnings.push(format!(
                    "reading the node's {dir}: {e}; skipping its CDI specs"
                )),
            }
        }
        files
    }

    /// CDI reads the files in `dir` that end in `.json` or `.yaml`.
    fn spec_names(&self, dir: &str) -> rustix::io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in Dir::new(self.open(dir, OFlags::RDONLY | OFlags::DIRECTORY)?)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            let ext = Path::new(&name).extension().and_then(|e| e.to_str());
            if matches!(ext, Some("json" | "yaml")) {
                names.push(name);
            }
        }
        Ok(names)
    }

    /// JSON is YAML, so this reads both kinds of spec.
    fn read_spec(&self, file: &str) -> anyhow::Result<Spec> {
        let mut bytes = Vec::new();
        (self.open(file, OFlags::RDONLY).map_err(io::Error::from))
            .and_then(|fd| std::fs::File::from(fd).read_to_end(&mut bytes))
            .with_context(|| format!("reading the node's {file}"))?;
        serde_saphyr::from_slice(&bytes).with_context(|| format!("parsing the node's {file}"))
    }

    /// The mountpoint for `mount`, if it is in a store path. A mountpoint
    /// never changes once it is in a view, so this doesn't guess the type of
    /// a host path that doesn't exist yet. A later read adds it once it does.
    /// A mount that isn't a bind, like a tmpfs, needs a directory.
    fn mountpoint(&self, mount: &Mount) -> anyhow::Result<Option<Mountpoint>> {
        let Some(mut mountpoint) = Mountpoint::parse(&self.store_dir, &mount.container_path)?
        else {
            return Ok(None);
        };
        mountpoint.dir = if mount.is_bind() {
            // Like the runtime, this follows a symlink to what it binds.
            let host = &mount.host_path;
            let stat = (self.open(host, OFlags::PATH))
                .and_then(fstat)
                .with_context(|| format!("reading the node's {host}"))?;
            FileType::from_raw_mode(stat.st_mode) == FileType::Directory
        } else {
            true
        };
        Ok(Some(mountpoint))
    }

    fn open(&self, path: &str, flags: OFlags) -> rustix::io::Result<OwnedFd> {
        let flags = flags | OFlags::CLOEXEC;
        openat2(
            &self.root,
            path,
            flags,
            Mode::empty(),
            ResolveFlags::IN_ROOT,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tracing_test::traced_test;

    use super::*;
    use crate::store::test_utils::{UNBOUND, hello_store};

    const DRIVER: &str = "0lmfzs6dkgbrwc4bc2d1lhm2fhh4r8xm-nvidia-x11-570.195.03";
    const GLIBC: &str = "1lmfzs6dkgbrwc4bc2d1lhm2fhh4r8xm-glibc-2.40-66";
    const GRAPHICS: &str = "2lmfzs6dkgbrwc4bc2d1lhm2fhh4r8xm-graphics-drivers";
    const SYSTEM: &str = "3lmfzs6dkgbrwc4bc2d1lhm2fhh4r8xm-nixos-system";
    const SW: &str = "4lmfzs6dkgbrwc4bc2d1lhm2fhh4r8xm-system-path";

    fn found(mountpoints: &Mountpoints) -> Vec<String> {
        mountpoints.read().iter().map(ToString::to_string).collect()
    }

    #[test]
    #[traced_test]
    fn reads_mountpoints_from_specs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let store = root.join("nix/store");
        std::fs::create_dir_all(store.join(DRIVER).join("lib")).unwrap();
        std::fs::write(store.join(DRIVER).join("lib/libcuda.so.1"), "").unwrap();
        std::fs::create_dir_all(store.join(GLIBC).join("lib")).unwrap();
        // glibc's lib64 is a relative symlink, and NixOS's /etc holds
        // absolute ones, which resolve in the node's root.
        symlink("lib", store.join(GLIBC).join("lib64")).unwrap();
        std::fs::create_dir_all(root.join("etc/static/cdi")).unwrap();
        symlink("/etc/static/cdi", root.join("etc/cdi")).unwrap();
        // Like the NixOS module's mounts, and those that nvidia-ctk adds for
        // each library.
        let bind = |host: String, container: String| serde_json::json!({"hostPath": host, "containerPath": container, "options": ["ro", "bind"]});
        let spec = serde_json::json!({
            "cdiVersion": "0.6.0",
            "kind": "nvidia.com/gpu",
            "containerEdits": {"mounts": [
                bind(format!("/nix/store/{DRIVER}"), format!("/nix/store/{DRIVER}")),
                bind(format!("/nix/store/{GLIBC}/lib"), format!("/nix/store/{GLIBC}/lib")),
                bind(format!("/nix/store/{GLIBC}/lib64"), format!("/nix/store/{GLIBC}/lib64")),
                bind("/run/opengl-driver".into(), "/run/opengl-driver".into()),
                bind("/nix/store".into(), "/nix/store/".into()),
            ]},
            "devices": [{"name": "0", "containerEdits": {"mounts": [
                bind(format!("/nix/store/{DRIVER}/lib/libcuda.so.1"), format!("/nix/store/{DRIVER}//lib/libcuda.so.1")),
            ]}}],
        });
        // Indented with tabs, which YAML allows in JSON's flow style.
        let pretty = serde_json::to_string_pretty(&spec)
            .unwrap()
            .replace("  ", "\t");
        std::fs::write(root.join("etc/static/cdi/nvidia.json"), pretty).unwrap();
        // A spec in YAML, with a tmpfs, and mounts that the plugin skips.
        std::fs::create_dir_all(root.join("var/run/cdi")).unwrap();
        let yaml = format!(
            "kind: example.com/test\ndevices:\n- name: a\n  containerEdits:\n    mounts:\n    \
             - {{containerPath: /nix/store/{GLIBC}/tmp, type: tmpfs}}\n    \
             - {{hostPath: /missing, containerPath: /nix/store/{GLIBC}/missing, options: [bind]}}\n    \
             - {{hostPath: /nix/store, containerPath: /nix/store/not-a-store-path, options: [bind]}}\n"
        );
        std::fs::write(root.join("var/run/cdi/test.yaml"), yaml).unwrap();
        std::fs::write(root.join("var/run/cdi/broken.json"), "{").unwrap();
        std::fs::write(root.join("var/run/cdi/README"), "").unwrap();

        // The list adds a file and repeats a directory of the specs.
        let paths = format!("/nix/store/{GLIBC}/lib/ /nix/store/{GLIBC}/etc/ld.so.conf");
        let mountpoints = Mountpoints::new(&paths, Some(root)).unwrap();
        assert_eq!(
            found(&mountpoints),
            [
                format!("{DRIVER}/"),
                format!("{DRIVER}/lib/libcuda.so.1"),
                format!("{GLIBC}/etc/ld.so.conf"),
                format!("{GLIBC}/lib/"),
                format!("{GLIBC}/lib64/"),
                format!("{GLIBC}/tmp/"),
            ]
        );
        for skipped in [
            "parsing the node's /var/run/cdi/broken.json",
            "reading the node's /missing",
            "/nix/store/not-a-store-path is not in a store path",
        ] {
            assert!(logs_contain(skipped), "{skipped}");
        }

        // A host path that appears later gets its mountpoint on the next read.
        std::fs::write(root.join("missing"), "").unwrap();
        assert!(found(&mountpoints).contains(&format!("{GLIBC}/missing")));
    }

    /// The links in `/run` point into the store. Resolved in the plugin's own
    /// root, their targets wouldn't exist.
    #[test]
    fn follows_absolute_symlinks_in_the_nodes_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let store = root.join("nix/store");
        std::fs::create_dir_all(store.join(GRAPHICS).join("lib")).unwrap();
        std::fs::create_dir_all(store.join(SW).join("lib")).unwrap();
        std::fs::write(store.join(SW).join("lib/libsw.so"), "").unwrap();
        std::fs::create_dir_all(store.join(SYSTEM)).unwrap();
        symlink(format!("/nix/store/{SW}"), store.join(SYSTEM).join("sw")).unwrap();
        std::fs::create_dir_all(root.join("run/cdi")).unwrap();
        symlink(
            format!("/nix/store/{GRAPHICS}"),
            root.join("run/opengl-driver"),
        )
        .unwrap();
        symlink(
            format!("/nix/store/{SYSTEM}"),
            root.join("run/current-system"),
        )
        .unwrap();
        // /var/run is a symlink to /run, as on most nodes.
        std::fs::create_dir(root.join("var")).unwrap();
        symlink("/run", root.join("var/run")).unwrap();
        let spec = serde_json::json!({"containerEdits": {"mounts": [
            {"hostPath": "/run/opengl-driver/lib", "containerPath": format!("/nix/store/{GRAPHICS}/lib"), "type": "bind"},
            {"hostPath": "/run/current-system/sw/lib/libsw.so", "containerPath": format!("/nix/store/{SW}/lib/libsw.so"), "type": "bind"},
        ]}});
        std::fs::write(root.join("run/cdi/spec.json"), spec.to_string()).unwrap();
        let mountpoints = Mountpoints::new("", Some(root)).unwrap();
        assert_eq!(
            found(&mountpoints),
            [format!("{GRAPHICS}/lib/"), format!("{SW}/lib/libsw.so")]
        );
    }

    /// The first read after a start counts as a change, so the views of
    /// running pods get the mountpoints, like after an upgrade turns them on.
    #[tokio::test]
    async fn adds_mountpoints_to_views_from_before_the_start() {
        let tmp = tempfile::tempdir().unwrap();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let roots = std::slice::from_ref(&hello);
        let target = tmp.path().join("target");
        let gc = store.gc_guard().await;
        let view = (store.view(&gc, "v1", roots, &paths, &target, UNBOUND)).unwrap();
        drop(gc);
        let mountpoints = Mountpoints::new(&format!("/nix/store/{GLIBC}/lib/"), None).unwrap();
        mountpoints.run(store).await;
        assert!(view.join(GLIBC).join("lib").is_dir());
    }

    #[test]
    fn reads_no_specs_from_a_node_without_them() {
        let tmp = tempfile::tempdir().unwrap();
        let mountpoints = Mountpoints::new("", Some(tmp.path())).unwrap();
        assert_eq!(mountpoints.read(), []);
    }

    #[test]
    fn refuses_paths_outside_store_paths() {
        assert!(Mountpoints::new("/usr/lib/libcuda.so.1", None).is_err());
    }
}
