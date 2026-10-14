//! Keeps the closures of the store paths listed in a directory, usually a
//! mounted `ConfigMap`, in the node store, so pods that need them start without
//! waiting for a fetch.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use harmonia_store_path::StorePath;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::store::{Ensuring, Store};
use crate::store_path;

/// The link that kubelet points at the current files of a `ConfigMap`'s volume.
const DATA: &str = "..data";

/// The file that names the list's revision, like a hash of its contents.
const REVISION: &str = "revision";

pub struct Prefetch {
    store: Arc<Store>,
    dir: PathBuf,
    system: String,
    /// The list's `revision` file, which the metrics report.
    revision: String,
    /// The store paths that `dir` lists for `system`, sorted.
    roots: Vec<StorePath>,
    /// The last ensure that succeeded, and its list. Until a new list succeeds
    /// or the list empties, collection spares its closure, and volumes don't
    /// count it towards the limit.
    held: Option<(Vec<StorePath>, Ensuring)>,
    running: Option<Ensuring>,
}

impl Prefetch {
    pub fn new(store: Arc<Store>, dir: PathBuf) -> Self {
        Self {
            store,
            dir,
            system: store_path::system(),
            revision: String::new(),
            roots: Vec::new(),
            held: None,
            running: None,
        }
    }

    /// Reads `dir` every minute and ensures the closure again each time. That
    /// fetches what a failed sync deleted, and counts as a use, so a closure
    /// that leaves the list stays for `--keep-recent`.
    pub async fn run(mut self) {
        let mut ticks = tokio::time::interval(crate::MINUTE);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticks.tick() => self.tick(),
                () = self.finish() => {}
            }
        }
    }

    /// Reads the list again, and starts an ensure of it unless one is running.
    fn tick(&mut self) {
        match self.read_list() {
            Ok(Some((revision, roots))) => {
                if roots != self.roots {
                    info!(store_paths = ?roots, "prefetching");
                    if roots.is_empty() {
                        self.held = None;
                    }
                    self.roots = roots;
                    // An ensure of the old list carries on in its own task.
                    self.running = None;
                }
                self.revision = revision;
                self.report();
            }
            Ok(None) => {}
            Err(e) => warn!("{e:#}; prefetching the list read before"),
        }
        if self.running.is_none() && !self.roots.is_empty() {
            self.running = Some(self.store.prefetch(&self.roots));
        }
    }

    /// Waits for the running ensure, and keeps it if it succeeded.
    async fn finish(&mut self) {
        let Some(running) = &mut self.running else {
            return std::future::pending().await;
        };
        let result = running.done().await;
        let Some(done) = self.running.take() else {
            return;
        };
        match result {
            Ok(_) => {
                if self
                    .held
                    .as_ref()
                    .is_none_or(|(roots, _)| *roots != self.roots)
                {
                    info!(store_paths = ?self.roots, "prefetched");
                }
                self.held = Some((self.roots.clone(), done));
                self.report();
            }
            Err(e) => {
                warn!(store_paths = ?self.roots, "prefetching: {e:#}; trying again in a minute");
            }
        }
    }

    /// Tells the metrics whether the node store holds the list's closures.
    fn report(&self) {
        let held = self.roots.is_empty()
            || (self.held.as_ref()).is_some_and(|(roots, _)| *roots == self.roots);
        self.store.metrics().prefetched(&self.revision, held);
    }

    /// Returns the list's revision and store paths, or `None` while kubelet
    /// updates the directory. A node whose system has no list gets an empty
    /// one.
    fn read_list(&self) -> anyhow::Result<Option<(String, Vec<StorePath>)>> {
        let files = read(&self.dir).with_context(|| format!("reading {}", self.dir.display()))?;
        let Some(files) = files else {
            return Ok(None);
        };
        let revision = files.get(REVISION).map_or("", |r| r.trim()).to_owned();
        let Some((name, list)) = store_path::for_system(&files, &self.system) else {
            return Ok(Some((revision, Vec::new())));
        };
        let mut roots = (self.store.roots(list.split_whitespace()))
            .with_context(|| format!("reading {}", self.dir.join(name).display()))?;
        roots.sort();
        roots.dedup();
        Ok(Some((revision, roots)))
    }
}

/// Returns the text of each file in `dir` by name. It reads through `..data`,
/// which kubelet points at a new directory on each update, and returns `None`
/// if the link moved meanwhile.
fn read(dir: &Path) -> io::Result<Option<HashMap<String, String>>> {
    let link = dir.join(DATA);
    let data = match std::fs::read_link(&link) {
        Ok(data) => Some(data),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let from = data
        .as_ref()
        .map_or_else(|| dir.to_owned(), |d| dir.join(d));
    let files = (|| {
        let mut files = HashMap::new();
        for entry in std::fs::read_dir(&from)? {
            let path = entry?.path();
            if std::fs::metadata(&path).is_ok_and(|m| m.is_dir()) {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                files.insert(name.to_owned(), std::fs::read_to_string(&path)?);
            }
        }
        Ok(files)
    })();
    // Kubelet deletes a directory only after `..data` stops pointing at it.
    if data.is_some() && std::fs::read_link(&link).ok() != data {
        return Ok(None);
    }
    files.map(Some)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{empty_cache, file_store, fixture_root, fixtures};
    use crate::store::test_utils::{ALL, UNBOUND, count};
    use crate::store::{Config, TooBig};

    const A: &str = "/nix/store/xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";
    const B: &str = "/nix/store/jxxp01dayz0pv6vp7n39r9042ycsiicx-hello-2.12.3";

    fn prefetch(cache: &Path, dir: &Path) -> Prefetch {
        let store = Store::new(Config {
            stores: vec![file_store(cache)],
            trusted_keys: None,
            state_dir: dir.with_extension("state"),
            jobs: 2,
            netrc_file: None,
            // Every closure is over it, and prefetches have no limit.
            max_closure_size: Some(1),
        });
        Prefetch::new(Arc::new(store.unwrap()), dir.to_owned())
    }

    /// Writes `files` into a new directory in `dir` and points `..data` at
    /// it, like kubelet does.
    fn update(dir: &Path, version: &str, files: &[(&str, &str)]) {
        let data = dir.join(format!("..{version}"));
        std::fs::create_dir_all(&data).unwrap();
        for (name, text) in files {
            std::fs::write(data.join(name), text).unwrap();
        }
        let tmp = dir.join("..data_tmp");
        std::os::unix::fs::symlink(data.file_name().unwrap(), &tmp).unwrap();
        std::fs::rename(&tmp, dir.join(DATA)).unwrap();
    }

    /// The store paths that `prefetch` reads, as text.
    fn listed(prefetch: &Prefetch) -> Option<Vec<String>> {
        let (_, roots) = prefetch.read_list().unwrap()?;
        Some(roots.iter().map(|r| format!("/nix/store/{r}")).collect())
    }

    #[tokio::test]
    async fn reads_kubelets_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = empty_cache();
        let dir = tmp.path().join("prefetch");
        let mut prefetch = prefetch(cache.path(), &dir);
        prefetch.system = "aarch64-linux".to_owned();

        // A missing directory is an error, which leaves the list as it was.
        let e = prefetch.read_list().unwrap_err();
        assert!(format!("{e:#}").contains("No such file"), "{e:#}");

        std::fs::create_dir(&dir).unwrap();
        update(
            &dir,
            "1",
            &[
                ("storePaths", A),
                ("storePaths.aarch64-linux", B),
                ("revision", "r1\n"),
            ],
        );
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned()]));
        let (revision, _) = prefetch.read_list().unwrap().unwrap();
        assert_eq!(revision, "r1");
        // The links next to `..data` lag behind it during an update.
        let link = dir.join("storePaths.aarch64-linux");
        std::os::unix::fs::symlink("..data/storePaths.aarch64-linux", link).unwrap();
        update(&dir, "2", &[("storePaths", &format!("{B}\n{A} {B}"))]);
        std::fs::remove_dir_all(dir.join("..1")).unwrap();
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned(), A.to_owned()]));
        update(&dir, "3", &[("storePaths.x86_64-linux", A)]);
        assert_eq!(listed(&prefetch), Some(vec![]));
        update(&dir, "4", &[("storePaths", "not a store path")]);
        assert!(prefetch.read_list().is_err());

        // A read that an update overtakes changes nothing. The pipe holds the
        // read until the update is done.
        update(&dir, "5", &[]);
        let fifo = dir.join("..5/storePaths");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        assert!(made.unwrap().success());
        let updating = std::thread::spawn({
            let dir = dir.clone();
            move || {
                let pipe = std::fs::OpenOptions::new().write(true).open(fifo);
                update(&dir, "6", &[("storePaths", B)]);
                pipe.unwrap().write_all(A.as_bytes()).unwrap();
            }
        });
        assert_eq!(listed(&prefetch), None);
        updating.join().unwrap();
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned()]));

        // A directory without `..data` is read as it is.
        prefetch.dir = tmp.path().join("plain");
        std::fs::create_dir(&prefetch.dir).unwrap();
        std::fs::write(prefetch.dir.join("storePaths"), A).unwrap();
        assert_eq!(listed(&prefetch), Some(vec![A.to_owned()]));
    }

    #[tokio::test]
    async fn spares_prefetched_closures() {
        let Some(fixtures) = fixtures() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("prefetch");
        std::fs::create_dir(&dir).unwrap();
        let mut prefetch = prefetch(&fixtures.join("cache-zstd"), &dir);
        let store = prefetch.store.clone();
        let hello = fixture_root(&fixtures, "hello.path");
        std::fs::write(dir.join("storePaths"), format!("/nix/store/{hello}\n")).unwrap();
        std::fs::write(dir.join("revision"), "r1").unwrap();
        let collect = || store.collect(UNBOUND, Duration::ZERO, ALL);
        let volume = || store.ensure(std::slice::from_ref(&hello));
        let registry = crate::metrics::registry(&store);
        let gauge = |revision: &str| {
            let name = format!("nix_store_csi_prefetched{{revision=\"{revision}\"}}");
            sample(&registry, &name)
        };

        prefetch.tick();
        assert_eq!(gauge("r1"), Some(0.0));
        prefetch.finish().await;
        assert_eq!(gauge("r1"), Some(1.0));
        // Collection spares paths until their syncs.
        store.sync_all().await.unwrap();
        assert_eq!(collect().await.unwrap(), 0);
        let paths = count(store.dir());
        assert_eq!(paths, 5);
        // A volume doesn't count the prefetched closure towards the limit.
        assert_eq!(volume().done().await.unwrap().len(), paths);

        // Each tick ensures the closure again, and the last ensure holds it
        // until the next one ends.
        prefetch.tick();
        assert_eq!(collect().await.unwrap(), 0);
        prefetch.finish().await;
        assert_eq!(collect().await.unwrap(), 0);

        // A new revision with the same store paths, like a change to another
        // system's list, is held immediately.
        std::fs::write(dir.join("revision"), "r1b").unwrap();
        prefetch.tick();
        assert_eq!((gauge("r1"), gauge("r1b")), (None, Some(1.0)));
        prefetch.finish().await;

        // A new list that fails keeps the old closure until it succeeds, and
        // tries again on the next tick.
        let missing = format!("/nix/store/{}-missing", "0".repeat(32));
        std::fs::write(dir.join("storePaths"), missing).unwrap();
        std::fs::write(dir.join("revision"), "r2").unwrap();
        prefetch.tick();
        prefetch.finish().await;
        assert_eq!((gauge("r1b"), gauge("r2")), (None, Some(0.0)));
        assert!(prefetch.held.is_some() && prefetch.running.is_none());
        assert_eq!(collect().await.unwrap(), 0);
        prefetch.tick();
        assert!(prefetch.running.is_some());

        // A store path that leaves the list counts towards the limit again,
        // and falls back to collection by last use.
        std::fs::write(dir.join("storePaths"), "").unwrap();
        prefetch.tick();
        assert!(volume().done().await.unwrap_err().is::<TooBig>());
        // A node holds an empty list as soon as it reads it.
        assert_eq!(gauge("r2"), Some(1.0));
        assert_eq!(collect().await.unwrap(), paths);
    }
}
