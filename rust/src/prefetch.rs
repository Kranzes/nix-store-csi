//! Keeps the closures of the store paths listed in a directory, usually a
//! mounted `ConfigMap`, in the node store, so pods that need them start without
//! waiting for a fetch.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use bytesize::ByteSize;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, HashFormat};
use inotify::{EventStream, Inotify, WatchMask};
use tokio::time::MissedTickBehavior;
use tracing::{info, instrument, warn};

use crate::store::{Ensuring, Store};
use crate::store_path::{self, Names, STORE_PATHS};

/// The link that kubelet points at the current files of a `ConfigMap`'s volume.
const DATA: &str = "..data";

pub struct Prefetch {
    store: Arc<Store>,
    dir: PathBuf,
    system: String,
    /// The lists that `dir` holds for `system`, by file name.
    lists: BTreeMap<String, List>,
}

/// A file of store paths. Each has its own ensure, so one that fails doesn't
/// stop the others.
struct List {
    /// The file's [`hash`], which the metrics report.
    hash: String,
    /// The store paths in the file, sorted, or if it doesn't parse, those read
    /// before.
    roots: Vec<StorePath>,
    /// Whether `roots` are those of the file as it is now.
    parsed: bool,
    /// When `roots` last changed.
    since: Instant,
    /// The closure of the last ensure that succeeded. Until a new list
    /// succeeds, the list empties or its file goes, collection spares it, and
    /// volumes don't count it towards the limit.
    held: Option<Held>,
    running: Option<Ensuring>,
}

struct Held {
    roots: Vec<StorePath>,
    closure: Arc<[StorePath]>,
    ensuring: Ensuring,
    store: Arc<Store>,
}

impl Drop for Held {
    /// Records a use of the closure before the hold ends, so collection keeps
    /// it for `--keep-recent` like a closure that a pod just used.
    fn drop(&mut self) {
        if let Err(e) = self.store.touch(self.closure.iter()) {
            warn!(
                store_paths = %Names(&self.roots),
                "{e:#}; collection counts the closure as last used at the list's last ensure"
            );
        }
    }
}

impl Prefetch {
    pub fn new(store: Arc<Store>, dir: PathBuf) -> Self {
        Self {
            store,
            dir,
            system: store_path::system(),
            lists: BTreeMap::new(),
        }
    }

    /// Reads `dir` when it changes and every minute, and ensures each list's
    /// closure again each time, which fetches what a failed sync deleted.
    #[instrument(name = "prefetch", skip_all)]
    pub async fn run(mut self) {
        let mut ticks = tokio::time::interval(crate::MINUTE);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let dir = self.dir.display().to_string();
        let unwatched = |e: io::Error| warn!("watching {dir}: {e}; reading it every minute");
        let mut watch = watch(&self.dir).map_err(unwatched).ok();
        loop {
            tokio::select! {
                _ = ticks.tick() => self.tick(),
                changed = changed(watch.as_mut()) => match changed {
                    Ok(()) => self.tick(),
                    Err(e) => {
                        unwatched(e);
                        watch = None;
                    }
                },
                () = self.finish() => {}
            }
        }
    }

    /// Reads the lists again, and starts an ensure of each unless one is
    /// running.
    fn tick(&mut self) {
        match self.read_lists() {
            Ok(Some(files)) => {
                self.lists.retain(|file, _| files.contains_key(file));
                for (file, text) in files {
                    let path = self.dir.join(&file);
                    (self.lists.entry(file).or_insert_with(List::new)).read(&path, &text);
                }
                self.report();
            }
            Ok(None) => {}
            Err(e) => warn!("{e:#}; prefetching the lists read before"),
        }
        for list in self.lists.values_mut() {
            if list.running.is_none() && !list.roots.is_empty() {
                list.running = Some(self.store.prefetch(&list.roots));
            }
        }
    }

    /// Waits for the first running ensure to end, and keeps it if it
    /// succeeded.
    async fn finish(&mut self) {
        let mut running: FuturesUnordered<_> = (self.lists.iter_mut())
            .filter_map(|(file, list)| {
                let running = list.running.as_mut()?;
                Some(async move {
                    let result = running.done().await;
                    (file.clone(), result)
                })
            })
            .collect();
        let Some((file, result)) = running.next().await else {
            return std::future::pending().await;
        };
        drop(running);
        let list = self.lists.get_mut(&file).expect("running until it ends");
        let done = list.running.take().expect("running until it ends");
        match result {
            Ok(closure) => match &mut list.held {
                // The ensure recorded a use of the closure, which stays held.
                Some(held) if held.roots == list.roots && held.closure == closure => {}
                held => {
                    *held = Some(Held {
                        roots: list.roots.clone(),
                        closure,
                        ensuring: done,
                        store: self.store.clone(),
                    });
                    let elapsed_ms = list.since.elapsed().as_millis();
                    info!(store_paths = %Names(&list.roots), elapsed_ms, "prefetched");
                    if let Ok(Some(limit)) = self.store.max_store_size() {
                        let size = self.store.prefetched_size();
                        if size > limit {
                            warn!(
                                "the prefetched closures take {}, over the --max-store-size of {}, so every publish that needs a fetch fails",
                                ByteSize(size),
                                ByteSize(limit)
                            );
                        }
                    }
                    self.report();
                }
            },
            Err(e) => {
                let path = self.dir.join(&file);
                warn!(
                    store_paths = %Names(&list.roots),
                    "prefetching {}: {e:#}; trying again in a minute",
                    path.display()
                );
            }
        }
    }

    /// Tells the metrics whether the node store holds the closure of each
    /// list, and the NAR size of the closure that each list holds.
    fn report(&self) {
        let lists: Vec<_> = (self.lists.iter())
            .map(|(file, list)| {
                let name = list_name(file, &self.system).unwrap_or("");
                let held = list.held.as_ref();
                let nar_size = held.and_then(|held| held.ensuring.nar_size()).unwrap_or(0);
                (name, list.hash.as_str(), list.is_held(), nar_size)
            })
            .collect();
        self.store.metrics().prefetched(&lists);
    }

    /// Returns the text of each list by file name, or `None` while kubelet
    /// updates the directory. The lists are the files for the node's system,
    /// or if it has none, `storePaths`.
    fn read_lists(&self) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        let files = read(&self.dir).with_context(|| format!("reading {}", self.dir.display()))?;
        let Some(mut files) = files else {
            return Ok(None);
        };
        let system = &self.system;
        if files.keys().any(|file| list_name(file, system).is_some()) {
            files.retain(|file, _| list_name(file, system).is_some());
        } else {
            files.retain(|file, _| file == STORE_PATHS);
        }
        Ok(Some(files))
    }
}

impl List {
    fn new() -> Self {
        Self {
            hash: String::new(),
            roots: Vec::new(),
            parsed: false,
            since: Instant::now(),
            held: None,
            running: None,
        }
    }

    /// Takes the text of the file at `path`.
    fn read(&mut self, path: &Path, text: &str) {
        self.hash = hash(text);
        let roots = match parse(text) {
            Ok(roots) => roots,
            Err(e) => {
                self.parsed = false;
                warn!(
                    "reading {}: {e:#}; prefetching the list read before",
                    path.display()
                );
                return;
            }
        };
        self.parsed = true;
        if roots != self.roots {
            info!(store_paths = %Names(&roots), "prefetching");
            if roots.is_empty() {
                self.held = None;
            }
            self.roots = roots;
            self.since = Instant::now();
            // An ensure of the old list carries on in its own task.
            self.running = None;
        }
    }

    /// Whether the node store holds the closure of the file as it is now.
    fn is_held(&self) -> bool {
        self.parsed
            && (self.roots.is_empty()
                || (self.held.as_ref()).is_some_and(|held| held.roots == self.roots))
    }
}

/// Parses a list into its store paths, sorted.
fn parse(text: &str) -> anyhow::Result<Vec<StorePath>> {
    let mut roots = store_path::roots(text.split_whitespace())?;
    roots.sort();
    roots.dedup();
    Ok(roots)
}

/// The first 12 hex digits of the SHA-256 of a list's text, as
/// `sha256sum | head -c 12` prints them.
fn hash(text: &str) -> String {
    let mut hash = (Algorithm::SHA256.digest(text).as_base16().as_bare()).to_string();
    hash.truncate(12);
    hash
}

/// Returns `<name>` for a file named `storePaths.<system>.<name>`, `""` for
/// `storePaths.<system>`, or `None` for other files.
fn list_name<'a>(file: &'a str, system: &str) -> Option<&'a str> {
    match file.strip_prefix(&format!("{STORE_PATHS}.{system}"))? {
        "" => Some(""),
        rest => rest.strip_prefix('.'),
    }
}

/// Watches `dir` for kubelet's rename of `..data`, and for files written or
/// deleted in a plain directory.
fn watch(dir: &Path) -> io::Result<EventStream<[u8; 4096]>> {
    let inotify = Inotify::init()?;
    let events = WatchMask::MOVED_TO | WatchMask::CLOSE_WRITE | WatchMask::DELETE;
    inotify.watches().add(dir, events)?;
    inotify.into_event_stream([0; 4096])
}

/// Waits for the next event on `watch`.
async fn changed(watch: Option<&mut EventStream<[u8; 4096]>>) -> io::Result<()> {
    let Some(watch) = watch else {
        return std::future::pending().await;
    };
    match watch.next().await {
        Some(event) => event.map(drop),
        None => Err(io::ErrorKind::UnexpectedEof.into()),
    }
}

/// Returns the text of each file in `dir` by name. It reads through `..data`,
/// which kubelet points at a new directory on each update, and returns `None`
/// if the link moved meanwhile.
fn read(dir: &Path) -> io::Result<Option<BTreeMap<String, String>>> {
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
        let mut files = BTreeMap::new();
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

    use prometheus_client::registry::Registry;

    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{empty_cache, fixture_root, fixtures};
    use crate::store::test_utils::{ALL, UNBOUND, age, count, ghost, unsigned_config};
    use crate::store::{Config, Excess, TooBig};

    const A: &str = "/nix/store/xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";
    const B: &str = "/nix/store/jxxp01dayz0pv6vp7n39r9042ycsiicx-hello-2.12.3";

    fn prefetch(cache: &Path, dir: &Path) -> Prefetch {
        let store = Store::new(Config {
            // Every closure is over it, and prefetches have no limit.
            max_closure_size: Some(1),
            ..unsigned_config(cache, &dir.with_extension("state"))
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

    /// The `prefetched` sample of the list `name` whose file holds `text`.
    fn prefetched(registry: &Registry, name: &str, text: &str) -> Option<f64> {
        list_sample(registry, "prefetched", name, text)
    }

    /// The sample of `metric` for the list `name` whose file holds `text`.
    fn list_sample(registry: &Registry, metric: &str, name: &str, text: &str) -> Option<f64> {
        let labels = format!("list=\"{name}\",hash=\"{}\"", hash(text));
        sample(registry, &format!("nix_store_csi_{metric}{{{labels}}}"))
    }

    /// Collects the store paths that nothing used within a day, and returns
    /// how many it deleted.
    async fn keep_a_day(store: &Arc<Store>) -> usize {
        let keep_recent = Duration::from_hours(24);
        (store.collect(UNBOUND, keep_recent, Excess::default()).await).unwrap()
    }

    /// The store paths of the lists that `prefetch` reads, as text.
    fn listed(prefetch: &Prefetch) -> Option<Vec<String>> {
        let lists = prefetch.read_lists().unwrap()?;
        let mut roots: Vec<_> = (lists.values())
            .flat_map(|text| parse(text).unwrap())
            .collect();
        roots.sort();
        roots.dedup();
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
        let e = prefetch.read_lists().unwrap_err();
        assert!(format!("{e:#}").contains("No such file"), "{e:#}");

        std::fs::create_dir(&dir).unwrap();
        update(
            &dir,
            "1",
            &[("storePaths", A), ("storePaths.aarch64-linux", B)],
        );
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned()]));
        // The links next to `..data` lag behind it during an update.
        let link = dir.join("storePaths.aarch64-linux");
        std::os::unix::fs::symlink("..data/storePaths.aarch64-linux", link).unwrap();
        update(&dir, "2", &[("storePaths", &format!("{B}\n{A} {B}"))]);
        std::fs::remove_dir_all(dir.join("..1")).unwrap();
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned(), A.to_owned()]));
        update(&dir, "3", &[("storePaths.x86_64-linux", A)]);
        assert_eq!(listed(&prefetch), Some(vec![]));

        // A node reads the union of its system's lists.
        let web = "storePaths.aarch64-linux.web";
        let batch = "storePaths.aarch64-linux.batch";
        update(&dir, "4", &[(web, B), (batch, &format!("{A} {B}"))]);
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned(), A.to_owned()]));
        update(&dir, "5", &[(web, B), ("storePaths", A)]);
        assert_eq!(listed(&prefetch), Some(vec![B.to_owned()]));

        // A read that an update overtakes changes nothing. The pipe holds the
        // read until the update is done.
        update(&dir, "6", &[]);
        let fifo = dir.join("..6/storePaths");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        assert!(made.unwrap().success());
        let updating = std::thread::spawn({
            let dir = dir.clone();
            move || {
                let pipe = std::fs::OpenOptions::new().write(true).open(fifo);
                update(&dir, "7", &[("storePaths", B)]);
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
    async fn wakes_on_kubelets_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let mut watch = watch(tmp.path()).unwrap();
        let wait = Duration::from_secs(5);
        let mut changed = async || tokio::time::timeout(wait, changed(Some(&mut watch))).await;
        update(tmp.path(), "1", &[("storePaths", A)]);
        changed().await.unwrap().unwrap();
        std::fs::write(tmp.path().join("storePaths"), B).unwrap();
        changed().await.unwrap().unwrap();
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
        let list = format!("/nix/store/{hello}\n");
        std::fs::write(dir.join("storePaths"), &list).unwrap();
        let collect = || store.collect(UNBOUND, Duration::ZERO, ALL);
        let volume = || store.ensure(std::slice::from_ref(&hello));
        let registry = crate::metrics::registry(&store);
        let gauge = |text: &str| prefetched(&registry, "", text);

        prefetch.tick();
        assert_eq!(gauge(&list), Some(0.0));
        prefetch.finish().await;
        assert_eq!(gauge(&list), Some(1.0));
        // Collection spares paths until their syncs.
        store.sync_all().await.unwrap();
        assert_eq!(collect().await.unwrap(), 0);
        let paths = count(store.dir());
        assert_eq!(paths, 5);
        // A volume doesn't count the prefetched closure towards the limit.
        let closure = volume().done().await.unwrap();
        assert_eq!(closure.len(), paths);

        // Each tick ensures the closure again, and the list holds it
        // throughout.
        prefetch.tick();
        assert_eq!(collect().await.unwrap(), 0);
        prefetch.finish().await;
        assert_eq!(collect().await.unwrap(), 0);

        // The node holds a new hash with the same store paths as soon as it
        // reads it.
        let same = list.trim();
        std::fs::write(dir.join("storePaths"), same).unwrap();
        prefetch.tick();
        assert_eq!((gauge(&list), gauge(same)), (None, Some(1.0)));
        prefetch.finish().await;

        // A new list that fails keeps the old closure until it succeeds, and
        // tries again on the next tick.
        let missing = format!("/nix/store/{}", ghost());
        std::fs::write(dir.join("storePaths"), &missing).unwrap();
        prefetch.tick();
        prefetch.finish().await;
        assert_eq!((gauge(same), gauge(&missing)), (None, Some(0.0)));
        let list = &prefetch.lists[STORE_PATHS];
        assert!(list.held.is_some() && list.running.is_none());
        assert_eq!(collect().await.unwrap(), 0);
        prefetch.tick();
        assert!(prefetch.lists[STORE_PATHS].running.is_some());

        // Once the node holds a new list, the old list's closure counts as
        // just used, even if the node last ensured it long ago, like while
        // the new list failed. Collection keeps it for --keep-recent.
        age(&store, &closure, 2);
        let dependency = closure.iter().find(|path| **path != hello).unwrap();
        std::fs::write(dir.join("storePaths"), format!("/nix/store/{dependency}")).unwrap();
        prefetch.tick();
        prefetch.finish().await;
        assert_eq!(keep_a_day(&store).await, 0);
        let listed = prefetch.lists[STORE_PATHS]
            .held
            .as_ref()
            .unwrap()
            .closure
            .len();
        age(&store, &closure, 2);
        assert_eq!(keep_a_day(&store).await, paths - listed);

        // A store path that leaves the list counts towards the limit again.
        std::fs::write(dir.join("storePaths"), "").unwrap();
        prefetch.tick();
        assert!(volume().done().await.unwrap_err().is::<TooBig>());
        // A node holds an empty list as soon as it reads it, and it takes no
        // space.
        assert_eq!(gauge(""), Some(1.0));
        let size = list_sample(&registry, "prefetched_size_bytes", "", "");
        assert_eq!(size, Some(0.0));
        assert_eq!(collect().await.unwrap(), listed);
    }

    #[tokio::test]
    async fn isolates_lists() {
        let Some(fixtures) = fixtures() else {
            return;
        };
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("prefetch");
        std::fs::create_dir(&dir).unwrap();
        let mut prefetch = prefetch(&fixtures.join("cache-zstd"), &dir);
        let store = prefetch.store.clone();
        let registry = crate::metrics::registry(&store);
        let collect = || store.collect(UNBOUND, Duration::ZERO, ALL);
        let system = prefetch.system.clone();
        let (web, batch) = (
            format!("storePaths.{system}.web"),
            format!("storePaths.{system}.batch"),
        );
        let write = |file: &str, text: &str| std::fs::write(dir.join(file), text).unwrap();
        let list = |name: &str, text: &str| prefetched(&registry, name, text);
        let held = |prefetch: &Prefetch, file: &str| prefetch.lists[file].held.is_some();
        let hello = format!("/nix/store/{}", fixture_root(&fixtures, "hello.path"));
        let typo = "not a store path";
        assert_eq!(hash(typo), "04db0c8b3395");

        // A list that doesn't parse doesn't stop the others.
        write(&web, &hello);
        write(&batch, typo);
        prefetch.tick();
        prefetch.finish().await;
        assert_eq!(
            (list("web", &hello), list("batch", typo)),
            (Some(1.0), Some(0.0))
        );
        assert!(held(&prefetch, &web) && !held(&prefetch, &batch));
        store.sync_all().await.unwrap();
        assert_eq!((collect().await.unwrap(), count(store.dir())), (0, 5));

        // Nor does a list with a store path that no cache has.
        let missing = format!("/nix/store/{}", ghost());
        write(&batch, &missing);
        prefetch.tick();
        prefetch.finish().await;
        prefetch.finish().await;
        assert_eq!(
            (list("web", &hello), list("batch", &missing)),
            (Some(1.0), Some(0.0))
        );
        assert!(held(&prefetch, &web) && !held(&prefetch, &batch));
        assert_eq!(collect().await.unwrap(), 0);

        // A held list whose file stops parsing keeps its closure.
        write(&web, typo);
        prefetch.tick();
        assert_eq!((list("web", &hello), list("web", typo)), (None, Some(0.0)));
        assert!(held(&prefetch, &web));
        assert_eq!(collect().await.unwrap(), 0);

        // The closure of a list whose file goes counts as just used.
        let closure = prefetch.lists[&web].held.as_ref().unwrap().closure.clone();
        age(&store, &closure, 2);
        std::fs::remove_file(dir.join(&web)).unwrap();
        prefetch.tick();
        assert_eq!(keep_a_day(&store).await, 0);

        // A store path in two lists counts in the size of each.
        write(&web, &hello);
        write(&batch, &hello);
        prefetch.tick();
        prefetch.finish().await;
        prefetch.finish().await;
        let store_size = sample(&registry, "nix_store_csi_store_size_bytes");
        assert!(store_size > Some(0.0));
        let size = |name| list_sample(&registry, "prefetched_size_bytes", name, &hello);
        assert_eq!((size("web"), size("batch")), (store_size, store_size));
    }

    /// A list's series goes when its hash changes or its file goes, so no
    /// series of an old hash stays at 0.
    #[tokio::test]
    async fn drops_old_series() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = empty_cache();
        let dir = tmp.path().join("prefetch");
        std::fs::create_dir(&dir).unwrap();
        let mut prefetch = prefetch(cache.path(), &dir);
        let registry = crate::metrics::registry(&prefetch.store);
        let web = dir.join(format!("storePaths.{}.web", prefetch.system));
        let series = |text: &str| prefetched(&registry, "web", text);

        std::fs::write(&web, A).unwrap();
        prefetch.tick();
        assert_eq!(series(A), Some(0.0));
        std::fs::write(&web, B).unwrap();
        prefetch.tick();
        assert_eq!((series(A), series(B)), (None, Some(0.0)));
        std::fs::remove_file(&web).unwrap();
        prefetch.tick();
        assert_eq!(series(B), None);
    }
}
