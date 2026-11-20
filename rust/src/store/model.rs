//! Replays traces of the model in `spec/store.qnt` against a [`Store`].
//!
//! `quint run` walks the model's `replay` action at random. The test takes
//! each step of a trace with a state dir and a cache of its own, and after
//! every step the state dir and the store's memory must hold what the model
//! says. The model checker covers the smaller steps of `step`, which a test
//! cannot make the code take one at a time.
//!
//! The cache serves each NAR from a FIFO, so a fetch waits with its NAR open
//! until the test writes it. The test binds no mounts and keeps the table of
//! binds itself. It takes every job of the store, and starts each fetch as a
//! feed would. A restart drops the runtime with its tasks. A reboot also
//! changes the recorded boot id, and loses no data.
//!
//! The test needs `quint` on the PATH. A failed run prints its seed, and
//! `QUINT_SEED=<seed> cargo test store::model` replays its traces.
//! `QUINT_VERBOSE=1` with `--nocapture` prints each step.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail, ensure};
use futures::FutureExt;
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, HashFormat};
use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use rustix::fs::{CWD, FileType, Mode, OFlags, mknodat};
use serde::Deserialize;
use tokio::runtime::Runtime;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use super::fetch::Ensuring;
use super::fs::Scratch;
use super::test_utils::{ALL, file_names, file_nar, open, resolved, scratch, unsigned_config};
use super::{Config, Store, narinfos, view_generation};

/// The store paths of the model's `sample` module, each with its references.
const REFERENCES: [(&str, &[&str]); 3] = [("a", &["b"]), ("b", &[]), ("c", &["b"])];

/// How long a step may wait for the store before the test fails.
const WAIT: Duration = Duration::from_secs(30);

fn store_path(name: &str) -> StorePath {
    StorePath::from_base_path(&format!("{}-{name}", name.repeat(32))).unwrap()
}

fn name(path: &StorePath) -> String {
    path.name().to_string()
}

/// The NAR that the cache's narinfo for `name` describes, or with `corrupt`
/// one of the same size with another hash.
fn nar(name: &str, corrupt: bool) -> Vec<u8> {
    file_nar(if corrupt { b"?" } else { name.as_bytes() })
}

/// Writes a cache of the model's store paths into `dir`, with a FIFO for
/// each NAR.
fn write_cache(dir: &Path) {
    std::fs::create_dir_all(dir.join("nar")).unwrap();
    std::fs::write(dir.join("nix-cache-info"), "StoreDir: /nix/store\n").unwrap();
    for (name, references) in REFERENCES {
        let nar = nar(name, false);
        let hash = Algorithm::SHA256.digest(&nar);
        let references: Vec<_> = (references.iter())
            .map(|name| store_path(name).to_string())
            .collect();
        let narinfo = format!(
            "StorePath: /nix/store/{}\nURL: nar/{name}.nar\nCompression: none\n\
             NarHash: sha256:{}\nNarSize: {}\nReferences: {}\n",
            store_path(name),
            hash.as_base32().as_bare(),
            nar.len(),
            references.join(" ")
        );
        std::fs::write(dir.join(format!("{}.narinfo", name.repeat(32))), narinfo).unwrap();
        let fifo = dir.join(format!("nar/{name}.nar"));
        mknodat(CWD, &fifo, FileType::Fifo, Mode::from_raw_mode(0o644), 0).unwrap();
    }
}

async fn soon<T>(what: &str, wait: impl Future<Output = T>) -> Result<T> {
    (tokio::time::timeout(WAIT, wait).await).map_err(|_| anyhow!("timed out waiting for {what}"))
}

/// Polls until `poll` returns a value.
async fn until<T>(what: &str, mut poll: impl FnMut() -> Option<T>) -> Result<T> {
    let started = Instant::now();
    let polled = async {
        loop {
            if let Some(found) = poll() {
                return found;
            }
            // Most waits end within a few turns of the runtime. The store's
            // blocking threads need time, so the longer ones sleep.
            if started.elapsed() < Duration::from_millis(1) {
                tokio::task::yield_now().await;
                std::thread::yield_now();
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    };
    soon(what, polled).await
}

/// Takes `Store::syncing`, so that the syncs which fetches start wait until
/// the result drops.
fn take_turn(runtime: &Runtime, store: &Arc<Store>) -> Result<oneshot::Sender<()>> {
    let (release, released) = oneshot::channel::<()>();
    let (holding, held) = oneshot::channel();
    let this = store.clone();
    runtime.spawn(async move {
        let _turn = this.syncing.lock().await;
        let _ = holding.send(());
        let _ = released.await;
    });
    runtime.block_on(soon("the syncs under way", held))??;
    Ok(release)
}

/// An ensure of the model.
struct Ensure {
    root: String,
    /// `None` once the caller has dropped it.
    handle: Option<Ensuring>,
    /// Whether its task has ended, or is about to.
    ended: Box<dyn Fn() -> bool>,
    /// Its closure or its error, once its task has ended.
    result: Option<std::result::Result<Arc<[StorePath]>, String>>,
}

/// A fetch that waits for its NAR.
struct Unpacking {
    /// The FIFO that the fetch reads.
    nar: std::fs::File,
    done: Pin<Box<dyn Future<Output = bool>>>,
}

/// One run of the plugin.
struct Plugin {
    runtime: Runtime,
    store: Arc<Store>,
    /// Every job of the store, so that its feeds start no fetch.
    _jobs: OwnedSemaphorePermit,
    /// Holds off the syncs.
    turn: Option<oneshot::Sender<()>>,
    ensures: BTreeMap<String, Ensure>,
    unpacking: BTreeMap<String, Unpacking>,
}

impl Plugin {
    /// Waits for the tasks of the ensures whose fetches have ended, and
    /// takes their results.
    fn settle(&mut self) -> Result {
        let running = self.ensures.values().filter(|e| !(e.ended)()).count();
        let ensures = &self.store.ensures;
        let ended = || (ensures.load(Ordering::Relaxed) == running).then_some(());
        self.runtime.block_on(until("the ensures to end", ended))?;
        for ensure in self.ensures.values_mut().filter(|e| (e.ended)()) {
            if let (None, Some(handle)) = (&ensure.result, &mut ensure.handle) {
                let result = handle.done().now_or_never();
                let result = result.context("an ensure ended with no result")?;
                ensure.result = Some(result.map_err(|e| format!("{e:#}")));
            }
        }
        self.ensures
            .retain(|_, e| e.handle.is_some() || !(e.ended)());
        Ok(())
    }
}

#[derive(Default)]
struct Node {
    /// Holds the cache, the state dir and the volumes' targets.
    dir: Option<Scratch>,
    plugin: Option<Plugin>,
    /// The view bound at each target, as `mounts::publish` would bind it.
    bound: BTreeMap<PathBuf, PathBuf>,
}

impl Node {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.as_ref().expect("an init").path().join(name)
    }

    fn target(&self, volume: &str) -> PathBuf {
        self.path("targets").join(volume)
    }

    fn plugin(&mut self) -> &mut Plugin {
        self.plugin.as_mut().expect("an init")
    }

    fn is_bound(&self) -> impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + use<> {
        let bound = self.bound.clone();
        move |view, target| Ok(bound.get(target).is_some_and(|bound| bound == view))
    }

    fn init(&mut self) -> Result {
        self.stop();
        self.bound.clear();
        let dir = scratch();
        write_cache(&dir.path().join("cache"));
        self.dir = Some(dir);
        self.start()
    }

    /// Starts the plugin as `csi` does, with the views pruned.
    fn start(&mut self) -> Result {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (cache, state) = (self.path("cache"), self.path("state"));
        let config = || Config {
            jobs: REFERENCES.len(),
            ..unsigned_config(&cache, &state)
        };
        let is_bound = self.is_bound();
        let starting = async {
            let store = open(config).await;
            store.prune_views(is_bound).await?;
            let jobs = u32::try_from(store.max_jobs)?;
            let jobs = store.jobs.clone().acquire_many_owned(jobs).await?;
            anyhow::Ok((store, jobs))
        };
        let (store, jobs) = runtime.block_on(starting)?;
        let turn = Some(take_turn(&runtime, &store)?);
        self.plugin = Some(Plugin {
            runtime,
            store,
            _jobs: jobs,
            turn,
            ensures: BTreeMap::new(),
            unpacking: BTreeMap::new(),
        });
        Ok(())
    }

    /// Ends the plugin like a kill does. Its tasks stop where they wait.
    fn stop(&mut self) {
        let Some(plugin) = self.plugin.take() else {
            return;
        };
        let Plugin { runtime, store, .. } = plugin;
        runtime.shutdown_background();
        // A blocking thread of the run may still hold the store, and with it
        // the lock on the state dir.
        let deadline = Instant::now() + WAIT;
        while Arc::strong_count(&store) > 1 {
            assert!(Instant::now() < deadline, "the plugin still runs");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn restart(&mut self, reboot: bool) -> Result {
        self.stop();
        if reboot {
            std::fs::write(self.path("state").join("boot-id"), "another boot")?;
            self.bound.clear();
        }
        self.start()
    }

    fn ensure(&mut self, e: String, root: String) -> Result {
        let plugin = self.plugin();
        let store = &plugin.store;
        let resolving = async {
            let handle = store.ensure(&[store_path(&root)]);
            resolved(&handle).await;
            handle
        };
        let handle = (plugin.runtime).block_on(soon("the ensure to resolve", resolving))?;
        let ensure = Ensure {
            root,
            ended: Box::new(handle.watch()),
            handle: Some(handle),
            result: None,
        };
        plugin.ensures.insert(e, ensure);
        plugin.settle()
    }

    fn release(&mut self, e: &str) -> Result {
        let plugin = self.plugin();
        plugin.ensures.get_mut(e).context("no such ensure")?.handle = None;
        plugin.settle()
    }

    /// Gives the fetch of `p` a job, and waits until it has read
    /// `failed_syncs` and opened its NAR.
    fn fetch_begin(&mut self, p: &str) -> Result {
        let fifo = self.path("cache").join(format!("nar/{p}.nar"));
        let plugin = self.plugin();
        let slot = Arc::new(Semaphore::new(1)).try_acquire_owned()?;
        let done = plugin.store.start_fetch(&store_path(p), slot).boxed();
        let flags = OFlags::WRONLY | OFlags::NONBLOCK | OFlags::CLOEXEC;
        // Fails until the fetch has the FIFO open.
        let open = || rustix::fs::open(&fifo, flags, Mode::empty()).ok();
        let nar = (plugin.runtime).block_on(until("the fetch to open its NAR", open))?;
        let unpacking = Unpacking {
            nar: nar.into(),
            done,
        };
        plugin.unpacking.insert(p.to_owned(), unpacking);
        Ok(())
    }

    /// Sends the NAR, whole or corrupt, and waits for the fetch to end.
    fn fetch_end(&mut self, p: &str, ok: bool) -> Result {
        let plugin = self.plugin();
        let Unpacking { mut nar, done } = plugin.unpacking.remove(p).context("no such fetch")?;
        nar.write_all(&self::nar(p, !ok))?;
        drop(nar);
        let fetched = plugin.runtime.block_on(soon("the fetch to end", done))?;
        ensure!(fetched == ok, "the fetch of {p} ended with {fetched}");
        plugin.settle()?;
        // The sync that the fetch started gets to wait for its turn.
        plugin.runtime.block_on(async {
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
        });
        Ok(())
    }

    fn fetch_fail(&mut self, p: &str) -> Result {
        if !self.plugin().unpacking.contains_key(p) {
            self.fetch_begin(p)?;
        }
        self.fetch_end(p, false)
    }

    /// Lets the waiting syncs take their turns, the first with a syncfs that
    /// fails unless `ok`.
    fn sync(&mut self, ok: bool) -> Result {
        let plugin = self.plugin();
        plugin.store.fail_syncfs.store(!ok, Ordering::SeqCst);
        drop(plugin.turn.take());
        // A new turn comes after the turns of the syncs that wait.
        plugin.turn = Some(take_turn(&plugin.runtime, &plugin.store)?);
        ensure!(
            !plugin.store.fail_syncfs.load(Ordering::SeqCst),
            "no sync ran"
        );
        Ok(())
    }

    fn collect(&mut self) -> Result {
        let is_bound = self.is_bound();
        let plugin = self.plugin();
        let collecting = plugin.store.collect(is_bound, Duration::ZERO, ALL);
        (plugin.runtime).block_on(soon("the collection", collecting))??;
        Ok(())
    }

    fn publish(&mut self, v: &str, e: &str) -> Result {
        let (target, is_bound) = (self.target(v), self.is_bound());
        let plugin = self.plugin();
        let ensure = plugin.ensures.get(e).context("no such ensure")?;
        let Some(Ok(paths)) = &ensure.result else {
            bail!("ensure {e} has no closure: {:?}", ensure.result);
        };
        let roots = [store_path(&ensure.root)];
        let held_in = ensure.handle.as_ref().map_or(0, Ensuring::generation);
        let store = &plugin.store;
        let view = plugin.runtime.block_on(async {
            let guard = store.gc_guard().await;
            store.view(&guard, v, &roots, paths, held_in, &target, is_bound)
        });
        // A publish that fails binds nothing, and the model changes nothing.
        if let Ok(view) = view {
            self.bound.insert(target, view);
        }
        Ok(())
    }

    fn unpublish(&mut self, v: &str) -> Result {
        let target = self.target(v);
        self.bound.remove(&target);
        drop(self.plugin().store.guarded_drop_view(v)?);
        Ok(())
    }
}

/// Dropping the runtime would wait for the fetches that read a FIFO.
impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Driver for Node {
    type State = Observed;

    fn config() -> quint_connect::Config {
        quint_connect::Config {
            state: &["sample::store::s"],
            ..Default::default()
        }
    }

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init()?,
            ensure(e, root) => self.ensure(e, root)?,
            release(e: String) => self.release(&e)?,
            fetchBegin(p: String) => self.fetch_begin(&p)?,
            fetchEnd(p: String) => self.fetch_end(&p, true)?,
            fetchFail(p: String) => self.fetch_fail(&p)?,
            sync(ok) => self.sync(ok)?,
            collect => self.collect()?,
            publish(v: String, e: String) => self.publish(&v, &e)?,
            unpublish(v: String) => self.unpublish(&v)?,
            restart => self.restart(false)?,
            reboot => self.restart(true)?,
        })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct View {
    generation: u64,
    root: String,
    paths: BTreeSet<String>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(tag = "tag")]
enum Phase {
    Queued,
    Unpacking,
    Marked,
    Written,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Fetch {
    phase: Phase,
}

/// An ensure in the model's state. After a whole step it has joined its
/// fetches.
#[derive(Deserialize)]
struct ModelEnsure {
    paths: BTreeSet<String>,
    waiting: BTreeSet<String>,
    failed: bool,
    caller: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(from = "BTreeMap<String, ModelEnsure>")]
struct Ensures {
    held: BTreeSet<String>,
    /// For each ensure that the caller still has, whether it has returned
    /// its closure, or failed.
    results: BTreeMap<String, Option<bool>>,
}

impl From<BTreeMap<String, ModelEnsure>> for Ensures {
    fn from(ensures: BTreeMap<String, ModelEnsure>) -> Self {
        Self {
            held: (ensures.values().flat_map(|e| e.paths.clone())).collect(),
            results: (ensures.into_iter())
                .filter(|(_, e)| e.caller)
                .map(|(id, e)| (id, e.waiting.is_empty().then_some(!e.failed)))
                .collect(),
        }
    }
}

/// The fields of the model's state that the store shows, which the test
/// compares after each step.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    store: BTreeSet<String>,
    synced: BTreeSet<String>,
    unsynced: BTreeSet<String>,
    views: BTreeSet<View>,
    volumes: BTreeMap<String, View>,
    bound: BTreeMap<String, View>,
    generation_record: u64,
    sync_failed: bool,
    generation: u64,
    marks: BTreeMap<String, u64>,
    failed_syncs: u64,
    fetches: BTreeMap<String, Fetch>,
    ensures: Ensures,
}

impl State<Node> for Observed {
    fn from_driver(node: &Node) -> Result<Self> {
        let plugin = node.plugin.as_ref().context("no init")?;
        let store = &plugin.store;

        let (mut synced, mut unsynced) = (BTreeSet::new(), BTreeSet::new());
        for file in file_names(store.narinfos.dir()) {
            let (path, durable) = narinfos::parse_name(OsStr::new(&file))
                .with_context(|| format!("{file} is not a narinfo"))?;
            if durable {
                synced.insert(name(&path));
            } else {
                unsynced.insert(name(&path));
            }
        }

        // A view shows its generation in its name, its root in .roots/, and
        // its closure in the store objects it links.
        let view = |key: &str| -> Result<View> {
            let dir = store.views.join(key);
            let roots = file_names(dir.join(super::views::ROOTS));
            let [root] = &roots[..] else {
                bail!("the view {key} has the roots {roots:?}");
            };
            let paths = file_names(&dir).into_iter();
            Ok(View {
                generation: view_generation(key).context("a view has another name")?,
                root: root.clone(),
                paths: (paths.filter_map(|file| StorePath::from_base_path(&file).ok()))
                    .map(|path| name(&path))
                    .collect(),
            })
        };
        let key = |view: &Path| view.file_name().unwrap().to_string_lossy().into_owned();
        let mut volumes = BTreeMap::new();
        for volume in file_names(&store.volumes) {
            let record = std::fs::read_to_string(store.volumes.join(&volume))?;
            let key = record.lines().next().context("an empty record")?;
            volumes.insert(volume, view(key)?);
        }
        let mut bound = BTreeMap::new();
        for (target, bound_view) in &node.bound {
            bound.insert(key(target), view(&key(bound_view))?);
        }

        let generation_record =
            std::fs::read_to_string(node.path("state").join("generation"))?.parse()?;
        let phase = |waits| {
            if waits {
                Phase::Queued
            } else {
                Phase::Unpacking
            }
        };
        let marks = store.unsynced.lock().unwrap().clone();
        Ok(Self {
            store: store.store_paths()?.iter().map(name).collect(),
            synced,
            unsynced,
            views: (file_names(&store.views).iter().map(|key| view(key))).collect::<Result<_>>()?,
            volumes,
            bound,
            generation_record,
            sync_failed: store.failed_record.exists(),
            generation: store.generation.load(Ordering::SeqCst),
            marks: (marks.iter().map(|(path, since)| (name(path), *since))).collect(),
            failed_syncs: store.failed_syncs.load(Ordering::SeqCst),
            fetches: (store.fetches().iter())
                .map(|(path, waits)| {
                    (
                        name(path),
                        Fetch {
                            phase: phase(*waits),
                        },
                    )
                })
                .collect(),
            ensures: Ensures {
                held: store.ensuring.paths().iter().map(name).collect(),
                results: (plugin.ensures.iter())
                    .filter(|(_, ensure)| ensure.handle.is_some())
                    .map(|(e, ensure)| {
                        (
                            e.clone(),
                            ensure.result.as_ref().map(std::result::Result::is_ok),
                        )
                    })
                    .collect(),
            },
        })
    }
}

#[quint_run(
    spec = "spec/store.qnt",
    main = "sample",
    step = "replay",
    max_samples = 100,
    max_steps = 40
)]
fn replays_the_model() -> impl Driver {
    Node::default()
}
