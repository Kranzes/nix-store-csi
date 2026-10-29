use std::fs::Permissions;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::{Context, bail, ensure};
use backon::{ConstantBuilder, Retryable};
use bytesize::ByteSize;
use foldhash::HashMap;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::{Stream, StreamExt, TryStreamExt};
use harmonia_file_nar::{NarWriteError, RestoreOptions, parse_nar_stream};
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, Hash};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::bytes::Bytes;
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{Instrument, debug, info, info_span, warn};

use crate::closure::Entry;
use crate::narinfo::Compression;
use crate::store_path::Names;
use crate::transport;

use super::fs::{Scratch, rename_writable};
use super::{Store, write_unsynced};

/// Restarts a NAR download that the transport couldn't resume.
const NAR_RESTARTS: ConstantBuilder = ConstantBuilder::new().with_max_times(2);

/// A fetch under way, which every ensure that needs its path shares.
#[derive(Clone)]
struct Fetch {
    done: Shared<BoxFuture<'static, Result<(), Arc<String>>>>,
    /// The NAR bytes it has read.
    bytes: Arc<AtomicU64>,
}

/// A fetch in [`Store::fetching`].
pub(super) struct Underway {
    fetch: Fetch,
    /// Gives the fetch its slot, until a feed takes it.
    start: Option<oneshot::Sender<OwnedSemaphorePermit>>,
}

impl Store {
    /// Starts fetching the paths in the closure of `roots` that the store
    /// lacks. Resolution and the fetches run in a task of their own, so they
    /// carry on if the caller stops waiting. A closure over the store's limit
    /// fails with [`TooBig`] before any fetch starts. Paths that a
    /// [`Store::prefetch`] holds don't count towards the limit.
    pub fn ensure(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        self.start_ensure(roots, false)
    }

    /// Like [`Store::ensure`] with no limit. Until its hold on the closure
    /// ends, the closure doesn't count towards the limit of other ensures.
    pub fn prefetch(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        self.start_ensure(roots, true)
    }

    fn start_ensure(self: &Arc<Self>, roots: &[StorePath], prefetch: bool) -> Ensuring {
        let (this, roots) = (self.clone(), roots.to_vec());
        let progress = Arc::<OnceLock<_>>::default();
        let resolved = Arc::<AtomicUsize>::default();
        let (set, count) = (progress.clone(), resolved.clone());
        let (tx, rx) = oneshot::channel();
        self.ensures.fetch_add(1, Ordering::Relaxed);
        let ensure = async move {
            let result = this.ensure_closure(&roots, prefetch, &set, &count).await;
            this.ensures.fetch_sub(1, Ordering::Relaxed);
            // From here on, only the caller's hold makes collection spare the
            // closure.
            drop(set);
            if let Err(Err(e)) = tx.send(result) {
                warn!(store_paths = %Names(&roots), "{e:#}; no publish is waiting any more");
            }
        };
        // The closure's lookups log in the caller's span.
        tokio::spawn(ensure.in_current_span());
        Ensuring {
            result: rx,
            resolved,
            progress,
        }
    }

    async fn ensure_closure(
        self: &Arc<Self>,
        roots: &[StorePath],
        prefetch: bool,
        progress: &OnceLock<Fetching>,
        resolved: &AtomicUsize,
    ) -> anyhow::Result<Arc<[StorePath]>> {
        let installs = self.installs.load(Ordering::SeqCst);
        let closure = self.caches.resolve(roots, resolved).await?;
        let total: u64 = closure.values().map(|e| e.info.nar_size()).sum();
        let paths: Arc<[StorePath]> = closure.keys().cloned().collect();
        // Under the guard, a collection either sees the hold or finishes
        // before this checks the limit and the store for the paths.
        let (held, missing) = {
            let _gc = self.gc_guard().await;
            let held = self.hold(&paths, prefetch);
            if let Some(limit) = self.max_closure_size.filter(|_| !prefetch) {
                let counted = {
                    let prefetched = self.prefetched.lock().unwrap();
                    (closure.iter())
                        .filter(|(path, _)| !prefetched.contains_key(*path))
                        .map(|(_, e)| e.info.nar_size())
                        .sum()
                };
                if counted > limit {
                    return Err(TooBig {
                        size: total,
                        counted,
                        limit,
                    }
                    .into());
                }
            }
            let (this, all) = (self.clone(), paths.clone());
            let missing = tokio::task::spawn_blocking(move || this.touch_present(&all)).await??;
            (held, missing)
        };
        let mut started = 0;
        let fetches: Vec<_> = (missing.iter())
            .map(|path| {
                let (fetch, new) = self.fetch_shared(closure[path].clone());
                started += usize::from(new);
                fetch
            })
            .collect();
        if !missing.is_empty() {
            tokio::spawn(self.clone().feed(missing.clone()));
        }
        let fetching: u64 = missing.iter().map(|p| closure[p].info.nar_size()).sum();
        // Fetching can take a while, and each fetch holds the entry it needs.
        drop(closure);
        // Only an ensure that starts a fetch logs. Kubelet's retries and
        // prefetch's minutely ensures join running fetches or find the
        // closure in the store, so they stay quiet.
        let logs = started > 0;
        let present = paths.len() - missing.len();
        let (store_paths, start) = (Names(roots), Instant::now());
        if logs {
            info!(%store_paths, fetching = missing.len(), present, nar_size = fetching, "fetching the closure");
        }
        let _ = progress.set(Fetching {
            present_bytes: total - fetching,
            total_bytes: total,
            present_paths: present,
            start,
            start_bytes: read(&fetches),
            fetches: fetches.clone(),
            _held: held,
        });
        // Returns at the first failure, and the other fetches carry on.
        futures::future::try_join_all(
            fetches
                .into_iter()
                .map(|fetch| async move { fetch.done.await.map_err(|e| anyhow::anyhow!("{e}")) }),
        )
        .await?;
        // Only fetches change the node store's narinfos.
        if self.installs.load(Ordering::SeqCst) != installs {
            let (this, all) = (self.clone(), paths.clone());
            tokio::task::spawn_blocking(move || this.check_references(&all)).await??;
        }
        if logs {
            let elapsed_ms = start.elapsed().as_millis();
            info!(%store_paths, fetched = missing.len(), nar_size = fetching, elapsed_ms, "fetched the closure");
        }
        Ok(paths)
    }

    /// Fails unless `paths` holds every reference of each store object in it.
    /// A store object that another ensure fetched, or that was fetched again
    /// after collection, may come from a narinfo with other references. A
    /// retry resolves the closure from the node store's narinfo, which matches.
    fn check_references(&self, paths: &[StorePath]) -> anyhow::Result<()> {
        let store_dir = self.caches.store_dir();
        for path in paths {
            let info = (self.caches.local_info(path)?)
                .with_context(|| format!("{} has no narinfo", store_dir.display(path)))?;
            let mut references = info.references().iter();
            if let Some(missing) = references.find(|r| paths.binary_search(r).is_err()) {
                bail!(
                    "{} came from a narinfo that references {}, unlike the one the closure was \
                     resolved from; the next try resolves it again",
                    store_dir.display(path),
                    store_dir.display(missing)
                );
            }
        }
        Ok(())
    }

    /// Makes collection spare `paths` until the result drops. A prefetch's
    /// paths also don't count towards the limit until then.
    fn hold(self: &Arc<Self>, paths: &Arc<[StorePath]>, prefetch: bool) -> Held {
        for holds in self.holds(prefetch) {
            let mut holds = holds.lock().unwrap();
            for path in paths.iter() {
                *holds.entry(path.clone()).or_default() += 1;
            }
        }
        Held {
            store: self.clone(),
            paths: paths.clone(),
            prefetch,
        }
    }

    /// The maps that count a hold: `ensuring`, and for a prefetch, `prefetched`.
    fn holds(&self, prefetch: bool) -> impl Iterator<Item = &Mutex<HashMap<StorePath, usize>>> {
        std::iter::once(&self.ensuring).chain(prefetch.then_some(&self.prefetched))
    }

    /// Records a use of the paths the store has, and returns the rest.
    fn touch_present(&self, paths: &[StorePath]) -> anyhow::Result<Vec<StorePath>> {
        let (present, missing): (Vec<_>, Vec<_>) =
            paths.iter().partition(|path| self.present(path));
        self.touch(present)?;
        Ok(missing.into_iter().cloned().collect())
    }

    /// Runs the fetch of `entry`'s path in its own task, so it finishes even
    /// if no publish waits for it any more. Every publish that needs the path
    /// shares the fetch. Also returns whether this call started the fetch.
    /// The fetch ends once the path is in store/, and the path's sync runs
    /// after that.
    fn fetch_shared(self: &Arc<Self>, entry: Arc<Entry>) -> (Fetch, bool) {
        let path = entry.info.path().clone();
        let mut fetching = self.fetching.lock().unwrap();
        let mut started = false;
        let underway = fetching.entry(path.clone()).or_insert_with(|| {
            started = true;
            let (this, bytes) = (self.clone(), Arc::<AtomicU64>::default());
            let read = bytes.clone();
            let (start, slot) = oneshot::channel();
            // Shared by every volume that needs the path, so no publish's span
            // is its parent.
            let span = info_span!(parent: None, "fetch", store_path = %path);
            let task = tokio::spawn(
                async move {
                    let _leave = Leave(this.clone(), path);
                    let _slot = slot.await.expect("a feed starts every fetch");
                    (this.install(entry, &read).await).map_err(|e| Arc::new(format!("{e:#}")))
                }
                .instrument(span),
            );
            let done = async move { task.await.unwrap_or_else(|e| Err(Arc::new(e.to_string()))) };
            Underway {
                fetch: Fetch {
                    done: done.boxed().shared(),
                    bytes,
                },
                start: Some(start),
            }
        });
        (underway.fetch.clone(), started)
    }

    /// Starts the fetches of `paths` that wait for a slot, one slot at a time.
    /// Each ensure has a feed of its own, and slots go to the feeds in the
    /// order they asked, so ensures take turns. A feed can also start a fetch
    /// that another ensure created.
    async fn feed(self: Arc<Self>, paths: Vec<StorePath>) {
        let mut paths = paths.into_iter();
        while let Ok(slot) = self.jobs.clone().acquire_owned().await {
            // Skips the paths that no longer wait, like those that another
            // ensure started meanwhile.
            let take = |path| self.fetching.lock().unwrap().get_mut(&path)?.start.take();
            let Some(start) = paths.find_map(take) else {
                return;
            };
            drop(start.send(slot));
        }
    }

    /// Counts the NAR bytes it reads in `bytes`, and starts a sync for the
    /// path once it's in store/.
    async fn install(
        self: &Arc<Self>,
        mut entry: Arc<Entry>,
        bytes: &Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        let path = entry.info.path().clone();
        // Another fetch may have finished while this one waited.
        if self.present(&path) {
            bytes.store(entry.info.nar_size(), Ordering::Relaxed);
            return Ok(());
        }
        self.flush().await?;
        let mut tried = Vec::new();
        loop {
            // The node store had the narinfo but no longer has the path.
            let Some(cache) = entry.cache else {
                entry = (self.caches.next(&entry, &tried).await)
                    .and_then(|next| next.context("no cache has it"))
                    .with_context(|| {
                        format!("fetching {}", self.caches.store_dir().display(&path))
                    })?;
                continue;
            };
            tried.push(cache);
            let started = Instant::now();
            let fetched = self.fetch(cache, &entry, bytes).await;
            (self.metrics).nar_fetch(
                self.caches.name(cache),
                fetched.is_ok(),
                started,
                entry.info.nar_size(),
            );
            let error = match fetched {
                Ok(()) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.sync(&path).await {
                            warn!("{e:#}");
                        }
                    });
                    return Ok(());
                }
                Err(e) => e.context(format!(
                    "fetching {}",
                    self.caches.store_dir().display(&path)
                )),
            };
            match self.caches.next(&entry, &tried).await {
                Ok(Some(next)) => {
                    warn!("{error:#}; trying the next cache");
                    entry = next;
                }
                Ok(None) => return Err(error),
                Err(e) => {
                    warn!("looking in the other caches: {e:#}; giving up");
                    return Err(error);
                }
            }
        }
    }

    /// Unpacks the store object into tmp/ and moves it into store/.
    /// Its narinfo stays unsynced until [`Store::sync`].
    async fn fetch(
        self: &Arc<Self>,
        cache: usize,
        entry: &Arc<Entry>,
        bytes: &Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        let info = &entry.info;
        let name = self.caches.name(cache);
        let url = transport::display_url(name, info.url());
        debug!(cache = %name, %url, nar_size = info.nar_size(), "fetching NAR");
        let failed_syncs = self.failed_syncs.load(Ordering::SeqCst);
        let base = info.path().to_string();
        let part = (|| async {
            let body = (self.caches)
                .stream(cache, info.url(), info.compression)
                .await?;
            let part = Scratch::new_in(&self.tmp)?;
            let dest = part.path().join(&base);
            let unpacked = self.sandbox.spawn(unpack_nar(
                relay(body),
                info.compression,
                info.nar_hash(),
                info.nar_size(),
                dest,
                bytes.clone(),
            ));
            match unpacked.await.map_err(anyhow::Error::from).flatten() {
                Ok(()) => Ok(part),
                Err(e) => {
                    // Deleting a partly unpacked tree can take a while.
                    self.discard([part]);
                    Err(e)
                }
            }
        })
        .retry(NAR_RESTARTS)
        .when(transport::broke_off)
        .notify(|e, _| warn!(%url, "{e:#}; starting the NAR over"))
        .await
        .with_context(|| format!("fetching {} from {name}", info.url()))?;

        let unpacked = part.path().join(&base);
        let unsynced = self.narinfos.unsynced(info.path());
        let text = info.to_text();
        let dest = self.unpacked(info.path());
        (self.unsynced.lock().unwrap()).insert(info.path().clone(), failed_syncs);
        // An inner error means only setting the mode failed after the move,
        // and the store object is whole without it.
        let moved = tokio::task::spawn_blocking(move || -> anyhow::Result<io::Result<()>> {
            let _part = part;
            // The narinfo goes in before the path, so every path in store/
            // has one. Its modification time is the path's last use, and a
            // fetch counts as a use.
            write_unsynced(&unsynced, &text)?;
            let dir = rename_writable(&unpacked, &dest)
                .with_context(|| format!("moving {} into the node store", unpacked.display()))?;
            Ok(if dir {
                std::fs::set_permissions(&dest, Permissions::from_mode(0o555))
            } else {
                Ok(())
            })
        })
        .await
        .map_err(anyhow::Error::from)
        .flatten();
        self.installs.fetch_add(1, Ordering::SeqCst);
        match moved {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("making the store object read-only: {e}; keeping it"),
            Err(e) => {
                self.unsynced.lock().unwrap().remove(info.path());
                return Err(e);
            }
        }
        if let Some(size) = &self.size {
            size.fetch_add(info.nar_size(), Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Takes a fetch out of [`Store::fetching`] when it ends, even by a panic, so
/// a later publish starts a new one.
struct Leave(Arc<Store>, StorePath);

impl Drop for Leave {
    fn drop(&mut self) {
        self.0.fetching.lock().unwrap().remove(&self.1);
    }
}

/// A closure on its way into the store, from [`Store::ensure`] or
/// [`Store::prefetch`].
pub struct Ensuring {
    result: oneshot::Receiver<anyhow::Result<Arc<[StorePath]>>>,
    /// The narinfos resolution has so far.
    resolved: Arc<AtomicUsize>,
    progress: Arc<OnceLock<Fetching>>,
}

/// The fetches of a closure's paths, set once the closure is resolved.
struct Fetching {
    /// NAR bytes of the paths the store already had.
    present_bytes: u64,
    total_bytes: u64,
    present_paths: usize,
    /// When the fetches started, and the NAR bytes they had read by then. A
    /// fetch that another ensure started may have read some already.
    start: Instant,
    start_bytes: u64,
    /// The fetch of each other path.
    fetches: Vec<Fetch>,
    /// Lasts until both the fetches and the caller are done with it, so
    /// collection spares the closure until the caller has it in a view.
    _held: Held,
}

/// Paths that collection spares, from [`Store::hold`].
struct Held {
    store: Arc<Store>,
    paths: Arc<[StorePath]>,
    prefetch: bool,
}

impl Drop for Held {
    fn drop(&mut self) {
        for holds in self.store.holds(self.prefetch) {
            let mut holds = holds.lock().unwrap();
            for path in self.paths.iter() {
                if let Some(n) = holds.get_mut(path) {
                    *n -= 1;
                    if *n == 0 {
                        holds.remove(path);
                    }
                }
            }
        }
    }
}

impl Ensuring {
    /// Returns the closure's paths once they're all in the store.
    pub async fn done(&mut self) -> anyhow::Result<Arc<[StorePath]>> {
        (&mut self.result).await?
    }

    pub fn progress(&self) -> Progress {
        let Some(f) = self.progress.get() else {
            return Progress::Resolving {
                paths: self.resolved.load(Ordering::Relaxed),
            };
        };
        let fetched = read(&f.fetches);
        let ms = f.start.elapsed().as_millis();
        let rate = (ms >= 1000).then(|| {
            let bytes = u128::from(fetched.saturating_sub(f.start_bytes));
            u64::try_from(bytes * 1000 / ms).unwrap_or(u64::MAX)
        });
        let done = (f.fetches.iter()).filter(|fetch| matches!(fetch.done.peek(), Some(Ok(()))));
        Progress::Fetching {
            bytes: f.present_bytes + fetched,
            total_bytes: f.total_bytes,
            paths: f.present_paths + done.count(),
            total_paths: f.present_paths + f.fetches.len(),
            rate,
        }
    }
}

/// The NAR bytes that `fetches` have read.
fn read(fetches: &[Fetch]) -> u64 {
    let bytes = |f: &Fetch| f.bytes.load(Ordering::Relaxed);
    fetches.iter().map(bytes).sum()
}

/// How far an [`Ensuring`] has got. It reads as the rest of a sentence, like
/// "resolving the closure: 12 store paths so far".
pub enum Progress {
    Resolving {
        paths: usize,
    },
    Fetching {
        /// NAR bytes in the store, counting the paths it already had.
        bytes: u64,
        total_bytes: u64,
        paths: usize,
        total_paths: usize,
        /// NAR bytes per second since the fetches started, or `None` in the
        /// first second.
        rate: Option<u64>,
    },
}

impl std::fmt::Display for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Resolving { paths } => {
                let s = if paths == 1 { "" } else { "s" };
                write!(f, "resolving the closure: {paths} store path{s} so far")
            }
            Self::Fetching {
                bytes,
                total_bytes,
                paths,
                total_paths,
                rate,
            } => {
                let bytes = fraction(bytes, total_bytes);
                write!(
                    f,
                    "fetching the closure: {paths}/{total_paths} store paths ({bytes})"
                )?;
                match rate {
                    Some(rate) => write!(f, ", {}/s", ByteSize(rate)),
                    None => Ok(()),
                }
            }
        }
    }
}

/// Formats `done` and `total` in the unit that suits `total`, like Nix's
/// `45.2/120.3 MiB`.
fn fraction(done: u64, total: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let exp = (1..UNITS.len())
        .rev()
        .find(|&i| total >> (10 * i) > 0)
        .unwrap_or(0);
    if exp == 0 {
        return format!("{done}/{total} B");
    }
    // Rounds to the nearest tenth, like ByteSize.
    let half = 1u128 << (10 * exp - 1);
    let tenths = |n: u64| (u128::from(n) * 10 + half) >> (10 * exp);
    let (d, t) = (tenths(done), tenths(total));
    format!("{}.{}/{}.{} {}", d / 10, d % 10, t / 10, t % 10, UNITS[exp])
}

/// A closure whose NARs, other than those that a prefetch holds, add up to
/// more than the store's limit.
#[derive(Debug)]
pub struct TooBig {
    /// The NAR size of the whole closure.
    pub size: u64,
    /// The part of `size` that counts towards the limit.
    pub counted: u64,
    pub limit: u64,
}

impl std::fmt::Display for TooBig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "the closure is {}", ByteSize(self.size))?;
        if self.counted < self.size {
            let counted = ByteSize(self.counted);
            write!(f, ", or {counted} without prefetched store paths")?;
        }
        write!(f, ", over the limit of {}", ByteSize(self.limit))
    }
}

impl std::error::Error for TooBig {}

/// Unpacks the NAR at `dest` as it streams in, with Nix's canonical modes and
/// modification time, and fails unless its size and hash match. The caller
/// moves the tree into the store only after that.
/// `bytes` counts the NAR bytes read so far.
async fn unpack_nar(
    body: impl AsyncBufRead + Send + Unpin + 'static,
    compression: Compression,
    nar_hash: Hash,
    nar_size: u64,
    dest: PathBuf,
    bytes: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let mut size = 0;
    // Reading one byte past the expected size catches a NAR that's too long.
    let (chunks, hashed) = decode_apart(compression.decoder(body).take(nar_size + 1));
    let mut nar = chunks.inspect_ok(|chunk| {
        size += chunk.len() as u64;
        bytes.store(size, Ordering::Relaxed);
    });
    // `restore` rejects entries that would leave `dest`, so a NAR that isn't
    // verified yet can't write elsewhere.
    let mut parse_error = None;
    let events = parse_nar_stream(&mut nar).map(|event| {
        event.map_err(|err| {
            // `restore` only takes its own error type, so the parser's error
            // goes in `parse_error`.
            parse_error = Some(err);
            NarWriteError::create_file_error(dest.clone(), io::ErrorKind::InvalidData.into())
        })
    });
    let restored = RestoreOptions::new()
        .canonical(true)
        .restore(events, &dest)
        .await;
    if let Some(err) = parse_error {
        return Err(anyhow::Error::from(err).context("parsing the NAR"));
    }
    restored?;
    // Bytes after the end of the archive count towards the size too.
    while nar.try_next().await?.is_some() {}
    ensure!(size <= nar_size, "NAR is longer than {nar_size} bytes");
    ensure!(
        size == nar_size,
        "NAR has {size} bytes, expected {nar_size}"
    );
    let hash = hashed.await?;
    ensure!(
        hash == nar_hash,
        "NAR hash mismatch: got {hash:x}, expected {nar_hash:x}"
    );
    Ok(())
}

/// Reads `body` in a task on the caller's runtime. A sandboxed thread can't
/// read it, since the transport may read credentials and resume the
/// download.
fn relay(body: impl AsyncRead + Send + Unpin + 'static) -> impl AsyncBufRead + Send + Unpin {
    StreamReader::new(pump(body, None))
}

/// Reads `decoded` in one task and hashes it in another, so decompression,
/// hashing and parsing each get a core. Returns the data's chunks and a task
/// that yields its SHA-256. The hash covers all the data once the chunks have
/// run out. On a 4-vCPU host, tokio workers fetched cold about 15% faster than
/// blocking threads.
fn decode_apart(
    decoded: impl AsyncRead + Send + 'static,
) -> (
    impl Stream<Item = io::Result<Bytes>> + Send + Unpin,
    tokio::task::JoinHandle<Hash>,
) {
    let (to_hash, mut hashing) = tokio::sync::mpsc::channel::<Bytes>(4);
    let hashed = tokio::spawn(async move {
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        while let Some(chunk) = hashing.recv().await {
            hasher.update(&chunk);
        }
        hasher.finish()
    });
    (pump(decoded, Some(to_hash)), hashed)
}

/// Reads `src` in a task, and sends each chunk to the returned stream and to
/// `tee`.
fn pump(
    src: impl AsyncRead + Send + 'static,
    tee: Option<tokio::sync::mpsc::Sender<Bytes>>,
) -> impl Stream<Item = io::Result<Bytes>> + Send + Unpin {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let mut chunks = std::pin::pin!(ReaderStream::with_capacity(src, transport::NAR_CHUNK));
        while let Some(chunk) = chunks.next().await {
            if let (Some(tee), Ok(chunk)) = (&tee, &chunk)
                && tee.send(chunk.clone()).await.is_err()
            {
                break;
            }
            if tx.send(chunk).await.is_err() {
                break;
            }
        }
    });
    ReceiverStream::new(rx)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    use harmonia_file_nar::archive::write_nar;
    use harmonia_file_nar::{NarByteStream, NarEvent};

    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{
        encode, file_store, fixture_narinfo, fixture_path, fixture_root, fixtures, without_lines,
        write_narinfos,
    };
    use crate::store::test_utils::*;

    fn sha256(data: &[u8]) -> Hash {
        Algorithm::SHA256.digest(data)
    }

    /// A NAR of one regular file.
    fn file_nar(contents: &[u8]) -> Vec<u8> {
        let file = NarEvent::File {
            name: Bytes::new(),
            executable: false,
            size: contents.len() as u64,
            reader: std::io::Cursor::new(contents.to_vec().into()),
        };
        write_nar([&file]).into()
    }

    /// Checks that the tree at `path` serializes to the NAR with `nar_hash`,
    /// and that every entry has Nix's canonical metadata: modification time
    /// 1, and mode 0o555 for directories and executables and 0o444 for other
    /// files. The modes need the umask 022, which `main` sets.
    async fn assert_canonical(path: &Path, nar_hash: Hash) {
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        let mut nar = NarByteStream::new(path.to_owned());
        while let Some(chunk) = nar.next().await {
            hasher.update(chunk.unwrap());
        }
        assert_eq!(hasher.finish(), nar_hash, "{}", path.display());
        for entry in walkdir::WalkDir::new(path).follow_root_links(false) {
            let entry = entry.unwrap();
            let (meta, at) = (entry.metadata().unwrap(), entry.path().display());
            assert_eq!((meta.mtime(), meta.mtime_nsec()), (1, 0), "{at}");
            let mode = meta.mode() & 0o7777;
            if meta.is_dir() {
                assert_eq!(mode, 0o555, "{at}");
            } else if meta.is_file() {
                assert!(matches!(mode, 0o444 | 0o555), "{at} has mode {mode:o}");
            }
        }
    }

    async fn unpack(
        compressed: &[u8],
        compression: Compression,
        hash: &Hash,
        size: u64,
    ) -> anyhow::Result<(Scratch, PathBuf)> {
        let tmp = scratch();
        let dest = tmp.path().join("out");
        let body = std::io::Cursor::new(compressed.to_vec());
        let bytes = Arc::default();
        unpack_nar(body, compression, *hash, size, dest.clone(), bytes).await?;
        Ok((tmp, dest))
    }

    #[tokio::test]
    async fn unpacks_other_compressions() {
        let contents = b"not really a program".repeat(100);
        let plain = file_nar(&contents);
        for compression in [
            Compression::Xz,
            Compression::Bzip2,
            Compression::Gzip,
            Compression::Brotli,
        ] {
            let compressed = encode(compression, &plain).await;
            let (_tmp, dest) = unpack(
                &compressed,
                compression,
                &sha256(&plain),
                plain.len() as u64,
            )
            .await
            .unwrap();
            assert_eq!(std::fs::read(dest).unwrap(), contents, "{compression:?}");
        }
    }

    #[tokio::test]
    async fn rejects_bad_nars() {
        let plain = file_nar(b"0123456789");
        let hash = sha256(&plain);
        let size = plain.len() as u64;
        for (what, hash, size) in [
            ("wrong hash", sha256(b"other"), size),
            ("too short", hash, size + 1),
            ("too long", hash, size - 1),
        ] {
            assert!(
                unpack(&plain, Compression::None, &hash, size)
                    .await
                    .is_err(),
                "{what}"
            );
        }

        // The parser's error comes back, not the one `restore` got instead.
        let garbage = b"not a NAR".repeat(8);
        let size = garbage.len() as u64;
        let Err(err) = unpack(&garbage, Compression::None, &sha256(&garbage), size).await else {
            panic!("unpacked garbage");
        };
        assert!(format!("{err:#}").contains("parsing the NAR"), "{err:#}");

        // A byte after the archive counts even in a chunk the parser never
        // reads.
        let mut nar = file_nar(&vec![0; transport::NAR_CHUNK - file_nar(b"").len()]);
        assert_eq!(nar.len(), transport::NAR_CHUNK);
        nar.push(0);
        let size = transport::NAR_CHUNK as u64;
        let Err(err) = unpack(&nar, Compression::None, &sha256(&nar), size).await else {
            panic!("unpacked a NAR with a byte after it");
        };
        assert!(format!("{err:#}").contains("longer"), "{err:#}");
    }

    #[test]
    fn formats_fractions() {
        assert_eq!(fraction(0, 512), "0/512 B");
        assert_eq!(fraction(1536, 3 << 20), "0.0/3.0 MiB");
        assert_eq!(fraction(5 << 29, 3 << 30), "2.5/3.0 GiB");
        assert_eq!(fraction((4 << 30) - (1 << 25), 4 << 30), "4.0/4.0 GiB");
    }

    #[tokio::test]
    async fn unpacks_closure() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o022));
        let tmp = scratch();
        // A copy of the cache, since the test deletes it below.
        let cache = tmp.path().join("cache");
        copy_cache(&dir.join("cache-zstd"), &cache);
        let state = tmp.path().join("state");

        let store = Arc::new(Store::new(config(&dir, &cache, &state)).unwrap());
        let err = Store::new(config(&dir, &cache, &state)).err().unwrap();
        assert!(
            format!("{err:#}").contains("another nix-store-csi"),
            "{err:#}"
        );
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        let paths = ensuring.done().await.unwrap();
        assert_eq!(paths.len(), 5);
        let Progress::Fetching {
            bytes,
            total_bytes,
            paths: done,
            total_paths,
            ..
        } = ensuring.progress()
        else {
            panic!("{}", ensuring.progress());
        };
        assert!(
            bytes == total_bytes && total_bytes > 0,
            "{bytes} of {total_bytes}"
        );
        assert_eq!((done, total_paths), (5, 5));
        drop(ensuring);
        assert_eq!(count(store.dir()), 5);
        assert_eq!(count(state.join("narinfo")), 5);
        assert_eq!(count(state.join("tmp")), 0);
        for path in paths.iter() {
            let info = fixture_narinfo(&dir, "cache-zstd", path);
            assert_canonical(&store.unpacked(path), info.nar_hash()).await;
        }

        // A new instance needs no cache for the paths it has.
        std::fs::remove_dir_all(&cache).unwrap();
        let store = reopen(store, || config(&dir, &cache, &state)).await;
        let mut info = fixture_narinfo(&dir, "cache-zstd", &hello);
        let _ = info.check_signatures(&[]);
        let stored = std::fs::read(store.narinfos.synced(&hello)).unwrap();
        assert_eq!(stored, info.to_text());
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }

    /// A closure over the limit fails before any fetch starts. The paths the
    /// store already has count towards it, unless a prefetch holds them.
    #[tokio::test]
    async fn limits_closure_size() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let limited = |limit| {
            let mut config = zstd_config(&dir, tmp.path());
            config.max_closure_size = Some(limit);
            config
        };
        let ensure = |store: &Arc<Store>| store.ensure(std::slice::from_ref(&hello));

        let store = Arc::new(Store::new(limited(1)).unwrap());
        let err = ensure(&store).done().await.unwrap_err();
        let &TooBig {
            size,
            counted,
            limit,
        } = err.downcast_ref().unwrap();
        assert_eq!((counted, limit), (size, 1));
        settled(&store).await;
        assert!(store.ensuring.lock().unwrap().is_empty());
        assert_eq!(count(store.dir()), 0);
        assert_eq!(count(tmp.path().join("narinfo")), 0);

        // A prefetch's closure stops counting once it is resolved, before its
        // fetches end, and counts again once its hold ends.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let glibc = store.prefetch(&[fixture_path(&dir, "glibc")]);
        resolved(&glibc).await;
        let err = ensure(&store).done().await.unwrap_err();
        let too_big: &TooBig = err.downcast_ref().unwrap();
        let info = fixture_narinfo(&dir, "cache-zstd", &hello);
        assert_eq!((too_big.size, too_big.counted), (size, info.nar_size()));
        assert!(format!("{err:#}").contains("without prefetched store paths"));
        drop((jobs, glibc));
        settled(&store).await;
        assert!(store.prefetched.lock().unwrap().is_empty());

        let store = reopen(store, || limited(size)).await;
        assert_eq!(ensure(&store).done().await.unwrap().len(), 5);

        let store = reopen(store, || limited(size - 1)).await;
        let err = ensure(&store).done().await.unwrap_err();
        assert!(err.is::<TooBig>(), "{err:#}");
    }

    #[tokio::test]
    async fn counts_fetches() {
        let tmp = scratch();
        let Some((store, _, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let registry = crate::metrics::registry(&store);
        let cache = store.caches.name(0);
        let metric = |name: &str| sample(&registry, &format!("nix_store_csi_{name}"));
        let fetched = f64::from(u32::try_from(paths.len()).unwrap());
        let count = format!("nar_fetch_duration_seconds_count{{cache=\"{cache}\",result=\"ok\"}}");
        assert_eq!(metric(&count), Some(fetched));
        let failed = count.replace("\"ok\"", "\"error\"");
        assert_eq!(metric(&failed), Some(0.0));
        let published = "publish_duration_seconds_count{grpc_code=\"OK\"}";
        assert_eq!(metric(published), Some(0.0));
        let bytes = format!("nar_fetched_bytes_total{{cache=\"{cache}\"}}");
        assert!(metric(&bytes).unwrap() > 0.0);
        assert_eq!(metric("store_paths"), Some(fetched));
        let size: u64 = (paths.iter())
            .map(|path| store.caches.local_info(path).unwrap().unwrap().nar_size())
            .sum();
        #[allow(clippy::cast_precision_loss)]
        let size = size as f64;
        assert_eq!(metric("store_size_bytes"), Some(size));
        assert_eq!(metric("closures_in_flight"), Some(0.0));
        assert_eq!(metric("unsynced_paths"), Some(0.0));
        assert_eq!(metric("nar_fetches_in_flight"), Some(0.0));
        assert_eq!(metric("failed_syncs_total"), Some(0.0));
        assert!(metric("free_bytes").unwrap() > 0.0);
        // What the test config keeps free.
        assert_eq!(metric("ensure_free_bytes"), Some(f64::from(1u32 << 30)));
        // Both inode gauges are there on a filesystem that limits inodes.
        assert_eq!(
            metric("free_inodes").is_some(),
            metric("ensure_free_inodes").is_some()
        );
        assert_eq!(
            metric(&format!("cache_paused{{cache=\"{cache}\"}}")),
            Some(0.0)
        );
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            metric(&format!("build_info{{version=\"{version}\"}}")),
            Some(1.0)
        );
    }

    /// Ensures whose closures share paths share their fetches.
    #[tokio::test]
    async fn shares_fetches() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let glibc = fixture_path(&dir, "glibc");
        let store = Arc::new(Store::new(zstd_config(&dir, tmp.path())).unwrap());
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let mut a = store.ensure(std::slice::from_ref(&hello));
        let mut b = store.ensure(std::slice::from_ref(&glibc));
        resolved(&a).await;
        resolved(&b).await;
        let fetches = |e: &Ensuring| e.progress.get().unwrap().fetches.clone();
        let (a_fetches, b_fetches) = (fetches(&a), fetches(&b));
        assert!(!b_fetches.is_empty() && b_fetches.len() < a_fetches.len());
        for fetch in &b_fetches {
            assert!((a_fetches.iter()).any(|a| Arc::ptr_eq(&a.bytes, &fetch.bytes)));
        }
        assert_eq!(store.fetching.lock().unwrap().len(), a_fetches.len());
        // No fetch can start while the test holds the jobs.
        let Progress::Fetching {
            paths, total_paths, ..
        } = a.progress()
        else {
            panic!("{}", a.progress());
        };
        assert_eq!((paths, total_paths), (0, a_fetches.len()));

        drop(jobs);
        assert_eq!(a.done().await.unwrap().len(), a_fetches.len());
        assert_eq!(b.done().await.unwrap().len(), b_fetches.len());
        // Each fetch leaves the map when it ends, so a later failure starts
        // a new one.
        assert!(store.fetching.lock().unwrap().is_empty());
    }

    /// Ensures take turns at the slots, so a closure doesn't wait for every
    /// fetch of one that came before it.
    #[tokio::test]
    async fn takes_turns() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let config = crate::store::Config {
            jobs: 1,
            ..zstd_config(&dir, tmp.path())
        };
        let store = Arc::new(Store::new(config).unwrap());
        let hello = fixture_root(&dir, "hello.path");
        // With glibc's closure present, warm's ensure fetches warm and
        // busybox, and hello's fetches hello.
        (store.ensure(&[fixture_path(&dir, "glibc")]).done().await).unwrap();
        let glibc = count(store.dir());
        let slot = store.jobs.acquire().await.unwrap();
        let mut a = store.ensure(&[fixture_path(&dir, "warm")]);
        resolved(&a).await;
        let mut b = store.ensure(std::slice::from_ref(&hello));
        resolved(&b).await;
        // The test queues for a slot behind both feeds.
        let mut next = std::pin::pin!(store.jobs.acquire());
        assert!(next.as_mut().now_or_never().is_none());
        drop(slot);
        let slot = next.await.unwrap();
        // Each feed had one turn.
        assert!(store.present(&hello));
        assert_eq!(count(store.dir()), glibc + 2);

        drop(slot);
        a.done().await.unwrap();
        b.done().await.unwrap();
    }

    /// An ensure carries on after its caller stops waiting. When kubelet gives
    /// up on a publish, the retry joins its fetches.
    #[tokio::test]
    async fn carries_on_without_the_caller() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let store = Arc::new(Store::new(zstd_config(&dir, tmp.path())).unwrap());
        // The caller leaves before the closure is resolved. A big closure can
        // take longer to resolve than kubelet waits.
        drop(store.ensure(&[fixture_path(&dir, "glibc")]));
        settled(&store).await;
        let glibc = count(store.dir());
        assert!(glibc > 0);

        // The caller leaves while the fetches wait for jobs.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let fetches = |e: &Ensuring| e.progress.get().unwrap().fetches.clone();
        let first = store.ensure(std::slice::from_ref(&hello));
        resolved(&first).await;
        let started = fetches(&first);
        drop(first);
        let retry = store.ensure(std::slice::from_ref(&hello));
        resolved(&retry).await;
        for fetch in fetches(&retry) {
            assert!(started.iter().any(|s| Arc::ptr_eq(&s.bytes, &fetch.bytes)));
        }

        // With no caller left, the fetches and their syncs still finish.
        drop(retry);
        drop(jobs);
        settled(&store).await;
        assert_eq!(count(store.dir()), glibc + started.len());
        let synced = |name: &String| name.ends_with(".narinfo");
        let narinfos = file_names(tmp.path().join("narinfo"));
        assert_eq!(narinfos.len(), glibc + started.len());
        assert!(narinfos.iter().all(synced));
    }

    /// A cache that compresses NARs as it serves them, like harmonia, gives
    /// the same store objects.
    #[tokio::test]
    async fn fetches_from_a_compressing_cache() {
        use crate::transport::tests::{Mode, server};
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let cache = Box::leak(dir.join("cache-none").into_boxed_path());
        let mut config = config(&dir, cache, tmp.path());
        let coding = "zstd";
        config.stores = vec![server(Mode::Cache { dir: cache, coding }).await];
        let store = Arc::new(Store::new(config).unwrap());
        let hello = fixture_root(&dir, "hello.path");
        let paths = store.ensure(&[hello]).done().await.unwrap();
        assert_eq!(count(store.dir()), paths.len());
    }

    /// A NAR that doesn't match its `NarHash` fails over to the next cache.
    #[tokio::test]
    async fn falls_back_after_a_corrupt_nar() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let corrupt = tmp.path().join("corrupt");
        copy_cache(&dir.join("cache-none"), &corrupt);
        let info = fixture_narinfo(&dir, "cache-none", &hello);
        let nar = corrupt.join(info.url());
        let mut bytes = std::fs::read(&nar).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        std::fs::write(&nar, bytes).unwrap();

        let state = tmp.path().join("state");
        let mut config = config(&dir, &corrupt, &state);
        config.stores[0].push_str("?priority=10");
        config.stores.push(file_store(&dir.join("cache-zstd")));
        let store = Arc::new(Store::new(config).unwrap());
        let paths = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap();
        settled(&store).await;
        assert_eq!(count(store.dir()), paths.len());
        let narinfo = std::fs::read_to_string(store.narinfos.synced(&hello)).unwrap();
        assert!(narinfo.contains("Compression: zstd"), "{narinfo}");
        assert_eq!(count(state.join("tmp")), 0);
    }

    /// An ensure that joins another ensure's fetch, started from a narinfo
    /// with different references, fails rather than leave out what the store
    /// object needs.
    #[tokio::test]
    async fn checks_references_of_shared_fetches() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        // The better cache has a narinfo for hello without references.
        let no_refs = tmp.path().join("no-refs");
        copy_cache(&dir.join("cache-zstd"), &no_refs);
        write_narinfos(&dir, "cache-zstd", &no_refs, &[&hello], |t| {
            without_lines(&t, "References:")
        });
        let mut config = config(&dir, &no_refs, &tmp.path().join("state"));
        config.trusted_keys = None;
        config.stores[0].push_str("?priority=10");
        let zstd = file_store(&dir.join("cache-zstd"));
        config.stores.push(format!("{zstd}?priority=20"));
        let store = Arc::new(Store::new(config).unwrap());

        // Another ensure's fetch gets hello from the narinfo with references.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let info = fixture_narinfo(&dir, "cache-zstd", &hello);
        let (fetch, _) = store.fetch_shared(Arc::new(Entry {
            cache: Some(1),
            info,
        }));
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        resolved(&ensuring).await;
        drop(jobs);
        fetch.done.await.unwrap();
        let err = ensuring.done().await.unwrap_err();
        assert!(format!("{err:#}").contains("the next try"), "{err:#}");
        // The next try finds hello in the node store, with its references.
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }
}
