use std::collections::{BTreeMap, VecDeque};
use std::fs::Permissions;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use anyhow::{Context, bail, ensure};
use backon::{ConstantBuilder, Retryable};
use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt, Shared};
use harmonia_file_nar::{NarWriteError, parse_nar, restore};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::{Algorithm, Hash};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{InspectReader, ReaderStream, StreamReader};
use tracing::{info, warn};

use crate::closure::Entry;
use crate::narinfo::Compression;
use crate::transport;

use super::fs::{Scratch, rename_tree, set_canonical_time};
use super::{Store, write_unsynced};

/// Restarts a NAR download at most twice, for three attempts in all.
const NAR_RESTARTS: ConstantBuilder = ConstantBuilder::new().with_max_times(2);

pub(super) type Fetch = Shared<BoxFuture<'static, Result<(), Arc<String>>>>;

impl Store {
    /// Starts fetching the paths in the closure of `roots` that the store
    /// lacks. Resolution and the fetches run in a task of their own, so they
    /// carry on if the caller stops waiting.
    pub fn ensure(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        let (this, roots) = (self.clone(), roots.to_vec());
        let progress = Arc::<OnceLock<_>>::default();
        let set = progress.clone();
        let (tx, rx) = oneshot::channel();
        self.ensures.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let result = this.ensure_closure(&roots, &set).await;
            this.ensures.fetch_sub(1, Ordering::Relaxed);
            // From here on, only the caller's hold makes collection spare the
            // closure.
            drop(set);
            if let Err(Err(e)) = tx.send(result) {
                warn!(store_paths = ?roots, "no publish is waiting any more: {e:#}");
            }
        });
        Ensuring {
            result: rx,
            progress,
        }
    }

    async fn ensure_closure(
        self: &Arc<Self>,
        roots: &[StorePath],
        progress: &OnceLock<Progress>,
    ) -> anyhow::Result<Arc<[StorePath]>> {
        let watch = self.watch_installs();
        let closure = self.caches.resolve(roots).await?;
        let total: u64 = closure.values().map(|e| e.info.nar_size()).sum();
        let paths: Arc<[StorePath]> = closure.keys().cloned().collect();
        // Under the guard, a collection either sees the hold or finishes
        // before this checks the store for the paths.
        let (held, missing) = {
            let _gc = self.gc_guard().await;
            let held = self.hold(&paths);
            let (this, all) = (self.clone(), paths.clone());
            let missing = tokio::task::spawn_blocking(move || this.touch_present(&all)).await??;
            (held, missing)
        };
        let fetches: Vec<_> = (missing.iter())
            .map(|path| self.fetch_shared(closure[path].clone()))
            .collect();
        let fetching: u64 = missing.iter().map(|p| closure[p].info.nar_size()).sum();
        // Fetching can take a while, and each fetch holds the entry it needs.
        drop(closure);
        let _ = progress.set(Progress {
            present: total - fetching,
            total,
            fetches: fetches.iter().map(|(_, bytes)| bytes.clone()).collect(),
            _held: held,
        });
        // Returns at the first failure, and the other fetches carry on.
        futures::future::try_join_all(
            fetches
                .into_iter()
                .map(|(fetch, _)| async move { fetch.await.map_err(|e| anyhow::anyhow!("{e}")) }),
        )
        .await?;
        let installs = self.installs.lock().unwrap();
        check_installed(self.caches.store_dir(), &paths, installs.since(watch.start))?;
        Ok(paths)
    }

    /// Keeps the store paths that fetches install, with their references, in
    /// [`Store::installs`] until the result drops. A fetch records its path
    /// after it writes the narinfo and before it moves the store object in.
    /// So a store object that resolution didn't find in the node store, but
    /// that is in store/ by the end, is in the log after the result's start.
    fn watch_installs(self: &Arc<Self>) -> Watch {
        let mut installs = self.installs.lock().unwrap();
        let start = installs.end();
        *installs.starts.entry(start).or_default() += 1;
        Watch {
            store: self.clone(),
            start,
        }
    }

    /// Makes collection spare `paths` until the result drops.
    fn hold(self: &Arc<Self>, paths: &Arc<[StorePath]>) -> Held {
        let mut ensuring = self.ensuring.lock().unwrap();
        for path in paths.iter() {
            *ensuring.entry(path.clone()).or_default() += 1;
        }
        Held {
            store: self.clone(),
            paths: paths.clone(),
        }
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
    /// shares the fetch. Also returns the NAR bytes the fetch has read so far.
    /// The fetch ends once the path is in store/, and the path's sync runs
    /// after that.
    fn fetch_shared(self: &Arc<Self>, entry: Arc<Entry>) -> (Fetch, Arc<AtomicU64>) {
        let path = entry.info.path().clone();
        let mut fetching = self.fetching.lock().unwrap();
        let (fetch, bytes) = fetching.entry(path.clone()).or_insert_with(|| {
            let (this, bytes) = (self.clone(), Arc::<AtomicU64>::default());
            let read = bytes.clone();
            let task = tokio::spawn(async move {
                let leave = Leave(this.clone(), path.clone());
                let result = this.install(entry, &read).await;
                drop(leave);
                if let Ok(true) = result {
                    tokio::spawn(async move {
                        if let Err(e) = this.sync(&path).await {
                            warn!("{e:#}");
                        }
                    });
                }
                result.map(drop).map_err(|e| Arc::new(format!("{e:#}")))
            });
            let fetch = async move { task.await.unwrap_or_else(|e| Err(Arc::new(e.to_string()))) };
            (fetch.boxed().shared(), bytes)
        });
        (fetch.clone(), bytes.clone())
    }

    /// Counts the NAR bytes it reads in `bytes`, and returns whether it
    /// fetched the path.
    async fn install(
        self: &Arc<Self>,
        mut entry: Arc<Entry>,
        bytes: &AtomicU64,
    ) -> anyhow::Result<bool> {
        let path = entry.info.path().clone();
        let _permit = self.jobs.acquire().await?;
        // Another fetch may have finished while this one waited.
        if self.present(&path) {
            bytes.store(entry.info.nar_size(), Ordering::Relaxed);
            return Ok(false);
        }
        let mut tried = Vec::new();
        loop {
            // The node store had the narinfo but no longer has the path.
            let Some(cache) = entry.cache else {
                entry = (self.caches.next(&entry, &tried).await)
                    .and_then(|next| next.context("no cache has it"))
                    .with_context(|| format!("fetching {path}"))?;
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
                Ok(()) => return Ok(true),
                Err(e) => e.context(format!("fetching {path}")),
            };
            match self.caches.next(&entry, &tried).await {
                Ok(Some(next)) => {
                    warn!("{error:#}; trying the next cache");
                    entry = next;
                }
                Ok(None) => return Err(error),
                Err(e) => {
                    warn!("looking for {path} in the other caches: {e:#}");
                    return Err(error);
                }
            }
        }
    }

    /// Unpacks the store object into tmp/, seals it, and moves it into store/.
    /// Its narinfo stays unsynced until [`Store::sync`].
    async fn fetch(
        self: &Arc<Self>,
        cache: usize,
        entry: &Arc<Entry>,
        bytes: &AtomicU64,
    ) -> anyhow::Result<()> {
        let info = &entry.info;
        let name = self.caches.name(cache);
        info!(cache = %name, url = %info.url, size = info.nar_size(), "fetching NAR");
        let failed_syncs = self.failed_syncs.load(Ordering::SeqCst);
        let base = info.path().to_string();
        let part = (|| async {
            let body = self.caches.stream(cache, &info.url).await?;
            let part = Scratch::new_in(&self.tmp)?;
            let dest = part.path().join(&base);
            match unpack_nar(
                body,
                info.compression,
                &info.nar_hash(),
                info.nar_size(),
                &dest,
                bytes,
            )
            .await
            {
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
        .notify(|e, _| warn!(url = %info.url, "fetching NAR: {e:#}; starting over"))
        .await
        .with_context(|| format!("fetching {} from {name}", info.url))?;

        let unpacked = part.path().join(&base);
        let unsynced = self.caches.local_unsynced(info.path());
        let text = info.to_text();
        let dest = self.unpacked(info.path());
        (self.unsynced.lock().unwrap()).insert(info.path().clone(), failed_syncs);
        let (this, entry) = (self.clone(), entry.clone());
        let moved = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _part = part;
            seal(&unpacked).with_context(|| format!("sealing {}", unpacked.display()))?;
            // The narinfo goes in before the path, so every path in store/
            // has one. Its modification time is the path's last use, and a
            // fetch counts as a use.
            write_unsynced(&unsynced, &text)?;
            this.installs.lock().unwrap().record(&entry);
            rename_tree(&unpacked, &dest)
                .with_context(|| format!("moving {} into the node store", unpacked.display()))
        })
        .await
        .map_err(anyhow::Error::from)
        .flatten();
        match moved {
            // Setting the path's mode is the only step that can fail after
            // the move, and the path is whole without it.
            Err(e) if self.present(info.path()) => warn!("{e:#}"),
            Err(e) => {
                self.unsynced.lock().unwrap().remove(info.path());
                return Err(e);
            }
            Ok(()) => {}
        }
        self.size.fetch_add(info.nar_size(), Ordering::Relaxed);
        Ok(())
    }
}

/// The store paths that fetches installed while ensures ran, each with its
/// references, oldest first. Each ensure reads the log from where it began.
#[derive(Default)]
pub(super) struct Installs {
    /// How many entries left the front of the log.
    dropped: u64,
    log: VecDeque<Arc<Entry>>,
    /// Where each running ensure began in the log, with how many began there.
    starts: BTreeMap<u64, usize>,
}

impl Installs {
    fn record(&mut self, entry: &Arc<Entry>) {
        if !self.starts.is_empty() {
            self.log.push_back(entry.clone());
        }
    }

    fn end(&self) -> u64 {
        self.dropped + self.log.len() as u64
    }

    fn since(&self, start: u64) -> impl Iterator<Item = &Arc<Entry>> {
        let skip = usize::try_from(start - self.dropped).expect("the log is in memory");
        self.log.iter().skip(skip)
    }
}

/// An ensure's start in [`Installs`], from [`Store::watch_installs`].
struct Watch {
    store: Arc<Store>,
    start: u64,
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut installs = self.store.installs.lock().unwrap();
        if let Some(n) = installs.starts.get_mut(&self.start) {
            *n -= 1;
            if *n == 0 {
                installs.starts.remove(&self.start);
            }
        }
        // No ensure reads what came before the oldest start.
        let keep = (installs.starts.first_key_value()).map_or(installs.end(), |(&start, _)| start);
        let gone = usize::try_from(keep - installs.dropped).expect("the log is in memory");
        installs.log.drain(..gone);
        installs.dropped = keep;
    }
}

/// Fails unless `paths` holds every reference of each store object in it that
/// a fetch `installed`. Two caches can have narinfos with different references
/// for one store path. A fetch that another ensure started installs the store
/// object from that ensure's narinfo. So does one that fetches it again after
/// collection deleted it during resolution. A retry resolves the closure from
/// the node store's narinfo, which matches the store object.
fn check_installed<'a>(
    store_dir: &StoreDir,
    paths: &[StorePath],
    installed: impl IntoIterator<Item = &'a Arc<Entry>>,
) -> anyhow::Result<()> {
    for entry in installed {
        let path = entry.info.path();
        if paths.binary_search(path).is_err() {
            continue;
        }
        let mut references = entry.info.references().iter();
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

/// Takes a fetch out of [`Store::fetching`] when it ends, even by a panic, so
/// a later publish starts a new one.
struct Leave(Arc<Store>, StorePath);

impl Drop for Leave {
    fn drop(&mut self) {
        self.0.fetching.lock().unwrap().remove(&self.1);
    }
}

/// A closure on its way into the store, from [`Store::ensure`].
pub struct Ensuring {
    result: oneshot::Receiver<anyhow::Result<Arc<[StorePath]>>>,
    progress: Arc<OnceLock<Progress>>,
}

/// Counts of a closure's NAR bytes, set once the closure is resolved.
struct Progress {
    /// NAR bytes of the paths the store already had.
    present: u64,
    total: u64,
    /// The NAR bytes that each fetch of the other paths has read.
    fetches: Vec<Arc<AtomicU64>>,
    /// Lasts until both the fetches and the caller are done with it, so
    /// collection spares the closure until the caller has it in a view.
    _held: Held,
}

/// Paths that collection spares, from [`Store::hold`].
struct Held {
    store: Arc<Store>,
    paths: Arc<[StorePath]>,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut ensuring = self.store.ensuring.lock().unwrap();
        for path in self.paths.iter() {
            if let Some(n) = ensuring.get_mut(path) {
                *n -= 1;
                if *n == 0 {
                    ensuring.remove(path);
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

    /// Returns the NAR bytes of the closure in the store so far, and the
    /// total, once the closure is resolved.
    pub fn progress(&self) -> Option<(u64, u64)> {
        let progress = self.progress.get()?;
        let fetched: u64 = (progress.fetches.iter())
            .map(|bytes| bytes.load(Ordering::Relaxed))
            .sum();
        Some((progress.present + fetched, progress.total))
    }
}

/// Gives the tree Nix's canonical modes and modification time.
fn seal(path: &Path) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.is_symlink() {
        return set_canonical_time(path);
    }
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            seal(&entry?.path())?;
        }
    }
    let mode = if meta.mode() & 0o100 == 0 {
        0o444
    } else {
        0o555
    };
    std::fs::set_permissions(path, Permissions::from_mode(mode))?;
    set_canonical_time(path)
}

/// Unpacks the NAR at `dest` as it streams in, and fails unless its size and
/// hash match. The caller moves the tree into the store only after that.
/// `bytes` counts the NAR bytes read so far.
async fn unpack_nar(
    body: impl AsyncBufRead + Send + Unpin + 'static,
    compression: Compression,
    nar_hash: &Hash,
    nar_size: u64,
    dest: &Path,
    bytes: &AtomicU64,
) -> anyhow::Result<()> {
    let mut size = 0;
    // Reading one byte past the expected size catches a NAR that's too long.
    let (body, hashed) = decode_apart(compression.decoder(body).take(nar_size + 1));
    let mut nar = InspectReader::new(body, |b| {
        size += b.len() as u64;
        bytes.store(size, Ordering::Relaxed);
    });
    // The parser rejects entry names that would leave `dest`, so a NAR that
    // isn't verified yet can't write elsewhere.
    let mut parse_error = None;
    let events = parse_nar(&mut nar).map(|event| {
        event.map_err(|err| {
            // `restore` only takes its own error type, so the parser's error
            // goes in `parse_error`.
            parse_error = Some(err);
            NarWriteError::create_file_error(dest.to_owned(), io::ErrorKind::InvalidData.into())
        })
    });
    let restored = restore(events, dest).await;
    if let Some(err) = parse_error {
        return Err(anyhow::Error::from(err).context("parsing the NAR"));
    }
    restored?;
    // Bytes after the end of the archive count towards the size too.
    tokio::io::copy(&mut nar, &mut tokio::io::sink()).await?;
    drop(nar);
    ensure!(size <= nar_size, "NAR is longer than {nar_size} bytes");
    ensure!(
        size == nar_size,
        "NAR has {size} bytes, expected {nar_size}"
    );
    let hash = hashed.await?;
    ensure!(
        hash == *nar_hash,
        "NAR hash mismatch: got {hash:x}, expected {nar_hash:x}"
    );
    Ok(())
}

/// Reads `decoded` in one task and hashes it in another, so decompression,
/// hashing and parsing each get a core. Returns a reader of the data and a task
/// that yields its SHA-256. The hash covers all the data once the reader has
/// read to the end. Both tasks stay on the tokio workers, since moving them
/// to blocking threads made cold fetches about 15% slower on a 4-vCPU host.
fn decode_apart(
    decoded: impl AsyncRead + Send + 'static,
) -> (
    impl AsyncBufRead + Send + Unpin,
    tokio::task::JoinHandle<Hash>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let (to_hash, mut hashing) = tokio::sync::mpsc::channel::<tokio_util::bytes::Bytes>(4);
    let hashed = tokio::spawn(async move {
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        while let Some(chunk) = hashing.recv().await {
            hasher.update(&chunk);
        }
        hasher.finish()
    });
    tokio::spawn(async move {
        let mut chunks = std::pin::pin!(ReaderStream::with_capacity(decoded, 256 * 1024));
        while let Some(chunk) = chunks.next().await {
            if let Ok(chunk) = &chunk
                && to_hash.send(chunk.clone()).await.is_err()
            {
                break;
            }
            if tx.send(chunk).await.is_err() {
                break;
            }
        }
    });
    let body = StreamReader::new(ReceiverStream::new(rx));
    (body, hashed)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{
        file_store, fixture_narinfo, fixture_path, fixture_root, fixtures, without_lines,
        write_narinfos,
    };
    use crate::store::test_utils::*;

    fn sha256(data: &[u8]) -> Hash {
        Algorithm::SHA256.digest(data)
    }

    /// A NAR of one regular file.
    fn file_nar(contents: &[u8]) -> Vec<u8> {
        let mut nar = Vec::new();
        for s in [
            &b"nix-archive-1"[..],
            b"(",
            b"type",
            b"regular",
            b"contents",
            contents,
            b")",
        ] {
            nar.extend((s.len() as u64).to_le_bytes());
            nar.extend(s);
            nar.resize(nar.len().next_multiple_of(8), 0);
        }
        nar
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
        unpack_nar(body, compression, hash, size, &dest, &AtomicU64::default()).await?;
        Ok((tmp, dest))
    }

    #[tokio::test]
    async fn unpacks_other_compressions() {
        use async_compression::tokio::bufread::{BrotliEncoder, BzEncoder, GzipEncoder, XzEncoder};
        use tokio::io::AsyncRead;

        async fn encode(mut encoder: impl AsyncRead + Unpin) -> Vec<u8> {
            let mut out = Vec::new();
            encoder.read_to_end(&mut out).await.unwrap();
            out
        }

        let contents = b"not really a program".repeat(100);
        let plain = file_nar(&contents);
        for (compression, compressed) in [
            (Compression::Xz, encode(XzEncoder::new(&plain[..])).await),
            (Compression::Bzip2, encode(BzEncoder::new(&plain[..])).await),
            (
                Compression::Gzip,
                encode(GzipEncoder::new(&plain[..])).await,
            ),
            (
                Compression::Brotli,
                encode(BrotliEncoder::new(&plain[..])).await,
            ),
        ] {
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
    }

    #[tokio::test]
    async fn unpacks_closure() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
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
        let (done, total) = ensuring.progress().unwrap();
        assert!(done == total && total > 0, "{done} of {total}");
        drop(ensuring);
        assert_eq!(count(store.dir()), 5);
        assert_eq!(count(state.join("narinfo")), 5);
        assert_eq!(count(state.join("tmp")), 0);
        let hello_dir = store.unpacked(&hello);
        for (file, mode) in [
            (hello_dir.clone(), 0o555),
            (hello_dir.join("bin/hello"), 0o555),
            (hello_dir.join("share/man/man1/hello.1.gz"), 0o444),
        ] {
            let meta = std::fs::symlink_metadata(&file).unwrap();
            assert_eq!(meta.mode() & 0o7777, mode, "{}", file.display());
            assert_eq!(meta.mtime(), 1, "{}", file.display());
        }

        // A new instance needs no cache for the paths it has.
        std::fs::remove_dir_all(&cache).unwrap();
        let store = reopen(store, || config(&dir, &cache, &state)).await;
        let (_, _, info) = fixture_narinfo(&dir, "cache-zstd", &hello);
        let stored = std::fs::read(store.caches.local_narinfo(&hello)).unwrap();
        assert_eq!(stored, info.to_text());
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
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
        let store =
            Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), tmp.path())).unwrap());
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let mut a = store.ensure(std::slice::from_ref(&hello));
        let mut b = store.ensure(std::slice::from_ref(&glibc));
        resolved(&a).await;
        resolved(&b).await;
        let fetches = |e: &Ensuring| e.progress.get().unwrap().fetches.clone();
        let (a_fetches, b_fetches) = (fetches(&a), fetches(&b));
        assert!(!b_fetches.is_empty() && b_fetches.len() < a_fetches.len());
        for fetch in &b_fetches {
            assert!(a_fetches.iter().any(|a| Arc::ptr_eq(a, fetch)));
        }
        assert_eq!(store.fetching.lock().unwrap().len(), a_fetches.len());

        drop(jobs);
        assert_eq!(a.done().await.unwrap().len(), a_fetches.len());
        assert_eq!(b.done().await.unwrap().len(), b_fetches.len());
        // Each fetch leaves the map when it ends, so a later failure starts
        // a new one.
        assert!(store.fetching.lock().unwrap().is_empty());
    }

    /// An ensure carries on after its caller stops waiting, as when kubelet
    /// gives up on a publish, and the retry joins its fetches.
    #[tokio::test]
    async fn carries_on_without_the_caller() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let store =
            Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), tmp.path())).unwrap());
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
            assert!(started.iter().any(|s| Arc::ptr_eq(s, &fetch)));
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
        let (_, _, info) = fixture_narinfo(&dir, "cache-none", &hello);
        let nar = corrupt.join(&info.url);
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
        let narinfo = std::fs::read_to_string(store.caches.local_narinfo(&hello)).unwrap();
        assert!(narinfo.contains("Compression: zstd"), "{narinfo}");
        assert_eq!(count(state.join("tmp")), 0);
    }

    /// Ensures share one log of what fetches install. It keeps what the
    /// oldest running ensure may check, and nothing while no ensure runs.
    #[tokio::test]
    async fn logs_installs_while_ensures_run() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let glibc = fixture_path(&dir, "glibc");
        let store =
            Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), tmp.path())).unwrap());
        let logged = || store.installs.lock().unwrap().log.len();
        let closure = (store.caches.resolve(std::slice::from_ref(&hello)).await).unwrap();
        let (fetch, _) = store.fetch_shared(closure[&glibc].clone());
        fetch.await.unwrap();
        assert_eq!(logged(), 0);

        let first = store.watch_installs();
        store.ensure(&[glibc]).done().await.unwrap();
        let glibc_refs = logged();
        assert!(glibc_refs > 0);
        let second = store.watch_installs();
        store.ensure(&[hello]).done().await.unwrap();
        assert_eq!(logged(), closure.len() - 1);
        drop(first);
        assert_eq!(logged(), closure.len() - 1 - glibc_refs);
        drop(second);
        assert_eq!(logged(), 0);
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
        let (_, _, info) = fixture_narinfo(&dir, "cache-zstd", &hello);
        let (fetch, _) = store.fetch_shared(Arc::new(Entry {
            cache: Some(1),
            info,
        }));
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        resolved(&ensuring).await;
        drop(jobs);
        fetch.await.unwrap();
        let err = ensuring.done().await.unwrap_err();
        assert!(format!("{err:#}").contains("the next try"), "{err:#}");
        // The next try finds hello in the node store, with its references.
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }
}
