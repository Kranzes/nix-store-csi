//! Helpers that the store's tests share.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, SystemTime};

use harmonia_file_nar::NarEvent;
use harmonia_file_nar::archive::write_nar;
use harmonia_store_path::StorePath;
use tokio_util::bytes::Bytes;

use super::fetch::{Closure, Ensuring, Progress};
use super::fs::Scratch;
use super::{Config, GcGuard, Size, Store};
use crate::narinfo::tests::{file_store, fixture_root, fixtures};

pub(super) fn scratch() -> Scratch {
    Scratch::new_in(&std::env::temp_dir()).unwrap()
}

pub(crate) const UNBOUND: fn(&Path, &Path) -> anyhow::Result<bool> = |_, _| Ok(false);

/// A NAR of one regular file.
pub(super) fn file_nar(contents: &[u8]) -> Vec<u8> {
    let file = NarEvent::File {
        name: Bytes::new(),
        executable: false,
        size: contents.len() as u64,
        reader: std::io::Cursor::new(contents.to_vec().into()),
    };
    write_nar([&file]).into()
}

/// Copies `src` to `dst`. The copy is writable, unlike the fixtures.
pub(super) fn copy_cache(src: &Path, dst: &Path) {
    let status = std::process::Command::new("cp")
        .args(["-r", "--no-preserve=mode"])
        .arg(src)
        .arg(dst)
        .status()
        .unwrap();
    assert!(status.success());
}

/// A config for `cache` that trusts any narinfo.
pub(crate) fn unsigned_config(cache: &Path, state: &Path) -> Config {
    Config {
        stores: vec![file_store(cache)],
        trusted_keys: None,
        state_dir: state.to_owned(),
        jobs: 2,
        netrc_file: None,
        max_closure_size: None,
        max_store_size: None,
        ensure_free: Size::Bytes(1 << 30),
        measure: true,
    }
}

pub(super) fn config(fixtures: &Path, cache: &Path, state: &Path) -> Config {
    let key = std::fs::read_to_string(fixtures.join("public.key")).unwrap();
    Config {
        trusted_keys: Some(vec![key.trim().parse().unwrap()]),
        ..unsigned_config(cache, state)
    }
}

/// A config for the fixtures' zstd cache.
pub(super) fn zstd_config(fixtures: &Path, state: &Path) -> Config {
    config(fixtures, &fixtures.join("cache-zstd"), state)
}

/// A store path that no cache has.
pub(crate) fn ghost() -> StorePath {
    StorePath::from_base_path(&format!("{}-ghost-1", "0".repeat(32))).unwrap()
}

/// Returns a store that holds hello's closure from the fixtures, along with
/// hello's store path and the closure.
pub(crate) async fn hello_store(state: &Path) -> Option<(Arc<Store>, StorePath, Closure)> {
    let dir = fixtures()?;
    let hello = fixture_root(&dir, "hello.path");
    let store = Arc::new(Store::new(zstd_config(&dir, state)).unwrap());
    let paths = store
        .ensure(std::slice::from_ref(&hello))
        .done()
        .await
        .unwrap();
    // Collection spares the paths until their syncs end.
    settled(&store).await;
    Some((store, hello, paths))
}

/// Waits for the tasks that hold `store`, such as syncs, to end.
pub(super) async fn settled(store: &Arc<Store>) {
    while Arc::strong_count(store) > 1 {
        tokio::task::yield_now().await;
    }
}

/// Runs `f` while every thread of `pool`, or of the global pool if `None`,
/// waits, and returns whether `f` ended within a few seconds.
pub(super) fn ends_while_busy(
    pool: Option<&rayon::ThreadPool>,
    f: impl FnOnce() + Send + 'static,
) -> bool {
    // Each thread waits at the barrier twice, first to show it is busy, then
    // until `f` ends or the wait times out.
    let threads = pool.map_or_else(
        rayon::current_num_threads,
        rayon::ThreadPool::current_num_threads,
    );
    let barrier = Arc::new(Barrier::new(threads + 1));
    let wait = {
        let barrier = barrier.clone();
        move |_: rayon::BroadcastContext<'_>| {
            barrier.wait();
            barrier.wait();
        }
    };
    match pool {
        Some(pool) => pool.spawn_broadcast(wait),
        None => rayon::spawn_broadcast(wait),
    }
    barrier.wait();
    let (done, ended) = mpsc::channel();
    let task = std::thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    let ended = ended.recv_timeout(Duration::from_secs(5)).is_ok();
    barrier.wait();
    task.join().unwrap();
    ended
}

impl Store {
    /// [`Store::view`] with a guard held for the call, as a publish holds one.
    pub(crate) fn guarded_view(
        self: &Arc<Self>,
        volume: &str,
        roots: &[StorePath],
        paths: &[StorePath],
        target: &Path,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
    ) -> anyhow::Result<PathBuf> {
        let closure = Closure {
            paths: paths.into(),
            generation: self.generation.load(std::sync::atomic::Ordering::SeqCst),
        };
        let guard = self.try_gc_guard();
        let (view, _) = self.view(&guard, volume, roots, &closure, target, is_bound)?;
        Ok(view)
    }

    /// [`Store::drop_view`] with a guard held for the call, as an unpublish
    /// holds one.
    pub(super) fn guarded_drop_view(&self, volume: &str) -> anyhow::Result<()> {
        self.drop_view(&self.try_gc_guard(), volume)
    }

    /// Like [`Store::gc_guard`], but panics instead of waiting while garbage
    /// collection or [`Store::forget`] holds the lock.
    fn try_gc_guard(&self) -> GcGuard {
        let read = self.gc.clone().try_read_owned();
        GcGuard {
            _read: read.expect("no garbage collection or forget holds the lock"),
        }
    }
}

/// Waits for `ensuring` to resolve its closure and start its fetches.
pub(super) async fn resolved(ensuring: &Ensuring) {
    while matches!(ensuring.progress(), Progress::Resolving { .. }) {
        tokio::task::yield_now().await;
    }
}

/// Waits for the tasks that hold `store` to end, so it releases its lock,
/// and opens the store again.
pub(super) async fn reopen(store: Arc<Store>, config: impl Fn() -> Config) -> Arc<Store> {
    settled(&store).await;
    drop(store);
    open(config).await
}

/// Opens a store whose last user has dropped it.
pub(super) async fn open(config: impl Fn() -> Config) -> Arc<Store> {
    try_open(config).await.unwrap()
}

/// Like [`open`] for a state dir that start-up may refuse. A child that a
/// test forks holds the last store's lock until it execs, so this retries
/// for about a second while the lock is taken.
pub(super) async fn try_open(config: impl Fn() -> Config) -> anyhow::Result<Arc<Store>> {
    for _ in 0..100 {
        match Store::new(config()) {
            Err(e) if e.to_string().starts_with("another nix-store-csi") => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            opened => return opened.map(Arc::new),
        }
    }
    Store::new(config()).map(Arc::new)
}

/// Makes `paths` look last used `days` ago.
pub(crate) fn age(store: &Store, paths: &[StorePath], days: u64) {
    let old = SystemTime::now() - Duration::from_hours(24 * days);
    for path in paths {
        let file = (store.narinfos.files(path).iter())
            .find_map(|file| std::fs::File::open(file).ok())
            .unwrap();
        file.set_modified(old).unwrap();
    }
}

/// Makes the views that collection keeps look first found unused `days` ago.
pub(crate) fn age_views(store: &Store, days: u64) {
    let old = SystemTime::now() - Duration::from_hours(24 * days);
    for name in file_names(&store.kept) {
        let file = std::fs::File::open(store.kept.join(name)).unwrap();
        file.set_modified(old).unwrap();
    }
}

/// Puts `path` back as its fetch left it, before its sync.
pub(super) fn unsync(store: &Store, path: &StorePath) {
    let narinfo = store.narinfos.synced(path);
    std::fs::rename(narinfo, store.narinfos.unsynced(path)).unwrap();
    store.unsynced.lock().unwrap().insert(path.clone(), 0);
}

pub(super) fn file_names(dir: impl AsRef<Path>) -> Vec<String> {
    let mut names: Vec<_> = (std::fs::read_dir(dir).unwrap())
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

pub(crate) fn count(dir: impl AsRef<Path>) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// An excess that makes garbage collection delete every path that nothing
/// holds.
pub(crate) const ALL: super::Excess = super::Excess {
    bytes: u64::MAX,
    inodes: u64::MAX,
};
