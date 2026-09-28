//! Helpers that the store's tests share.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use harmonia_store_path::{StoreDir, StorePath};

use super::fetch::Ensuring;
use super::fs::Scratch;
use super::{Config, Store};
use crate::narinfo::tests::{file_store, fixture_root, fixtures};

pub(super) fn scratch() -> Scratch {
    Scratch::new_in(&std::env::temp_dir()).unwrap()
}

pub(super) const UNBOUND: fn(&Path, &Path) -> anyhow::Result<bool> = |_, _| Ok(false);

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

pub(super) fn config(fixtures: &Path, cache: &Path, state: &Path) -> Config {
    Config {
        stores: vec![file_store(cache)],
        trusted_keys: Some(vec![
            std::fs::read_to_string(fixtures.join("public.key"))
                .unwrap()
                .trim()
                .parse()
                .unwrap(),
        ]),
        state_dir: state.to_owned(),
        store_dir: StoreDir::default(),
        jobs: 2,
        netrc_file: None,
    }
}

/// Returns a store that holds hello's closure from the fixtures, with hello
/// and the closure.
pub(super) async fn hello_store(state: &Path) -> Option<(Arc<Store>, StorePath, Arc<[StorePath]>)> {
    let dir = fixtures()?;
    let hello = fixture_root(&dir, "hello.path");
    let store = Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), state)).unwrap());
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

/// Waits for `ensuring` to resolve its closure and start its fetches.
pub(super) async fn resolved(ensuring: &Ensuring) {
    while ensuring.progress().is_none() {
        tokio::task::yield_now().await;
    }
}

/// Waits for the tasks that hold `store` to end, so it releases its lock,
/// and opens the store again. A child that a test forks holds the lock too
/// until it execs, so this retries for about a second.
pub(super) async fn reopen(store: Arc<Store>, config: impl Fn() -> Config) -> Arc<Store> {
    settled(&store).await;
    drop(store);
    for _ in 0..100 {
        if let Ok(store) = Store::new(config()) {
            return Arc::new(store);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Arc::new(Store::new(config()).unwrap())
}

/// Makes `paths` look last used `days` ago.
pub(super) fn age(store: &Store, paths: &[StorePath], days: u64) {
    let old = SystemTime::now() - Duration::from_hours(24 * days);
    for path in paths {
        let file = (store.caches.local_files(path).iter())
            .find_map(|file| std::fs::File::open(file).ok())
            .unwrap();
        file.set_modified(old).unwrap();
    }
}

/// Puts `path` back as its fetch left it, before its sync.
pub(super) fn unsync(store: &Store, path: &StorePath) {
    let narinfo = store.caches.local_narinfo(path);
    std::fs::rename(narinfo, store.caches.local_unsynced(path)).unwrap();
    store.unsynced.lock().unwrap().insert(path.clone(), 0);
}

pub(super) fn file_names(dir: impl AsRef<Path>) -> Vec<String> {
    let mut names: Vec<_> = (std::fs::read_dir(dir).unwrap())
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

pub(super) fn count(dir: impl AsRef<Path>) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}
