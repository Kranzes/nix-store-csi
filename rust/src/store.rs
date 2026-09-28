//! Verified store paths, unpacked once per node into a directory that every
//! volume shares.

use std::collections::{HashMap, HashSet};
use std::fs::{FileType, Permissions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail, ensure};
use backon::{ConstantBuilder, Retryable};
use futures::future::{BoxFuture, FutureExt, Shared};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::{Algorithm, Hash, HashFormat};
use rustix::fs::{
    AtFlags, CWD, FlockOperation, Timespec, Timestamps, UTIME_NOW, UTIME_OMIT, flock, utimensat,
};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, Semaphore, oneshot};
use tokio_util::io::InspectReader;
use tracing::{info, warn};

use crate::closure::{Caches, Entry};
use crate::nar;
use crate::narinfo::{Compression, NarInfo, PublicKey};
use crate::store_path;
use crate::transport;

/// Three attempts in all.
const NAR_RESTARTS: ConstantBuilder = ConstantBuilder::new().with_max_times(2);

pub struct Config {
    pub stores: Vec<String>,
    /// `None` skips the signature check.
    pub trusted_keys: Option<Vec<PublicKey>>,
    pub state_dir: PathBuf,
    /// Narinfo signatures cover it.
    pub store_dir: StoreDir,
    pub jobs: usize,
}

type Fetch = Shared<BoxFuture<'static, Result<(), Arc<String>>>>;

pub struct Store {
    caches: Caches,
    dir: PathBuf,
    views: PathBuf,
    volumes: PathBuf,
    /// Held while a view gains or loses a volume.
    views_lock: Mutex<()>,
    tmp: PathBuf,
    jobs: Semaphore,
    /// Fetches under way, so volumes that need a path share one, with the NAR
    /// bytes each has read. Each leaves when it ends, so a publish after a
    /// failure tries again.
    fetching: Mutex<HashMap<StorePath, (Fetch, Arc<AtomicU64>)>>,
    /// The paths of closures under [`Store::ensure`], with how many hold each.
    /// Collection keeps them too, however full the disk.
    ensuring: Mutex<HashMap<StorePath, usize>>,
    /// Publishes and unpublishes hold it shared, so collection never runs in
    /// the middle of one.
    gc: Arc<RwLock<()>>,
    /// Held for as long as the store is open.
    _lock: std::fs::File,
}

impl Store {
    /// Call it within a Tokio runtime, since caches open in the background.
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let state = &config.state_dir;
        std::fs::create_dir_all(state).with_context(|| format!("creating {}", state.display()))?;
        let lock = std::fs::File::create(state.join("lock"))?;
        if flock(&lock, FlockOperation::NonBlockingLockExclusive).is_err() {
            bail!("another nix-store-csi is using {}", state.display());
        }
        let dir = state.join("store");
        let narinfos = state.join("narinfo");
        let views = state.join("views");
        let volumes = state.join("volumes");
        let tmp = state.join("tmp");
        // Whatever a crash left in tmp/ is incomplete.
        remove_tree(&tmp).with_context(|| format!("clearing {}", tmp.display()))?;
        for d in [&dir, &narinfos, &views, &volumes, &tmp] {
            std::fs::create_dir_all(d).with_context(|| format!("creating {}", d.display()))?;
        }
        Ok(Self {
            caches: Caches::new(
                &config.stores,
                config.store_dir,
                config.trusted_keys,
                narinfos,
            )?,
            dir,
            views,
            volumes,
            views_lock: Mutex::default(),
            tmp,
            jobs: Semaphore::new(config.jobs),
            fetching: Mutex::default(),
            ensuring: Mutex::default(),
            gc: Arc::default(),
            _lock: lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn unpacked(&self, path: &StorePath) -> PathBuf {
        self.dir.join(path.to_string())
    }

    async fn has(&self, path: &StorePath) -> bool {
        tokio::fs::symlink_metadata(self.unpacked(path))
            .await
            .is_ok()
    }

    fn present(&self, path: &StorePath) -> bool {
        self.unpacked(path).symlink_metadata().is_ok()
    }

    /// Each of `paths` may point inside a store path.
    pub fn roots<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> anyhow::Result<Vec<StorePath>> {
        (paths.into_iter())
            .map(|path| {
                store_path::parse(self.caches.store_dir(), path)
                    .with_context(|| format!("root {path}"))
            })
            .collect()
    }

    /// Starts fetching what the store lacks of the closure of `roots`. The
    /// work carries on if the caller stops waiting.
    pub fn ensure(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        let (this, roots) = (self.clone(), roots.to_vec());
        let progress = Arc::<OnceLock<_>>::default();
        let set = progress.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = this.ensure_closure(&roots, &set).await;
            // From here only the caller keeps the closure from collection.
            drop(set);
            if let Err(Err(e)) = tx.send(result) {
                warn!(?roots, "no publish is waiting any more: {e:#}");
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
        let closure = self.caches.resolve(roots).await?;
        let total: u64 = closure.values().map(|e| e.info.nar_size()).sum();
        let paths: Arc<[StorePath]> = closure.keys().cloned().collect();
        // Under the guard a collection either sees the hold or ends before the
        // store is checked for the paths.
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
        Ok(paths)
    }

    /// Keeps collection off `paths` until the result drops.
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

    /// Marks the paths the store has as used, so collection keeps them while
    /// the rest downloads, and returns the rest.
    fn touch_present(&self, paths: &[StorePath]) -> anyhow::Result<Vec<StorePath>> {
        let (present, missing): (Vec<_>, Vec<_>) =
            paths.iter().partition(|path| self.present(path));
        self.touch(present)?;
        Ok(missing.into_iter().cloned().collect())
    }

    /// Runs the fetch of `entry`'s path in its own task, so it finishes even
    /// if no publish waits for it any more, and shares it with every publish
    /// that needs the path. Also returns the NAR bytes it has read so far.
    fn fetch_shared(self: &Arc<Self>, entry: Arc<Entry>) -> (Fetch, Arc<AtomicU64>) {
        let path = entry.info.path().clone();
        let mut fetching = self.fetching.lock().unwrap();
        let (fetch, bytes) = fetching.entry(path.clone()).or_insert_with(|| {
            let (this, bytes) = (self.clone(), Arc::<AtomicU64>::default());
            let read = bytes.clone();
            let task = tokio::spawn(async move {
                let result = this.install(entry, &read).await;
                this.fetching.lock().unwrap().remove(&path);
                result.map_err(|e| Arc::new(format!("{e:#}")))
            });
            let fetch = async move { task.await.unwrap_or_else(|e| Err(Arc::new(e.to_string()))) };
            (fetch.boxed().shared(), bytes)
        });
        (fetch.clone(), bytes.clone())
    }

    /// Counts the NAR bytes it reads in `bytes`.
    async fn install(&self, mut entry: Arc<Entry>, bytes: &AtomicU64) -> anyhow::Result<()> {
        let path = entry.info.path().clone();
        // Another fetch may have just finished.
        if self.has(&path).await {
            bytes.store(entry.info.nar_size(), Ordering::Relaxed);
            return Ok(());
        }
        let _permit = self.jobs.acquire().await?;
        let mut tried = Vec::new();
        loop {
            tried.extend(entry.cache);
            let error = match entry.cache {
                Some(cache) => match self.fetch(cache, &entry.info, bytes).await {
                    Ok(()) => return Ok(()),
                    Err(e) => e,
                },
                None => anyhow::anyhow!("it left the node store"),
            }
            .context(format!("fetching {path}"));
            match self.caches.next(&entry, &tried).await {
                Ok(Some(next)) => {
                    warn!("{error:#}; trying the next cache");
                    entry = next;
                }
                _ => return Err(error),
            }
        }
    }

    async fn fetch(&self, cache: usize, info: &NarInfo, bytes: &AtomicU64) -> anyhow::Result<()> {
        let name = self.caches.name(cache);
        info!(cache = %name, url = %info.url, size = info.nar_size(), "fetching NAR");
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
                    tokio::task::spawn_blocking(move || drop(part));
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
        let narinfo = self.caches.local_narinfo(info.path());
        let (dir, text) = (self.dir.clone(), info.text.clone());
        let dest = self.unpacked(info.path());
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _part = part;
            seal(&unpacked).with_context(|| format!("sealing {}", unpacked.display()))?;
            // Its time is the path's last use, and a fetch counts as one.
            write_synced(&narinfo, text.as_bytes())?;
            rename_tree(&unpacked, &dest)
                .with_context(|| format!("moving {} into the store", unpacked.display()))?;
            std::fs::File::open(&dir)?.sync_all()?;
            Ok(())
        })
        .await?
    }

    /// Holds off [`Store::collect`] until the guard drops.
    pub async fn gc_guard(&self) -> OwnedRwLockReadGuard<()> {
        self.gc.clone().read_owned().await
    }

    /// Records a use of `paths`. [`Store::collect`] reads the modification
    /// time of a path's narinfo as its last use.
    fn touch<'a>(&self, paths: impl IntoIterator<Item = &'a StorePath>) -> anyhow::Result<()> {
        let now = Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_NOW,
        };
        let times = Timestamps {
            last_access: now,
            last_modification: now,
        };
        for path in paths {
            let file = self.caches.local_narinfo(path);
            match utimensat(CWD, &file, &times, AtFlags::empty()) {
                Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                Err(e) => return Err(e).with_context(|| format!("touching {}", file.display())),
            }
        }
        Ok(())
    }

    /// Returns the view of `paths` for `volume` to mount at `target`. Volumes
    /// with the same closure share a view.
    pub fn view(
        &self,
        volume: &str,
        paths: &[StorePath],
        target: &Path,
    ) -> anyhow::Result<PathBuf> {
        let record = self.volumes.join(check_volume(volume)?);
        let volume = Volume {
            key: view_key(paths),
            target: target.to_owned(),
        };
        let dir = self.views.join(&volume.key);
        {
            let _views = self.views_lock.lock().unwrap();
            if dir.exists() {
                volume.write(&record)?;
                return Ok(dir);
            }
        }
        // A view only gets its final name once it's whole. Other views
        // needn't wait for it meanwhile.
        let part = Scratch::new_in(&self.tmp)?;
        let view = part.path().join(&volume.key);
        std::fs::create_dir(&view)?;
        for path in paths {
            let src = self.unpacked(path);
            let kind = std::fs::symlink_metadata(&src)
                .with_context(|| format!("{}", src.display()))?
                .file_type();
            link_tree(&src, &view.join(path.to_string()), kind)
                .with_context(|| format!("linking {path} into the view"))?;
        }
        let _views = self.views_lock.lock().unwrap();
        // Another publish may have built it first.
        if !dir.exists() {
            std::fs::rename(&view, &dir)
                .with_context(|| format!("moving {} into place", dir.display()))?;
        }
        volume.write(&record)?;
        Ok(dir)
    }

    /// Forgets `volume`, and deletes its view once no other volume uses it.
    /// Call it with the guard from [`Store::gc_guard`] held.
    pub fn drop_view(&self, volume: &str) -> anyhow::Result<()> {
        let record = self.volumes.join(check_volume(volume)?);
        let trash = {
            let _views = self.views_lock.lock().unwrap();
            let old = Volume::read(&record)?;
            remove_file(&record)?;
            let Some(Volume { key, .. }) = old else {
                return Ok(());
            };
            for entry in std::fs::read_dir(&self.volumes)? {
                if Volume::read(&entry?.path())?.is_some_and(|v| v.key == key) {
                    return Ok(());
                }
            }
            let trash = Scratch::new_in(&self.tmp)?;
            self.trash_view(&key, trash.path())?;
            trash
        };
        Ok(trash.close()?)
    }

    /// Moves the view into `trash`, so views/ only ever holds whole views.
    /// Its paths count as used.
    fn trash_view(&self, key: &str, trash: &Path) -> anyhow::Result<()> {
        let dir = self.views.join(key);
        self.touch(&view_paths(&dir).unwrap_or_default())?;
        match std::fs::rename(&dir, trash.join(key)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }

    /// Forgets the volumes that `is_bound(view, target)` says aren't mounted,
    /// and deletes the views no volume uses. The plugin does this before it
    /// serves, since after a reboot kubelet publishes again, and a view from
    /// before might lack links that never reached the disk.
    pub async fn prune_views(
        self: &Arc<Self>,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + 'static,
    ) -> anyhow::Result<()> {
        self.exclusively(move |this, trash| this.drop_unmounted(trash, is_bound).map(drop))
            .await
    }

    /// Prunes the views like [`Store::prune_views`], then deletes the store
    /// paths that no view holds and that weren't used for `unused_for`, if
    /// that isn't zero. Then it deletes more such paths, least
    /// recently used first, until their NARs add up to `free` bytes. Returns
    /// how many paths it deleted.
    pub async fn collect(
        self: &Arc<Self>,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + 'static,
        unused_for: Duration,
        free: u64,
    ) -> anyhow::Result<usize> {
        self.exclusively(move |this, trash| {
            let mut in_use = this.drop_unmounted(trash, is_bound)?;
            in_use.extend(this.ensuring.lock().unwrap().keys().cloned());
            this.drop_unused(trash, &in_use, unused_for, free)
        })
        .await
    }

    /// How many bytes to delete to bring the store's filesystem from above
    /// `high` percent full down to `low`.
    pub fn excess(&self, high: u8, low: u8) -> io::Result<u64> {
        let stat = rustix::fs::statvfs(&self.dir)?;
        Ok(excess(
            stat.f_blocks * stat.f_frsize,
            stat.f_bavail * stat.f_frsize,
            high,
            low,
        ))
    }

    /// Runs `f` with no publish or unpublish under way. What `f` moves into
    /// the trash is deleted after, since deleting a big closure is slow.
    async fn exclusively<T: Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(&Self, &Path) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<T> {
        let trash = Scratch::new_in(&self.tmp)?;
        let (this, trash_path) = (self.clone(), trash.path().to_owned());
        let guard = self.gc.write().await;
        let result = tokio::task::spawn_blocking(move || f(&this, &trash_path)).await;
        drop(guard);
        tokio::task::spawn_blocking(move || trash.close()).await??;
        result?
    }

    /// Returns the store paths that the remaining views hold.
    fn drop_unmounted(
        &self,
        trash: &Path,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
    ) -> anyhow::Result<HashSet<StorePath>> {
        let mut mounted = HashSet::new();
        for entry in std::fs::read_dir(&self.volumes)? {
            let record = entry?.path();
            if let Some(volume) = Volume::read(&record)?
                && is_bound(&self.views.join(&volume.key), &volume.target)?
            {
                mounted.insert(volume.key);
                continue;
            }
            // A reboot drops the mounts without kubelet ever unpublishing them.
            info!(volume = %record.display(), "forgetting a volume that isn't mounted");
            remove_file(&record)?;
        }
        let mut in_use = HashSet::new();
        for entry in std::fs::read_dir(&self.views)? {
            let entry = entry?;
            let key = entry.file_name().to_string_lossy().into_owned();
            if mounted.contains(&key) {
                in_use.extend(view_paths(&entry.path())?);
            } else {
                self.trash_view(&key, trash)?;
            }
        }
        Ok(in_use)
    }

    fn drop_unused(
        &self,
        trash: &Path,
        in_use: &HashSet<StorePath>,
        unused_for: Duration,
        free: u64,
    ) -> anyhow::Result<usize> {
        let last_use = |file: &Path| {
            std::fs::metadata(file)
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        };
        let old = |used: SystemTime| {
            !unused_for.is_zero() && used.elapsed().unwrap_or_default() >= unused_for
        };
        let mut kept = HashSet::new();
        let mut doomed = Vec::new();
        let mut spared = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let Some(path) = parse_name(&entry?.file_name()) else {
                continue;
            };
            if in_use.contains(&path) {
                kept.insert(path);
                continue;
            }
            let used = last_use(&self.caches.local_narinfo(&path));
            if old(used) {
                doomed.push(path);
            } else {
                spared.push((used, path));
            }
        }
        if free == 0 {
            kept.extend(spared.into_iter().map(|(_, path)| path));
        } else {
            let mut freed: u64 = doomed.iter().map(|path| self.nar_size(path)).sum();
            spared.sort_unstable();
            for (_, path) in spared {
                if freed < free {
                    freed += self.nar_size(&path);
                    doomed.push(path);
                } else {
                    kept.insert(path);
                }
            }
        }
        for path in &doomed {
            // Out of store/ first, since a name there means a whole path.
            rename_tree(&self.unpacked(path), &trash.join(path.to_string()))?;
            remove_file(&self.caches.local_narinfo(path))?;
        }
        // A crash can leave a narinfo without its path.
        for entry in std::fs::read_dir(self.caches.local_dir())? {
            let file = entry?.path();
            let has_path =
                (file.file_stem().and_then(parse_name)).is_some_and(|p| kept.contains(&p));
            if !has_path && old(last_use(&file)) {
                remove_file(&file)?;
            }
        }
        Ok(doomed.len())
    }

    /// From the path's narinfo, as a guess at what deleting it frees.
    fn nar_size(&self, path: &StorePath) -> u64 {
        match self.caches.local_info(path) {
            Ok(info) => info.map_or(0, |info| info.nar_size()),
            Err(e) => {
                warn!("{e:#}");
                0
            }
        }
    }
}

/// A closure on its way into the store, from [`Store::ensure`].
pub struct Ensuring {
    result: oneshot::Receiver<anyhow::Result<Arc<[StorePath]>>>,
    progress: Arc<OnceLock<Progress>>,
}

/// NAR bytes of a closure, set once it's resolved.
struct Progress {
    /// Of the paths the store had already.
    present: u64,
    total: u64,
    /// What each of the other paths' fetches has read.
    fetches: Vec<Arc<AtomicU64>>,
    /// Lasts while the fetch or the caller does, so collection keeps the
    /// closure until the caller has it in a view.
    _held: Held,
}

/// Paths that collection keeps, from [`Store::hold`].
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

    /// The bytes of the closure's NARs in the store so far, and in all, once
    /// it's resolved.
    pub fn progress(&self) -> Option<(u64, u64)> {
        let progress = self.progress.get()?;
        let fetched: u64 = (progress.fetches.iter())
            .map(|bytes| bytes.load(Ordering::Relaxed))
            .sum();
        Some((progress.present + fetched, progress.total))
    }
}

fn excess(total: u64, available: u64, high: u8, low: u8) -> u64 {
    let used = total.saturating_sub(available);
    if used * 100 <= total * u64::from(high) {
        return 0;
    }
    used.saturating_sub(total * u64::from(low) / 100)
}

/// A directory that goes when dropped. Unlike a `TempDir`, it can hold sealed,
/// read-only trees.
struct Scratch(PathBuf);

impl Scratch {
    fn new_in(dir: &Path) -> io::Result<Self> {
        Ok(Self(tempfile::tempdir_in(dir)?.keep()))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Deletes it, and says if that failed.
    fn close(mut self) -> io::Result<()> {
        remove_tree(&std::mem::take(&mut self.0))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = remove_tree(&self.0);
        }
    }
}

fn parse_name(name: &std::ffi::OsStr) -> Option<StorePath> {
    StorePath::from_base_path(name.to_str()?).ok()
}

fn view_paths(view: &Path) -> io::Result<Vec<StorePath>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(view)? {
        paths.extend(parse_name(&entry?.file_name()));
    }
    Ok(paths)
}

/// Names the view of a closure, so volumes with the same one share it.
fn view_key(paths: &[StorePath]) -> String {
    let mut names: Vec<String> = paths.iter().map(ToString::to_string).collect();
    names.sort();
    let hash = Algorithm::SHA256.digest(names.join("\n"));
    hash.as_base32().as_bare().to_string()
}

/// The record of a published volume, so [`Store::collect`] can tell whether
/// it's still mounted.
struct Volume {
    /// Names its view.
    key: String,
    target: PathBuf,
}

impl Volume {
    fn read(record: &Path) -> io::Result<Option<Self>> {
        let bytes = match std::fs::read(record) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let Some(newline) = bytes.iter().position(|&b| b == b'\n') else {
            return Ok(None);
        };
        Ok(Some(Self {
            key: String::from_utf8_lossy(&bytes[..newline]).into_owned(),
            target: std::ffi::OsString::from_vec(bytes[newline + 1..].to_vec()).into(),
        }))
    }

    fn write(&self, record: &Path) -> io::Result<()> {
        let target = self.target.as_os_str().as_encoded_bytes();
        write_synced(record, &[self.key.as_bytes(), b"\n", target].concat())
    }
}

/// Replaces `dest` whole, and syncs it and its directory to disk.
fn write_synced(dest: &Path, contents: &[u8]) -> io::Result<()> {
    let dir = dest.parent().expect("a file is in a directory");
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    file.write_all(contents)?;
    file.as_file().sync_all()?;
    file.persist(dest)?;
    std::fs::File::open(dir)?.sync_all()
}

fn check_volume(volume: &str) -> anyhow::Result<&str> {
    ensure!(
        !volume.is_empty() && volume != "." && volume != ".." && !volume.contains('/'),
        "volume ID {volume:?} can't name a directory"
    );
    Ok(volume)
}

/// Nix gives every file in the store this modification time.
fn set_canonical_time(path: &Path) -> io::Result<()> {
    let times = Timestamps {
        last_access: Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_OMIT,
        },
        last_modification: Timespec {
            tv_sec: 1,
            tv_nsec: 0,
        },
    };
    Ok(utimensat(CWD, path, &times, AtFlags::SYMLINK_NOFOLLOW)?)
}

/// Gives the tree Nix's canonical modes and modification time, and syncs it
/// to disk, so a crash can't leave a path in store/ with missing data.
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
    set_canonical_time(path)?;
    std::fs::File::open(path)?.sync_all()
}

/// Makes directories writable on the way, as Nix does, since sealed ones
/// aren't.
fn remove_tree(path: &Path) -> io::Result<()> {
    fn remove_dir(dir: &Path) -> io::Result<()> {
        std::fs::set_permissions(dir, Permissions::from_mode(0o755))?;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                remove_dir(&entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
        std::fs::remove_dir(dir)
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => remove_dir(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Moving a directory into another needs write access to it, for its `..`, and
/// a sealed one is read-only.
fn rename_tree(from: &Path, to: &Path) -> io::Result<()> {
    let is_dir = std::fs::symlink_metadata(from)?.is_dir();
    if is_dir {
        std::fs::set_permissions(from, Permissions::from_mode(0o755))?;
    }
    std::fs::rename(from, to)?;
    if is_dir {
        std::fs::set_permissions(to, Permissions::from_mode(0o555))?;
    }
    Ok(())
}

fn remove_file(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Hard links let the view share inodes and page cache with the store.
fn link_tree(src: &Path, dst: &Path, kind: FileType) -> anyhow::Result<()> {
    if kind.is_symlink() {
        std::os::unix::fs::symlink(std::fs::read_link(src)?, dst)?;
        set_canonical_time(dst)?;
    } else if kind.is_dir() {
        std::fs::create_dir(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            link_tree(
                &entry.path(),
                &dst.join(entry.file_name()),
                entry.file_type()?,
            )?;
        }
        // The mode every sealed directory has.
        std::fs::set_permissions(dst, Permissions::from_mode(0o555))?;
        set_canonical_time(dst)?;
    } else {
        std::fs::hard_link(src, dst)
            .with_context(|| format!("linking {} to {}", dst.display(), src.display()))?;
    }
    Ok(())
}

/// Unpacks the NAR at `dest` as it streams in, and fails unless its size and
/// hash match. Only then does the tree move into the store. `bytes` follows how
/// much of the NAR it has read.
async fn unpack_nar(
    body: impl AsyncBufRead + Send + Unpin + 'static,
    compression: Compression,
    nar_hash: &Hash,
    nar_size: u64,
    dest: &Path,
    bytes: &AtomicU64,
) -> anyhow::Result<()> {
    let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
    let mut size = 0;
    // Reading one byte past the expected size catches a NAR that's too long.
    let body = compression.decoder(body);
    let mut nar = InspectReader::new(body.take(nar_size + 1), |b| {
        hasher.update(b);
        size += b.len() as u64;
        bytes.store(size, Ordering::Relaxed);
    });
    nar::unpack(&mut nar, dest).await?;
    // Anything after the end of the archive counts too.
    tokio::io::copy(&mut nar, &mut tokio::io::sink()).await?;
    drop(nar);
    ensure!(size <= nar_size, "NAR is longer than {nar_size} bytes");
    ensure!(
        size == nar_size,
        "NAR has {size} bytes, expected {nar_size}"
    );
    let hash = hasher.finish();
    ensure!(
        hash == *nar_hash,
        "NAR hash mismatch: got {hash:x}, expected {nar_hash:x}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nar::tests::Nar;
    use crate::narinfo::tests::{CACHES, file_store, fixture_narinfos, fixture_root, fixtures};

    fn scratch() -> Scratch {
        Scratch::new_in(&std::env::temp_dir()).unwrap()
    }

    const UNBOUND: fn(&Path, &Path) -> anyhow::Result<bool> = |_, _| Ok(false);

    /// `(compression, compressed NAR, uncompressed NAR)` for each narinfo in
    /// fixture `cache`.
    fn fixture_nars(dir: &Path, cache: &str) -> Vec<(Compression, PathBuf, PathBuf)> {
        fixture_narinfos(dir, cache)
            .into_iter()
            .map(|(_, info)| {
                // cache-none names NARs by their hash.
                let hash = info.nar_hash();
                let plain = dir.join(format!("cache-none/nar/{}.nar", hash.as_base32().as_bare()));
                (info.compression, dir.join(cache).join(&info.url), plain)
            })
            .collect()
    }

    /// The copy is writable, unlike the fixtures.
    fn copy_cache(src: &Path, dst: &Path) {
        let status = std::process::Command::new("cp")
            .args(["-r", "--no-preserve=mode"])
            .arg(src)
            .arg(dst)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn sha256(data: &[u8]) -> Hash {
        Algorithm::SHA256.digest(data)
    }

    fn file_nar(contents: &[u8]) -> Vec<u8> {
        Nar::default()
            .ss(&["nix-archive-1", "(", "type", "regular", "contents"])
            .s(contents)
            .s(")")
            .0
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
    async fn unpacks_fixture_nars() {
        let Some(dir) = fixtures() else { return };
        for cache in CACHES {
            for (compression, nar, plain) in fixture_nars(&dir, cache) {
                let plain = std::fs::read(plain).unwrap();
                let compressed = std::fs::read(&nar).unwrap();
                let (_tmp, dest) = unpack(
                    &compressed,
                    compression,
                    &sha256(&plain),
                    plain.len() as u64,
                )
                .await
                .unwrap();
                assert!(dest.symlink_metadata().is_ok(), "{nar:?}");
            }
        }
    }

    #[tokio::test]
    async fn unpacks_other_compressions() {
        use async_compression::tokio::bufread::{BrotliEncoder, BzEncoder, GzipEncoder};
        use tokio::io::AsyncRead;

        async fn encode(mut encoder: impl AsyncRead + Unpin) -> Vec<u8> {
            let mut out = Vec::new();
            encoder.read_to_end(&mut out).await.unwrap();
            out
        }

        let contents = b"not really a program".repeat(100);
        let plain = file_nar(&contents);
        for (compression, compressed) in [
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
        for (what, compression, hash, size) in [
            ("wrong hash", Compression::None, sha256(b"other"), size),
            ("too short", Compression::None, hash, size + 1),
            ("too long", Compression::None, hash, size - 1),
            ("not xz", Compression::Xz, hash, size),
        ] {
            assert!(
                unpack(&plain, compression, &hash, size).await.is_err(),
                "{what}"
            );
        }
    }

    fn config(fixtures: &Path, cache: &Path, state: &Path) -> Config {
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
        }
    }

    /// A store with hello's closure from the fixtures, and that closure.
    async fn hello_store(state: &Path) -> Option<(Arc<Store>, StorePath, Arc<[StorePath]>)> {
        let dir = fixtures()?;
        let hello = fixture_root(&dir, "hello.path");
        let store = Arc::new(Store::new(config(&dir, &dir.join("cache-zstd"), state)).unwrap());
        let paths = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap();
        Some((store, hello, paths))
    }

    /// Waits for the tasks that hold `store` to end, so its lock goes.
    async fn reopen(store: Arc<Store>, config: Config) -> Arc<Store> {
        while Arc::strong_count(&store) > 1 {
            tokio::task::yield_now().await;
        }
        drop(store);
        Arc::new(Store::new(config).unwrap())
    }

    /// Makes `paths` look last used `days` ago.
    fn age(store: &Store, paths: &[StorePath], days: u64) {
        let old = SystemTime::now() - Duration::from_hours(24 * days);
        for path in paths {
            let file = std::fs::File::open(store.caches.local_narinfo(path)).unwrap();
            file.set_modified(old).unwrap();
        }
    }

    fn count(dir: impl AsRef<Path>) -> usize {
        std::fs::read_dir(dir).unwrap().count()
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
        let bin = hello_dir.join("bin/hello");
        assert_eq!(
            std::fs::read(&bin).unwrap(),
            std::fs::read(format!("/nix/store/{hello}/bin/hello")).unwrap()
        );
        for (file, mode) in [
            (hello_dir.clone(), 0o555),
            (bin, 0o555),
            (hello_dir.join("share/man/man1/hello.1.gz"), 0o444),
        ] {
            let meta = std::fs::symlink_metadata(&file).unwrap();
            assert_eq!(meta.mode() & 0o7777, mode, "{}", file.display());
            assert_eq!(meta.mtime(), 1, "{}", file.display());
        }

        // A new instance needs no cache for the paths it has.
        std::fs::remove_dir_all(&cache).unwrap();
        let store = reopen(store, config(&dir, &cache, &state)).await;
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn follows_nix_cache_info() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = scratch();

        let mut other = config(&dir, &dir.join("cache-zstd"), &tmp.path().join("a"));
        other.store_dir = StoreDir::new("/gnu/store").unwrap();
        let store = Arc::new(Store::new(other).unwrap());
        let err = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("holds paths for"), "{err:#}");

        // This cache has narinfos but no NARs, and the better priority, so
        // every NAR falls back to the next cache.
        let broken = tmp.path().join("broken");
        copy_cache(&dir.join("cache-none"), &broken);
        std::fs::remove_dir_all(broken.join("nar")).unwrap();
        std::fs::write(
            broken.join("nix-cache-info"),
            "StoreDir: /nix/store\nPriority: 10\n",
        )
        .unwrap();
        let mut config = config(&dir, &broken, &tmp.path().join("b"));
        config.stores.push(file_store(&dir.join("cache-zstd")));
        let store = Arc::new(Store::new(config).unwrap());
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn collects_unused_paths() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let day = Duration::from_hours(24);

        // v1 is mounted and holds hello alone. v2 isn't, so its view goes,
        // and that counts as a use of its paths.
        age(&store, &paths, 2);
        let t1 = tmp.path().join("t1");
        let view = store.view("v1", std::slice::from_ref(&hello), &t1).unwrap();
        store.view("v2", &paths, &tmp.path().join("t2")).unwrap();
        let bound = move |v: &Path, t: &Path| Ok(v == view && t == t1);
        assert_eq!(store.collect(bound.clone(), day, 0).await.unwrap(), 0);
        let volumes: Vec<_> = std::fs::read_dir(tmp.path().join("volumes"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(volumes, ["v1"]);
        assert_eq!(count(tmp.path().join("views")), 1);

        age(&store, &paths, 2);
        assert_eq!(store.collect(bound, day, 0).await.unwrap(), 4);
        assert_eq!(count(store.dir()), 1);
        assert_eq!(count(tmp.path().join("narinfo")), 1);
        assert!(store.unpacked(&hello).exists());

        // Dropping a view counts as a use too.
        age(&store, std::slice::from_ref(&hello), 2);
        store.drop_view("v1").unwrap();
        assert_eq!(store.collect(UNBOUND, day, 0).await.unwrap(), 0);
        age(&store, std::slice::from_ref(&hello), 2);
        assert_eq!(store.collect(UNBOUND, day, 0).await.unwrap(), 1);

        // Deleted paths come back on the next ensure.
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
        assert_eq!(count(store.dir()), 5);
        assert_eq!(count(tmp.path().join("tmp")), 0);
    }

    #[tokio::test]
    async fn collects_for_space() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let oldest = paths.iter().find(|&p| *p != hello).unwrap();
        age(&store, &paths, 1);
        age(&store, std::slice::from_ref(oldest), 3);
        let target = tmp.path().join("target");
        let view = store
            .view("v1", std::slice::from_ref(&hello), &target)
            .unwrap();
        let bound = move |v: &Path, t: &Path| Ok(v == view && t == target);

        // Space goes to the least recently used first, whatever `unused_for`.
        let collect = |free| store.collect(bound.clone(), Duration::ZERO, free);
        assert_eq!(collect(1).await.unwrap(), 1);
        assert!(!store.unpacked(oldest).exists());
        assert_eq!(count(store.dir()), 4);

        // Paths in use stay however much is wanted.
        assert_eq!(collect(u64::MAX).await.unwrap(), 3);
        assert_eq!(count(store.dir()), 1);
        assert!(store.unpacked(&hello).exists());
    }

    #[test]
    fn measures_excess() {
        // 90 of 100 bytes used, so 10 above 80%.
        assert_eq!(excess(100, 10, 85, 80), 10);
        assert_eq!(excess(100, 15, 85, 80), 0);
        assert_eq!(excess(100, 0, 100, 80), 0);
    }

    #[tokio::test]
    async fn builds_views() {
        let tmp = scratch();
        let Some((store, hello, paths)) = hello_store(tmp.path()).await else {
            return;
        };

        let target = tmp.path().join("target");
        let view = store.view("v1", &paths, &target).unwrap();
        assert_eq!(count(&view), 5);
        let meta = |p: &Path| std::fs::symlink_metadata(p).unwrap();
        let bin = format!("{hello}/bin/hello");
        assert_eq!(
            meta(&view.join(&bin)).ino(),
            meta(&store.dir().join(&bin)).ino()
        );
        let share = format!("{hello}/share");
        assert_eq!(
            meta(&view.join(&share)).mode(),
            meta(&store.dir().join(&share)).mode()
        );
        assert_eq!(meta(&view.join(&share)).mtime(), 1);
        assert_eq!(count(tmp.path().join("tmp")), 0);

        // Volumes with the same closure share a view, and a retried publish
        // gets the same one.
        let other_target = tmp.path().join("other");
        assert_eq!(store.view("v2", &paths, &other_target).unwrap(), view);
        assert_eq!(store.view("v1", &paths, &target).unwrap(), view);
        let other = store.view("v3", &paths[..1], &other_target).unwrap();
        assert_ne!(other, view);
        assert_eq!(count(&other), 1);
        assert_eq!(count(tmp.path().join("views")), 2);

        // A view goes with the last volume that uses it.
        store.drop_view("v1").unwrap();
        assert!(view.exists());
        store.drop_view("v2").unwrap();
        assert!(!view.exists());
        store.drop_view("v2").unwrap();
        for bad in ["", ".", "..", "a/b"] {
            assert!(store.view(bad, &paths, &target).is_err(), "{bad:?}");
        }
    }
}
