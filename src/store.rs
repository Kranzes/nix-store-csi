//! Verified store paths, unpacked once per node into a directory that every
//! volume shares.

use std::collections::{HashMap, HashSet};
use std::fs::FileType;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use async_compression::tokio::bufread::{BzDecoder, XzDecoder, ZstdDecoder};
use backon::{ConstantBuilder, Retryable};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::{Algorithm, Hash};
use rustix::fs::{AtFlags, CWD, Timespec, Timestamps, UTIME_NOW, utimensat};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{OnceCell, RwLock, RwLockReadGuard, Semaphore};
use tokio_util::io::InspectReader;
use tracing::{info, warn};
use url::Url;

use crate::cache_info::{self, CacheInfo};
use crate::closure::{Caches, Entry};
use crate::nar;
use crate::narinfo::{Compression, PublicKey};
use crate::store_path;
use crate::transport::{self, Transport};

/// Three attempts in all.
const NAR_RESTARTS: ConstantBuilder = ConstantBuilder::new().with_max_times(2);

const BUF_SIZE: usize = 256 * 1024;

pub struct Config {
    pub stores: Vec<String>,
    /// `None` skips the signature check.
    pub trusted_keys: Option<Vec<PublicKey>>,
    pub state_dir: PathBuf,
    /// Narinfo signatures cover it.
    pub store_dir: StoreDir,
    pub jobs: usize,
}

pub struct Store {
    caches: Caches,
    dir: PathBuf,
    views: PathBuf,
    tmp: PathBuf,
    jobs: Semaphore,
    /// One cell per store path, so concurrent volumes share a fetch.
    paths: Mutex<HashMap<StorePath, Arc<OnceCell<()>>>>,
    /// Publishes and unpublishes hold it shared, so collection never runs in
    /// the middle of one.
    gc: RwLock<()>,
}

impl Store {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let store_dir = &config.store_dir;
        let mut stores =
            futures::future::try_join_all(config.stores.iter().map(|url| async move {
                open(url, store_dir)
                    .await
                    .with_context(|| format!("store {url}"))
            }))
            .await?;
        // Stable, so equal priorities keep the order given.
        stores.sort_by_key(|(priority, _)| *priority);
        let stores = stores.into_iter().map(|(_, t)| t).collect();
        let dir = config.state_dir.join("store");
        let views = config.state_dir.join("views");
        let tmp = config.state_dir.join("tmp");
        // Whatever a crash left in tmp/ is incomplete.
        if tmp.exists() {
            tokio::fs::remove_dir_all(&tmp)
                .await
                .with_context(|| format!("clearing {}", tmp.display()))?;
        }
        for d in [&dir, &views, &tmp] {
            tokio::fs::create_dir_all(d)
                .await
                .with_context(|| format!("creating {}", d.display()))?;
        }
        Ok(Self {
            caches: Caches::new(stores, config.store_dir, config.trusted_keys),
            dir,
            views,
            tmp,
            jobs: Semaphore::new(config.jobs),
            paths: Mutex::default(),
            gc: RwLock::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn unpacked(&self, path: &StorePath) -> PathBuf {
        self.dir.join(path.to_string())
    }

    /// `path` may point inside the store path.
    pub fn root(&self, path: &str) -> anyhow::Result<StorePath> {
        store_path::parse(self.caches.store_dir(), path).with_context(|| format!("root {path}"))
    }

    /// Returns the closure of `roots`, fetching what the store lacks. The work
    /// carries on if the caller stops waiting.
    pub async fn ensure(self: &Arc<Self>, roots: &[StorePath]) -> anyhow::Result<Vec<StorePath>> {
        let this = self.clone();
        let roots = roots.to_vec();
        tokio::spawn(async move {
            let closure = this.caches.resolve(&roots).await?;
            let paths = closure.keys().cloned().collect();
            let tasks: Vec<_> = closure
                .into_values()
                .map(|entry| {
                    let this = this.clone();
                    tokio::spawn(async move { this.ensure_path(&entry).await })
                })
                .collect();
            for task in tasks {
                task.await??;
            }
            Ok(paths)
        })
        .await?
    }

    async fn ensure_path(&self, entry: &Entry) -> anyhow::Result<()> {
        let path = entry.info.path();
        let cell = self
            .paths
            .lock()
            .unwrap()
            .entry(path.clone())
            .or_default()
            .clone();
        cell.get_or_try_init(|| async {
            // A path only gets its final name once it's verified and unpacked.
            if tokio::fs::symlink_metadata(self.unpacked(path))
                .await
                .is_ok()
            {
                return Ok(());
            }
            let _permit = self.jobs.acquire().await?;
            self.fetch(entry)
                .await
                .with_context(|| format!("fetching {path}"))
        })
        .await?;
        Ok(())
    }

    async fn fetch(&self, entry: &Entry) -> anyhow::Result<()> {
        let info = &entry.info;
        info!(url = %info.url, size = info.nar_size(), "fetching NAR");
        let nar_path = self.tmp.join(format!("{}.nar", info.path()));
        (|| async {
            let body = self.caches.store(entry).stream(&info.url).await?;
            install_nar(
                body,
                info.compression,
                &info.nar_hash(),
                info.nar_size(),
                &nar_path,
            )
            .await
        })
        .retry(NAR_RESTARTS)
        .when(transport::broke_off)
        .notify(|e, _| warn!(url = %info.url, "fetching NAR: {e:#}; starting over"))
        .await
        .with_context(|| format!("fetching {}", info.url))?;

        let part = tempfile::tempdir_in(&self.tmp)?;
        let unpacked = part.path().join(info.path().to_string());
        let file = tokio::fs::File::open(&nar_path).await?;
        nar::unpack(BufReader::with_capacity(BUF_SIZE, file), &unpacked)
            .await
            .with_context(|| format!("unpacking {}", info.url))?;
        tokio::fs::rename(&unpacked, self.unpacked(info.path()))
            .await
            .with_context(|| format!("moving {} into the store", unpacked.display()))?;
        tokio::fs::remove_file(&nar_path).await?;
        Ok(())
    }

    /// Holds off [`Store::collect`] until the guard drops.
    pub async fn pin(&self) -> RwLockReadGuard<'_, ()> {
        self.gc.read().await
    }

    /// [`Store::collect`] reads the modification time as the last use.
    pub fn touch(&self, paths: &[StorePath]) -> anyhow::Result<()> {
        let now = Timespec {
            tv_sec: 0,
            tv_nsec: UTIME_NOW,
        };
        let times = Timestamps {
            last_access: now,
            last_modification: now,
        };
        for path in paths {
            let path = self.unpacked(path);
            utimensat(CWD, &path, &times, AtFlags::SYMLINK_NOFOLLOW)
                .with_context(|| format!("touching {}", path.display()))?;
        }
        Ok(())
    }

    /// Reuses an existing view as it is. The view records `target` so
    /// [`Store::collect`] can tell whether it's still mounted.
    pub fn view(
        &self,
        volume: &str,
        paths: &[StorePath],
        target: &Path,
    ) -> anyhow::Result<PathBuf> {
        let dir = self.views.join(check_volume(volume)?);
        // A view only gets its final name once it's whole.
        if dir.exists() {
            return Ok(dir.join("store"));
        }
        let part = tempfile::tempdir_in(&self.tmp)?;
        let view = part.path().join(volume);
        std::fs::create_dir_all(view.join("store"))?;
        std::fs::write(view.join("target"), target.as_os_str().as_encoded_bytes())?;
        for path in paths {
            let src = self.unpacked(path);
            let kind = std::fs::symlink_metadata(&src)
                .with_context(|| format!("{}", src.display()))?
                .file_type();
            link_tree(&src, &view.join("store").join(path.to_string()), kind)
                .with_context(|| format!("linking {path} into the view"))?;
        }
        std::fs::rename(&view, &dir)
            .with_context(|| format!("moving {} into place", dir.display()))?;
        Ok(dir.join("store"))
    }

    /// Call it with the guard from [`Store::pin`] held.
    pub fn drop_view(&self, volume: &str) -> anyhow::Result<()> {
        let dir = self.views.join(check_volume(volume)?);
        match std::fs::remove_dir_all(&dir) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                Err(e).with_context(|| format!("removing {}", dir.display()))
            }
            _ => Ok(()),
        }
    }

    /// Deletes the views that `is_bound(view, target)` says aren't mounted,
    /// then the store paths that no view holds and nothing touched for
    /// `unused_for`. Returns how many paths it deleted.
    pub async fn collect(
        &self,
        is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool> + Send + 'static,
        unused_for: Duration,
    ) -> anyhow::Result<usize> {
        let mut doomed = Vec::new();
        let moved: anyhow::Result<()> = async {
            let _guard = self.gc.write().await;
            let views = self.views.clone();
            let in_use =
                tokio::task::spawn_blocking(move || prune_views(&views, is_bound)).await??;
            let mut entries = tokio::fs::read_dir(&self.dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let Some(path) = parse_name(&entry.file_name()) else {
                    continue;
                };
                if in_use.contains(&path) {
                    continue;
                }
                let meta = entry.metadata().await?;
                if meta.modified()?.elapsed().unwrap_or_default() < unused_for {
                    continue;
                }
                // Out of store/ first, since a name there means a whole path.
                let doomed_path = self.tmp.join(format!("{path}.gc"));
                tokio::fs::rename(entry.path(), &doomed_path).await?;
                self.paths.lock().unwrap().remove(&path);
                self.caches.forget(&path);
                doomed.push((doomed_path, meta.is_dir()));
            }
            Ok(())
        }
        .await;
        // Deleting a big closure is slow, so it happens after the lock drops.
        for (path, is_dir) in &doomed {
            if *is_dir {
                tokio::fs::remove_dir_all(path).await?;
            } else {
                tokio::fs::remove_file(path).await?;
            }
        }
        moved?;
        Ok(doomed.len())
    }
}

fn parse_name(name: &std::ffi::OsStr) -> Option<StorePath> {
    StorePath::from_base_path(name.to_str()?).ok()
}

/// Returns the store paths that the remaining views hold.
fn prune_views(
    views: &Path,
    is_bound: impl Fn(&Path, &Path) -> anyhow::Result<bool>,
) -> anyhow::Result<HashSet<StorePath>> {
    let mut in_use = HashSet::new();
    for entry in std::fs::read_dir(views)? {
        let dir = entry?.path();
        let store = dir.join("store");
        let target = std::ffi::OsString::from_vec(std::fs::read(dir.join("target"))?);
        // A reboot drops the mounts without kubelet ever unpublishing them.
        if !is_bound(&store, Path::new(&target))? {
            info!(view = %dir.display(), "removing a view that isn't mounted");
            std::fs::remove_dir_all(&dir)?;
            continue;
        }
        for entry in std::fs::read_dir(&store)? {
            in_use.extend(parse_name(&entry?.file_name()));
        }
    }
    Ok(in_use)
}

fn check_volume(volume: &str) -> anyhow::Result<&str> {
    ensure!(
        !volume.is_empty() && volume != "." && volume != ".." && !volume.contains('/'),
        "volume ID {volume:?} can't name a directory"
    );
    Ok(volume)
}

/// Hard links let the view share inodes and page cache with the store.
fn link_tree(src: &Path, dst: &Path, kind: FileType) -> anyhow::Result<()> {
    if kind.is_symlink() {
        std::os::unix::fs::symlink(std::fs::read_link(src)?, dst)?;
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
        std::fs::set_permissions(dst, std::fs::metadata(src)?.permissions())?;
    } else {
        std::fs::hard_link(src, dst)
            .with_context(|| format!("linking {} to {}", dst.display(), src.display()))?;
    }
    Ok(())
}

/// Refuses a cache for another store dir, as Nix does.
async fn open(url: &str, store_dir: &StoreDir) -> anyhow::Result<(u32, Transport)> {
    let url = Url::parse(url).context("parsing the URL")?;
    let transport = Transport::new(&url)?;
    let info = match transport.get("nix-cache-info").await? {
        Some(text) => cache_info::parse(&String::from_utf8_lossy(&text))?,
        // Hand-made local caches often lack one.
        None if transport.is_local() => CacheInfo::default(),
        None => bail!("no nix-cache-info, so it isn't a binary cache"),
    };
    if let Some(dir) = &info.store_dir
        && dir.trim_end_matches('/') != store_dir.to_str()
    {
        bail!("it holds paths for {dir}, not {store_dir}");
    }
    Ok((cache_info::priority(&url, &info)?, transport))
}

/// `dest` appears only if the NAR's size and hash match.
async fn install_nar(
    body: impl AsyncRead + Send + Unpin + 'static,
    compression: Compression,
    nar_hash: &Hash,
    nar_size: u64,
    dest: &Path,
) -> anyhow::Result<()> {
    let part = dest.with_extension("part");
    let result = async {
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        // Reading one byte past the expected size catches a NAR that's too long.
        let mut nar = InspectReader::new(decoder(body, compression).take(nar_size + 1), |b| {
            hasher.update(b);
        });
        let mut out = BufWriter::with_capacity(BUF_SIZE, tokio::fs::File::create(&part).await?);
        let size = tokio::io::copy(&mut nar, &mut out).await?;
        out.flush().await?;
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
        tokio::fs::rename(&part, dest).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&part).await;
    }
    result
}

/// Accepts concatenated streams, as Nix's decompressors do.
fn decoder(
    body: impl AsyncRead + Send + Unpin + 'static,
    compression: Compression,
) -> Pin<Box<dyn AsyncRead + Send>> {
    let body = BufReader::with_capacity(BUF_SIZE, body);
    match compression {
        Compression::None => Box::pin(body),
        Compression::Xz => {
            let mut d = XzDecoder::new(body);
            d.multiple_members(true);
            Box::pin(d)
        }
        Compression::Zstd => {
            let mut d = ZstdDecoder::new(body);
            d.multiple_members(true);
            Box::pin(d)
        }
        Compression::Bzip2 => {
            let mut d = BzDecoder::new(body);
            d.multiple_members(true);
            Box::pin(d)
        }
    }
}

#[cfg(test)]
mod tests {
    use harmonia_utils_hash::HashFormat;

    use super::*;
    use crate::narinfo::tests::{CACHES, fixture_narinfos, fixture_root, fixtures};

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

    async fn install(
        compressed: &[u8],
        compression: Compression,
        hash: &Hash,
        size: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("x.nar");
        let body = std::io::Cursor::new(compressed.to_vec());
        let result = install_nar(body, compression, hash, size, &dest)
            .await
            .map(|()| std::fs::read(&dest).unwrap());
        if result.is_err() {
            assert!(!dest.exists());
        }
        assert!(!tmp.path().join("x.part").exists());
        result
    }

    #[tokio::test]
    async fn installs_fixture_nars() {
        let Some(dir) = fixtures() else { return };
        for cache in CACHES {
            for (compression, nar, plain) in fixture_nars(&dir, cache) {
                let plain = std::fs::read(plain).unwrap();
                let compressed = std::fs::read(&nar).unwrap();
                let result = install(
                    &compressed,
                    compression,
                    &sha256(&plain),
                    plain.len() as u64,
                )
                .await;
                assert!(result.unwrap() == plain, "{nar:?}");
            }
        }
    }

    #[tokio::test]
    async fn installs_bzip2() {
        let plain = b"not really a NAR".repeat(100);
        let mut compressed = Vec::new();
        async_compression::tokio::bufread::BzEncoder::new(&plain[..])
            .read_to_end(&mut compressed)
            .await
            .unwrap();
        let result = install(
            &compressed,
            Compression::Bzip2,
            &sha256(&plain),
            plain.len() as u64,
        )
        .await;
        assert_eq!(result.unwrap(), plain);
    }

    #[tokio::test]
    async fn rejects_bad_nars() {
        let plain = b"0123456789".to_vec();
        let hash = sha256(&plain);
        for (what, compression, hash, size) in [
            ("wrong hash", Compression::None, sha256(b"other"), 10),
            ("too short", Compression::None, hash, 11),
            ("too long", Compression::None, hash, 9),
            ("not xz", Compression::Xz, hash, 10),
        ] {
            assert!(
                install(&plain, compression, &hash, size).await.is_err(),
                "{what}"
            );
        }
    }

    fn config(fixtures: &Path, cache: &Path, state: &Path) -> Config {
        Config {
            stores: vec![format!("file://{}", cache.display())],
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

    #[tokio::test]
    async fn unpacks_closure() {
        use std::os::unix::fs::PermissionsExt;

        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = tempfile::tempdir().unwrap();
        // A copy of the cache, since the test deletes its NARs below.
        let cache = tmp.path().join("cache");
        copy_cache(&dir.join("cache-zstd"), &cache);
        let state = tmp.path().join("state");

        let store = Arc::new(Store::new(config(&dir, &cache, &state)).await.unwrap());
        let paths = store.ensure(std::slice::from_ref(&hello)).await.unwrap();
        assert_eq!(paths.len(), 5);
        assert_eq!(std::fs::read_dir(store.dir()).unwrap().count(), 5);
        let bin = store.unpacked(&hello).join("bin/hello");
        assert_eq!(
            std::fs::read(&bin).unwrap(),
            std::fs::read(format!("/nix/store/{hello}/bin/hello")).unwrap()
        );
        assert!(std::fs::metadata(&bin).unwrap().permissions().mode() & 0o111 != 0);
        assert_eq!(std::fs::read_dir(state.join("tmp")).unwrap().count(), 0);

        // A new instance finds the paths unpacked and fetches nothing.
        std::fs::remove_dir_all(cache.join("nar")).unwrap();
        let store = Arc::new(Store::new(config(&dir, &cache, &state)).await.unwrap());
        assert_eq!(store.ensure(&[hello]).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn follows_nix_cache_info() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = tempfile::tempdir().unwrap();

        let mut other = config(&dir, &dir.join("cache-zstd"), &tmp.path().join("a"));
        other.store_dir = StoreDir::new("/gnu/store").unwrap();
        let err = Store::new(other).await.err().unwrap();
        assert!(format!("{err:#}").contains("holds paths for"), "{err:#}");

        // This cache has narinfos but no NARs and a worse priority than the
        // default, so listing it first passes only if the store tries it last.
        let broken = tmp.path().join("broken");
        copy_cache(&dir.join("cache-none"), &broken);
        std::fs::remove_dir_all(broken.join("nar")).unwrap();
        std::fs::write(
            broken.join("nix-cache-info"),
            "StoreDir: /nix/store\nPriority: 60\n",
        )
        .unwrap();
        let mut config = config(&dir, &broken, &tmp.path().join("b"));
        config
            .stores
            .push(format!("file://{}", dir.join("cache-zstd").display()));
        let store = Arc::new(Store::new(config).await.unwrap());
        assert_eq!(store.ensure(&[hello]).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn collects_unused_paths() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = tempfile::tempdir().unwrap();
        let cache = dir.join("cache-zstd");
        let store = Arc::new(Store::new(config(&dir, &cache, tmp.path())).await.unwrap());
        let paths = store.ensure(std::slice::from_ref(&hello)).await.unwrap();

        let old = std::time::SystemTime::now() - Duration::from_hours(48);
        let old = Timespec {
            tv_sec: old
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .try_into()
                .unwrap(),
            tv_nsec: 0,
        };
        let times = Timestamps {
            last_access: old,
            last_modification: old,
        };
        for path in &paths {
            utimensat(CWD, store.unpacked(path), &times, AtFlags::SYMLINK_NOFOLLOW).unwrap();
        }

        // Only v1's view is mounted, and it holds hello alone.
        let t1 = tmp.path().join("t1");
        let view = store.view("v1", std::slice::from_ref(&hello), &t1).unwrap();
        store.view("v2", &paths, &tmp.path().join("t2")).unwrap();
        let bound = move |v: &Path, t: &Path| Ok(v == view && t == t1);
        let day = Duration::from_hours(24);
        assert_eq!(store.collect(bound, day).await.unwrap(), 4);
        assert_eq!(std::fs::read_dir(store.dir()).unwrap().count(), 1);
        assert!(store.unpacked(&hello).exists());
        let views: Vec<_> = std::fs::read_dir(tmp.path().join("views"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(views, ["v1"]);

        // Touched paths stay, and deleted ones come back on the next ensure.
        store.drop_view("v1").unwrap();
        store.touch(std::slice::from_ref(&hello)).unwrap();
        assert_eq!(store.collect(|_, _| Ok(false), day).await.unwrap(), 0);
        assert_eq!(store.ensure(&[hello]).await.unwrap().len(), 5);
        assert_eq!(std::fs::read_dir(store.dir()).unwrap().count(), 5);
    }

    #[tokio::test]
    async fn builds_views() {
        use std::os::unix::fs::MetadataExt;

        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = tempfile::tempdir().unwrap();
        let cache = dir.join("cache-zstd");
        let store = Arc::new(Store::new(config(&dir, &cache, tmp.path())).await.unwrap());
        let paths = store.ensure(std::slice::from_ref(&hello)).await.unwrap();

        let target = tmp.path().join("target");
        let view = store.view("v1", &paths, &target).unwrap();
        assert_eq!(std::fs::read_dir(&view).unwrap().count(), 5);
        assert_eq!(
            std::fs::read(tmp.path().join("views/v1/target")).unwrap(),
            target.as_os_str().as_encoded_bytes()
        );
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
        assert_eq!(
            std::fs::read_dir(tmp.path().join("tmp")).unwrap().count(),
            0
        );

        // An existing view is reused as it is.
        assert_eq!(store.view("v1", &[], &target).unwrap(), view);
        assert_eq!(std::fs::read_dir(&view).unwrap().count(), 5);

        store.drop_view("v1").unwrap();
        assert!(!view.exists());
        store.drop_view("v1").unwrap();
        for bad in ["", ".", "..", "a/b"] {
            assert!(store.view(bad, &paths, &target).is_err(), "{bad:?}");
        }
    }
}
