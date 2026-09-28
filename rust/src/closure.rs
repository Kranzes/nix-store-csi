use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use futures::StreamExt;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use harmonia_store_path::{StoreDir, StorePath};
use tokio::io::AsyncBufRead;
use tokio::sync::{OnceCell, watch};
use tokio::time::Instant;
use url::Url;

use crate::cache_info::{self, CacheInfo};
use crate::narinfo::{NarInfo, PublicKey};
use crate::transport::{self, Transport};

const PARALLEL: usize = 32;

/// How long a cache is skipped after a request to it fails, as in Nix.
const PAUSE: Duration = Duration::from_mins(1);

pub struct Caches {
    /// In the order given.
    list: Vec<Arc<Cache>>,
    store_dir: StoreDir,
    trusted_keys: Option<Vec<PublicKey>>,
    /// Narinfos of the paths in the node store, which come before any cache.
    local: PathBuf,
}

struct Cache {
    /// The URL without credentials.
    name: Url,
    transport: Transport,
    /// `None` until the first read of `nix-cache-info` ends, then the
    /// priority it gave or the error.
    state: watch::Sender<Option<Result<u32, String>>>,
    /// When a request last failed, and why.
    failed: Mutex<Option<(Instant, String)>>,
}

pub struct Entry {
    /// Index of the cache in the order given, or `None` for the node store.
    pub cache: Option<usize>,
    pub info: NarInfo,
}

impl Caches {
    /// Reads each cache's `nix-cache-info` in the background. A cache that
    /// fails is skipped and tried again every minute, as one that's down
    /// shouldn't stop the others.
    pub fn new(
        urls: &[String],
        store_dir: StoreDir,
        trusted_keys: Option<Vec<PublicKey>>,
        local: PathBuf,
    ) -> anyhow::Result<Self> {
        let list = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                // Errors don't quote a URL that could hold credentials.
                let url = Url::parse(url).with_context(|| format!("substituter {}", i + 1))?;
                let name = transport::redact(&url);
                let transport =
                    Transport::new(&url).with_context(|| format!("substituter {name}"))?;
                Ok(Arc::new(Cache {
                    name,
                    transport,
                    state: watch::Sender::new(None),
                    failed: Mutex::default(),
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        for cache in &list {
            tokio::spawn(cache.clone().open(store_dir.clone()));
        }
        Ok(Self {
            list,
            store_dir,
            trusted_keys,
            local,
        })
    }

    pub fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }

    pub async fn stream(
        &self,
        cache: usize,
        path: &str,
    ) -> anyhow::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        let cache = &*self.list[cache];
        cache.request(cache.transport.stream(path)).await
    }

    pub fn name(&self, cache: usize) -> &Url {
        &self.list[cache].name
    }

    pub fn local_dir(&self) -> &Path {
        &self.local
    }

    /// Where the node store keeps the narinfo of `path`.
    pub fn local_narinfo(&self, path: &StorePath) -> PathBuf {
        self.local.join(format!("{path}.narinfo"))
    }

    /// The caches that opened, grouped by priority, best first, once each has
    /// had its first try.
    async fn tiers(&self) -> Vec<Vec<usize>> {
        for cache in &self.list {
            let _ = cache.state.subscribe().wait_for(Option::is_some).await;
        }
        let mut open: Vec<_> = self
            .list
            .iter()
            .enumerate()
            .filter_map(|(i, c)| match *c.state.borrow() {
                Some(Ok(priority)) => Some((priority, i)),
                _ => None,
            })
            .collect();
        open.sort_unstable();
        (open.chunk_by(|a, b| a.0 == b.0))
            .map(|tier| tier.iter().map(|&(_, i)| i).collect())
            .collect()
    }

    /// Takes each narinfo from the node store, or else from the first cache
    /// that has it with a trusted signature.
    pub async fn resolve(
        &self,
        roots: &[StorePath],
    ) -> anyhow::Result<BTreeMap<StorePath, Arc<Entry>>> {
        let tiers = OnceCell::new();
        let mut found = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::new();
        for root in roots {
            if seen.insert(root.clone()) {
                queue.push_back(root.clone());
            }
        }
        let mut missing = Vec::new();
        let mut in_flight = FuturesUnordered::new();
        loop {
            while in_flight.len() < PARALLEL
                && let Some(path) = queue.pop_front()
            {
                let tiers = &tiers;
                in_flight.push(async move {
                    let entry = self.find(&path, tiers).await;
                    (path, entry)
                });
            }
            let Some((path, entry)) = in_flight.next().await else {
                break;
            };
            let Some(entry) = entry? else {
                missing.push(path);
                continue;
            };
            for r in entry.info.references() {
                if !seen.contains(r) {
                    seen.insert(r.clone());
                    queue.push_back(r.clone());
                }
            }
            found.insert(path, entry);
        }

        if !missing.is_empty() {
            missing.sort();
            let what = if self.trusted_keys.is_some() {
                "in any store, with a trusted signature"
            } else {
                "in any store"
            };
            let mut msg = format!("not found {what}:");
            for m in &missing {
                let m_path = self.store_dir.display(m);
                let by: Vec<String> = (found.iter())
                    .filter(|(_, e)| e.info.references().contains(m))
                    .map(|(path, _)| path.to_string())
                    .collect();
                if by.is_empty() {
                    write!(msg, "\n  {m_path} (a root)")?;
                } else {
                    write!(msg, "\n  {m_path}, referenced by {}", by.join(", "))?;
                }
            }
            for cache in &self.list {
                if let Some(Err(e)) = &*cache.state.borrow() {
                    write!(msg, "\nskipped {}: {e}", cache.name)?;
                }
            }
            bail!(msg);
        }
        tracing::debug!("resolved {} store paths", found.len());
        Ok(found)
    }

    /// Looks in the caches not `tried` yet, for when a NAR fails, as Nix does.
    /// The closure came from `entry`'s references, so only a narinfo with the
    /// same ones will do.
    pub async fn next(&self, entry: &Entry, tried: &[usize]) -> anyhow::Result<Option<Arc<Entry>>> {
        let mut tiers = self.tiers().await;
        for tier in &mut tiers {
            tier.retain(|cache| !tried.contains(cache));
        }
        self.lookup(entry.info.path(), &tiers, |info| {
            info.references() == entry.info.references()
        })
        .await
    }

    async fn find(
        &self,
        path: &StorePath,
        tiers: &OnceCell<Vec<Vec<usize>>>,
    ) -> anyhow::Result<Option<Arc<Entry>>> {
        match self.local_info(path) {
            Ok(Some(info)) => return Ok(Some(Arc::new(Entry { cache: None, info }))),
            Ok(None) => {}
            Err(e) => tracing::warn!("{e:#}"),
        }
        // So a closure that's all on the node needn't wait for caches to open.
        let tiers = tiers.get_or_init(|| self.tiers()).await;
        self.lookup(path, tiers, |_| true).await
    }

    /// The narinfo the node store keeps for `path`, whose signature it checked
    /// when it fetched the path. It's small and read from the page cache.
    pub fn local_info(&self, path: &StorePath) -> anyhow::Result<Option<NarInfo>> {
        let file = self.local_narinfo(path);
        match std::fs::read(&file) {
            Ok(text) => self.parse(text, path, file.display()).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", file.display())),
        }
    }

    /// Asks every cache at once. A better priority always wins, so a tier's
    /// answer only counts once the tiers before it have missed, but within a
    /// tier the first cache to have the path wins, as in ncro. An error only
    /// counts if no cache has the path.
    async fn lookup(
        &self,
        path: &StorePath,
        tiers: &[Vec<usize>],
        accept: impl Fn(&NarInfo) -> bool,
    ) -> anyhow::Result<Option<Arc<Entry>>> {
        let file = format!("{}.narinfo", path.hash());
        let mut races: FuturesOrdered<_> = (tiers.iter())
            .map(|tier| self.race(path, &file, tier, &accept))
            .collect();
        let mut error = None;
        while let Some(found) = races.next().await {
            match found {
                Ok(Some(entry)) => return Ok(Some(entry)),
                Ok(None) => {}
                Err(e) => {
                    error.get_or_insert(e);
                }
            }
        }
        match error {
            Some(e) => Err(e.context(format!("looking up {path}"))),
            None => Ok(None),
        }
    }

    /// The first of `tier` to answer with a narinfo that it trusts and `accept`s.
    async fn race(
        &self,
        path: &StorePath,
        file: &str,
        tier: &[usize],
        accept: &impl Fn(&NarInfo) -> bool,
    ) -> anyhow::Result<Option<Arc<Entry>>> {
        let mut answers: FuturesUnordered<_> = (tier.iter())
            .map(|&cache| {
                let c = &*self.list[cache];
                async move { (cache, c.request(c.transport.get(file)).await) }
            })
            .collect();
        let mut error = None;
        while let Some((cache, answer)) = answers.next().await {
            let name = &self.list[cache].name;
            let info = match answer {
                Ok(Some(text)) => self.parse(text, path, name),
                Ok(None) => continue,
                Err(e) => Err(e),
            };
            match info {
                Ok(info) if !self.trusted(&info) => {
                    tracing::warn!("{path} in {name} has no trusted signature, skipping it");
                }
                Ok(info) if accept(&info) => {
                    tracing::trace!("found {path} in {name}");
                    return Ok(Some(Arc::new(Entry {
                        cache: Some(cache),
                        info,
                    })));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!("{e:#}");
                    error.get_or_insert(e);
                }
            }
        }
        error.map_or(Ok(None), Err)
    }

    /// Parses the narinfo that `source` has for `path`.
    fn parse(
        &self,
        text: Vec<u8>,
        path: &StorePath,
        source: impl Display,
    ) -> anyhow::Result<NarInfo> {
        let info = String::from_utf8(text)
            .map_err(anyhow::Error::from)
            .and_then(|text| NarInfo::parse(&self.store_dir, text))
            .with_context(|| format!("parsing the narinfo of {path} from {source}"))?;
        ensure!(
            info.path() == path,
            "{source} has a narinfo for {} under the hash of {path}",
            info.path()
        );
        Ok(info)
    }

    fn trusted(&self, info: &NarInfo) -> bool {
        (self.trusted_keys.as_ref()).is_none_or(|keys| info.verify(keys))
    }
}

impl Cache {
    async fn open(self: Arc<Self>, store_dir: StoreDir) {
        loop {
            match self.read_info(&store_dir).await {
                Ok(priority) => {
                    self.state.send_replace(Some(Ok(priority)));
                    return;
                }
                Err(e) => {
                    tracing::warn!("skipping {} for a minute: {e:#}", self.name);
                    self.state.send_replace(Some(Err(format!("{e:#}"))));
                }
            }
            tokio::time::sleep(PAUSE).await;
        }
    }

    /// Refuses a cache for another store dir, as Nix does.
    async fn read_info(&self, store_dir: &StoreDir) -> anyhow::Result<u32> {
        let info = match self.transport.get("nix-cache-info").await? {
            Some(text) => cache_info::parse(&String::from_utf8_lossy(&text))?,
            // Hand-made local caches often lack one.
            None if self.transport.is_local() => CacheInfo::default(),
            None => bail!("no nix-cache-info, so it isn't a binary cache"),
        };
        if let Some(dir) = &info.store_dir
            && dir.trim_end_matches('/') != store_dir.to_str()
        {
            bail!("it holds paths for {dir}, not {store_dir}");
        }
        cache_info::priority(&self.name, &info)
    }

    /// Skips the cache for [`PAUSE`] after a request to it fails, so a cache
    /// that's down costs one timeout rather than one per path.
    async fn request<T>(
        &self,
        request: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        if let Some((at, e)) = &*self.failed.lock().unwrap()
            && at.elapsed() < PAUSE
        {
            bail!("skipped {} for a minute after: {e}", self.name);
        }
        let result = request.await;
        if let Err(e) = &result {
            let mut failed = self.failed.lock().unwrap();
            if failed.as_ref().is_none_or(|(at, _)| at.elapsed() >= PAUSE) {
                tracing::warn!("skipping {} for a minute: {e:#}", self.name);
            }
            *failed = Some((Instant::now(), format!("{e:#}")));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::narinfo::tests::{
        NIXOS_KEY, TEST_KEY, file_store, fixture_narinfos, fixture_path, fixture_root, fixtures,
        network_tests,
    };

    const HELLO: &str = "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";
    const GLIBC: &str = "lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84";

    fn caches(stores: &[String], trusted_key: Option<&str>) -> Caches {
        with_local(stores, trusted_key, Path::new("/nonexistent"))
    }

    fn with_local(stores: &[String], trusted_key: Option<&str>, local: &Path) -> Caches {
        let keys = trusted_key.map(|k| vec![k.parse().unwrap()]);
        Caches::new(stores, StoreDir::default(), keys, local.to_owned()).unwrap()
    }

    /// Copies the narinfos of fixture cache `from` into `to`, through `edit`.
    fn copy_narinfos(dir: &Path, from: &str, to: &Path, edit: fn(String) -> String) {
        for (file, info) in fixture_narinfos(dir, from) {
            std::fs::write(to.join(file.file_name().unwrap()), edit(info.text)).unwrap();
        }
    }

    async fn resolve_err(caches: &Caches, roots: &[StorePath]) -> String {
        let Err(e) = caches.resolve(roots).await else {
            panic!("{roots:?} resolved");
        };
        format!("{e:#}")
    }

    fn sources(closure: &BTreeMap<StorePath, Arc<Entry>>) -> BTreeSet<Option<usize>> {
        closure.values().map(|e| e.cache).collect()
    }

    #[tokio::test]
    async fn fixture_closure() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let tmp = tempfile::tempdir().unwrap();
        let stores = [
            format!("{}?priority=10", file_store(tmp.path())),
            format!("{}?priority=20", file_store(&dir.join("cache-zstd"))),
            format!("{}?priority=30", file_store(&dir.join("cache-none"))),
        ];
        let caches_zstd = caches(&stores, Some(TEST_KEY));
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert_eq!(closure.len(), 5);
        assert!(closure.contains_key(&fixture_path(&dir, "glibc")));
        // The best store that has a path wins.
        assert_eq!(sources(&closure), [Some(1)].into());

        // With an untrusted key even the root is missing.
        let wrong = NIXOS_KEY.replace("cache.nixos.org-1", "other");
        let msg = resolve_err(&caches(&stores, Some(&wrong)), &roots).await;
        assert!(msg.contains("(a root)"), "{msg}");
        assert!(caches(&stores, None).resolve(&roots).await.is_ok());

        copy_narinfos(&dir, "cache-none", tmp.path(), |t| t);
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert_eq!(sources(&closure), [Some(0)].into());

        // A failed NAR falls back to a store not tried yet with the same narinfo.
        let entry = &closure[&roots[0]];
        for (tried, want) in [(&[0][..], Some(1)), (&[0, 1], Some(2)), (&[0, 1, 2], None)] {
            let next = caches_zstd.next(entry, tried).await.unwrap();
            assert_eq!(next.and_then(|e| e.cache), want, "{tried:?}");
        }
    }

    #[tokio::test]
    async fn prefers_the_node_store() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let local = tempfile::tempdir().unwrap();
        for (_, info) in fixture_narinfos(&dir, "cache-none") {
            std::fs::write(
                local.path().join(format!("{}.narinfo", info.path())),
                &info.text,
            )
            .unwrap();
        }
        // No cache at all, and no signature check for what the store holds.
        let closure = with_local(&[], Some(NIXOS_KEY), local.path())
            .resolve(&roots)
            .await
            .unwrap();
        assert_eq!(closure.len(), 5);
        assert_eq!(sources(&closure), [None].into());

        // A path the store lacks comes from the caches, like one that left
        // the store after its narinfo was read.
        let glibc = fixture_path(&dir, "glibc");
        std::fs::remove_file(local.path().join(format!("{glibc}.narinfo"))).unwrap();
        let caches = with_local(
            &[file_store(&dir.join("cache-zstd"))],
            Some(TEST_KEY),
            local.path(),
        );
        let closure = caches.resolve(&roots).await.unwrap();
        assert_eq!(closure[&glibc].cache, Some(0));
        assert_eq!(sources(&closure), [None, Some(0)].into());
        let from_cache = caches
            .next(&closure[&roots[0]], &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from_cache.cache, Some(0));
    }

    #[tokio::test]
    async fn skips_broken_stores() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let broken = tempfile::tempdir().unwrap();
        copy_narinfos(&dir, "cache-none", broken.path(), |t| {
            t.replace("StorePath:", "Garbage:")
        });
        let wrong_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            wrong_dir.path().join("nix-cache-info"),
            "StoreDir: /gnu/store\n",
        )
        .unwrap();
        let good = file_store(&dir.join("cache-zstd"));
        let stores = [file_store(wrong_dir.path()), file_store(broken.path())];

        let closure = caches(
            &[stores[0].clone(), stores[1].clone(), good],
            Some(TEST_KEY),
        )
        .resolve(&roots)
        .await
        .unwrap();
        assert_eq!(sources(&closure), [Some(2)].into());

        // Without a good store the errors show.
        let msg = resolve_err(&caches(&stores, Some(TEST_KEY)), &roots).await;
        assert!(msg.contains("parsing the narinfo"), "{msg}");
        let msg = resolve_err(&caches(&stores[..1], Some(TEST_KEY)), &roots).await;
        assert!(
            msg.contains("(a root)") && msg.contains("holds paths for /gnu/store"),
            "{msg}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pauses_a_failing_store() {
        let Some(dir) = fixtures() else { return };
        let hello = fixture_root(&dir, "hello.path");
        let glibc = fixture_path(&dir, "glibc");
        // Every narinfo but hello's reads fine.
        let flaky = tempfile::tempdir().unwrap();
        copy_narinfos(&dir, "cache-none", flaky.path(), |t| t);
        let file = flaky.path().join(format!("{}.narinfo", hello.hash()));
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        let caches = caches(
            &[
                format!("{}?priority=10", file_store(flaky.path())),
                file_store(&dir.join("cache-zstd")),
            ],
            Some(TEST_KEY),
        );

        let closure = caches.resolve(&[hello]).await.unwrap();
        assert_eq!(sources(&closure), [Some(1)].into());
        let closure = caches.resolve(std::slice::from_ref(&glibc)).await.unwrap();
        assert_eq!(sources(&closure), [Some(1)].into());
        tokio::time::advance(PAUSE).await;
        let closure = caches.resolve(&[glibc]).await.unwrap();
        assert_eq!(sources(&closure), [Some(0)].into());
    }

    #[tokio::test]
    async fn fastest_of_a_priority_wins() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let hung = crate::transport::tests::server(crate::transport::tests::Mode::Hangs).await;
        let stores = [hung, file_store(&dir.join("cache-zstd"))];
        let caches = caches(&stores, Some(TEST_KEY));
        let closure = tokio::time::timeout(Duration::from_secs(10), caches.resolve(&roots))
            .await
            .expect("waited for the hung cache")
            .unwrap();
        assert_eq!(sources(&closure), [Some(1)].into());
    }

    #[tokio::test]
    async fn orders_by_priority() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let dirs: Vec<_> = ["", "Priority: 60\n", "Priority: 10\n"]
            .map(|info| {
                let dir = tempfile::tempdir().unwrap();
                std::fs::write(dir.path().join("nix-cache-info"), info).unwrap();
                dir
            })
            .into();
        for d in &dirs {
            copy_narinfos(&dir, "cache-none", d.path(), |t| t);
        }
        let mut stores: Vec<_> = dirs.iter().map(|d| file_store(d.path())).collect();
        stores[0].push_str("?priority=70");
        let caches = caches(&stores, Some(TEST_KEY));
        let closure = caches.resolve(&roots).await.unwrap();
        assert_eq!(caches.tiers().await, [[2], [1], [0]]);
        assert_eq!(sources(&closure), [Some(2)].into());
    }

    #[tokio::test]
    async fn missing_reference() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let glibc = fixture_path(&dir, "glibc");
        // A copy of the plain cache without glibc.
        let tmp = tempfile::tempdir().unwrap();
        copy_narinfos(&dir, "cache-none", tmp.path(), |t| t);
        std::fs::remove_file(tmp.path().join(format!("{}.narinfo", glibc.hash()))).unwrap();
        let msg = resolve_err(&caches(&[file_store(tmp.path())], None), &roots).await;
        assert!(
            msg.contains(&format!("/nix/store/{glibc}, referenced by")),
            "{msg}"
        );
        assert!(msg.contains(&roots[0].to_string()), "{msg}");
    }

    #[tokio::test]
    async fn cache_nixos_org() {
        if !network_tests() {
            return;
        }
        let closure = caches(&["https://cache.nixos.org".into()], Some(NIXOS_KEY))
            .resolve(&[StorePath::from_base_path(HELLO).unwrap()])
            .await
            .unwrap();
        assert_eq!(closure.len(), 5);
        assert!(closure.contains_key(&StorePath::from_base_path(GLIBC).unwrap()));
    }
}
