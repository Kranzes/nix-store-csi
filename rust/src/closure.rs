use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsStr;
use std::fmt::{Display, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use futures::StreamExt;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use harmonia_store_nar_info::CacheInfo;
use harmonia_store_path::{StoreDir, StorePath};
use tokio::io::AsyncBufRead;
use tokio::sync::{OnceCell, watch};
use tokio::time::Instant;
use url::Url;

use crate::narinfo::{NarInfo, PublicKey};
use crate::transport::{self, Transport};

/// Nix's priority for a cache whose `nix-cache-info` sets none.
const DEFAULT_PRIORITY: i32 = 0;

/// How many narinfo lookups run at once while resolving a closure.
const LOOKUPS: usize = 32;

/// How long a cache is skipped after a request to it fails, as in Nix.
const PAUSE: Duration = Duration::from_mins(1);

/// The suffix of a narinfo in the node store whose path isn't durable yet. A
/// durable path's narinfo ends in `.narinfo` instead. Both suffixes fit in a
/// file name for the longest store paths.
const UNSYNCED: &str = ".unsynced";

/// The store path whose narinfo the node store keeps in file `name`, and
/// whether the path is durable.
pub fn local_name(name: &OsStr) -> Option<(StorePath, bool)> {
    let name = name.to_str()?;
    let (base, durable) = match name.strip_suffix(".narinfo") {
        Some(base) => (base, true),
        None => (name.strip_suffix(UNSYNCED)?, false),
    };
    Some((StorePath::from_base_path(base).ok()?, durable))
}

pub struct Caches {
    /// In the order given.
    list: Vec<Arc<Cache>>,
    store_dir: StoreDir,
    trusted_keys: Option<Vec<PublicKey>>,
    /// The directory with the narinfos of the paths in the node store. The
    /// node store comes before any cache.
    local: PathBuf,
}

struct Cache {
    /// The URL without credentials.
    name: Url,
    /// The priority the URL sets, if any.
    priority: Option<i32>,
    transport: Transport,
    /// `None` until the first read of `nix-cache-info` ends. Then it holds the
    /// cache's priority or the error.
    state: watch::Sender<Option<Result<i32, String>>>,
    /// When a request last failed, and why.
    failed: Mutex<Option<(Instant, String)>>,
}

pub struct Entry {
    /// Index of the cache in the order given, or `None` for the node store.
    pub cache: Option<usize>,
    pub info: NarInfo,
}

impl Caches {
    /// Reads each cache's `nix-cache-info` in the background. It skips a cache
    /// that fails and tries it again every minute, so a cache that is down
    /// doesn't hold up the others.
    pub fn new(
        urls: &[String],
        store_dir: StoreDir,
        trusted_keys: Option<Vec<PublicKey>>,
        local: PathBuf,
        netrc: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let list = urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                // Errors don't quote a URL that could hold credentials.
                let url = Url::parse(url).with_context(|| format!("substituter {}", i + 1))?;
                let name = transport::redact(&url);
                let priority = url_priority(&url)
                    .and_then(|priority| Ok((priority, Transport::new(&url, netrc)?)))
                    .with_context(|| format!("substituter {name}"));
                let (priority, transport) = priority?;
                Ok(Arc::new(Cache {
                    name,
                    priority,
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

    /// A bad `path` fails only the narinfo it came from and doesn't pause the
    /// cache.
    pub async fn stream(
        &self,
        cache: usize,
        path: &str,
    ) -> anyhow::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        let cache = &*self.list[cache];
        cache.transport.check(path)?;
        cache.request(cache.transport.stream(path)).await
    }

    pub fn name(&self, cache: usize) -> &Url {
        &self.list[cache].name
    }

    pub fn names(&self) -> impl Iterator<Item = &Url> {
        self.list.iter().map(|cache| &cache.name)
    }

    /// Each cache, and whether it is paused after a failed request. A cache
    /// whose `nix-cache-info` read failed counts as paused, since
    /// [`Caches::tiers`] leaves it out until [`Cache::open`] reads the file.
    pub fn paused(&self) -> Vec<(String, bool)> {
        (self.list.iter())
            .map(|cache| {
                let unopened = matches!(*cache.state.borrow(), Some(Err(_)));
                let failed = pause_reason(cache.failed.lock().unwrap().as_ref()).is_some();
                (cache.name.to_string(), unopened || failed)
            })
            .collect()
    }

    pub fn local_dir(&self) -> &Path {
        &self.local
    }

    /// Where the node store keeps the narinfo of `path`, once the path is
    /// durable.
    pub fn local_narinfo(&self, path: &StorePath) -> PathBuf {
        self.local.join(format!("{path}.narinfo"))
    }

    /// Where the node store keeps the narinfo of `path` until the path is
    /// durable.
    pub fn local_unsynced(&self, path: &StorePath) -> PathBuf {
        self.local.join(format!("{path}{UNSYNCED}"))
    }

    /// The files that may hold the node store's narinfo of `path`. A sync
    /// renames the first to the second, so trying them in this order can't
    /// miss both.
    pub fn local_files(&self, path: &StorePath) -> [PathBuf; 2] {
        [self.local_unsynced(path), self.local_narinfo(path)]
    }

    /// Waits for each cache's first read of `nix-cache-info`, then groups the
    /// caches that opened by priority, best first.
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

    /// Resolves the closure of `roots`. Each narinfo comes from the node
    /// store, or else from the first cache that has it with a trusted
    /// signature.
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
            while in_flight.len() < LOOKUPS
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
                if seen.insert(r.clone()) {
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
                let by: Vec<_> = (found.iter())
                    .filter(|(_, e)| e.info.references().contains(m))
                    .map(|(path, _)| self.store_dir.display(path).to_string())
                    .collect();
                let m = self.store_dir.display(m);
                match by.as_slice() {
                    [] => write!(msg, "\n  {m} (requested)")?,
                    [a, b, c, rest @ ..] if !rest.is_empty() => write!(
                        msg,
                        "\n  {m}, referenced by {a}, {b}, {c} and {} more",
                        rest.len()
                    )?,
                    _ => write!(msg, "\n  {m}, referenced by {}", by.join(", "))?,
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

    /// Finds another cache for `entry` after its NAR fails, as Nix does. It
    /// looks only in the caches not `tried` yet. The closure came from
    /// `entry`'s references, so it accepts only a narinfo with the same
    /// references.
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
        // Only a path the node store lacks waits for the caches to open.
        let tiers = tiers.get_or_init(|| self.tiers()).await;
        self.lookup(path, tiers, |_| true).await
    }

    /// The narinfo the node store keeps for `path`. The node store trusted it
    /// when it fetched the path, so a change of trusted keys only affects new
    /// paths. The file is small and read from the page cache, so a blocking
    /// read is fine.
    pub fn local_info(&self, path: &StorePath) -> anyhow::Result<Option<NarInfo>> {
        for file in self.local_files(path) {
            match std::fs::read(&file) {
                Ok(text) => return self.parse(text, path, file.display()).map(Some),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("reading {}", file.display())),
            }
        }
        Ok(None)
    }

    /// Asks every cache at once. A better priority always wins, so a tier's
    /// answer counts only after every better tier misses. Within a tier, the
    /// first cache to have the path wins. An error counts only if no cache has
    /// the path.
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

    /// The first answer from `tier` with a trusted narinfo that passes
    /// `accept`.
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
                Ok(mut info) => {
                    if !self.trust(&mut info) {
                        tracing::warn!("{path} in {name} has no trusted signature, skipping it");
                    } else if accept(&info) {
                        tracing::trace!("found {path} in {name}");
                        return Ok(Some(Arc::new(Entry {
                            cache: Some(cache),
                            info,
                        })));
                    }
                }
                Err(e) => {
                    tracing::debug!("{e:#}");
                    error.get_or_insert(e);
                }
            }
        }
        error.map_or(Ok(None), Err)
    }

    fn parse(
        &self,
        text: Vec<u8>,
        path: &StorePath,
        source: impl Display,
    ) -> anyhow::Result<NarInfo> {
        let info = String::from_utf8(text)
            .map_err(anyhow::Error::from)
            .and_then(|text| NarInfo::parse(&self.store_dir, &text))
            .with_context(|| format!("parsing the narinfo of {path} from {source}"))?;
        ensure!(
            info.path() == path,
            "{source} has a narinfo for {} under the hash of {path}",
            info.path()
        );
        Ok(info)
    }

    /// Returns whether a trusted key signed `info`, and keeps only the first
    /// signature that a trusted key verifies. Without trusted keys it accepts
    /// `info` and drops every signature, since nothing checked them.
    fn trust(&self, info: &mut NarInfo) -> bool {
        let keys = self.trusted_keys.as_deref();
        info.retain_verified(keys.unwrap_or_default()) || keys.is_none()
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

    /// Returns the cache's priority. It refuses a cache for another store dir,
    /// as Nix does. It tries once, because lookups wait for it and
    /// [`Cache::open`] tries again later.
    async fn read_info(&self, store_dir: &StoreDir) -> anyhow::Result<i32> {
        let info = match self.transport.get_once("nix-cache-info").await? {
            Some(text) => String::from_utf8_lossy(&text).parse()?,
            // Hand-made local caches often lack one.
            None if self.transport.is_local() => CacheInfo::default(),
            None => bail!("no nix-cache-info, so it isn't a binary cache"),
        };
        if let Some(dir) = &info.store_dir
            && dir != store_dir
        {
            bail!("it holds paths for {dir}, not {store_dir}");
        }
        Ok((self.priority.or(info.priority)).unwrap_or(DEFAULT_PRIORITY))
    }

    /// Skips the cache for [`PAUSE`] after a request to it fails, so a cache
    /// that is down costs one timeout rather than one per path. A cache that
    /// answers that it lacks a file is up, so it stays in use for the paths it
    /// has.
    async fn request<T>(
        &self,
        request: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        if let Some(e) = pause_reason(self.failed.lock().unwrap().as_ref()) {
            bail!("skipped {} for a minute after: {e}", self.name);
        }
        let result = request.await;
        if let Err(e) = &result
            && !transport::lacks_file(e)
        {
            let mut failed = self.failed.lock().unwrap();
            if pause_reason(failed.as_ref()).is_none() {
                tracing::warn!("skipping {} for a minute: {e:#}", self.name);
            }
            *failed = Some((Instant::now(), format!("{e:#}")));
        }
        result
    }
}

/// Why the last request to a cache failed, if it failed less than [`PAUSE`]
/// ago.
fn pause_reason(failed: Option<&(Instant, String)>) -> Option<&str> {
    (failed.filter(|(at, _)| at.elapsed() < PAUSE)).map(|(_, e)| e.as_str())
}

/// The URL's `?priority=`. As in Nix, it overrides the one in `nix-cache-info`.
fn url_priority(url: &Url) -> anyhow::Result<Option<i32>> {
    (url.query_pairs().find(|(k, _)| k == "priority"))
        .map(|(_, value)| value.parse().with_context(|| format!("priority {value:?}")))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::narinfo::tests::{
        NIXOS_KEY, TEST_KEY, copy_narinfos, file_store, fixture_narinfo, fixture_narinfos,
        fixture_path, fixture_root, fixtures, network_tests, without_lines, write_narinfos,
    };

    const HELLO: &str = "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";
    const GLIBC: &str = "lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84";

    #[test]
    fn reads_url_priority() {
        let priority = |url: &str| url_priority(&url.parse().unwrap());
        assert_eq!(priority("https://c").unwrap(), None);
        assert_eq!(priority("https://c?priority=10").unwrap(), Some(10));
        assert_eq!(priority("https://c?a=b&priority=-1").unwrap(), Some(-1));
        assert!(priority("https://c?priority=x").is_err());
    }

    fn caches(stores: &[String], trusted_key: Option<&str>) -> Caches {
        with_local(stores, trusted_key, Path::new("/nonexistent"))
    }

    fn with_local(stores: &[String], trusted_key: Option<&str>, local: &Path) -> Caches {
        let keys = trusted_key.map(|k| vec![k.parse().unwrap()]);
        Caches::new(stores, StoreDir::default(), keys, local.to_owned(), None).unwrap()
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
        // The best cache that has a path wins.
        assert_eq!(sources(&closure), [Some(1)].into());

        // With the wrong trusted key, even the root is missing.
        let wrong = NIXOS_KEY.replace("cache.nixos.org-1", "other");
        let msg = resolve_err(&caches(&stores, Some(&wrong)), &roots).await;
        assert!(msg.contains("(requested)"), "{msg}");
        assert!(caches(&stores, None).resolve(&roots).await.is_ok());

        copy_narinfos(&dir, "cache-none", tmp.path(), |t| t);
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert_eq!(sources(&closure), [Some(0)].into());

        // After a NAR fails, `next` finds a cache not tried yet with the same
        // narinfo.
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
        for (file, _, info) in fixture_narinfos(&dir, "cache-none") {
            let local = local.path().join(format!("{}.narinfo", info.path()));
            std::fs::copy(file, local).unwrap();
        }
        // A path that isn't synced yet counts too.
        let glibc = fixture_path(&dir, "glibc");
        let narinfo = local.path().join(format!("{glibc}.narinfo"));
        let unsynced = local.path().join(format!("{glibc}.unsynced"));
        std::fs::rename(&narinfo, &unsynced).unwrap();
        // No caches. The node store's narinfos need no trusted signature.
        let closure = with_local(&[], Some(NIXOS_KEY), local.path())
            .resolve(&roots)
            .await
            .unwrap();
        assert_eq!(closure.len(), 5);
        assert_eq!(sources(&closure), [None].into());

        // A path the node store lacks comes from the caches. So does a path
        // that left the node store after its narinfo was read.
        std::fs::remove_file(&unsynced).unwrap();
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

    #[test]
    fn names_local_narinfos() {
        let name = format!("{}-hello-2.12.3", "0".repeat(32));
        let path = StorePath::from_base_path(&name).unwrap();
        for (file, want) in [
            (format!("{name}.narinfo"), Some((path.clone(), true))),
            (format!("{name}.unsynced"), Some((path, false))),
            (name.clone(), None),
            // The last suffix decides, so a name that ends in `.narinfo` works.
            (
                format!("{name}.narinfo.unsynced"),
                Some((
                    StorePath::from_base_path(&format!("{name}.narinfo")).unwrap(),
                    false,
                )),
            ),
        ] {
            assert_eq!(local_name(OsStr::new(&file)), want, "{file}");
        }

        // Store path names are at most 211 bytes, and file names 255.
        let longest = format!("{}-{}", "0".repeat(32), "a".repeat(211));
        let path = StorePath::from_base_path(&longest).unwrap();
        for file in caches(&[], None).local_files(&path) {
            let name = file.file_name().unwrap();
            assert!(name.len() <= 255, "{}", name.len());
            assert_eq!(local_name(name).unwrap().0, path);
        }
    }

    #[tokio::test]
    async fn checks_what_caches_serve() {
        let Some(dir) = fixtures() else { return };
        let hello = fixture_root(&dir, "hello.path");
        let glibc = fixture_path(&dir, "glibc");
        let zstd = file_store(&dir.join("cache-zstd"));

        // A cache serves a narinfo for another path under the hash asked for.
        let swapped = tempfile::tempdir().unwrap();
        write_narinfos(&dir, "cache-none", swapped.path(), &[&glibc], |t| t);
        std::fs::rename(
            swapped.path().join(format!("{}.narinfo", glibc.hash())),
            swapped.path().join(format!("{}.narinfo", hello.hash())),
        )
        .unwrap();
        let swapped = caches(&[file_store(swapped.path())], None);
        let msg = resolve_err(&swapped, std::slice::from_ref(&hello)).await;
        assert!(msg.contains("under the hash of"), "{msg}");

        // An untrusted narinfo in a better tier doesn't hide a trusted one.
        let unsigned = tempfile::tempdir().unwrap();
        copy_narinfos(&dir, "cache-none", unsigned.path(), |t| {
            without_lines(&t, "Sig:")
        });
        let stores = [
            format!("{}?priority=10", file_store(unsigned.path())),
            zstd.clone(),
        ];
        let closure = caches(&stores, Some(TEST_KEY))
            .resolve(std::slice::from_ref(&hello))
            .await
            .unwrap();
        assert_eq!(sources(&closure), [Some(1)].into());

        // A fallback must have the references the closure came from. Without
        // the line the narinfo still parses, with no references.
        let other_refs = tempfile::tempdir().unwrap();
        write_narinfos(&dir, "cache-none", other_refs.path(), &[&hello], |t| {
            without_lines(&t, "References:")
        });
        let stores = [
            format!("{zstd}?priority=10"),
            format!("{}?priority=20", file_store(other_refs.path())),
            format!("{}?priority=30", file_store(&dir.join("cache-none"))),
        ];
        let fallbacks = caches(&stores, None);
        let closure = fallbacks
            .resolve(std::slice::from_ref(&hello))
            .await
            .unwrap();
        let next = fallbacks.next(&closure[&hello], &[0]).await.unwrap();
        assert_eq!(next.and_then(|e| e.cache), Some(2));
    }

    /// Without trusted keys the plugin checks no signatures, so the node
    /// store keeps none.
    #[tokio::test]
    async fn drops_signatures_without_keys() {
        let Some(dir) = fixtures() else { return };
        let glibc = fixture_path(&dir, "glibc");
        let (_, text, _) = fixture_narinfo(&dir, "cache-none", &glibc);
        assert!(text.contains("\nSig: "));
        let caches = caches(&[], None);
        let mut info = caches.parse(text.into(), &glibc, "test").unwrap();
        assert!(caches.trust(&mut info));
        let kept = String::from_utf8(info.to_text()).unwrap();
        assert!(!kept.contains("Sig:"), "{kept}");
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

        let all = caches(
            &[stores[0].clone(), stores[1].clone(), good],
            Some(TEST_KEY),
        );
        let closure = all.resolve(&roots).await.unwrap();
        assert_eq!(sources(&closure), [Some(2)].into());
        // The cache for another store dir counts as paused for the metrics.
        let paused: Vec<_> = all.paused().into_iter().map(|(_, p)| p).collect();
        assert_eq!(paused, [true, false, false]);

        // Without a good cache, the error lists what failed.
        let msg = resolve_err(&caches(&stores, Some(TEST_KEY)), &roots).await;
        assert!(msg.contains("parsing the narinfo"), "{msg}");
        let msg = resolve_err(&caches(&stores[..1], Some(TEST_KEY)), &roots).await;
        assert!(
            msg.contains("(requested)") && msg.contains("holds paths for /gnu/store"),
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
                format!("{}?priority=20", file_store(&dir.join("cache-zstd"))),
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

    /// A missing NAR fails only its own path.
    #[tokio::test]
    async fn keeps_a_store_that_lacks_a_file() {
        use crate::transport::tests::{Mode, server};
        let caches = caches(&[server(Mode::Normal).await], None);
        assert!(caches.stream(0, "missing").await.is_err());
        assert!(caches.stream(0, "obj").await.is_ok());
        assert!(caches.stream(0, "private").await.is_err());
        let Err(e) = caches.stream(0, "obj").await else {
            panic!("a cache was used right after it failed");
        };
        assert!(format!("{e:#}").contains("skipped"), "{e:#}");
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
        // A copy of the uncompressed cache without glibc.
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
