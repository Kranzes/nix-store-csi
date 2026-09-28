use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail, ensure};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use harmonia_store_path::{StoreDir, StorePath};

use crate::narinfo::{NarInfo, PublicKey};
use crate::transport::Transport;

const PARALLEL: usize = 32;

pub struct Caches {
    /// In order of priority.
    stores: Vec<Transport>,
    store_dir: StoreDir,
    trusted_keys: Option<Vec<PublicKey>>,
    found: Mutex<HashMap<StorePath, Arc<Entry>>>,
}

pub struct Entry {
    pub store: usize,
    pub info: NarInfo,
}

impl Caches {
    pub fn new(
        stores: Vec<Transport>,
        store_dir: StoreDir,
        trusted_keys: Option<Vec<PublicKey>>,
    ) -> Self {
        Self {
            stores,
            store_dir,
            trusted_keys,
            found: Mutex::default(),
        }
    }

    pub fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }

    pub fn store(&self, entry: &Entry) -> &Transport {
        &self.stores[entry.store]
    }

    /// Takes each narinfo from the first store that has it with a trusted
    /// signature, and reuses what earlier calls found.
    pub async fn resolve(
        &self,
        roots: &[StorePath],
    ) -> anyhow::Result<BTreeMap<StorePath, Arc<Entry>>> {
        let mut found = BTreeMap::new();
        // Every path seen so far, with the paths that referenced it.
        let mut referrers: BTreeMap<StorePath, BTreeSet<StorePath>> = BTreeMap::new();
        let mut queue = VecDeque::new();
        for root in roots {
            if referrers.insert(root.clone(), BTreeSet::new()).is_none() {
                queue.push_back(root.clone());
            }
        }
        let mut missing = Vec::new();
        let mut in_flight = FuturesUnordered::new();
        loop {
            while in_flight.len() < PARALLEL
                && let Some(path) = queue.pop_front()
            {
                in_flight.push(async move {
                    let entry = self.find(&path).await;
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
                let seen = referrers.contains_key(r);
                referrers.entry(r.clone()).or_default().insert(path.clone());
                if !seen {
                    queue.push_back(r.clone());
                }
            }
            found.insert(path, entry);
        }

        if !missing.is_empty() {
            missing.sort();
            let lines: Vec<String> = missing
                .iter()
                .map(|m| {
                    let m_path = self.store_dir.display(m);
                    let by = &referrers[m];
                    if by.is_empty() {
                        format!("  {m_path} (a root)")
                    } else {
                        let by: Vec<String> = by.iter().map(ToString::to_string).collect();
                        format!("  {m_path}, referenced by {}", by.join(", "))
                    }
                })
                .collect();
            let what = if self.trusted_keys.is_some() {
                "in any store, with a trusted signature"
            } else {
                "in any store"
            };
            bail!("not found {what}:\n{}", lines.join("\n"));
        }
        tracing::debug!("resolved {} store paths", found.len());
        Ok(found)
    }

    pub fn forget(&self, path: &StorePath) {
        self.found.lock().unwrap().remove(path);
    }

    async fn find(&self, path: &StorePath) -> anyhow::Result<Option<Arc<Entry>>> {
        if let Some(entry) = self.found.lock().unwrap().get(path) {
            return Ok(Some(entry.clone()));
        }
        let file = format!("{}.narinfo", path.hash());
        for (store, transport) in self.stores.iter().enumerate() {
            let Some(text) = transport
                .get(&file)
                .await
                .with_context(|| format!("fetching the narinfo of {path} from store {store}"))?
            else {
                continue;
            };
            let info = std::str::from_utf8(&text)
                .map_err(anyhow::Error::from)
                .and_then(|text| NarInfo::parse(&self.store_dir, text))
                .with_context(|| format!("parsing the narinfo of {path} from store {store}"))?;
            ensure!(
                info.path() == path,
                "store {store} has a narinfo for {} under the hash of {path}",
                info.path()
            );
            if let Some(keys) = &self.trusted_keys
                && !info.verify(keys)
            {
                tracing::warn!("{path} in store {store} has no trusted signature, skipping it");
                continue;
            }
            tracing::trace!("found {path} in store {store}");
            let entry = Arc::new(Entry { store, info });
            self.found
                .lock()
                .unwrap()
                .insert(path.clone(), entry.clone());
            return Ok(Some(entry));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::narinfo::tests::{
        NIXOS_KEY, TEST_KEY, fixture_path, fixture_root, fixtures, network_tests,
    };

    const HELLO: &str = "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";
    const GLIBC: &str = "lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84";

    fn caches(stores: Vec<Transport>, trusted_key: Option<&str>) -> Caches {
        let keys = trusted_key.map(|k| vec![k.parse().unwrap()]);
        Caches::new(stores, StoreDir::default(), keys)
    }

    fn file_store(dir: &std::path::Path) -> Transport {
        Transport::new(&format!("file://{}", dir.display()).parse().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn fixture_closure() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let tmp = tempfile::tempdir().unwrap();
        let stores = || {
            vec![
                file_store(tmp.path()),
                file_store(&dir.join("cache-zstd")),
                file_store(&dir.join("cache-none")),
            ]
        };
        let caches_zstd = caches(stores(), Some(TEST_KEY));
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert_eq!(closure.len(), 5);
        assert!(closure.contains_key(&fixture_path(&dir, "glibc")));
        // The first store that has a path wins.
        assert!(closure.values().all(|e| e.store == 1));

        // With an untrusted key even the root is missing.
        let wrong = NIXOS_KEY.replace("cache.nixos.org-1", "other");
        let err = caches(stores(), Some(&wrong))
            .resolve(&roots)
            .await
            .err()
            .unwrap();
        assert!(format!("{err:#}").contains("(a root)"), "{err:#}");
        assert!(caches(stores(), None).resolve(&roots).await.is_ok());

        // Store 0 gaining copies changes nothing until the path is forgotten.
        let copy = |from: &str| {
            for e in std::fs::read_dir(dir.join(from)).unwrap() {
                let path = e.unwrap().path();
                if path.extension().is_some_and(|x| x == "narinfo") {
                    std::fs::copy(&path, tmp.path().join(path.file_name().unwrap())).unwrap();
                }
            }
        };
        copy("cache-none");
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert!(closure.values().all(|e| e.store == 1));
        caches_zstd.forget(&roots[0]);
        let closure = caches_zstd.resolve(&roots).await.unwrap();
        assert_eq!(closure[&roots[0]].store, 0);
    }

    #[tokio::test]
    async fn missing_reference() {
        let Some(dir) = fixtures() else { return };
        let roots = [fixture_root(&dir, "hello.path")];
        let glibc = fixture_path(&dir, "glibc");
        // A copy of the plain cache without glibc.
        let tmp = tempfile::tempdir().unwrap();
        for entry in std::fs::read_dir(dir.join("cache-none")).unwrap() {
            let path = entry.unwrap().path();
            let file = path.file_name().unwrap().to_str().unwrap();
            if file.ends_with(".narinfo") && !file.starts_with(&glibc.hash().to_string()) {
                std::fs::copy(&path, tmp.path().join(file)).unwrap();
            }
        }
        let err = caches(vec![file_store(tmp.path())], None)
            .resolve(&roots)
            .await
            .err()
            .unwrap();
        let msg = format!("{err:#}");
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
        let stores = vec![Transport::new(&"https://cache.nixos.org".parse().unwrap()).unwrap()];
        let closure = caches(stores, Some(NIXOS_KEY))
            .resolve(&[StorePath::from_base_path(HELLO).unwrap()])
            .await
            .unwrap();
        assert_eq!(closure.len(), 5);
        assert!(closure.contains_key(&StorePath::from_base_path(GLIBC).unwrap()));
    }
}
