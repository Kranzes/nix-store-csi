//! The closures of the roots that pods' volumes list, resolved from the
//! caches.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harmonia_store_path::{StorePath, StorePathHash};
use lru::LruCache;
use tokio::time::Instant;
use tracing::{Instrument, info_span, warn};

use super::Metrics;
use crate::closure::Caches;

/// Roots whose closures stay cached.
const CLOSURES: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// How long a root whose closure failed to resolve scores 0.
const RETRY: Duration = Duration::from_mins(1);

/// Each store path of a closure, and its NAR size.
pub type Closure = Arc<[(StorePathHash, u64)]>;

enum Entry {
    Resolving,
    Resolved(Closure),
    Failed(Instant),
}

pub struct Closures {
    caches: Caches,
    entries: Mutex<LruCache<StorePath, Entry>>,
    metrics: Arc<Metrics>,
}

impl Closures {
    pub fn new(caches: Caches, metrics: Arc<Metrics>) -> Self {
        Self {
            caches,
            entries: Mutex::new(LruCache::new(CLOSURES)),
            metrics,
        }
    }

    /// The closure of `root`, or `None` until it resolves. The scheduler
    /// waits for each answer, so a miss resolves the closure in the
    /// background.
    pub fn get(self: &Arc<Self>, root: &StorePath) -> Option<Closure> {
        let mut entries = self.entries.lock().unwrap();
        match entries.get(root) {
            Some(Entry::Resolved(closure)) => return Some(closure.clone()),
            Some(Entry::Resolving) => return None,
            Some(Entry::Failed(at)) if at.elapsed() < RETRY => return None,
            Some(Entry::Failed(_)) | None => {}
        }
        entries.put(root.clone(), Entry::Resolving);
        let (closures, root) = (self.clone(), root.clone());
        let span = info_span!(parent: None, "resolve", store_path = %root);
        tokio::spawn(async move { closures.resolve(root).await }.instrument(span));
        None
    }

    async fn resolve(&self, root: StorePath) {
        let entry = match self.caches.closure(std::slice::from_ref(&root)).await {
            Ok(closure) => {
                self.metrics.resolve(true);
                let closure = closure
                    .values()
                    .map(|entry| (*entry.info.path().hash(), entry.info.nar_size()));
                Entry::Resolved(closure.collect())
            }
            Err(e) => {
                warn!("resolving the closure: {e:#}; scoring 0 on every node for a minute");
                self.metrics.resolve(false);
                Entry::Failed(Instant::now())
            }
        };
        self.entries.lock().unwrap().put(root, entry);
    }

    #[cfg(test)]
    pub fn insert(&self, root: StorePath, closure: Closure) {
        self.entries
            .lock()
            .unwrap()
            .put(root, Entry::Resolved(closure));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::narinfo::tests::{file_store, fixture_narinfos, fixtures};
    use crate::store::test_utils::ghost;

    async fn resolved(closures: &Arc<Closures>, root: &StorePath) -> Option<Closure> {
        assert!(closures.get(root).is_none());
        while matches!(
            closures.entries.lock().unwrap().peek(root),
            Some(Entry::Resolving)
        ) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        closures.get(root)
    }

    #[tokio::test]
    async fn resolves_in_the_background() {
        let Some(dir) = fixtures() else { return };
        let cache = file_store(&dir.join("cache-zstd"));
        let caches = Caches::new(&[cache], None, None, None).unwrap();
        let closures = Arc::new(Closures::new(caches, Arc::new(Metrics::default())));
        let narinfos = fixture_narinfos(&dir, "cache-zstd");
        let sizes: HashMap<_, _> = (narinfos.iter())
            .map(|(_, _, info)| (*info.path().hash(), info.nar_size()))
            .collect();
        let root = narinfos[0].2.path().clone();
        let closure = resolved(&closures, &root).await.unwrap();
        assert!(closure.iter().any(|(hash, _)| hash == root.hash()));
        for (hash, nar_size) in closure.iter() {
            assert_eq!(sizes[hash], *nar_size);
        }
        // A failed resolve scores 0 until the retry.
        assert!(resolved(&closures, &ghost()).await.is_none());
        let entries = closures.entries.lock().unwrap();
        assert!(matches!(entries.peek(&ghost()), Some(Entry::Failed(_))));
    }
}
