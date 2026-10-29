use std::sync::{Arc, Mutex};

use foldhash::HashMap;
use harmonia_store_path::StorePath;

/// The number of holds on each store path. A path leaves the map when its
/// last hold drops.
#[derive(Clone, Default)]
pub(super) struct Holds(Arc<Mutex<HashMap<StorePath, usize>>>);

impl Holds {
    /// Holds each of `paths` until the result drops.
    pub(super) fn hold(&self, paths: &Arc<[StorePath]>) -> Held {
        let mut holds = self.0.lock().unwrap();
        for path in paths.iter() {
            *holds.entry(path.clone()).or_default() += 1;
        }
        Held {
            holds: self.clone(),
            paths: paths.clone(),
        }
    }

    pub(super) fn contains(&self, path: &StorePath) -> bool {
        self.0.lock().unwrap().contains_key(path)
    }

    pub(super) fn paths(&self) -> Vec<StorePath> {
        self.0.lock().unwrap().keys().cloned().collect()
    }
}

/// The holds from one [`Holds::hold`].
pub(super) struct Held {
    holds: Holds,
    paths: Arc<[StorePath]>,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut holds = self.holds.0.lock().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_holds() {
        let path = |name| StorePath::from_base_path(&format!("{}-{name}", "0".repeat(32))).unwrap();
        let (a, b) = (path("a"), path("b"));
        let holds = Holds::default();
        let both = holds.hold(&[a.clone(), b.clone()].into());
        let one = holds.hold(&[a.clone()].into());
        assert!(holds.contains(&a) && holds.contains(&b));
        drop(both);
        assert_eq!(holds.paths(), std::slice::from_ref(&a));
        drop(one);
        assert_eq!(holds.paths(), []);
    }
}
