//! The names of the node store's narinfos in `narinfo/`.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use harmonia_store_path::StorePath;

/// The suffix of a narinfo whose path isn't durable yet. A durable path's
/// narinfo ends in `.narinfo` instead. Both suffixes fit in a file name for
/// the longest store paths.
const UNSYNCED: &str = ".unsynced";

#[derive(Clone)]
pub struct Narinfos {
    dir: PathBuf,
}

impl Narinfos {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the narinfo of `path` is once the path is durable.
    pub fn synced(&self, path: &StorePath) -> PathBuf {
        self.dir.join(format!("{path}.narinfo"))
    }

    /// Where the narinfo of `path` is until the path is durable.
    pub fn unsynced(&self, path: &StorePath) -> PathBuf {
        self.dir.join(format!("{path}{UNSYNCED}"))
    }

    /// The files that may hold the narinfo of `path`. A sync renames the
    /// first to the second, so trying them in this order can't miss both.
    pub fn files(&self, path: &StorePath) -> [PathBuf; 2] {
        [self.unsynced(path), self.synced(path)]
    }
}

/// The store path whose narinfo is in file `name`, and whether the path is
/// durable.
pub fn parse_name(name: &OsStr) -> Option<(StorePath, bool)> {
    let name = name.to_str()?;
    let (base, durable) = match name.strip_suffix(".narinfo") {
        Some(base) => (base, true),
        None => (name.strip_suffix(UNSYNCED)?, false),
    };
    Some((StorePath::from_base_path(base).ok()?, durable))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_narinfos() {
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
            assert_eq!(parse_name(OsStr::new(&file)), want, "{file}");
        }

        // Store path names are at most 211 bytes, and file names 255.
        let longest = format!("{}-{}", "0".repeat(32), "a".repeat(211));
        let path = StorePath::from_base_path(&longest).unwrap();
        for file in Narinfos::new("/nonexistent".into()).files(&path) {
            let name = file.file_name().unwrap();
            assert!(name.len() <= 255, "{}", name.len());
            assert_eq!(parse_name(name).unwrap().0, path);
        }
    }
}
