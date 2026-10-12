use std::collections::HashMap;

use harmonia_store_path::{StoreDir, StorePath};

/// The volume attribute that lists space-separated store paths, or paths
/// inside them. On a node of Nix system `<system>`, `storePaths.<system>`
/// replaces it, so one manifest can serve multiple systems. These are the
/// only volume attributes because pods on a node share the node store. A pod
/// that could pick a cache or key could put its own content under any store
/// path for every pod. The files in `--prefetch-dir` have the same names.
pub const STORE_PATHS: &str = "storePaths";

/// The Nix system of this node. Rust and Nix use the same names for `x86_64`
/// and `aarch64`, the architectures the plugin is built for.
pub fn system() -> String {
    format!("{}-linux", std::env::consts::ARCH)
}

/// Returns the attribute that lists the store paths for `system`, and its
/// value.
pub fn for_system<'a>(
    attributes: &'a HashMap<String, String>,
    system: &str,
) -> Option<(String, &'a str)> {
    [format!("{STORE_PATHS}.{system}"), STORE_PATHS.to_owned()]
        .into_iter()
        .find_map(|attribute| {
            let value = attributes.get(&attribute)?;
            Some((attribute, value.as_str()))
        })
}

/// Accepts a base name like `<hash>-hello`, a store path, or a path inside one.
pub fn parse(store_dir: &StoreDir, path: &str) -> anyhow::Result<StorePath> {
    let name = if path.contains('/') {
        let rest = store_dir.strip_prefix(path)?;
        rest.split_once('/').map_or(rest, |(name, _)| name)
    } else {
        path
    };
    Ok(StorePath::from_base_path(name)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3";

    #[test]
    fn parses_store_paths() {
        let nix = StoreDir::default();
        for path in [
            NAME.to_owned(),
            format!("/nix/store/{NAME}"),
            format!("/nix/store/{NAME}/"),
            format!("/nix/store/{NAME}/bin/hello"),
        ] {
            assert_eq!(parse(&nix, &path).unwrap().to_string(), NAME, "{path}");
        }
        for path in ["/nix/store/", "/usr/bin/hello"] {
            assert!(parse(&nix, path).is_err(), "{path:?}");
        }
        let other = StoreDir::new("/other/store").unwrap();
        let path = parse(&other, &format!("/other/store/{NAME}/x")).unwrap();
        assert_eq!(path.to_string(), NAME);
    }
}
