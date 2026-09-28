use anyhow::ensure;
use harmonia_store_path::{StoreDir, StorePath};

pub fn parse_store_dir(dir: &str) -> anyhow::Result<StoreDir> {
    let trimmed = dir.trim_end_matches('/');
    ensure!(
        trimmed.starts_with('/'),
        "{dir:?} isn't an absolute directory below /"
    );
    Ok(StoreDir::new(trimmed)?)
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
        for dir in ["/", "nix/store"] {
            assert!(parse_store_dir(dir).is_err(), "{dir:?}");
        }
        let other = parse_store_dir("/other/store/").unwrap();
        let path = parse(&other, &format!("/other/store/{NAME}/x")).unwrap();
        assert_eq!(path.to_string(), NAME);
    }
}
