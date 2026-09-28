use anyhow::Context;
use harmonia_store_path::{StoreDir, StoreDirError, StorePath};

pub fn parse_store_dir(dir: &str) -> Result<StoreDir, StoreDirError> {
    StoreDir::new(dir.trim_end_matches('/'))
}

/// Accepts a base name like `<hash>-hello`, a store path, or a path inside one.
pub fn parse(store_dir: &StoreDir, path: &str) -> anyhow::Result<StorePath> {
    let name = if path.contains('/') {
        let rest = store_dir
            .strip_prefix(path)
            .with_context(|| format!("{path:?} is not in the store dir {store_dir}"))?;
        rest.split('/').next().unwrap_or_default()
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
    fn accepted_forms() {
        let nix = StoreDir::default();
        for path in [
            NAME.to_owned(),
            format!("/nix/store/{NAME}"),
            format!("/nix/store/{NAME}/"),
            format!("/nix/store/{NAME}/bin/hello"),
        ] {
            assert_eq!(parse(&nix, &path).unwrap().to_string(), NAME, "{path}");
        }
        let other = parse_store_dir("/other/store/").unwrap();
        let path = parse(&other, &format!("/other/store/{NAME}/x")).unwrap();
        assert_eq!(path.to_string(), NAME);
        assert_eq!(path.hash().to_string(), "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi");
    }

    #[test]
    fn rejected() {
        for path in [
            "",
            "/nix/store",
            "/nix/store/",
            "/nix/storefoo/xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello",
            "/usr/bin/hello",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyihello",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwye-hello",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwy-hello",
            "xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-he llo",
        ] {
            assert!(parse(&StoreDir::default(), path).is_err(), "{path:?}");
        }
    }
}
