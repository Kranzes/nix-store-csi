use std::collections::BTreeSet;

use anyhow::{Context, bail};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_store_path_info::fingerprint_path;
use harmonia_utils_hash::Hash;

pub use harmonia_utils_signature::PublicKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Xz,
    Zstd,
    Bzip2,
}

impl Compression {
    fn parse(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "none" | "" => Self::None,
            "xz" => Self::Xz,
            "zstd" => Self::Zstd,
            "bzip2" => Self::Bzip2,
            other => bail!("unsupported compression {other:?}"),
        })
    }
}

/// Signatures cover the fields behind the accessors, not `url` or `compression`.
#[derive(Debug, Clone)]
pub struct NarInfo {
    /// Relative to the cache's base URL, like `nar/<filehash>.nar.xz`.
    pub url: String,
    pub compression: Compression,
    parsed: harmonia_store_nar_info::NarInfo,
}

impl NarInfo {
    pub fn parse(store_dir: &StoreDir, text: &str) -> anyhow::Result<Self> {
        let mut parsed = harmonia_store_nar_info::parse_narinfo_txt(store_dir, text)?;
        let url = parsed.info.url.take().context("narinfo has no URL")?;
        // Nix's default, from before the field existed.
        let compression = parsed.info.compression.take();
        let compression = Compression::parse(compression.as_deref().unwrap_or("bzip2"))?;
        Ok(Self {
            url,
            compression,
            parsed,
        })
    }

    pub fn path(&self) -> &StorePath {
        &self.parsed.path
    }

    pub fn nar_hash(&self) -> Hash {
        self.parsed.info.info.nar_hash.into()
    }

    pub fn nar_size(&self) -> u64 {
        self.parsed.info.info.nar_size
    }

    pub fn references(&self) -> &BTreeSet<StorePath> {
        &self.parsed.info.info.references
    }

    pub fn verify(&self, keys: &[PublicKey]) -> bool {
        let info = &self.parsed.info.info;
        let fingerprint = fingerprint_path(
            &info.store_dir,
            &self.parsed.path,
            &info.nar_hash,
            info.nar_size,
            &info.references,
        );
        info.signatures.iter().any(|sig| {
            keys.iter()
                .any(|k| k.name() == sig.name() && k.verify(&fingerprint, sig))
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::{Path, PathBuf};

    use harmonia_utils_hash::Algorithm;

    use super::*;
    use crate::store_path;

    pub const TEST_KEY: &str = "test-1:moc/aM6YNJhbirrTIvIK1J2tTvPdwtGvnUH1tzsF7ZI=";
    pub const NIXOS_KEY: &str = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
    /// All three hold the same paths.
    pub const CACHES: [&str; 3] = ["cache-none", "cache-xz", "cache-zstd"];

    const GLIBC: &str = "\
StorePath: /nix/store/lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84
URL: nar/06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s.nar
Compression: none
FileHash: sha256:06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s
FileSize: 35073128
NarHash: sha256:06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s
NarSize: 35073128
References: i3jw341xs88r6wxf1j22rjx1mmd8cfjw-libidn2-2.3.8 lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84 m07syxhld8hpprrdmzq565ziia6vlw9l-xgcc-15.3.0-libgcc
Deriver: sayi2sas6kn7q181jy7ic8h8hy2jiqn7-glibc-2.42-84.drv
Sig: cache.nixos.org-1:tsZFm/X1kvsybHu4gnDR7XxYpTAkTKwaDHSNkZGiALxhhvmKeSvCsD2cDKbWutDVI06TJWhzvHnVwloe/J6eAQ==
Sig: test-1:uLy+NsnprYjac4dL2b/QKAqXkcZ2kMJmSWAGrfdHnRb4ycbZsCv7MMXVqp4aPJci0GM6u8LeVhzyxzA2aT/nAw==
";

    /// The caches from nix/fixtures.nix, which the dev shell and the package
    /// build point `NIX_STORE_CSI_FIXTURES` at.
    pub fn fixtures() -> Option<PathBuf> {
        let Some(dir) = std::env::var_os("NIX_STORE_CSI_FIXTURES") else {
            eprintln!("NIX_STORE_CSI_FIXTURES unset, skipping");
            return None;
        };
        let dir = PathBuf::from(dir);
        assert!(dir.is_dir(), "no fixtures at {}", dir.display());
        Some(dir)
    }

    pub fn network_tests() -> bool {
        let on = std::env::var_os("NIX_STORE_CSI_NETWORK_TESTS").is_some();
        if !on {
            eprintln!("NIX_STORE_CSI_NETWORK_TESTS unset, skipping");
        }
        on
    }

    pub fn fixture_narinfos(dir: &Path, cache: &str) -> Vec<(PathBuf, NarInfo)> {
        let infos: Vec<_> = std::fs::read_dir(dir.join(cache))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "narinfo"))
            .map(|p| {
                let info = parse(&std::fs::read_to_string(&p).unwrap()).unwrap();
                (p, info)
            })
            .collect();
        let all = std::fs::read_to_string(dir.join("closure")).unwrap();
        assert_eq!(infos.len(), all.lines().count(), "{cache}");
        infos
    }

    pub fn fixture_root(dir: &Path, file: &str) -> StorePath {
        let path = std::fs::read_to_string(dir.join(file)).unwrap();
        store_path::parse(&StoreDir::default(), path.trim()).unwrap()
    }

    pub fn fixture_path(dir: &Path, pname: &str) -> StorePath {
        fixture_narinfos(dir, "cache-none")
            .into_iter()
            .map(|(_, info)| info.path().clone())
            .find(|path| path.name().to_string().starts_with(&format!("{pname}-")))
            .unwrap_or_else(|| panic!("no {pname} in the fixtures"))
    }

    fn key(s: &str) -> PublicKey {
        s.trim().parse().unwrap()
    }

    fn parse(text: &str) -> anyhow::Result<NarInfo> {
        NarInfo::parse(&StoreDir::default(), text)
    }

    #[test]
    fn parse_glibc() {
        let info = parse(GLIBC).unwrap();
        assert_eq!(
            info.path().to_string(),
            "lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84"
        );
        assert_eq!(
            info.url,
            "nar/06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s.nar"
        );
        assert_eq!(info.compression, Compression::None);
        assert_eq!(
            format!("{:#x}", info.nar_hash()),
            "ba18a2a310ec8f8700141130ba7ba5d51b07c1962db3a7e70fc187313bbd731a"
        );
        assert_eq!(info.nar_size(), 35_073_128);
        let references: Vec<_> = info.references().iter().map(ToString::to_string).collect();
        assert_eq!(
            references,
            [
                "i3jw341xs88r6wxf1j22rjx1mmd8cfjw-libidn2-2.3.8",
                "lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84",
                "m07syxhld8hpprrdmzq565ziia6vlw9l-xgcc-15.3.0-libgcc",
            ]
        );
    }

    #[test]
    fn parse_defaults_and_errors() {
        let minimal = "StorePath: /nix/store/lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84\n\
                       URL: nar/x.nar.bz2\nNarSize: 1\n\
                       NarHash: sha256:06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s\n";
        let info = parse(minimal).unwrap();
        assert_eq!(info.compression, Compression::Bzip2);
        assert!(info.references().is_empty());

        assert!(parse(&format!("{minimal}Compression: br\n")).is_err());
        assert!(parse(&minimal.replace("URL: nar/x.nar.bz2\n", "")).is_err());
        assert!(parse(&minimal.replace("sha256:", "md5:")).is_err());
        let gnu = StoreDir::new("/gnu/store").unwrap();
        assert!(NarInfo::parse(&gnu, minimal).is_err());
    }

    #[test]
    fn verify() {
        let info = parse(GLIBC).unwrap();
        assert!(info.verify(&[key(TEST_KEY)]));
        assert!(info.verify(&[key(NIXOS_KEY)]));
        assert!(info.verify(&[key(NIXOS_KEY), key(TEST_KEY)]));
        assert!(!info.verify(&[]));

        // Right key bytes under the wrong name.
        let renamed = key(&TEST_KEY.replace("test-1", "other"));
        assert!(!info.verify(&[renamed]));

        let keys = [key(TEST_KEY), key(NIXOS_KEY)];
        for tampered in [
            GLIBC.replace("NarSize: 35073128", "NarSize: 35073129"),
            GLIBC.replace(" m07syxhld8hpprrdmzq565ziia6vlw9l-xgcc-15.3.0-libgcc", ""),
            GLIBC.replace("uLy+", "uLz+").replace("tsZF", "tsZG"),
        ] {
            assert!(!parse(&tampered).unwrap().verify(&keys));
        }
    }

    #[test]
    fn public_key() {
        assert_eq!(key(TEST_KEY).name(), "test-1");
        assert!("test-1".parse::<PublicKey>().is_err());
        assert!("test-1:AAAA".parse::<PublicKey>().is_err());
    }

    #[test]
    fn fixture_caches() {
        let Some(dir) = fixtures() else { return };
        let keys = [key(
            &std::fs::read_to_string(dir.join("public.key")).unwrap()
        )];
        for cache in CACHES {
            for (path, info) in fixture_narinfos(&dir, cache) {
                assert!(info.verify(&keys), "{}", path.display());
                let stem = path.file_stem().unwrap().to_str().unwrap();
                assert_eq!(info.path().hash().to_string(), stem);
                if info.compression == Compression::None {
                    let nar = std::fs::read(dir.join(cache).join(&info.url)).unwrap();
                    assert_eq!(nar.len() as u64, info.nar_size());
                    assert_eq!(Algorithm::SHA256.digest(&nar), info.nar_hash());
                }
            }
        }
    }
}
