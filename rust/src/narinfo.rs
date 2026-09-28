use std::collections::BTreeSet;
use std::pin::Pin;

use anyhow::{Context, bail, ensure};
use async_compression::tokio::bufread::{
    BrotliDecoder, BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder,
};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_store_path_info::fingerprint_path;
use harmonia_utils_hash::Hash;
use tokio::io::{AsyncBufRead, AsyncRead};

pub use harmonia_utils_signature::PublicKey;

/// Far longer than any cache's NAR URLs. Signatures don't cover the URL, so
/// without a cap a cache could make each narinfo the node store keeps huge.
const MAX_URL: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Xz,
    Zstd,
    Bzip2,
    Gzip,
    Brotli,
}

impl Compression {
    /// Parses Nix's names.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            "none" => Self::None,
            "xz" => Self::Xz,
            "zstd" => Self::Zstd,
            "bzip2" => Self::Bzip2,
            "gzip" => Self::Gzip,
            "br" => Self::Brotli,
            other => bail!("unsupported compression {other:?}"),
        })
    }

    /// Accepts concatenated streams, like Nix.
    pub fn decoder(
        self,
        body: impl AsyncBufRead + Send + Unpin + 'static,
    ) -> Pin<Box<dyn AsyncRead + Send>> {
        macro_rules! decoder {
            ($decoder:ident) => {{
                let mut d = $decoder::new(body);
                d.multiple_members(true);
                Box::pin(d)
            }};
        }
        match self {
            Self::None => Box::pin(body),
            Self::Xz => decoder!(XzDecoder),
            Self::Zstd => decoder!(ZstdDecoder),
            Self::Bzip2 => decoder!(BzDecoder),
            Self::Gzip => decoder!(GzipDecoder),
            Self::Brotli => decoder!(BrotliDecoder),
        }
    }
}

/// Signatures cover the fields behind the accessors, not `url` or `compression`.
pub struct NarInfo {
    /// Relative to the cache's base URL, like `nar/<filehash>.nar.xz`.
    pub url: String,
    pub compression: Compression,
    parsed: harmonia_store_nar_info::NarInfo,
}

impl NarInfo {
    pub fn parse(store_dir: &StoreDir, text: &str) -> anyhow::Result<Self> {
        let parsed = harmonia_store_nar_info::parse_narinfo_txt(store_dir, text)?;
        let url = parsed.info.url.clone().context("narinfo has no URL")?;
        ensure!(!url.is_empty(), "narinfo has an empty URL");
        ensure!(
            url.len() <= MAX_URL,
            "narinfo has a URL longer than {MAX_URL} bytes"
        );
        // Nix uses bzip2 when the field is missing or empty.
        let name = (parsed.info.compression.as_deref())
            .filter(|c| !c.is_empty())
            .unwrap_or("bzip2");
        let compression = Compression::parse(name)?;
        Ok(Self {
            url,
            compression,
            parsed,
        })
    }

    /// The narinfo for the node store to keep. A cache can add lines that no
    /// signature covers, so this writes only the fields Harmonia knows.
    pub fn to_text(&self) -> Vec<u8> {
        harmonia_store_nar_info::format_narinfo_txt(&self.parsed.info.info.store_dir, &self.parsed)
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

    /// Returns whether one of `keys` signed it, and drops its signatures.
    pub fn check_signatures(&mut self, keys: &[PublicKey]) -> bool {
        let info = &mut self.parsed.info.info;
        let signatures = std::mem::take(&mut info.signatures);
        let fingerprint = fingerprint_path(
            &info.store_dir,
            &self.parsed.path,
            &info.nar_hash,
            info.nar_size,
            &info.references,
        );
        signatures.iter().any(|sig| {
            keys.iter()
                .any(|k| k.name() == sig.name() && k.verify(&fingerprint, sig))
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::store_path;

    pub const TEST_KEY: &str = "test-1:moc/aM6YNJhbirrTIvIK1J2tTvPdwtGvnUH1tzsF7ZI=";
    pub const NIXOS_KEY: &str = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";

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

    /// The caches from `nix/fixtures.nix`. The dev shell and the package build
    /// point `NIX_STORE_CSI_FIXTURES` at them.
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

    /// Each narinfo file in fixture cache `cache`, with its text.
    pub fn fixture_narinfos(dir: &Path, cache: &str) -> Vec<(PathBuf, String, NarInfo)> {
        let infos: Vec<_> = std::fs::read_dir(dir.join(cache))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "narinfo"))
            .map(|p| {
                let text = std::fs::read_to_string(&p).unwrap();
                let info = parse(&text).unwrap();
                (p, text, info)
            })
            .collect();
        let all = std::fs::read_to_string(dir.join("closure")).unwrap();
        assert_eq!(infos.len(), all.lines().count(), "{cache}");
        infos
    }

    pub fn fixture_narinfo(
        dir: &Path,
        cache: &str,
        path: &StorePath,
    ) -> (PathBuf, String, NarInfo) {
        (fixture_narinfos(dir, cache).into_iter())
            .find(|(_, _, info)| info.path() == path)
            .unwrap()
    }

    /// Copies the narinfos of fixture cache `from` into `to`, with `edit`
    /// applied to each.
    pub fn copy_narinfos(dir: &Path, from: &str, to: &Path, edit: fn(String) -> String) {
        for (file, text, _) in fixture_narinfos(dir, from) {
            std::fs::write(to.join(file.file_name().unwrap()), edit(text)).unwrap();
        }
    }

    /// Writes the narinfos of fixture cache `from` for `paths` into `to`, with
    /// `edit` applied to each.
    pub fn write_narinfos(
        dir: &Path,
        from: &str,
        to: &Path,
        paths: &[&StorePath],
        edit: impl Fn(String) -> String,
    ) {
        for (file, text, info) in fixture_narinfos(dir, from) {
            if paths.contains(&info.path()) {
                std::fs::write(to.join(file.file_name().unwrap()), edit(text)).unwrap();
            }
        }
    }

    pub fn without_lines(text: &str, prefix: &str) -> String {
        (text.lines())
            .filter(|l| !l.starts_with(prefix))
            .flat_map(|l| [l, "\n"])
            .collect()
    }

    pub fn fixture_root(dir: &Path, file: &str) -> StorePath {
        let path = std::fs::read_to_string(dir.join(file)).unwrap();
        store_path::parse(&StoreDir::default(), path.trim()).unwrap()
    }

    pub fn fixture_path(dir: &Path, pname: &str) -> StorePath {
        fixture_narinfos(dir, "cache-none")
            .into_iter()
            .map(|(_, _, info)| info.path().clone())
            .find(|path| path.name().to_string().starts_with(&format!("{pname}-")))
            .unwrap_or_else(|| panic!("no {pname} in the fixtures"))
    }

    /// A binary cache with no narinfos, in a temporary directory.
    pub fn empty_cache() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nix-cache-info"), "StoreDir: /nix/store\n").unwrap();
        dir
    }

    pub fn file_store(dir: &Path) -> String {
        format!("file://{}", dir.display())
    }

    fn key(s: &str) -> PublicKey {
        s.trim().parse().unwrap()
    }

    fn parse(text: &str) -> anyhow::Result<NarInfo> {
        NarInfo::parse(&StoreDir::default(), text)
    }

    #[test]
    fn parse_defaults_and_errors() {
        let minimal = "StorePath: /nix/store/lm3pknxi0ipypy3lxh1wmm8wvvavdwrn-glibc-2.42-84\n\
                       URL: nar/x.nar.bz2\nNarSize: 1\n\
                       NarHash: sha256:06kkplxk31y11zksgcrdjv0hf6ymlmxvlc0i2h08g3zc22is465s\n";
        let info = parse(minimal).unwrap();
        assert_eq!(info.compression, Compression::Bzip2);

        assert!(parse(&format!("{minimal}Compression: lz4\n")).is_err());
        assert!(parse(&minimal.replace("URL: nar/x.nar.bz2\n", "")).is_err());
        let other = StoreDir::new("/other/store").unwrap();
        assert!(NarInfo::parse(&other, minimal).is_err());
        let long = format!("nar/{}.nar", "x".repeat(MAX_URL));
        assert!(parse(&minimal.replace("nar/x.nar.bz2", &long)).is_err());
    }

    #[test]
    fn verify() {
        let verify = |text: &str, keys: &[PublicKey]| parse(text).unwrap().check_signatures(keys);
        assert!(verify(GLIBC, &[key(TEST_KEY)]));
        assert!(verify(GLIBC, &[key(NIXOS_KEY)]));
        assert!(!verify(GLIBC, &[]));

        // The right key bytes under the wrong name.
        let renamed = key(&TEST_KEY.replace("test-1", "other"));
        assert!(!verify(GLIBC, &[renamed]));

        let keys = [key(TEST_KEY), key(NIXOS_KEY)];
        for tampered in [
            GLIBC.replace("NarSize: 35073128", "NarSize: 35073129"),
            GLIBC.replace(" m07syxhld8hpprrdmzq565ziia6vlw9l-xgcc-15.3.0-libgcc", ""),
        ] {
            assert!(!verify(&tampered, &keys));
        }
    }

    /// The node store keeps only the fields Harmonia knows, without
    /// signatures.
    #[test]
    fn writes_verified_narinfos() {
        let mut info = parse(&format!("{GLIBC}Padding: {}\n", "x".repeat(100))).unwrap();
        assert!(info.check_signatures(&[key(TEST_KEY), key(NIXOS_KEY)]));
        let text = String::from_utf8(info.to_text()).unwrap();
        assert_eq!(text, without_lines(GLIBC, "Sig:"));
    }
}
