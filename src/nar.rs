use std::path::Path;

use futures::StreamExt;
use harmonia_file_nar::archive::{NarWriteError, parse_nar, restore};
use tokio::io::AsyncRead;

/// `dest` must not exist yet.
pub async fn unpack(reader: impl AsyncRead + Unpin + Send, dest: &Path) -> anyhow::Result<()> {
    let events = parse_nar(reader)
        .map(|event| event.map_err(|e| NarWriteError::create_file_error(dest.to_owned(), e)));
    restore(events, dest).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[derive(Default)]
    struct Nar(Vec<u8>);

    impl Nar {
        fn s(mut self, s: impl AsRef<[u8]>) -> Self {
            let s = s.as_ref();
            self.0.extend((s.len() as u64).to_le_bytes());
            self.0.extend(s);
            self.0.resize(self.0.len().next_multiple_of(8), 0);
            self
        }

        fn ss(self, ss: &[&str]) -> Self {
            ss.iter().fold(self, Nar::s)
        }
    }

    #[tokio::test]
    async fn unpacks_small_nar() {
        let nar = Nar::default()
            .ss(&["nix-archive-1", "(", "type", "directory"])
            .ss(&["entry", "(", "name", "a", "node"])
            .ss(&["(", "type", "regular", "executable", "", "contents"])
            .s("hello")
            .ss(&[")", ")"])
            .ss(&["entry", "(", "name", "b", "node"])
            .ss(&["(", "type", "symlink", "target", "a", ")", ")"])
            .ss(&["entry", "(", "name", "c", "node"])
            .ss(&["(", "type", "directory", ")", ")"])
            .s(")");
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out");
        unpack(&nar.0[..], &dest).await.unwrap();

        assert_eq!(std::fs::read(dest.join("a")).unwrap(), b"hello");
        let mode = std::fs::metadata(dest.join("a"))
            .unwrap()
            .permissions()
            .mode();
        assert!(mode & 0o111 != 0, "{mode:o}");
        assert_eq!(std::fs::read_link(dest.join("b")).unwrap(), Path::new("a"));
        assert!(dest.join("c").is_dir());
    }

    #[tokio::test]
    async fn unpacks_single_file() {
        let nar = Nar::default()
            .ss(&["nix-archive-1", "(", "type", "regular", "contents"])
            .s("just a file")
            .s(")");
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out");
        unpack(&nar.0[..], &dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"just a file");
    }

    #[tokio::test]
    async fn rejects_malformed() {
        let tmp = tempfile::tempdir().unwrap();
        let truncated = Nar::default().ss(&["nix-archive-1", "(", "type", "regular"]);
        for (what, nar) in [
            ("not a NAR", Nar::default().s("hello")),
            ("truncated", truncated),
        ] {
            let dest = tmp.path().join(what);
            assert!(unpack(&nar.0[..], &dest).await.is_err(), "{what}");
        }
    }
}
