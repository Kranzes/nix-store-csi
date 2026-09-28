use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use harmonia_file_nar::archive::{NarEvent, parse_nar};
use tokio::io::{AsyncBufReadExt, AsyncRead};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// The most file data the parser hands the writer at once.
const CHUNK: usize = 256 * 1024;

/// The most file data on its way to the writer, so a slow disk can't make an
/// unpack hold much memory.
const IN_FLIGHT: usize = 8 * CHUNK;

enum Op {
    Entry(Entry),
    /// More of the last file, with its share of [`IN_FLIGHT`].
    Data(Vec<u8>, OwnedSemaphorePermit),
}

enum Entry {
    Dir(PathBuf),
    Symlink(PathBuf, PathBuf),
    File(PathBuf, bool),
}

/// `dest` must not exist yet. The parser rejects entry names that would
/// leave `dest`, so a NAR that isn't verified yet can't write elsewhere.
// TODO: Harmonia's `restore` does each file operation and write through
// `tokio::fs`, which hops to a blocking thread every time. Fix that upstream,
// with a cap like `IN_FLIGHT`, and use it again.
pub async fn unpack(reader: impl AsyncRead + Unpin + Send, dest: &Path) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel(64);
    let writer = tokio::task::spawn_blocking(move || write(rx));
    let parsed = parse(reader, dest, tx).await;
    // The writer's error explains a parse that stopped because it went away.
    writer.await??;
    parsed
}

async fn parse(
    reader: impl AsyncRead + Unpin + Send,
    dest: &Path,
    tx: mpsc::Sender<Op>,
) -> anyhow::Result<()> {
    let join = |dir: &Path, name: &[u8]| {
        if name.is_empty() {
            dir.to_owned()
        } else {
            dir.join(OsStr::from_bytes(name))
        }
    };
    let mut dirs = vec![dest.to_owned()];
    let in_flight = Arc::new(Semaphore::new(IN_FLIGHT));
    let mut events = std::pin::pin!(parse_nar(reader));
    while let Some(event) = events.next().await {
        let dir = dirs
            .last()
            .context("NAR ends a directory it didn't start")?;
        match event.context("parsing the NAR")? {
            NarEvent::StartDirectory { name } => {
                let path = join(dir, &name);
                dirs.push(path.clone());
                tx.send(Op::Entry(Entry::Dir(path))).await?;
            }
            NarEvent::EndDirectory => {
                dirs.pop();
            }
            NarEvent::Symlink { name, target } => {
                let target = OsStr::from_bytes(&target).into();
                tx.send(Op::Entry(Entry::Symlink(join(dir, &name), target)))
                    .await?;
            }
            NarEvent::File {
                name,
                executable,
                size,
                mut reader,
            } => {
                tx.send(Op::Entry(Entry::File(join(dir, &name), executable)))
                    .await?;
                let mut left = size;
                loop {
                    // The parser's reads are small, so they go in bigger chunks.
                    let want = usize::try_from(left).map_or(CHUNK, |left| left.min(CHUNK));
                    let permit = (in_flight.clone())
                        .acquire_many_owned(u32::try_from(want)?)
                        .await?;
                    let mut chunk = Vec::with_capacity(want);
                    // Reading on to the end drains the file's padding.
                    while chunk.len() < CHUNK {
                        let buf = reader.fill_buf().await.context("parsing the NAR")?;
                        if buf.is_empty() {
                            break;
                        }
                        let n = buf.len().min(CHUNK - chunk.len());
                        chunk.extend_from_slice(&buf[..n]);
                        reader.consume(n);
                    }
                    left = left.saturating_sub(chunk.len() as u64);
                    let last = chunk.len() < CHUNK;
                    if !chunk.is_empty() {
                        tx.send(Op::Data(chunk, permit)).await?;
                    }
                    if last {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

fn write(mut rx: mpsc::Receiver<Op>) -> anyhow::Result<()> {
    let mut file: Option<(PathBuf, File)> = None;
    while let Some(op) = rx.blocking_recv() {
        let entry = match op {
            Op::Data(chunk, _permit) => {
                let (path, file) = file.as_mut().context("NAR data outside a file")?;
                file.write_all(&chunk)
                    .with_context(|| format!("writing {}", path.display()))?;
                continue;
            }
            Op::Entry(entry) => entry,
        };
        file = None;
        match entry {
            Entry::Dir(path) => std::fs::create_dir(&path)
                .with_context(|| format!("creating {}", path.display()))?,
            Entry::Symlink(path, target) => std::os::unix::fs::symlink(target, &path)
                .with_context(|| format!("creating {}", path.display()))?,
            Entry::File(path, executable) => {
                let created = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(if executable { 0o777 } else { 0o666 })
                    .open(&path)
                    .with_context(|| format!("creating {}", path.display()))?;
                file = Some((path, created));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[derive(Default)]
    pub struct Nar(pub Vec<u8>);

    impl Nar {
        pub fn s(mut self, s: impl AsRef<[u8]>) -> Self {
            let s = s.as_ref();
            self.0.extend((s.len() as u64).to_le_bytes());
            self.0.extend(s);
            self.0.resize(self.0.len().next_multiple_of(8), 0);
            self
        }

        pub fn ss(self, ss: &[&str]) -> Self {
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
