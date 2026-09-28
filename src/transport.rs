use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use backon::{ExponentialBuilder, Retryable};
use futures::StreamExt;
use reqwest::StatusCode;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use url::Url;

pub struct Transport {
    kind: Kind,
}

enum Kind {
    File(PathBuf),
    Http { base: Url, client: reqwest::Client },
}

/// Caps HTTP bodies from [`Transport::get`], which only fetches small files like narinfos.
const MAX_GET: u64 = 1 << 20;

const BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_millis(200))
    .with_max_delay(Duration::from_secs(5))
    .with_total_delay(Some(Duration::from_mins(1)))
    .without_max_times();

struct Failure {
    error: anyhow::Error,
    transient: bool,
}

impl From<reqwest::Error> for Failure {
    fn from(e: reqwest::Error) -> Self {
        Self {
            transient: e.is_connect() || e.is_timeout() || e.is_request() || e.is_body(),
            error: e.into(),
        }
    }
}

fn check_status(resp: &reqwest::Response) -> Result<(), Failure> {
    let status = resp.status();
    if status == StatusCode::OK {
        return Ok(());
    }
    Err(Failure {
        error: anyhow::anyhow!("{} returned {status}", resp.url()),
        transient: status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT,
    })
}

/// Same as Nix, which counts 403 because S3 answers it for a missing file.
fn is_missing(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::NOT_FOUND | StatusCode::FORBIDDEN | StatusCode::GONE
    )
}

async fn retry<T, F, Fut>(url: &str, attempt: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    attempt
        .retry(BACKOFF)
        .when(|f| f.transient)
        .notify(|f, delay| {
            tracing::warn!("fetching {url}: {:#}; retrying in {delay:?}", f.error);
        })
        .await
        .map_err(|f| f.error.context(format!("fetching {url}")))
}

/// Whether `e` is a response body breaking off, which a new request may fix.
pub fn broke_off(e: &anyhow::Error) -> bool {
    e.chain().any(|e| {
        let inner = e.downcast_ref::<io::Error>().and_then(io::Error::get_ref);
        matches!(inner, Some(inner) if inner.is::<reqwest::Error>())
    })
}

fn client() -> anyhow::Result<reqwest::Client> {
    // reqwest loads the system root certificates, so `SSL_CERT_FILE` and
    // `SSL_CERT_DIR` work.
    reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(Duration::from_mins(1))
        .build()
        .context("building HTTP client")
}

impl Transport {
    pub fn new(url: &Url) -> anyhow::Result<Self> {
        let mut base = url.clone();
        // Store settings like `?priority=40` don't affect fetching, and the
        // store reads `priority` itself.
        base.set_query(None);
        base.set_fragment(None);
        let kind = match base.scheme() {
            "file" => Kind::File(
                base.to_file_path()
                    .ok()
                    .with_context(|| format!("{url} is not an absolute local file URL"))?,
            ),
            "http" | "https" => Kind::Http {
                base,
                client: client()?,
            },
            _ => bail!("unsupported store URL {url}: expected http(s):// or file://"),
        };
        Ok(Self { kind })
    }

    pub fn is_local(&self) -> bool {
        matches!(self.kind, Kind::File(_))
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
        check_path(path)?;
        match &self.kind {
            Kind::File(base) => {
                let file = base.join(path);
                match tokio::fs::read(&file).await {
                    Ok(data) => Ok(Some(data)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("reading {}", file.display())),
                }
            }
            Kind::Http { base, client } => {
                let url = request_url(base, path);
                retry(&url, || async {
                    let mut resp = client.get(&url).send().await?;
                    if is_missing(resp.status()) {
                        return Ok(None);
                    }
                    check_status(&resp)?;
                    // Decoded bodies can be far bigger than what was sent.
                    let mut body = Vec::new();
                    while let Some(chunk) = resp.chunk().await? {
                        if (body.len() + chunk.len()) as u64 > MAX_GET {
                            return Err(Failure {
                                error: anyhow::anyhow!("{url} is larger than {MAX_GET} bytes"),
                                transient: false,
                            });
                        }
                        body.extend_from_slice(&chunk);
                    }
                    Ok(Some(body))
                })
                .await
            }
        }
    }

    /// Retries stop once the response starts, so a later failure is a read
    /// error that [`broke_off`] matches, and the caller has to start over.
    pub async fn stream(&self, path: &str) -> anyhow::Result<Box<dyn AsyncRead + Send + Unpin>> {
        check_path(path)?;
        match &self.kind {
            Kind::File(base) => {
                let src = base.join(path);
                let file = tokio::fs::File::open(&src)
                    .await
                    .with_context(|| format!("opening {}", src.display()))?;
                Ok(Box::new(file))
            }
            Kind::Http { base, client } => {
                let url = request_url(base, path);
                let resp = retry(&url, || async {
                    let resp = client.get(&url).send().await?;
                    check_status(&resp)?;
                    Ok(resp)
                })
                .await?;
                let body = resp.bytes_stream().map(|r| r.map_err(io::Error::other));
                Ok(Box::new(StreamReader::new(body)))
            }
        }
    }
}

fn request_url(base: &Url, path: &str) -> String {
    let mut url = base.clone();
    url.path_segments_mut()
        .expect("http URLs have a path")
        .pop_if_empty()
        .extend(path.split('/'));
    url.into()
}

/// Paths come from narinfos, so reject any that would escape the cache.
fn check_path(path: &str) -> anyhow::Result<()> {
    ensure!(
        !path.starts_with('/') && !path.split('/').any(|c| c == ".." || c.is_empty()),
        "bad path in binary cache: {path:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::*;

    const BODY: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    fn open(url: &str) -> anyhow::Result<Transport> {
        Transport::new(&url.parse()?)
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Normal,
        FlakyOnce,
        Truncated,
    }

    async fn server(mode: Mode) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let hits = hits.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = sock.into_split();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut request_line = String::new();
                        if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let path = request_line.split(' ').nth(1).unwrap();
                        // The same files under `/prefix`, for bases with a path.
                        let path = path.strip_prefix("/prefix").unwrap_or(path).to_owned();
                        loop {
                            let mut line = String::new();
                            reader.read_line(&mut line).await.unwrap();
                            if line == "\r\n" {
                                break;
                            }
                        }
                        let hit = hits.fetch_add(1, Ordering::SeqCst);
                        let (status, body) = match (path.as_str(), mode) {
                            (_, Mode::FlakyOnce) if hit == 0 => {
                                ("503 Service Unavailable", Vec::new())
                            }
                            ("/obj", _) => ("200 OK", BODY.to_vec()),
                            ("/big", _) => {
                                ("200 OK", vec![0; usize::try_from(MAX_GET).unwrap() + 1])
                            }
                            _ => ("404 Not Found", Vec::new()),
                        };
                        let head = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n",
                            body.len()
                        );
                        writer.write_all(head.as_bytes()).await.unwrap();
                        if mode == Mode::Truncated {
                            let _ = writer
                                .write_all(&body[..body.len().saturating_sub(1)])
                                .await;
                            return;
                        }
                        writer.write_all(&body).await.unwrap();
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn http_retries() {
        let t = open(&server(Mode::FlakyOnce).await).unwrap();
        assert_eq!(t.get("obj").await.unwrap().unwrap(), BODY);
    }

    #[tokio::test]
    async fn http_base_url() {
        let server = server(Mode::Normal).await;
        for base in [
            format!("{server}?priority=40&trusted=1"),
            format!("{server}/prefix"),
            format!("{server}/prefix/"),
            format!("{server}/prefix/?priority=40"),
        ] {
            let t = open(&base).unwrap();
            assert_eq!(t.get("obj").await.unwrap().unwrap(), BODY, "{base}");
        }
    }

    #[test]
    fn request_urls() {
        let base = Url::parse("https://host/some/prefix/").unwrap();
        assert_eq!(
            request_url(&base, "nar/abc.nar.xz"),
            "https://host/some/prefix/nar/abc.nar.xz"
        );
        assert_eq!(
            request_url(&base, "nar/a b?#%.nar"),
            "https://host/some/prefix/nar/a%20b%3F%23%25.nar"
        );
    }

    async fn read_all(t: &Transport, path: &str) -> anyhow::Result<Vec<u8>> {
        let mut out = Vec::new();
        t.stream(path).await?.read_to_end(&mut out).await?;
        Ok(out)
    }

    #[tokio::test]
    async fn http_get_and_stream() {
        let base = server(Mode::Normal).await;
        let t = open(&base).unwrap();
        assert_eq!(t.get("obj").await.unwrap().unwrap(), BODY);
        let err = t.get("big").await.unwrap_err();
        assert!(format!("{err:#}").contains("larger than"), "{err:#}");

        assert_eq!(read_all(&t, "big").await.unwrap().len() as u64, MAX_GET + 1);
        assert_eq!(read_all(&t, "obj").await.unwrap(), BODY);
        let err = read_all(&t, "missing").await.unwrap_err();
        assert!(!broke_off(&err), "{err:#}");
    }

    #[tokio::test]
    async fn http_broken_body() {
        let t = open(&server(Mode::Truncated).await).unwrap();
        let err = read_all(&t, "obj").await.unwrap_err();
        assert!(broke_off(&err), "{err:#}");
    }

    #[tokio::test]
    async fn file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("nar")).unwrap();
        std::fs::write(dir.path().join("nar/x.nar"), BODY).unwrap();
        let t = open(&format!("file://{}/", dir.path().display())).unwrap();
        assert_eq!(t.get("nar/x.nar").await.unwrap().unwrap(), BODY);
        assert_eq!(t.get("nar/y.nar").await.unwrap(), None);
        assert!(t.get("../x").await.is_err());
        assert!(t.get("/etc/passwd").await.is_err());

        assert_eq!(read_all(&t, "nar/x.nar").await.unwrap(), BODY);
        assert!(read_all(&t, "nar/y.nar").await.is_err());
    }

    #[tokio::test]
    async fn file_url_decoding() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("my cache")).unwrap();
        std::fs::write(dir.path().join("my cache/x"), BODY).unwrap();
        let dir = dir.path().display();
        for url in [
            format!("file://{dir}/my%20cache"),
            format!("file://{dir}/my%20cache/?compression=zstd"),
            format!("file://localhost{dir}/my%20cache"),
        ] {
            let t = open(&url).unwrap();
            assert_eq!(t.get("x").await.unwrap().unwrap(), BODY, "{url}");
        }
    }

    #[test]
    fn rejected_urls() {
        for (url, msg) in [
            ("file://relative", "not an absolute local file URL"),
            ("file://host/x", "not an absolute local file URL"),
            ("s3://bucket?region=eu-west-1", "unsupported store URL"),
            ("ssh://host", "unsupported store URL"),
        ] {
            let Err(err) = open(url) else {
                panic!("{url} accepted");
            };
            assert!(err.to_string().contains(msg), "{err:#}");
        }
    }

    /// cache.nixos.org serves narinfos, and NARs byte for byte.
    #[tokio::test]
    async fn cache_nixos_org() {
        if !crate::narinfo::tests::network_tests() {
            return;
        }
        let t = open("https://cache.nixos.org").unwrap();
        let narinfo = t
            .get("xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi.narinfo")
            .await
            .unwrap()
            .unwrap();
        assert!(narinfo.starts_with(b"StorePath: "));
        assert_eq!(
            t.get("00000000000000000000000000000000.narinfo")
                .await
                .unwrap(),
            None
        );

        let path = "nar/1ixxaz0n8nfxd2w29qq8frn6pjvdnjj3pl139gbi7jksrj9jd7jg.nar.zst";
        let zst = read_all(&t, path).await.unwrap();
        let mut nar = Vec::new();
        async_compression::tokio::bufread::ZstdDecoder::new(&zst[..])
            .read_to_end(&mut nar)
            .await
            .unwrap();
        assert!(nar.starts_with(b"\x0d\0\0\0\0\0\0\0nix-archive-1"));
    }
}
