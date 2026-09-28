use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use backon::{ExponentialBuilder, Retryable};
use futures::StreamExt;
use percent_encoding::percent_decode_str;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, ETAG, HeaderValue, IF_RANGE, LAST_MODIFIED, RANGE};
use tokio::io::{AsyncBufRead, AsyncReadExt, BufReader};
use tokio::time::Instant;
use tokio_util::io::StreamReader;
use url::Url;

use crate::narinfo::Compression;

pub struct Transport {
    kind: Kind,
}

enum Kind {
    File(PathBuf),
    Http(Http),
}

#[derive(Clone)]
struct Http {
    base: Url,
    client: reqwest::Client,
    /// User and password from the URL, which `base` leaves out so that no
    /// message showing a URL can leak them.
    auth: Option<(String, Option<String>)>,
}

impl Http {
    fn get(&self, url: &Url) -> reqwest::RequestBuilder {
        let request = self.client.get(url.clone());
        match &self.auth {
            Some((user, password)) => request.basic_auth(user, password.as_ref()),
            None => request,
        }
    }

    /// Retries until a response starts, which has to be 200, or 206 for the part
    /// from byte `from` of the file that `validator` names.
    async fn fetch(
        &self,
        url: &Url,
        range: Option<(u64, &HeaderValue)>,
    ) -> anyhow::Result<reqwest::Response> {
        retry(url, || async {
            let mut request = self.get(url);
            if let Some((from, validator)) = range {
                request =
                    (request.header(RANGE, format!("bytes={from}-"))).header(IF_RANGE, validator);
            }
            let resp = request.send().await?;
            if !(range.is_some() && resp.status() == StatusCode::PARTIAL_CONTENT) {
                check_status(&resp)?;
            }
            Ok(resp)
        })
        .await
    }
}

/// Caps HTTP bodies from [`Transport::get`], which only fetches small files like narinfos.
const MAX_GET: u64 = 1 << 20;

/// Each read of a local file is a trip to a blocking thread.
const FILE_BUF: usize = 256 * 1024;

/// How long a request keeps retrying. backon's total delay would count only
/// the sleeps, so a cache that times out every attempt would take far longer.
const RETRY_FOR: Duration = Duration::from_mins(1);

const BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_millis(200))
    .with_max_delay(Duration::from_secs(5))
    .without_max_times();

struct Failure {
    error: anyhow::Error,
    transient: bool,
}

impl From<reqwest::Error> for Failure {
    fn from(e: reqwest::Error) -> Self {
        Self {
            // reqwest reports a body that breaks off as a decode error.
            transient: e.is_connect()
                || e.is_timeout()
                || e.is_request()
                || e.is_body()
                || e.is_decode(),
            // retry() adds the URL.
            error: e.without_url().into(),
        }
    }
}

impl From<anyhow::Error> for Failure {
    fn from(error: anyhow::Error) -> Self {
        Self {
            error,
            transient: false,
        }
    }
}

/// Nix sets it on the narinfos it compresses on S3.
fn content_encoding(resp: &reqwest::Response) -> anyhow::Result<Compression> {
    let Some(value) = resp.headers().get(reqwest::header::CONTENT_ENCODING) else {
        return Ok(Compression::None);
    };
    match value.to_str()? {
        "identity" => Ok(Compression::None),
        value => Compression::parse(value).context("Content-Encoding"),
    }
}

/// Decoding can make a body far bigger, so the cap applies again.
async fn decode(body: Vec<u8>, encoding: Compression) -> anyhow::Result<Vec<u8>> {
    if encoding == Compression::None {
        return Ok(body);
    }
    let mut data = Vec::new();
    encoding
        .decoder(io::Cursor::new(body))
        .take(MAX_GET + 1)
        .read_to_end(&mut data)
        .await
        .context("decoding the body")?;
    ensure!(
        data.len() as u64 <= MAX_GET,
        "body decodes to more than {MAX_GET} bytes"
    );
    Ok(data)
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

async fn retry<T, F, Fut>(url: &Url, attempt: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    let start = Instant::now();
    attempt
        .retry(BACKOFF)
        .when(|f| f.transient && start.elapsed() < RETRY_FOR)
        .notify(|f, delay| {
            tracing::warn!("fetching {url}: {:#}; retrying in {delay:?}", f.error);
        })
        .await
        .map_err(|f| f.error.context(format!("fetching {url}")))
}

/// `url` without its user info, which can hold a password or token.
pub fn redact(url: &Url) -> Url {
    let mut url = url.clone();
    // These fail only for URLs that can't have user info.
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url
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
        // The default windows hold a NAR download to about half the link's speed.
        .http2_adaptive_window(true)
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
                    .context("not an absolute local file URL")?,
            ),
            "http" | "https" => {
                let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
                let auth = (!base.username().is_empty() || base.password().is_some())
                    .then(|| (decode(base.username()), base.password().map(decode)));
                Kind::Http(Http {
                    base: redact(&base),
                    client: client()?,
                    auth,
                })
            }
            scheme => bail!("unsupported scheme {scheme}: expected http(s):// or file://"),
        };
        Ok(Self { kind })
    }

    pub fn is_local(&self) -> bool {
        matches!(self.kind, Kind::File(_))
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match &self.kind {
            Kind::File(base) => {
                let file = file_path(base, path)?;
                match tokio::fs::read(&file).await {
                    Ok(data) => Ok(Some(data)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("reading {}", file.display())),
                }
            }
            Kind::Http(http) => {
                let url = request_url(&http.base, path)?;
                retry(&url, || async {
                    let mut resp = http.get(&url).send().await?;
                    if is_missing(resp.status()) {
                        return Ok(None);
                    }
                    check_status(&resp)?;
                    let encoding = content_encoding(&resp)?;
                    let mut body = Vec::new();
                    while let Some(chunk) = resp.chunk().await? {
                        if (body.len() + chunk.len()) as u64 > MAX_GET {
                            return Err(
                                anyhow::anyhow!("body is larger than {MAX_GET} bytes").into()
                            );
                        }
                        body.extend_from_slice(&chunk);
                    }
                    Ok(Some(decode(body, encoding).await?))
                })
                .await
            }
        }
    }

    /// Retries stop once the response starts, so a later failure is a read
    /// error that [`broke_off`] matches, and the caller has to start over.
    pub async fn stream(&self, path: &str) -> anyhow::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        match &self.kind {
            Kind::File(base) => {
                let src = file_path(base, path)?;
                let file = tokio::fs::File::open(&src)
                    .await
                    .with_context(|| format!("opening {}", src.display()))?;
                Ok(Box::new(BufReader::with_capacity(FILE_BUF, file)))
            }
            Kind::Http(http) => {
                let url = request_url(&http.base, path)?;
                let resp = http.fetch(&url, None).await?;
                Ok(resumable(http.clone(), url, resp))
            }
        }
    }
}

/// A NAR body that [`resumable`] picks up where it broke off.
struct Resuming<S> {
    http: Http,
    url: Url,
    /// Names the file, so a resume can't mix two versions of it.
    validator: Option<HeaderValue>,
    body: S,
    read: u64,
    read_since_resume: u64,
}

/// The body of `resp`. When it breaks off, a range request picks it up where
/// it stopped, if the server supports that and the file is unchanged, as long
/// as each try gets further. Otherwise reading fails with an error that
/// [`broke_off`] matches.
fn resumable(
    http: Http,
    url: Url,
    resp: reqwest::Response,
) -> Box<dyn AsyncBufRead + Send + Unpin> {
    let headers = resp.headers();
    // A weak ETag doesn't promise the same bytes, so If-Range can't take one.
    let validator = (headers.get(ETAG))
        .filter(|etag| !etag.as_bytes().starts_with(b"W/"))
        .or_else(|| headers.get(LAST_MODIFIED))
        .cloned();
    let state = Resuming {
        http,
        url,
        validator,
        body: resp.bytes_stream().boxed(),
        read: 0,
        read_since_resume: 0,
    };
    let chunks = futures::stream::try_unfold(state, |mut s| async move {
        loop {
            let error = match s.body.next().await {
                None => return Ok(None),
                Some(Ok(chunk)) => {
                    s.read += chunk.len() as u64;
                    s.read_since_resume += chunk.len() as u64;
                    return Ok(Some((chunk, s)));
                }
                Some(Err(e)) => e,
            };
            let resumed = match &s.validator {
                Some(validator) if s.read_since_resume > 0 => {
                    resume(&s.http, &s.url, validator, s.read).await
                }
                _ => None,
            };
            let Some(resp) = resumed else {
                return Err(io::Error::other(error));
            };
            tracing::warn!("fetching {}: {error}; resumed at byte {}", s.url, s.read);
            s.body = resp.bytes_stream().boxed();
            s.read_since_resume = 0;
        }
    });
    Box::new(StreamReader::new(chunks.boxed()))
}

/// Asks for what follows byte `from`, if the file is still the one that
/// `validator` names.
async fn resume(
    http: &Http,
    url: &Url,
    validator: &HeaderValue,
    from: u64,
) -> Option<reqwest::Response> {
    let resp = http.fetch(url, Some((from, validator))).await.ok()?;
    // The whole file means it changed, or the server ignores ranges.
    let range = resp.headers().get(CONTENT_RANGE)?.to_str().ok()?;
    (resp.status() == StatusCode::PARTIAL_CONTENT && range.starts_with(&format!("bytes {from}-")))
        .then_some(resp)
}

/// Keeps a query in `path`, since Harmonia's narinfos put one in their NAR
/// URLs and Nix passes it on.
fn request_url(base: &Url, path: &str) -> anyhow::Result<Url> {
    let (path, query) = path
        .split_once('?')
        .map_or((path, None), |(p, q)| (p, Some(q)));
    check_path(path)?;
    let mut url = base.clone();
    url.path_segments_mut()
        .expect("http URLs have a path")
        .pop_if_empty()
        .extend(path.split('/'));
    url.set_query(query);
    Ok(url)
}

fn file_path(base: &std::path::Path, path: &str) -> anyhow::Result<PathBuf> {
    check_path(path)?;
    Ok(base.join(path))
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
pub(crate) mod tests {
    use std::fmt::Write as _;
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
    pub(crate) enum Mode {
        Normal,
        FlakyOnce,
        Truncated,
        /// `/obj` breaks off halfway the first time.
        BreaksOnce {
            ranges: bool,
        },
        /// A cache that never answers but for `/nix-cache-info`.
        Hangs,
    }

    /// Where a `Range: bytes=N-` header starts.
    fn range_start(line: &str) -> Option<usize> {
        let line = line.to_ascii_lowercase();
        let from = line.strip_prefix("range: bytes=")?.trim_end();
        from.strip_suffix('-')?.parse().ok()
    }

    pub(crate) async fn server(mode: Mode) -> String {
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
                        let mut authorized = false;
                        let mut range = None;
                        loop {
                            let mut line = String::new();
                            reader.read_line(&mut line).await.unwrap();
                            if line == "\r\n" {
                                break;
                            }
                            // user:secret
                            authorized |= line
                                .eq_ignore_ascii_case("authorization: basic dxnlcjpzzwnyzxq=\r\n");
                            range = range.or_else(|| range_start(&line));
                        }
                        let hit = hits.fetch_add(1, Ordering::SeqCst);
                        let mut encoding = "identity";
                        let mut headers = String::new();
                        let (mut status, mut body) = match (path.as_str(), mode) {
                            ("/nix-cache-info", Mode::Hangs) => {
                                ("200 OK", b"StoreDir: /nix/store\n".to_vec())
                            }
                            (_, Mode::Hangs) => std::future::pending().await,
                            (_, Mode::FlakyOnce) if hit == 0 => {
                                ("503 Service Unavailable", Vec::new())
                            }
                            ("/private", _) if !authorized => ("401 Unauthorized", Vec::new()),
                            ("/private" | "/obj" | "/harmonia.nar?hash=x", _) => {
                                ("200 OK", BODY.to_vec())
                            }
                            ("/big", _) => {
                                ("200 OK", vec![0; usize::try_from(MAX_GET).unwrap() + 1])
                            }
                            ("/xz" | "/xz-bomb", _) => {
                                encoding = "xz";
                                let size = if path == "/xz" {
                                    BODY.len()
                                } else {
                                    usize::try_from(MAX_GET).unwrap() + 1
                                };
                                let plain =
                                    BODY.iter().copied().cycle().take(size).collect::<Vec<_>>();
                                let mut xz = Vec::new();
                                async_compression::tokio::bufread::XzEncoder::new(&plain[..])
                                    .read_to_end(&mut xz)
                                    .await
                                    .unwrap();
                                ("200 OK", xz)
                            }
                            _ => ("404 Not Found", Vec::new()),
                        };
                        if let Mode::BreaksOnce { ranges } = mode
                            && path == "/obj"
                        {
                            headers.push_str("ETag: \"x\"\r\n");
                            if let Some(from) = range.filter(|_| ranges) {
                                status = "206 Partial Content";
                                let (last, len) = (BODY.len() - 1, BODY.len());
                                write!(headers, "Content-Range: bytes {from}-{last}/{len}\r\n")
                                    .unwrap();
                                body = BODY[from..].to_vec();
                            }
                        }
                        let head = format!(
                            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Encoding: {encoding}\r\n{headers}\r\n",
                            body.len()
                        );
                        writer.write_all(head.as_bytes()).await.unwrap();
                        let sent = match mode {
                            Mode::BreaksOnce { .. } if hit == 0 => body.len() / 2,
                            Mode::Truncated => body.len().saturating_sub(1),
                            _ => body.len(),
                        };
                        if writer.write_all(&body[..sent]).await.is_err() || sent < body.len() {
                            return;
                        }
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
        for (path, url) in [
            ("nar/abc.nar.xz", "https://host/some/prefix/nar/abc.nar.xz"),
            (
                "nar/a b#%.nar",
                "https://host/some/prefix/nar/a%20b%23%25.nar",
            ),
            (
                "nar/abc.nar?hash=xyz",
                "https://host/some/prefix/nar/abc.nar?hash=xyz",
            ),
        ] {
            assert_eq!(request_url(&base, path).unwrap().as_str(), url);
        }
        for path in ["../x", "/x", "nar//x", "../x?y"] {
            assert!(request_url(&base, path).is_err(), "{path}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_for_a_minute() {
        let url = Url::parse("http://host/x").unwrap();
        for attempt_takes in [0, 15, 60].map(Duration::from_secs) {
            let start = Instant::now();
            let err = retry(&url, || async {
                tokio::time::sleep(attempt_takes).await;
                Err::<(), _>(Failure {
                    error: anyhow::anyhow!("timed out"),
                    transient: true,
                })
            })
            .await
            .unwrap_err();
            assert!(format!("{err:#}").contains("timed out"), "{err:#}");
            let elapsed = start.elapsed();
            assert!(elapsed >= RETRY_FOR, "{attempt_takes:?}: {elapsed:?}");
            assert!(
                elapsed <= RETRY_FOR + attempt_takes + Duration::from_secs(5),
                "{attempt_takes:?}: {elapsed:?}"
            );
        }
    }

    #[tokio::test]
    async fn http_credentials() {
        let server = server(Mode::Normal).await;
        assert!(open(&server).unwrap().get("private").await.is_err());
        let t = open(&server.replace("://", "://user:secret@")).unwrap();
        assert_eq!(t.get("private").await.unwrap().unwrap(), BODY);
        assert_eq!(read_all(&t, "private").await.unwrap(), BODY);
        // No message shows them.
        for err in [
            t.get("big").await.unwrap_err(),
            read_all(&t, "missing").await.unwrap_err(),
        ] {
            let msg = format!("{err:#}");
            assert!(
                msg.contains("127.0.0.1") && !msg.contains("secret"),
                "{msg}"
            );
        }
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
        assert_eq!(t.get("xz").await.unwrap().unwrap(), BODY);
        for big in ["big", "xz-bomb"] {
            let err = t.get(big).await.unwrap_err();
            assert!(format!("{err:#}").contains(" than "), "{err:#}");
        }

        assert_eq!(read_all(&t, "big").await.unwrap().len() as u64, MAX_GET + 1);
        assert_eq!(read_all(&t, "obj").await.unwrap(), BODY);
        assert_eq!(read_all(&t, "harmonia.nar?hash=x").await.unwrap(), BODY);
        let err = read_all(&t, "missing").await.unwrap_err();
        assert!(!broke_off(&err), "{err:#}");
    }

    #[tokio::test]
    async fn http_resumes_a_broken_body() {
        let t = open(&server(Mode::BreaksOnce { ranges: true }).await).unwrap();
        assert_eq!(read_all(&t, "obj").await.unwrap(), BODY);
        // Without ranges it has to start over.
        let t = open(&server(Mode::BreaksOnce { ranges: false }).await).unwrap();
        let err = read_all(&t, "obj").await.unwrap_err();
        assert!(broke_off(&err), "{err:#}");
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
            ("s3://bucket?region=eu-west-1", "unsupported scheme s3"),
            ("ssh://host", "unsupported scheme ssh"),
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
