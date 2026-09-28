use std::io;
use std::path::{Path, PathBuf};
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

pub enum Transport {
    File(PathBuf),
    Http(Http),
}

#[derive(Clone)]
pub struct Http {
    base: Url,
    client: reqwest::Client,
    auth: Option<Auth>,
}

#[derive(Clone)]
enum Auth {
    /// User and password from the URL. `base` leaves them out, so no message
    /// that shows a URL can leak them.
    Basic(String, Option<String>),
    /// The URL's `bearer-token-file`. Each request reads it, so a token that
    /// kubelet rotates takes effect at once.
    Bearer(PathBuf),
    /// The `--netrc-file` entry for the cache's host, or its `default` entry.
    /// Each request reads the file, so an updated Secret takes effect at once.
    Netrc(PathBuf),
}

impl Http {
    fn request(&self, url: &Url) -> anyhow::Result<reqwest::RequestBuilder> {
        let request = self.client.get(url.clone());
        Ok(match &self.auth {
            None => request,
            Some(Auth::Basic(user, password)) => request.basic_auth(user, password.as_ref()),
            // A projected token is on a tmpfs, so the read doesn't block for long.
            Some(Auth::Bearer(file)) => {
                let token = std::fs::read_to_string(file)
                    .with_context(|| format!("reading {}", file.display()))?;
                request.bearer_auth(token.trim())
            }
            Some(Auth::Netrc(file)) => {
                let text = std::fs::read_to_string(file)
                    .with_context(|| format!("reading {}", file.display()))?;
                // The parser's errors quote the text that failed to parse,
                // which can be part of a password.
                let netrc: netrc::Netrc = (text.parse())
                    .map_err(|_| anyhow::anyhow!("{} isn't in netrc format", file.display()))?;
                match netrc_entry(&netrc, url.host_str().unwrap_or_default()) {
                    Some(m) => {
                        request.basic_auth(&m.login, Some(&m.password).filter(|p| !p.is_empty()))
                    }
                    None => request,
                }
            }
        })
    }

    /// Retries the GET until a response starts. The status must be 200. With
    /// `range`, it asks for the bytes from offset `from` on, if the file is
    /// still the one that `validator` names, and also accepts 206.
    async fn fetch(
        &self,
        url: &Url,
        range: Option<(u64, &HeaderValue)>,
    ) -> anyhow::Result<reqwest::Response> {
        retry(url, true, || async {
            let mut request = self.request(url)?;
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

/// The entry for `host`, or else the `default` one. Host names match
/// regardless of case, as in curl, which Nix uses.
fn netrc_entry<'a>(netrc: &'a netrc::Netrc, host: &str) -> Option<&'a netrc::Authenticator> {
    (netrc
        .hosts
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(host)))
    .or_else(|| netrc.hosts.get_key_value("default"))
    .map(|(_, auth)| auth)
}

/// Caps HTTP bodies from [`Transport::get`], which only fetches small files
/// like narinfos.
const MAX_GET: u64 = 1 << 20;

/// Large, because tokio runs each read of a local file on a blocking thread.
const FILE_BUF: usize = 256 * 1024;

/// How long a request keeps retrying. backon's total delay counts only the
/// sleeps, so a cache that times out on every attempt would retry far longer.
const RETRY_FOR: Duration = Duration::from_mins(1);

/// Before jitter, which adds up to as much again.
const MAX_DELAY: Duration = Duration::from_secs(5);

/// Jittered, so a closure's many lookups don't retry in step.
const BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_millis(200))
    .with_max_delay(MAX_DELAY)
    .with_jitter()
    .without_max_times();

struct Failure {
    error: anyhow::Error,
    transient: bool,
}

impl From<reqwest::Error> for Failure {
    fn from(e: reqwest::Error) -> Self {
        Self {
            // reqwest reports a body that breaks off as a decode error.
            transient: (e.is_connect()
                || e.is_timeout()
                || e.is_request()
                || e.is_body()
                || e.is_decode())
                && !failed_tls(&e),
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

/// Whether `e` is a failed TLS handshake, as on a certificate the system
/// doesn't trust. A retry would fail the same way. The TLS stack reports the
/// failure as invalid data while connecting.
fn failed_tls(e: &reqwest::Error) -> bool {
    if !e.is_connect() {
        return false;
    }
    let mut source = std::error::Error::source(e);
    while let Some(e) = source {
        if (e.downcast_ref::<io::Error>()).is_some_and(|e| e.kind() == io::ErrorKind::InvalidData) {
            return true;
        }
        source = e.source();
    }
    false
}

/// The response's `Content-Encoding`. Nix sets it on the narinfos it
/// compresses on S3.
fn content_encoding(resp: &reqwest::Response) -> anyhow::Result<Compression> {
    let Some(value) = resp.headers().get(reqwest::header::CONTENT_ENCODING) else {
        return Ok(Compression::None);
    };
    match value.to_str().context("Content-Encoding")? {
        "identity" | "" => Ok(Compression::None),
        value => Compression::parse(value).context("Content-Encoding"),
    }
}

/// Decoding can make a body far bigger, so [`MAX_GET`] applies again.
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
    // The message leaves out the URL, which after a redirect can hold a
    // signature.
    let msg = format!("got {status}");
    Err(Failure {
        error: if is_missing(status) {
            Missing(msg).into()
        } else {
            anyhow::anyhow!(msg)
        },
        transient: status.is_server_error()
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT,
    })
}

/// An error that says the cache hasn't got a file.
#[derive(Debug)]
struct Missing(String);

impl std::fmt::Display for Missing {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Missing {}

/// Whether `e` says the cache hasn't got a file. The cache may still have its
/// other files.
pub fn lacks_file(e: &anyhow::Error) -> bool {
    e.chain().any(<dyn std::error::Error>::is::<Missing>)
}

/// Whether `status` means a missing file, as in Nix. Nix counts 403 because
/// S3 sends 403 for a missing file.
fn is_missing(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::NOT_FOUND | StatusCode::FORBIDDEN | StatusCode::GONE
    )
}

/// Tries `attempt` once, or for [`RETRY_FOR`] while it fails transiently.
async fn retry<T, F, Fut>(url: &Url, retries: bool, attempt: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    let start = Instant::now();
    attempt
        .retry(BACKOFF)
        .when(|f| retries && f.transient && start.elapsed() < RETRY_FOR)
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

/// Whether `e` is a response body that broke off. A new request may fix that.
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
        // With the default windows, a NAR download reaches about half the
        // link's speed.
        .http2_adaptive_window(true)
        .build()
        .context("building HTTP client")
}

impl Transport {
    /// `netrc` supplies credentials when `url` has neither user info nor a
    /// `bearer-token-file`.
    pub fn new(url: &Url, netrc: Option<&Path>) -> anyhow::Result<Self> {
        let token_file = (url.query_pairs())
            .find(|(k, _)| k == "bearer-token-file")
            .map(|(_, v)| PathBuf::from(v.into_owned()));
        let mut base = url.clone();
        // Other store settings like `?priority=40` don't affect fetching.
        // `Caches` reads `priority` from the URL itself.
        base.set_query(None);
        base.set_fragment(None);
        Ok(match base.scheme() {
            "file" => {
                ensure!(token_file.is_none(), "bearer-token-file needs http(s)://");
                Self::File(
                    base.to_file_path()
                        .ok()
                        .context("not an absolute local file URL")?,
                )
            }
            "http" | "https" => {
                let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
                let basic = (!base.username().is_empty() || base.password().is_some())
                    .then(|| Auth::Basic(decode(base.username()), base.password().map(decode)));
                let auth = match (basic, token_file) {
                    (Some(_), Some(_)) => bail!("give either user info or bearer-token-file"),
                    (basic, token_file) => (basic.or(token_file.map(Auth::Bearer)))
                        .or(netrc.map(|file| Auth::Netrc(file.to_owned()))),
                };
                Self::Http(Http {
                    base: redact(&base),
                    client: client()?,
                    auth,
                })
            }
            scheme => bail!("unsupported scheme {scheme}: expected http(s):// or file://"),
        })
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::File(_))
    }

    /// A small file like a narinfo, or `None` if the cache hasn't got it.
    pub async fn get(&self, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.get_with(path, true).await
    }

    /// Like [`Transport::get`], without retrying.
    pub async fn get_once(&self, path: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.get_with(path, false).await
    }

    async fn get_with(&self, path: &str, retries: bool) -> anyhow::Result<Option<Vec<u8>>> {
        match self {
            Self::File(base) => {
                let file = file_path(base, path)?;
                match tokio::fs::read(&file).await {
                    Ok(data) => Ok(Some(data)),
                    // A missing cache directory is an error, not a missing file.
                    Err(e) if e.kind() == io::ErrorKind::NotFound && base.is_dir() => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("reading {}", file.display())),
                }
            }
            Self::Http(http) => {
                let url = request_url(&http.base, path)?;
                retry(&url, retries, || async {
                    let mut resp = http.request(&url)?.send().await?;
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

    /// A reader for the NAR at `path`. Over HTTP, it is [`resumable`].
    pub async fn stream(&self, path: &str) -> anyhow::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        match self {
            Self::File(base) => {
                let src = file_path(base, path)?;
                let file = match tokio::fs::File::open(&src).await {
                    Err(e) if e.kind() == io::ErrorKind::NotFound && base.is_dir() => {
                        return Err(Missing(format!("no {}", src.display())).into());
                    }
                    file => file.with_context(|| format!("opening {}", src.display()))?,
                };
                Ok(Box::new(BufReader::with_capacity(FILE_BUF, file)))
            }
            Self::Http(http) => {
                let url = request_url(&http.base, path)?;
                let resp = http.fetch(&url, None).await?;
                Ok(resumable(http.clone(), url, resp))
            }
        }
    }

    /// Fails for a `path` that [`Transport::stream`] would refuse.
    pub fn check(&self, path: &str) -> anyhow::Result<()> {
        match self {
            Self::File(base) => file_path(base, path).map(drop),
            Self::Http(http) => request_url(&http.base, path).map(drop),
        }
    }
}

struct Resuming<S> {
    http: Http,
    url: Url,
    /// Sent as `If-Range`, so a resume can't mix two versions of the file.
    validator: Option<HeaderValue>,
    body: S,
    read: u64,
    read_since_resume: u64,
}

/// The body of `resp`. When it breaks off, a range request picks it up where
/// it stopped. That needs a server that supports ranges and an unchanged file,
/// and each try must read some bytes. Otherwise reading fails with an error
/// that [`broke_off`] matches.
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
            let error = anyhow::Error::from(error);
            tracing::warn!("fetching {}: {error:#}; resumed at byte {}", s.url, s.read);
            s.body = resp.bytes_stream().boxed();
            s.read_since_resume = 0;
        }
    });
    Box::new(StreamReader::new(chunks.boxed()))
}

/// Asks for the bytes from offset `from` on, if the file is still the one
/// that `validator` names.
async fn resume(
    http: &Http,
    url: &Url,
    validator: &HeaderValue,
    from: u64,
) -> Option<reqwest::Response> {
    let resp = match http.fetch(url, Some((from, validator))).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::debug!("resuming {url}: {e:#}");
            return None;
        }
    };
    // A 200 with the whole file means the file changed or the server ignores
    // ranges. A server may also send less than the rest of the file, and its
    // body would then end early.
    let range = resp
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|r| r.to_str().ok());
    let resumed = resp.status() == StatusCode::PARTIAL_CONTENT
        && (range.and_then(content_range))
            .is_some_and(|(first, last, len)| first == from && last.checked_add(1) == Some(len));
    if !resumed {
        tracing::debug!("resuming {url}: got {} for {range:?}", resp.status());
    }
    resumed.then_some(resp)
}

/// The first byte, last byte and length in a `Content-Range`, if the length
/// is known.
fn content_range(range: &str) -> Option<(u64, u64, u64)> {
    let (span, len) = range.strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    Some((first.parse().ok()?, last.parse().ok()?, len.parse().ok()?))
}

/// Resolves `path` against the cache's URL as Nix does. Escapes and any query
/// in `path` stay as they are. Harmonia puts a query in NAR URLs. Requests
/// carry the cache's credentials, so the URL must stay within the cache.
fn request_url(base: &Url, path: &str) -> anyhow::Result<Url> {
    check_path(path.split_once('?').map_or(path, |(p, _)| p))?;
    let mut dir = base.clone();
    if !dir.path().ends_with('/') {
        dir.set_path(&format!("{}/", dir.path()));
    }
    let url = dir.join(path)?;
    ensure!(
        url.origin() == dir.origin() && url.path().starts_with(dir.path()),
        "bad path in binary cache: {path:?}"
    );
    Ok(url)
}

fn file_path(base: &Path, path: &str) -> anyhow::Result<PathBuf> {
    check_path(path)?;
    Ok(base.join(path))
}

/// Rejects a `path` that would escape the cache, since paths come from
/// narinfos.
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
        Transport::new(&url.parse()?, None)
    }

    #[derive(Clone, Copy)]
    pub(crate) enum Mode {
        Normal,
        FlakyOnce,
        Truncated,
        /// `/obj` breaks off halfway the first time. With `ranges`, a resume
        /// gets the rest of it, or with `short` all but its last byte.
        BreaksOnce {
            ranges: bool,
            short: bool,
        },
        /// A cache that answers only `/nix-cache-info`.
        Hangs,
    }

    fn range_start(line: &str) -> Option<usize> {
        let line = line.to_ascii_lowercase();
        let from = line.strip_prefix("range: bytes=")?.trim_end();
        from.strip_suffix('-')?.parse().ok()
    }

    /// A request's path, `Authorization` and `Range` start, or `None` once the
    /// client has closed the connection. The `Range` counts only if `If-Range`
    /// holds the `ETag` that `Mode::BreaksOnce` serves.
    async fn read_request(
        reader: &mut (impl AsyncBufRead + Unpin),
    ) -> Option<(String, String, Option<usize>)> {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
            return None;
        }
        let path = request_line.split(' ').nth(1).unwrap();
        // Serves the same files under `/prefix`, for bases with a path.
        let path = path.strip_prefix("/prefix").unwrap_or(path).to_owned();
        let (mut authorization, mut if_range, mut range) = (String::new(), String::new(), None);
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                let range = range.filter(|_| if_range == "\"x\"");
                return Some((path, authorization, range));
            }
            let lower = line.to_ascii_lowercase();
            if lower.starts_with("authorization:") {
                authorization = line["authorization:".len()..].trim().to_owned();
            }
            if lower.starts_with("if-range:") {
                if_range = line["if-range:".len()..].trim().to_owned();
            }
            range = range.or_else(|| range_start(&line));
        }
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
                        let Some((path, authorization, range)) = read_request(&mut reader).await
                        else {
                            return;
                        };
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
                            // user:secret
                            ("/private", _) if authorization != "Basic dXNlcjpzZWNyZXQ=" => {
                                ("401 Unauthorized", Vec::new())
                            }
                            ("/whoami", _) => ("200 OK", authorization.into_bytes()),
                            (p, _) if p.starts_with("/redirect?to=") => {
                                let to = &p["/redirect?to=".len()..];
                                write!(headers, "Location: {to}\r\n").unwrap();
                                ("307 Temporary Redirect", Vec::new())
                            }
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
                        if let Mode::BreaksOnce { ranges, short } = mode
                            && path == "/obj"
                        {
                            headers.push_str("ETag: \"x\"\r\n");
                            if let Some(from) = range.filter(|_| ranges) {
                                status = "206 Partial Content";
                                let len = BODY.len();
                                let last = len - 1 - usize::from(short);
                                write!(headers, "Content-Range: bytes {from}-{last}/{len}\r\n")
                                    .unwrap();
                                body = BODY[from..=last].to_vec();
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
            format!("{server}/prefix?priority=40"),
            format!("{server}/prefix/"),
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
            ("nar/a%3Ab.nar", "https://host/some/prefix/nar/a%3Ab.nar"),
            (
                "nar/abc.nar?hash=xyz",
                "https://host/some/prefix/nar/abc.nar?hash=xyz",
            ),
        ] {
            assert_eq!(request_url(&base, path).unwrap().as_str(), url);
        }
        let no_slash = Url::parse("https://host/some/prefix").unwrap();
        let url = request_url(&no_slash, "nar/x").unwrap();
        assert_eq!(url.as_str(), "https://host/some/prefix/nar/x");
        for path in [
            "../x",
            "/x",
            "nar//x",
            "../x?y",
            "//other/x",
            "c:x",
            "https://other/x",
        ] {
            assert!(request_url(&base, path).is_err(), "{path}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_for_a_minute() {
        let url = Url::parse("http://host/x").unwrap();
        for attempt_takes in [0, 60].map(Duration::from_secs) {
            let start = Instant::now();
            let err = retry(&url, true, || async {
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
                elapsed <= RETRY_FOR + attempt_takes + 2 * MAX_DELAY,
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
        // No error message shows the credentials.
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

    #[tokio::test]
    async fn http_bearer_token() {
        let base = server(Mode::Normal).await;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        let t = open(&format!(
            "{base}?priority=30&bearer-token-file={}",
            file.display()
        ))
        .unwrap();
        let err = t.get("whoami").await.unwrap_err();
        assert!(format!("{err:#}").contains("token"), "{err:#}");
        // Each request reads the file, because kubelet may have rotated it.
        for token in ["one", "two\n"] {
            std::fs::write(&file, token).unwrap();
            let seen = t.get("whoami").await.unwrap().unwrap();
            assert_eq!(seen, format!("Bearer {}", token.trim()).as_bytes());
        }
        // niks3 redirects NARs to presigned S3 URLs, which must not get the
        // token.
        let other = server(Mode::Normal).await;
        let seen = t.get(&format!("redirect?to={other}/whoami")).await.unwrap();
        assert_eq!(seen.unwrap(), b"");
        let seen = t.get(&format!("redirect?to={base}/whoami")).await.unwrap();
        assert_eq!(seen.unwrap(), b"Bearer two");
    }

    #[tokio::test]
    async fn http_netrc() {
        let base = server(Mode::Normal).await;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("netrc");
        let open_with = |netrc: &str| {
            std::fs::write(&file, netrc).unwrap();
            Transport::new(&base.parse().unwrap(), Some(&file)).unwrap()
        };
        let t = open_with("machine 127.0.0.1 login user password secret");
        assert_eq!(t.get("private").await.unwrap().unwrap(), BODY);
        let t = open_with("machine example.com login user password secret");
        assert!(t.get("private").await.is_err());
        // A typo doesn't put part of a password in the error.
        let t = open_with("machine 127.0.0.1 login user password sec ret");
        let err = format!("{:#}", t.get("private").await.unwrap_err());
        assert!(
            err.contains("isn't in netrc format") && !err.contains("ret"),
            "{err}"
        );
        // User info in the URL overrides the netrc file.
        let url = base.replace("://", "://user:secret@").parse().unwrap();
        std::fs::write(&file, "default login user password wrong").unwrap();
        let t = Transport::new(&url, Some(&file)).unwrap();
        assert_eq!(t.get("private").await.unwrap().unwrap(), BODY);
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
        assert_eq!(t.get("xz").await.unwrap().unwrap(), BODY);
        for big in ["big", "xz-bomb"] {
            let err = t.get(big).await.unwrap_err();
            assert!(format!("{err:#}").contains(" than "), "{err:#}");
        }

        assert_eq!(read_all(&t, "big").await.unwrap().len() as u64, MAX_GET + 1);
        assert_eq!(read_all(&t, "harmonia.nar?hash=x").await.unwrap(), BODY);
        let err = read_all(&t, "missing").await.unwrap_err();
        assert!(!broke_off(&err), "{err:#}");
        assert!(lacks_file(&err), "{err:#}");
        let err = read_all(&t, "private").await.unwrap_err();
        assert!(!lacks_file(&err), "{err:#}");
    }

    #[tokio::test]
    async fn http_resumes_a_broken_body() {
        let breaks = |ranges, short| Mode::BreaksOnce { ranges, short };
        let t = open(&server(breaks(true, false)).await).unwrap();
        assert_eq!(read_all(&t, "obj").await.unwrap(), BODY);
        // Without ranges, with a range short of the end, or without an ETag,
        // the caller has to start over.
        for mode in [breaks(false, false), breaks(true, true), Mode::Truncated] {
            let t = open(&server(mode).await).unwrap();
            let err = read_all(&t, "obj").await.unwrap_err();
            assert!(broke_off(&err), "{err:#}");
        }
    }

    #[test]
    fn matches_netrc_hosts() {
        let netrc: netrc::Netrc = "machine Cache.Example.com login a password b\ndefault login c"
            .parse()
            .unwrap();
        let login = |host| netrc_entry(&netrc, host).map(|m| m.login.as_str());
        assert_eq!(login("cache.example.com"), Some("a"));
        assert_eq!(login("other.example.com"), Some("c"));
    }

    #[tokio::test]
    async fn file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("my cache");
        std::fs::create_dir_all(cache.join("nar")).unwrap();
        std::fs::write(cache.join("nar/x.nar"), BODY).unwrap();
        // `Transport::new` percent-decodes the URL's path and accepts localhost
        // as its host.
        let url = format!("file://localhost{}/my%20cache/", dir.path().display());
        let t = open(&url).unwrap();
        assert_eq!(t.get("nar/x.nar").await.unwrap().unwrap(), BODY);
        assert_eq!(t.get("nar/y.nar").await.unwrap(), None);
        assert!(t.get("../x").await.is_err());
        assert!(t.get("/etc/passwd").await.is_err());

        assert_eq!(read_all(&t, "nar/x.nar").await.unwrap(), BODY);
        assert!(lacks_file(&read_all(&t, "nar/y.nar").await.unwrap_err()));

        // A missing cache directory doesn't count as a missing file.
        let gone = open(&format!("file://{}/gone/", dir.path().display())).unwrap();
        assert!(gone.get("nar/x.nar").await.is_err());
        assert!(!lacks_file(
            &read_all(&gone, "nar/x.nar").await.unwrap_err()
        ));
    }

    #[test]
    fn rejected_urls() {
        for url in [
            "file://host/x",
            "s3://bucket?region=eu-west-1",
            "file:///x?bearer-token-file=/t",
            "https://user:pw@host?bearer-token-file=/t",
        ] {
            assert!(open(url).is_err(), "{url}");
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
