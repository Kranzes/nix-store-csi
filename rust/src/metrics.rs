//! Prometheus metrics, which `csi --metrics-address` serves at `/metrics`,
//! and the node store's paths, which it serves at `/paths`.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use harmonia_store_path::StoreDir;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{
    DescriptorEncoder, EncodeLabel, EncodeLabelSet, EncodeMetric, LabelSetEncoder, text,
};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::{Registry, Unit};
use tokio::net::TcpListener;
use tracing::{debug, warn};

use crate::store::{HeldPath, Reason, Store};

const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

const PATHS_CONTENT_TYPE: &str = "application/x-ndjson";

/// The labels of a publish or an unpublish. Only publishes have `fetched`.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CallLabels {
    grpc_code: &'static str,
    fetched: Option<bool>,
}

impl EncodeLabelSet for CallLabels {
    fn encode(&self, encoder: &mut LabelSetEncoder) -> fmt::Result {
        EncodeLabel::encode(&("grpc_code", self.grpc_code), encoder.encode_label())?;
        match self.fetched {
            Some(fetched) => EncodeLabel::encode(&("fetched", fetched), encoder.encode_label()),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResultLabels {
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FetchLabels {
    cache: String,
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CacheLabels {
    cache: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RevisionLabels {
    revision: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
}

type Histograms<L> = Family<L, Histogram, fn() -> Histogram>;

/// The metrics that the plugin updates as it works. [`StoreGauges`] reads the
/// others from the store at each scrape.
pub struct Metrics {
    publishes: Histograms<CallLabels>,
    unpublishes: Histograms<CallLabels>,
    nar_fetches: Histograms<FetchLabels>,
    nar_bytes: Family<CacheLabels, Counter>,
    collections: Histograms<ResultLabels>,
    deleted_paths: Counter,
    freed_bytes: Counter,
    failed_syncs: Counter,
    prefetched: Family<RevisionLabels, Gauge>,
}

impl Default for Metrics {
    fn default() -> Self {
        // A publish gives up at 90 seconds and a NAR can take minutes.
        let calls = || Histogram::new([0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 90.0]);
        let fetches = || {
            Histogram::new([
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
            ])
        };
        let collections = || Histogram::new([0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]);
        Self {
            publishes: Family::new_with_constructor(calls),
            unpublishes: Family::new_with_constructor(calls),
            nar_fetches: Family::new_with_constructor(fetches),
            nar_bytes: Family::default(),
            collections: Family::new_with_constructor(collections),
            deleted_paths: Counter::default(),
            freed_bytes: Counter::default(),
            failed_syncs: Counter::default(),
            prefetched: Family::default(),
        }
    }
}

impl Metrics {
    /// Creates the label sets that the plugin is sure to use, so they read 0
    /// before their first event instead of missing.
    pub fn init<'a>(&self, caches: impl IntoIterator<Item = &'a url::Url>) {
        for fetched in [Some(false), Some(true)] {
            let ok = CallLabels {
                grpc_code: "OK",
                fetched,
            };
            let _ = self.publishes.get_or_create(&ok);
        }
        let ok = CallLabels {
            grpc_code: "OK",
            fetched: None,
        };
        let _ = self.unpublishes.get_or_create(&ok);
        for cache in caches {
            let cache = cache.to_string();
            for result in ["ok", "error"] {
                let _ = (self.nar_fetches).get_or_create(&FetchLabels {
                    cache: cache.clone(),
                    result,
                });
            }
            let _ = self.nar_bytes.get_or_create(&CacheLabels { cache });
        }
        for result in ["ok", "error"] {
            let _ = self.collections.get_or_create(&ResultLabels { result });
        }
    }

    /// Times a `NodePublishVolume` call, which counts as one that didn't
    /// wait for a fetch unless [`Call::fetched`] says it did.
    pub fn publish(&self) -> Call<'_> {
        Call::new(&self.publishes, Some(false))
    }

    /// Times a `NodeUnpublishVolume` call.
    pub fn unpublish(&self) -> Call<'_> {
        Call::new(&self.unpublishes, None)
    }

    /// Counts one try at fetching a NAR of `size` bytes from `cache`.
    pub fn nar_fetch(&self, cache: &url::Url, ok: bool, started: Instant, size: u64) {
        let cache = cache.to_string();
        if ok {
            (self.nar_bytes.get_or_create(&CacheLabels {
                cache: cache.clone(),
            }))
            .inc_by(size);
        }
        let result = if ok { "ok" } else { "error" };
        (self
            .nar_fetches
            .get_or_create(&FetchLabels { cache, result }))
        .observe(started.elapsed().as_secs_f64());
    }

    pub fn collection(&self, ok: bool, started: Instant) {
        let result = if ok { "ok" } else { "error" };
        (self.collections.get_or_create(&ResultLabels { result }))
            .observe(started.elapsed().as_secs_f64());
    }

    /// Counts a path that collection deleted, whose NAR had `size` bytes.
    pub fn deleted(&self, size: u64) {
        self.deleted_paths.inc();
        self.freed_bytes.inc_by(size);
    }

    /// Counts a sync of the node store that failed.
    pub fn failed_sync(&self) {
        self.failed_syncs.inc();
    }

    /// Reports whether the node store holds the closures of revision
    /// `revision` of the prefetch list.
    pub fn prefetched(&self, revision: &str, held: bool) {
        self.prefetched.clear();
        let labels = RevisionLabels {
            revision: revision.to_owned(),
        };
        self.prefetched.get_or_create(&labels).set(i64::from(held));
    }

    fn register(&self, registry: &mut Registry) {
        registry.register_with_unit(
            "publish_duration",
            "NodePublishVolume calls, by gRPC status code and whether they waited for a fetch",
            Unit::Seconds,
            self.publishes.clone(),
        );
        registry.register_with_unit(
            "unpublish_duration",
            "NodeUnpublishVolume calls, by gRPC status code",
            Unit::Seconds,
            self.unpublishes.clone(),
        );
        registry.register_with_unit(
            "nar_fetch_duration",
            "Tries at fetching a NAR, by cache and result",
            Unit::Seconds,
            self.nar_fetches.clone(),
        );
        registry.register_with_unit(
            "nar_fetched",
            "Uncompressed size of the NARs fetched, by cache",
            Unit::Bytes,
            self.nar_bytes.clone(),
        );
        registry.register_with_unit(
            "gc_duration",
            "Collections of the node store, by result",
            Unit::Seconds,
            self.collections.clone(),
        );
        registry.register(
            "gc_deleted_paths",
            "Store paths that collection deleted",
            self.deleted_paths.clone(),
        );
        registry.register_with_unit(
            "gc_freed",
            "NAR size of the store objects that collection deleted",
            Unit::Bytes,
            self.freed_bytes.clone(),
        );
        registry.register(
            "failed_syncs",
            "Syncs of the node store that failed, so the store paths they covered were deleted",
            self.failed_syncs.clone(),
        );
        registry.register(
            "prefetched",
            "Whether the node store holds the closures of a revision of the prefetch list",
            self.prefetched.clone(),
        );
    }
}

/// A call being timed. It counts as `Canceled` if dropped before
/// [`Call::finish`], which tonic does to a call whose deadline passed.
pub struct Call<'a> {
    family: &'a Histograms<CallLabels>,
    started: Instant,
    labels: CallLabels,
}

impl<'a> Call<'a> {
    fn new(family: &'a Histograms<CallLabels>, fetched: Option<bool>) -> Self {
        Self {
            family,
            started: Instant::now(),
            labels: CallLabels {
                grpc_code: grpc_code_name(tonic::Code::Cancelled),
                fetched,
            },
        }
    }

    /// Records that the publish waited for a fetch.
    pub fn fetched(&mut self) {
        self.labels.fetched = Some(true);
    }

    pub fn finish<T>(mut self, result: &Result<T, tonic::Status>) {
        self.labels.grpc_code = match result {
            Ok(_) => "OK",
            Err(status) => grpc_code_name(status.code()),
        };
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        (self.family.get_or_create(&self.labels)).observe(self.started.elapsed().as_secs_f64());
    }
}

/// The status code's name as gRPC spells it, which tonic's `Debug` doesn't.
fn grpc_code_name(code: tonic::Code) -> &'static str {
    use tonic::Code;
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "Canceled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

/// Reads gauges from the store at each scrape.
struct StoreGauges {
    store: Arc<Store>,
}

impl fmt::Debug for StoreGauges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StoreGauges")
    }
}

impl Collector for StoreGauges {
    fn encode(&self, mut encoder: DescriptorEncoder) -> fmt::Result {
        let gauges = self.store.gauges();
        let mut gauge = |name, help, unit, value: Option<u64>| {
            let Some(value) = value else { return Ok(()) };
            let value = i64::try_from(value).unwrap_or(i64::MAX);
            ConstGauge::new(value).encode(encoder.encode_descriptor(
                name,
                help,
                unit,
                MetricType::Gauge,
            )?)
        };
        let count = |n: usize| u64::try_from(n).ok();
        gauge(
            "store_paths",
            "Store paths in the node store",
            None,
            gauges.store_paths.and_then(count),
        )?;
        gauge(
            "store_size",
            "NAR size of the store objects in the node store",
            Some(&Unit::Bytes),
            gauges.store_size,
        )?;
        gauge(
            "uncollectable",
            "NAR size of the store objects that garbage collection can't delete",
            Some(&Unit::Bytes),
            gauges.uncollectable_size,
        )?;
        gauge(
            "max_store_size",
            "Limit on the NAR size that garbage collection can't delete, from --max-store-size",
            Some(&Unit::Bytes),
            gauges.max_store_size,
        )?;
        gauge(
            "unsynced_paths",
            "Fetched store objects that wait for a sync",
            None,
            count(gauges.unsynced_paths),
        )?;
        gauge(
            "views",
            "Views, one for each closure that volumes use",
            None,
            gauges.views.and_then(count),
        )?;
        gauge(
            "free",
            "Free space on the node store's filesystem",
            Some(&Unit::Bytes),
            gauges.free_bytes,
        )?;
        gauge(
            "free_inodes",
            "Free inodes on the node store's filesystem, if it limits them",
            None,
            gauges.free_inodes,
        )?;
        gauge(
            "ensure_free",
            "Free space that collection keeps on the node store's filesystem, from --ensure-free",
            Some(&Unit::Bytes),
            gauges.ensure_free_bytes,
        )?;
        gauge(
            "ensure_free_inodes",
            "Free inodes that collection keeps on the node store's filesystem, if it limits them",
            None,
            gauges.ensure_free_inodes,
        )?;
        gauge(
            "nar_fetches_in_flight",
            "NARs being fetched",
            None,
            count(gauges.fetches_in_flight),
        )?;
        gauge(
            "closures_in_flight",
            "Closures being resolved or fetched for publishes",
            None,
            count(gauges.closures_in_flight),
        )?;
        let mut paused = encoder.encode_descriptor(
            "cache_paused",
            "Whether a cache is skipped for a minute after failed requests",
            None,
            MetricType::Gauge,
        )?;
        for (cache, is_paused) in gauges.paused_caches {
            let labels = CacheLabels { cache };
            ConstGauge::new(i64::from(is_paused)).encode(paused.encode_family(&labels)?)?;
        }
        Ok(())
    }
}

/// The registry that `/metrics` encodes, with every metric under
/// `nix_store_csi_`.
pub fn registry(store: &Arc<Store>) -> Registry {
    let mut registry = Registry::with_prefix("nix_store_csi");
    store.metrics().register(&mut registry);
    registry.register(
        "build",
        "The running version",
        Info::new(BuildLabels {
            version: env!("CARGO_PKG_VERSION"),
        }),
    );
    registry.register_collector(Box::new(StoreGauges {
        store: store.clone(),
    }));
    registry
}

/// What [`serve`] answers with.
struct Served {
    registry: Registry,
    store: Arc<Store>,
    /// `--keep-recent`, for the `recent` reason, if `--serve-paths` turns
    /// `/paths` on.
    keep_recent: Option<Duration>,
    /// Lets one `/paths` listing run at a time, since each reads every
    /// narinfo.
    listing: Arc<tokio::sync::Mutex<()>>,
}

/// Serves the store's metrics at `/metrics` to every connection on
/// `listener`, and with `keep_recent` its paths at `/paths`.
pub async fn serve(listener: TcpListener, store: Arc<Store>, keep_recent: Option<Duration>) {
    let served = Arc::new(Served {
        registry: registry(&store),
        store,
        keep_recent,
        listing: Arc::default(),
    });
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Errors here, such as running out of file descriptors, would
                // recur on an immediate retry.
                warn!("accepting a metrics connection: {e}; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let served = served.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| respond(request, served.clone()));
            // The timer enforces hyper's default 30 second limit on reading
            // the request head, so idle connections don't pile up.
            if let Err(e) = http1::Builder::new()
                .timer(TokioTimer::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!("serving metrics: {e}");
            }
        });
    }
}

async fn respond<B>(
    request: Request<B>,
    served: Arc<Served>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = |status, content_type, body: Bytes| {
        let mut response = Response::new(Full::new(body));
        *response.status_mut() = status;
        let value = header::HeaderValue::from_static(content_type);
        response.headers_mut().insert(header::CONTENT_TYPE, value);
        Ok(response)
    };
    // Both read directories, so they run on a blocking thread.
    let (content_type, encoded) = match (request.uri().path(), served.keep_recent) {
        ("/metrics", _) => {
            let encoded = move || Ok(encode(&served.registry)?.into());
            (CONTENT_TYPE, tokio::task::spawn_blocking(encoded))
        }
        ("/paths", Some(keep_recent)) => {
            // Waiting for the turn would let callers queue without limit.
            let Ok(turn) = served.listing.clone().try_lock_owned() else {
                let body = "a listing is already running\n".into();
                return response(StatusCode::SERVICE_UNAVAILABLE, "text/plain", body);
            };
            // The turn lasts until the listing ends, even if the client
            // leaves.
            let encoded = move || {
                let _turn = turn;
                let paths = served
                    .store
                    .held_paths(crate::mounts::is_bound, keep_recent)?;
                Ok(json_lines(&paths)?.into())
            };
            (PATHS_CONTENT_TYPE, tokio::task::spawn_blocking(encoded))
        }
        _ => return response(StatusCode::NOT_FOUND, "text/plain", "not found\n".into()),
    };
    match encoded.await.map_err(anyhow::Error::from).flatten() {
        Ok(body) => response(StatusCode::OK, content_type, body),
        Err(e) => {
            let path = request.uri().path();
            warn!("serving {path}: {e:#}");
            let body = format!("serving {path} failed\n").into();
            response(StatusCode::INTERNAL_SERVER_ERROR, "text/plain", body)
        }
    }
}

/// Encodes `paths` as JSON lines.
fn json_lines(paths: &[HeldPath]) -> serde_json::Result<Vec<u8>> {
    #[derive(serde::Serialize)]
    struct Line {
        path: String,
        nar_size: u64,
        last_use: String,
        held: Reason,
    }
    let store_dir = StoreDir::default();
    let mut body = Vec::new();
    for path in paths {
        let line = Line {
            path: store_dir.display(&path.path).to_string(),
            nar_size: path.nar_size,
            last_use: humantime::format_rfc3339_seconds(path.last_use).to_string(),
            held: path.held,
        };
        serde_json::to_writer(&mut body, &line)?;
        body.push(b'\n');
    }
    Ok(body)
}

fn encode(registry: &Registry) -> Result<String, fmt::Error> {
    let mut body = String::new();
    text::encode(&mut body, registry)?;
    Ok(body)
}

#[cfg(test)]
pub mod tests {
    use std::time::SystemTime;

    use harmonia_store_path::StorePath;

    use super::*;
    use crate::store::test_utils::unsigned_config;

    /// The value of `sample`, a metric name with its labels, in `registry`.
    pub fn sample(registry: &Registry, sample: &str) -> Option<f64> {
        let text = encode(registry).unwrap();
        text.lines()
            .find_map(|line| line.strip_prefix(sample)?.strip_prefix(' '))
            .map(|value| value.parse().unwrap())
    }

    #[tokio::test]
    async fn refuses_a_second_listing() {
        let tmp = tempfile::tempdir().unwrap();
        let config = unsigned_config(tmp.path(), &tmp.path().join("state"));
        let store = Arc::new(Store::new(config).unwrap());
        let served = Arc::new(Served {
            registry: registry(&store),
            store,
            keep_recent: Some(Duration::ZERO),
            listing: Arc::default(),
        });
        let paths = || respond(Request::get("/paths").body(()).unwrap(), served.clone());
        let turn = served.listing.clone().try_lock_owned().unwrap();
        let busy = paths().await.unwrap();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(turn);
        assert_eq!(paths().await.unwrap().status(), StatusCode::OK);
    }

    #[test]
    fn lists_paths_as_json_lines() {
        let hash = "0".repeat(32);
        let path = |name| StorePath::from_base_path(&format!("{hash}-{name}")).unwrap();
        let held = |name, held| HeldPath {
            path: path(name),
            nar_size: 42,
            last_use: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            held,
        };
        let body = json_lines(&[held("a", Reason::Prefetch), held("b", Reason::None)]).unwrap();
        assert_eq!(
            String::from_utf8(body).unwrap(),
            format!(
                "{{\"path\":\"/nix/store/{hash}-a\",\"nar_size\":42,\"last_use\":\"2023-11-14T22:13:20Z\",\"held\":\"prefetch\"}}\n\
                 {{\"path\":\"/nix/store/{hash}-b\",\"nar_size\":42,\"last_use\":\"2023-11-14T22:13:20Z\",\"held\":\"none\"}}\n"
            )
        );
    }

    #[test]
    fn names_grpc_codes() {
        assert_eq!(grpc_code_name(tonic::Code::Ok), "OK");
        assert_eq!(grpc_code_name(tonic::Code::Cancelled), "Canceled");
        assert_eq!(grpc_code_name(tonic::Code::Unavailable), "Unavailable");
    }
}
