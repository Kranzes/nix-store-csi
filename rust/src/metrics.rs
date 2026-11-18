//! Prometheus metrics, which `csi --metrics-address` serves at `/metrics`,
//! and the node store's store paths, which it serves at `/store-paths`.

use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use harmonia_store_path::{StoreDir, StorePath};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode, header};
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
use tracing::warn;

use crate::http::{self, response};
use crate::store::Store;
use crate::store_path::system;

pub(crate) const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

const STORE_PATHS_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// Headers of `/store-paths` that tell `locality` which node it read and which
/// attribute of a volume that node uses.
pub(crate) const NODE_HEADER: &str = "nix-store-csi-node";
pub(crate) const SYSTEM_HEADER: &str = "nix-store-csi-system";

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
pub(crate) struct ResultLabels {
    pub result: &'static str,
}

/// The `result` label of an event.
pub(crate) fn result(ok: bool) -> &'static str {
    if ok { "ok" } else { "error" }
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
struct ListLabels {
    list: String,
    hash: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
    system: String,
}

pub(crate) type Histograms<L> = Family<L, Histogram, fn() -> Histogram>;

/// The metrics that the plugin updates as it works. [`StoreGauges`] reads the
/// others from the store at each scrape.
pub struct Metrics {
    publishes: Histograms<CallLabels>,
    unpublishes: Histograms<CallLabels>,
    nar_fetches: Histograms<FetchLabels>,
    nar_bytes: Family<CacheLabels, Counter>,
    nar_received: Family<CacheLabels, Counter>,
    collections: Histograms<ResultLabels>,
    deleted_paths: Counter,
    freed_bytes: Counter,
    failed_syncs: Counter,
    prefetched: Family<ListLabels, Gauge>,
    prefetched_sizes: Family<ListLabels, Gauge>,
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
            nar_received: Family::default(),
            collections: Family::new_with_constructor(collections),
            deleted_paths: Counter::default(),
            freed_bytes: Counter::default(),
            failed_syncs: Counter::default(),
            prefetched: Family::default(),
            prefetched_sizes: Family::default(),
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
            let _ = (self.nar_bytes).get_or_create(&CacheLabels {
                cache: cache.clone(),
            });
            let _ = self.nar_received.get_or_create(&CacheLabels { cache });
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

    /// The counter of NAR bytes received from `cache`.
    pub fn nar_received(&self, cache: &url::Url) -> Counter {
        let cache = cache.to_string();
        self.nar_received
            .get_or_create_owned(&CacheLabels { cache })
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
        let result = result(ok);
        (self
            .nar_fetches
            .get_or_create(&FetchLabels { cache, result }))
        .observe(started.elapsed().as_secs_f64());
    }

    pub fn collection(&self, ok: bool, started: Instant) {
        let result = result(ok);
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

    /// Reports each prefetch list by name and hash, whether the node store
    /// holds its closure, and the NAR size of the closure it holds.
    pub fn prefetched(&self, lists: &[(&str, &str, bool, u64)]) {
        self.prefetched.clear();
        self.prefetched_sizes.clear();
        for &(list, hash, held, nar_size) in lists {
            let labels = ListLabels {
                list: list.to_owned(),
                hash: hash.to_owned(),
            };
            self.prefetched.get_or_create(&labels).set(i64::from(held));
            let nar_size = i64::try_from(nar_size).unwrap_or(i64::MAX);
            self.prefetched_sizes.get_or_create(&labels).set(nar_size);
        }
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
            "nar_received",
            "Uncompressed NAR bytes received, by cache, also from tries that failed or started over",
            Unit::Bytes,
            self.nar_received.clone(),
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
            "Whether the node store holds the closure of a prefetch list, by its name and the hash of its file",
            self.prefetched.clone(),
        );
        registry.register_with_unit(
            "prefetched_size",
            "NAR size of the closure that a prefetch list holds, by its name and the hash of its file",
            Unit::Bytes,
            self.prefetched_sizes.clone(),
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
            "Closures being resolved or fetched, for publishes and prefetch lists",
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
        "The running version, and the Nix system of the node",
        Info::new(BuildLabels {
            version: env!("CARGO_PKG_VERSION"),
            system: crate::store_path::system(),
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
    /// Set if `--serve-paths` turns `/store-paths` on.
    paths: Option<Paths>,
}

/// `/store-paths`, which `--serve-paths` turns on.
pub struct Paths {
    /// The node and its Nix system, for `locality`.
    headers: header::HeaderMap,
}

impl Paths {
    pub fn new(node: &str) -> anyhow::Result<Self> {
        let mut headers = header::HeaderMap::new();
        let node =
            header::HeaderValue::try_from(node).context("--node-id is not a header value")?;
        headers.insert(NODE_HEADER, node);
        headers.insert(SYSTEM_HEADER, header::HeaderValue::try_from(system())?);
        Ok(Self { headers })
    }
}

/// Serves the store's metrics at `/metrics` to every connection on
/// `listener`, and with `paths` its store paths at `/store-paths`.
pub async fn serve(listener: TcpListener, store: Arc<Store>, paths: Option<Paths>) {
    let served = Arc::new(Served {
        registry: registry(&store),
        store,
        paths,
    });
    http::serve(listener, "metrics", move |request| {
        respond(request, served.clone())
    })
    .await;
}

async fn respond<B>(request: Request<B>, served: Arc<Served>) -> Response<Full<Bytes>> {
    let path = request.uri().path();
    match (path, &served.paths) {
        ("/metrics", _) => {
            let served = served.clone();
            let encoded = move || Ok(encode(&served.registry)?.into());
            answer(path, CONTENT_TYPE, encoded).await
        }
        ("/store-paths", Some(paths)) => {
            // Read before the listing, so a change during it changes the next
            // ETag.
            let etag = format!("\"{:x}\"", served.store.changes());
            let etag = header::HeaderValue::from_str(&etag).expect("an ETag is ASCII");
            let mut response = if request.headers().get(header::IF_NONE_MATCH) == Some(&etag) {
                let mut response = Response::new(Full::default());
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                response
            } else {
                let store = served.store.clone();
                let encoded = move || Ok(store_path_lines(&store.store_paths()?).into());
                answer(path, STORE_PATHS_CONTENT_TYPE, encoded).await
            };
            if !response.status().is_server_error() {
                response.headers_mut().extend(paths.headers.clone());
                response.headers_mut().insert(header::ETAG, etag);
            }
            response
        }
        _ => response(StatusCode::NOT_FOUND, "text/plain", "not found\n"),
    }
}

/// Answers `path` with the body that `encode` returns. It reads directories,
/// so it runs on a blocking thread.
async fn answer(
    path: &str,
    content_type: &'static str,
    encode: impl FnOnce() -> anyhow::Result<Bytes> + Send + 'static,
) -> Response<Full<Bytes>> {
    let encoded = tokio::task::spawn_blocking(encode).await;
    match encoded.map_err(anyhow::Error::from).flatten() {
        Ok(body) => response(StatusCode::OK, content_type, body),
        Err(e) => {
            warn!("serving {path}: {e:#}");
            let body = format!("serving {path} failed\n");
            response(StatusCode::INTERNAL_SERVER_ERROR, "text/plain", body)
        }
    }
}

/// Encodes `paths` as a store path on each line.
fn store_path_lines(paths: &[StorePath]) -> String {
    let store_dir = StoreDir::default();
    paths.iter().fold(String::new(), |mut body, path| {
        let _ = writeln!(body, "{}", store_dir.display(path));
        body
    })
}

pub(crate) fn encode(registry: &Registry) -> Result<String, fmt::Error> {
    let mut body = String::new();
    text::encode(&mut body, registry)?;
    Ok(body)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// The value of `sample`, a metric name with its labels, in `registry`.
    pub fn sample(registry: &Registry, sample: &str) -> Option<f64> {
        let text = encode(registry).unwrap();
        text.lines()
            .find_map(|line| line.strip_prefix(sample)?.strip_prefix(' '))
            .map(|value| value.parse().unwrap())
    }

    #[test]
    fn names_grpc_codes() {
        assert_eq!(grpc_code_name(tonic::Code::Ok), "OK");
        assert_eq!(grpc_code_name(tonic::Code::Cancelled), "Canceled");
        assert_eq!(grpc_code_name(tonic::Code::Unavailable), "Unavailable");
    }
}
