//! Prometheus metrics, which `csi --metrics-address` serves at `/metrics`.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric, text};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::{Registry, Unit};
use tokio::net::TcpListener;
use tracing::{debug, warn};

use crate::store::Store;

const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct GrpcLabels {
    grpc_code: &'static str,
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
struct BuildLabels {
    version: &'static str,
}

type Histograms<L> = Family<L, Histogram, fn() -> Histogram>;

/// The metrics that the plugin updates as it works. [`StoreGauges`] reads the
/// others from the store at each scrape.
pub struct Metrics {
    publishes: Histograms<GrpcLabels>,
    unpublishes: Histograms<GrpcLabels>,
    nar_fetches: Histograms<FetchLabels>,
    nar_bytes: Family<CacheLabels, Counter>,
    collections: Histograms<ResultLabels>,
    deleted_paths: Counter,
    freed_bytes: Counter,
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
        }
    }
}

impl Metrics {
    /// Creates the label sets that the plugin is sure to use, so they read 0
    /// before their first event instead of missing.
    pub fn init<'a>(&self, caches: impl IntoIterator<Item = &'a url::Url>) {
        for calls in [&self.publishes, &self.unpublishes] {
            let _ = calls.get_or_create(&GrpcLabels { grpc_code: "OK" });
        }
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

    /// Times a `NodePublishVolume` call.
    pub fn publish(&self) -> Call<'_> {
        Call::new(&self.publishes)
    }

    /// Times a `NodeUnpublishVolume` call.
    pub fn unpublish(&self) -> Call<'_> {
        Call::new(&self.unpublishes)
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

    fn register(&self, registry: &mut Registry) {
        registry.register_with_unit(
            "publish_duration",
            "NodePublishVolume calls, by gRPC status code",
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
    }
}

/// A call being timed. It counts as `Canceled` if dropped before
/// [`Call::finish`], which tonic does to a call whose deadline passed.
pub struct Call<'a> {
    family: &'a Histograms<GrpcLabels>,
    started: Instant,
    grpc_code: &'static str,
}

impl<'a> Call<'a> {
    fn new(family: &'a Histograms<GrpcLabels>) -> Self {
        Self {
            family,
            started: Instant::now(),
            grpc_code: grpc_code_name(tonic::Code::Cancelled),
        }
    }

    pub fn finish<T>(mut self, result: &Result<T, tonic::Status>) {
        self.grpc_code = match result {
            Ok(_) => "OK",
            Err(status) => grpc_code_name(status.code()),
        };
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        let labels = GrpcLabels {
            grpc_code: self.grpc_code,
        };
        (self.family.get_or_create(&labels)).observe(self.started.elapsed().as_secs_f64());
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
struct StoreGauges(Arc<Store>);

impl fmt::Debug for StoreGauges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StoreGauges")
    }
}

impl Collector for StoreGauges {
    fn encode(&self, mut encoder: DescriptorEncoder) -> fmt::Result {
        let gauges = self.0.gauges();
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
            Some(gauges.store_size),
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
            "free_inodes",
            "Free inodes on the node store's filesystem, if it limits them",
            None,
            gauges.free_inodes,
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
            "Whether a cache is skipped for a minute after a failed request",
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
    registry.register_collector(Box::new(StoreGauges(store.clone())));
    registry
}

/// Serves `registry` at `/metrics` to every connection on `listener`.
pub async fn serve(listener: TcpListener, registry: Registry) {
    let registry = Arc::new(registry);
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Errors here, such as running out of file descriptors, would
                // recur on an immediate retry.
                warn!("accepting a metrics connection: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let registry = registry.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| respond(request, registry.clone()));
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

async fn respond(
    request: Request<Incoming>,
    registry: Arc<Registry>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = |status, content_type, body: String| {
        let mut response = Response::new(Full::new(Bytes::from(body)));
        *response.status_mut() = status;
        let value = header::HeaderValue::from_static(content_type);
        response.headers_mut().insert(header::CONTENT_TYPE, value);
        Ok(response)
    };
    if request.uri().path() != "/metrics" {
        return response(StatusCode::NOT_FOUND, "text/plain", "not found\n".into());
    }
    // Encoding reads directories for the gauges, so it runs on a blocking
    // thread.
    let encoded = tokio::task::spawn_blocking(move || encode(&registry)).await;
    match encoded {
        Ok(Ok(body)) => response(StatusCode::OK, CONTENT_TYPE, body),
        _ => response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain",
            "encoding the metrics failed\n".into(),
        ),
    }
}

fn encode(registry: &Registry) -> Result<String, fmt::Error> {
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
