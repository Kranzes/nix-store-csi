//! `locality`, a scheduler extender that scores nodes by the share of a pod's
//! closure, by NAR size, that each node's store holds.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::TryStreamExt;
use harmonia_store_path::StorePathHash;
use http_body_util::{BodyDataStream, Full};
use hyper::body::{Body, Bytes};
use hyper::{Method, Request, Response, StatusCode};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{debug, info, warn};

use crate::closure::Caches;
use crate::csi::DRIVER;
use crate::http::{self, response};
use crate::metrics::{Histograms, ResultLabels, result};

mod closures;
mod nodes;

use closures::Closures;
pub use nodes::Plugins;
use nodes::{Node, Nodes};

/// kube-scheduler's `MaxExtenderPriority`.
const KUBE_SCHEDULER_MAX: u32 = 10;

/// Volcano's highest node score.
const VOLCANO_MAX: u32 = 100;

/// Reads the plugins' `/store-paths` every `interval`, and serves the
/// scheduler extender, `/healthz` and `/metrics` on `address` until SIGTERM or
/// SIGINT.
pub async fn run(
    caches: Caches,
    plugins: Plugins,
    address: SocketAddr,
    interval: Duration,
) -> anyhow::Result<()> {
    let listener = http::listen(address)
        .await
        .with_context(|| format!("listening on {address}"))?;
    let url = format!("http://{}", listener.local_addr()?);
    let metrics = Arc::new(Metrics::default());
    let nodes = Nodes::watch(plugins, interval, metrics.clone())?;
    let served = Arc::new(Served::new(caches, nodes, metrics));
    let shutdown = crate::shutdown_signal()?;
    info!(%url, "serving the scheduler extender");
    let serve = http::serve(listener, "scheduler extender", move |request| {
        respond(request, served.clone())
    });
    tokio::select! {
        () = serve => {}
        () = shutdown => {}
    }
    Ok(())
}

struct Served {
    nodes: Arc<Nodes>,
    closures: Arc<Closures>,
    metrics: Arc<Metrics>,
    registry: Registry,
}

impl Served {
    fn new(caches: Caches, nodes: Arc<Nodes>, metrics: Arc<Metrics>) -> Self {
        Self {
            nodes,
            closures: Arc::new(Closures::new(caches, metrics.clone())),
            registry: metrics.registry(),
            metrics,
        }
    }
}

async fn respond<B>(request: Request<B>, served: Arc<Served>) -> Response<Full<Bytes>>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let started = Instant::now();
    let (scheduler, answer) = match (request.method(), request.uri().path()) {
        (&Method::POST, "/prioritize") => {
            let answer = prioritize(request.into_body(), served.clone(), kube_scheduler);
            ("kube-scheduler", answer.await)
        }
        (&Method::POST, "/volcano/prioritize") => {
            let answer = prioritize(request.into_body(), served.clone(), volcano);
            ("volcano", answer.await)
        }
        (&Method::GET, "/healthz") => return response(StatusCode::OK, "text/plain", "ok\n"),
        (&Method::GET, "/metrics") => {
            let body = crate::metrics::encode(&served.registry).expect("the metrics encode");
            return response(StatusCode::OK, crate::metrics::CONTENT_TYPE, body);
        }
        _ => return response(StatusCode::NOT_FOUND, "text/plain", "not found\n"),
    };
    served
        .metrics
        .prioritize(scheduler, answer.is_ok(), started);
    match answer {
        Ok(json) => response(StatusCode::OK, "application/json", json),
        Err(e) => {
            warn!("reading {scheduler}'s request: {e:#}; answering 400");
            response(StatusCode::BAD_REQUEST, "text/plain", format!("{e:#}\n"))
        }
    }
}

/// Parses a request as it arrives, keeping only the fields of `T`, and
/// answers it with `answer`, on a blocking thread. Volcano sends every pod on
/// each node, so a whole request may not fit in memory.
async fn prioritize<B, T, A>(body: B, served: Arc<Served>, answer: A) -> anyhow::Result<Vec<u8>>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    T: DeserializeOwned,
    A: FnOnce(T, &Served) -> Vec<u8> + Send + 'static,
{
    let stream = BodyDataStream::new(body).map_err(std::io::Error::other);
    let reader = SyncIoBridge::new(StreamReader::new(stream));
    tokio::task::spawn_blocking(move || {
        let request = serde_json::from_reader(std::io::BufReader::new(reader))?;
        Ok(answer(request, &served))
    })
    .await?
}

fn kube_scheduler(args: ExtenderArgs, served: &Served) -> Vec<u8> {
    let ExtenderArgs {
        pod,
        nodes,
        node_names,
    } = args;
    let names = node_names.unwrap_or_else(|| {
        let nodes = nodes.into_iter().flat_map(|list| list.items);
        nodes.map(|node| node.metadata.name).collect()
    });
    let scores = scores(&pod, names, served, KUBE_SCHEDULER_MAX).into_iter();
    let list: Vec<_> = (scores.map(|(host, score)| HostPriority { host, score })).collect();
    json(&list)
}

fn volcano(args: PrioritizeRequest, served: &Served) -> Vec<u8> {
    let names = args.nodes.into_iter().flatten().map(|node| node.name);
    let scores = scores(&args.task.pod, names.collect(), served, VOLCANO_MAX).into_iter();
    let node_score = scores
        .map(|(name, score)| (name, f64::from(score)))
        .collect();
    json(&PrioritizeResponse { node_score })
}

fn json(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).expect("scores encode as JSON")
}

/// The share of the closure of `pod`'s volumes that each of `nodes` holds,
/// from 0 to `max`. A node scores 0 if no plugin read lists it, or if a
/// closure it would use hasn't resolved yet.
fn scores(pod: &Pod, nodes: Vec<String>, served: &Served, max: u32) -> Vec<(String, u32)> {
    let volumes: Vec<_> = (pod.spec.volumes.iter())
        .filter_map(|volume| volume.csi.as_ref())
        .filter(|csi| csi.driver == DRIVER)
        .map(|csi| &csi.volume_attributes)
        .collect();
    if volumes.is_empty() {
        return nodes.into_iter().map(|name| (name, 0)).collect();
    }
    // Nodes of a system use the same attributes, and so the same closure.
    let mut closures: HashMap<String, Option<PodClosure>> = HashMap::new();
    let mut score = |name: &str| {
        let node = served.nodes.get(name)?;
        let closure = (closures.entry(node.system.clone()))
            .or_insert_with(|| PodClosure::new(&volumes, &node.system, &served.closures));
        Some(closure.as_ref()?.score(&node, max))
    };
    let scores: Vec<_> = (nodes.into_iter())
        .map(|name| {
            let score = score(&name).unwrap_or(0);
            (name, score)
        })
        .collect();
    let pod = &pod.metadata;
    debug!(
        pod = format_args!("{}/{}", pod.namespace, pod.name),
        ?scores,
        "scored nodes"
    );
    scores
}

/// The closure of a pod's volumes on the nodes of a system.
struct PodClosure {
    /// Each store path's NAR size.
    paths: foldhash::HashMap<StorePathHash, u64>,
    nar_size: u64,
}

impl PodClosure {
    /// The closure of the store paths that `volumes` list for `system`, if
    /// the plugin accepts each volume and their closures are resolved.
    fn new(
        volumes: &[&HashMap<String, String>],
        system: &str,
        closures: &Arc<Closures>,
    ) -> Option<Self> {
        let mut roots = Vec::new();
        for attributes in volumes {
            roots.extend(crate::csi::volume_roots(attributes, system).ok()?);
        }
        // Every miss starts resolving, so a later pod finds them all.
        let resolved: Vec<_> = roots.iter().map(|root| closures.get(root)).collect();
        let mut paths = foldhash::HashMap::default();
        for closure in resolved {
            paths.extend(closure?.iter().copied());
        }
        let nar_size = paths.values().sum();
        (nar_size > 0).then_some(Self { paths, nar_size })
    }

    /// The share of the NAR size that `node` holds, from 0 to `max`, rounded
    /// down.
    fn score(&self, node: &Node, max: u32) -> u32 {
        let held: u64 = (self.paths.iter())
            .filter(|(hash, _)| node.paths.contains(*hash))
            .map(|(_, nar_size)| nar_size)
            .sum();
        let score = u128::from(held) * u128::from(max) / u128::from(self.nar_size);
        u32::try_from(score).unwrap_or(max)
    }
}

/// The fields of a `v1.Pod` that scoring reads.
#[derive(Default, Deserialize)]
#[serde(default)]
struct Pod {
    metadata: Metadata,
    spec: PodSpec,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Metadata {
    name: String,
    namespace: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct PodSpec {
    volumes: Vec<Volume>,
}

#[derive(Deserialize)]
struct Volume {
    csi: Option<CsiVolume>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CsiVolume {
    driver: String,
    #[serde(default)]
    volume_attributes: HashMap<String, String>,
}

/// kube-scheduler's `ExtenderArgs`. It sends `NodeNames` with
/// `nodeCacheCapable`, and else `Nodes`.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ExtenderArgs {
    pod: Pod,
    nodes: Option<NodeList>,
    node_names: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct NodeList {
    #[serde(default)]
    items: Vec<NodeObject>,
}

#[derive(Deserialize)]
struct NodeObject {
    metadata: Metadata,
}

/// An item of kube-scheduler's `HostPriorityList`.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct HostPriority {
    host: String,
    score: u32,
}

/// The request of Volcano's `extender` plugin.
#[derive(Deserialize)]
struct PrioritizeRequest {
    task: Task,
    nodes: Option<Vec<NodeInfo>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Task {
    pod: Pod,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NodeInfo {
    name: String,
}

/// Volcano's answer. Its `errorMessage` fails the task even with
/// `extender.ignorable`, so errors answer 400 instead and leave it out.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrioritizeResponse {
    node_score: HashMap<String, f64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PrioritizeLabels {
    scheduler: &'static str,
    result: &'static str,
}

pub struct Metrics {
    scrapes: Family<ResultLabels, Counter>,
    resolves: Family<ResultLabels, Counter>,
    prioritizes: Histograms<PrioritizeLabels>,
    nodes: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        let prioritizes = || Histogram::new([0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]);
        let metrics = Self {
            scrapes: Family::default(),
            resolves: Family::default(),
            prioritizes: Family::new_with_constructor(prioritizes),
            nodes: Gauge::default(),
        };
        // The alerts divide by these, so they read 0 before the first event.
        for result in ["ok", "error"] {
            let _ = metrics.scrapes.get_or_create(&ResultLabels { result });
            let _ = metrics.resolves.get_or_create(&ResultLabels { result });
        }
        metrics
    }
}

impl Metrics {
    fn scrape(&self, ok: bool) {
        let labels = ResultLabels { result: result(ok) };
        self.scrapes.get_or_create(&labels).inc();
    }

    fn resolve(&self, ok: bool) {
        let labels = ResultLabels { result: result(ok) };
        self.resolves.get_or_create(&labels).inc();
    }

    fn nodes(&self, n: usize) {
        self.nodes.set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    fn prioritize(&self, scheduler: &'static str, ok: bool, started: Instant) {
        let labels = PrioritizeLabels {
            scheduler,
            result: result(ok),
        };
        (self.prioritizes.get_or_create(&labels)).observe(started.elapsed().as_secs_f64());
    }

    /// A registry with every metric under `nix_store_csi_locality_`.
    fn registry(&self) -> Registry {
        let mut registry = Registry::with_prefix("nix_store_csi_locality");
        registry.register(
            "scrapes",
            "Reads of the node plugins' /store-paths, by result",
            self.scrapes.clone(),
        );
        registry.register(
            "nodes",
            "Nodes whose plugin was read within the last 3 intervals",
            self.nodes.clone(),
        );
        registry.register(
            "closure_resolves",
            "Closures resolved from the caches, by result",
            self.resolves.clone(),
        );
        registry.register_with_unit(
            "prioritize_duration",
            "Prioritize requests, by scheduler and result",
            Unit::Seconds,
            self.prioritizes.clone(),
        );
        registry
    }
}

#[cfg(test)]
mod tests {
    use harmonia_store_path::StorePath;
    use http_body_util::BodyExt;
    use serde_json::json;

    use super::*;
    use crate::metrics::tests::sample;
    use crate::store_path;

    fn path(n: u8, name: &str) -> StorePath {
        let hash = char::from(b'0' + n).to_string().repeat(32);
        StorePath::from_base_path(&format!("{hash}-{name}")).unwrap()
    }

    fn served() -> Served {
        let caches = Caches::new(&["file:///nonexistent".to_owned()], None, None, None).unwrap();
        let nodes = Arc::new(Nodes::new(Duration::from_secs(15)));
        Served::new(caches, nodes, Arc::new(Metrics::default()))
    }

    /// Makes `paths` the closure of `root`.
    fn closure(served: &Served, root: &StorePath, paths: &[(&StorePath, u64)]) {
        let closure = paths.iter().map(|(p, size)| (*p.hash(), *size)).collect();
        served.closures.insert(root.clone(), closure);
    }

    fn pod(attributes: &serde_json::Value) -> serde_json::Value {
        json!({
            "metadata": {"name": "web", "namespace": "default"},
            "spec": {"volumes": [
                {"name": "data", "emptyDir": {}},
                {"name": "nix", "csi": {"driver": "nix-store-csi", "readOnly": true, "volumeAttributes": attributes}},
            ]},
        })
    }

    /// The scores of `nodes` for `pod`.
    fn scored(pod: serde_json::Value, nodes: &[&str], served: &Served, max: u32) -> Vec<u32> {
        let pod = serde_json::from_value(pod).unwrap();
        let nodes = nodes.iter().map(|&node| node.to_owned()).collect();
        scores(&pod, nodes, served, max)
            .into_iter()
            .map(|(_, score)| score)
            .collect()
    }

    #[tokio::test]
    async fn weighs_by_nar_size() {
        let served = served();
        let (app, glibc, tzdata, icons) = (
            path(1, "app"),
            path(2, "glibc"),
            path(3, "tzdata"),
            path(4, "icons"),
        );
        // `big` holds most of the bytes, `many` most of the store paths.
        closure(
            &served,
            &app,
            &[
                (&app, 1_000),
                (&glibc, 30_000_000),
                (&tzdata, 2_000),
                (&icons, 3_000),
            ],
        );
        let system = store_path::system();
        nodes::tests::node(&served.nodes, "big", &system, &[&glibc]);
        nodes::tests::node(&served.nodes, "many", &system, &[&app, &tzdata, &icons]);
        let pod = pod(&json!({"storePaths": app.to_string()}));
        let nodes = ["big", "many", "unknown"];
        assert_eq!(
            scored(pod.clone(), &nodes, &served, KUBE_SCHEDULER_MAX),
            [9, 0, 0]
        );
        assert_eq!(scored(pod, &nodes, &served, VOLCANO_MAX), [99, 0, 0]);
    }

    #[tokio::test]
    async fn scores_each_node_by_its_system() {
        let served = served();
        let (x86, arm, shared) = (path(1, "app"), path(2, "app"), path(3, "shared"));
        closure(&served, &x86, &[(&x86, 100), (&shared, 100)]);
        closure(&served, &arm, &[(&arm, 300), (&shared, 100)]);
        nodes::tests::node(&served.nodes, "x86", "x86_64-linux", &[&shared]);
        nodes::tests::node(&served.nodes, "arm", "aarch64-linux", &[&shared]);
        nodes::tests::node(&served.nodes, "riscv", "riscv64-linux", &[&shared]);
        let pod = pod(&json!({
            "storePaths.x86_64-linux": x86.to_string(),
            "storePaths.aarch64-linux": format!("/nix/store/{arm}/bin/app"),
        }));
        // riscv has no attribute, so its plugin would refuse the volume.
        let nodes = ["x86", "arm", "riscv"];
        assert_eq!(scored(pod, &nodes, &served, VOLCANO_MAX), [50, 25, 0]);
    }

    #[tokio::test]
    async fn adds_up_volumes_and_roots_once() {
        let served = served();
        let (a, b, shared) = (path(1, "a"), path(2, "b"), path(3, "shared"));
        closure(&served, &a, &[(&a, 100), (&shared, 200)]);
        closure(&served, &b, &[(&b, 100), (&shared, 200)]);
        nodes::tests::node(&served.nodes, "node", &store_path::system(), &[&shared]);
        let mut pod = pod(&json!({"storePaths": format!("{a} {b}")}));
        let volumes = pod["spec"]["volumes"].as_array_mut().unwrap();
        volumes.push(volumes[1].clone());
        assert_eq!(scored(pod, &["node"], &served, VOLCANO_MAX), [50]);
    }

    #[tokio::test]
    async fn scores_0_until_every_closure_resolves() {
        let served = served();
        let (a, unresolved) = (path(1, "a"), path(2, "b"));
        closure(&served, &a, &[(&a, 100)]);
        nodes::tests::node(&served.nodes, "node", &store_path::system(), &[&a]);
        for pod in [
            pod(&json!({"storePaths": format!("{a} {unresolved}")})),
            pod(&json!({"storePaths": "not a store path"})),
            // The plugin refuses a volume with two roots of the same name.
            pod(&json!({"storePaths": format!("{a} {}", path(3, "a"))})),
            json!({"spec": {"containers": []}}),
        ] {
            assert_eq!(scored(pod, &["node"], &served, VOLCANO_MAX), [0]);
        }
    }

    async fn post(
        served: &Arc<Served>,
        uri: &str,
        body: &serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let request = Request::post(uri).body(Full::new(Bytes::from(body.to_string())));
        let response = respond(request.unwrap(), served.clone()).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body).into_owned();
        (
            status,
            serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
        )
    }

    #[tokio::test]
    async fn answers_kube_scheduler() {
        let served = Arc::new(served());
        let app = path(1, "app");
        closure(&served, &app, &[(&app, 100)]);
        nodes::tests::node(&served.nodes, "warm", &store_path::system(), &[&app]);
        let pod = pod(&json!({"storePaths": app.to_string()}));
        let node = |name| json!({"metadata": {"name": name, "labels": {}}, "status": {}});
        for args in [
            json!({"Pod": pod, "NodeNames": ["warm", "cold"]}),
            json!({"Pod": pod, "Nodes": {"items": [node("warm"), node("cold")]}, "NodeNames": null}),
        ] {
            let (status, body) = post(&served, "/prioritize", &args).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                body,
                json!([{"Host": "warm", "Score": 10}, {"Host": "cold", "Score": 0}])
            );
        }
        let (status, _) = post(&served, "/prioritize", &json!({"Pod": 1})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let ok = "nix_store_csi_locality_prioritize_duration_seconds_count{scheduler=\"kube-scheduler\",result=\"ok\"}";
        assert_eq!(sample(&served.registry, ok), Some(2.0));
    }

    #[tokio::test]
    async fn answers_volcano() {
        let served = Arc::new(served());
        let app = path(1, "app");
        closure(&served, &app, &[(&app, 100)]);
        nodes::tests::node(&served.nodes, "warm", &store_path::system(), &[&app]);
        let pod = pod(&json!({"storePaths": app.to_string()}));
        let node = |name| json!({"Name": name, "Node": {"metadata": {"name": name}}, "Tasks": {"t": {"Pod": {}}}});
        let request = json!({"task": {"UID": "t", "Pod": pod, "Resreq": {}}, "nodes": [node("warm"), node("cold")]});
        let (status, body) = post(&served, "/volcano/prioritize", &request).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"nodeScore": {"warm": 100.0, "cold": 0.0}}));
        let (status, _) = post(&served, "/volcano/prioritize", &json!({"task": 1})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
