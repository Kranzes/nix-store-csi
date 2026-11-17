//! What each node's store holds, from the `/store-paths` of its plugin.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use foldhash::HashSetExt;
use futures::StreamExt;
use harmonia_store_path::{StoreDir, StorePathHash};
use hickory_resolver::TokioResolver;
use hickory_resolver::config::LookupIpStrategy;
use reqwest::StatusCode;
use reqwest::header::{ETAG, IF_NONE_MATCH};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};

use super::Metrics;
use crate::metrics::{NODE_HEADER, SYSTEM_HEADER};
use crate::store_path;

/// A node that misses this many reads in a row no longer counts.
const MISSED_READS: u32 = 3;

/// Plugins to read at once.
const READS: usize = 32;

pub type PathSet = foldhash::HashSet<StorePathHash>;

/// A node's store at the last full read of its plugin.
pub struct Node {
    /// The Nix system, which picks the attribute of a volume that the node
    /// uses.
    pub system: String,
    pub paths: PathSet,
}

struct Entry {
    node: Arc<Node>,
    /// The last read, full or answered with 304.
    read: Instant,
}

/// The nodes by name.
pub struct Nodes {
    nodes: RwLock<HashMap<String, Entry>>,
    interval: Duration,
}

impl Nodes {
    pub fn new(interval: Duration) -> Self {
        Self {
            nodes: RwLock::default(),
            interval,
        }
    }

    /// Reads every plugin's `/store-paths` every `interval`, in the
    /// background.
    pub fn watch(
        plugins: Plugins,
        interval: Duration,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Arc<Self>> {
        // Unlike musl's getaddrinfo, which keeps 48 addresses, hickory
        // returns every plugin.
        let mut resolver = TokioResolver::builder_tokio().context("reading /etc/resolv.conf")?;
        // A dual-stack Service would list each plugin twice.
        resolver.options_mut().ip_strategy = LookupIpStrategy::Ipv4thenIpv6;
        let resolver = resolver.build().context("building the DNS resolver")?;
        let client = crate::transport::client()?;
        let nodes = Arc::new(Self::new(interval));
        tokio::spawn(nodes.clone().read_all(resolver, client, plugins, metrics));
        Ok(nodes)
    }

    pub fn get(&self, name: &str) -> Option<Arc<Node>> {
        let nodes = self.nodes.read().unwrap();
        let entry = nodes.get(name)?;
        self.fresh(entry).then(|| entry.node.clone())
    }

    fn fresh(&self, entry: &Entry) -> bool {
        entry.read.elapsed() <= self.interval * MISSED_READS
    }

    fn insert(&self, name: String, node: Node) {
        let node = Arc::new(node);
        let entry = Entry {
            node,
            read: Instant::now(),
        };
        self.nodes.write().unwrap().insert(name, entry);
    }

    /// Records a read that found the node store as it was, and returns
    /// whether the node is still known.
    fn touch(&self, name: &str) -> bool {
        let mut nodes = self.nodes.write().unwrap();
        let entry = nodes.get_mut(name);
        entry.map(|entry| entry.read = Instant::now()).is_some()
    }

    /// Drops the nodes that missed too many reads, and returns how many are
    /// left.
    fn expire(&self) -> usize {
        let mut nodes = self.nodes.write().unwrap();
        nodes.retain(|_, entry| self.fresh(entry));
        nodes.len()
    }

    async fn read_all(
        self: Arc<Self>,
        resolver: TokioResolver,
        client: reqwest::Client,
        plugins: Plugins,
        metrics: Arc<Metrics>,
    ) {
        let every = humantime::format_duration(self.interval).to_string();
        let mut ticks = tokio::time::interval(self.interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The ETag of each plugin's last listing, by URL.
        let mut etags: HashMap<String, String> = HashMap::new();
        loop {
            ticks.tick().await;
            let addresses: Vec<IpAddr> = match resolver.lookup_ip(plugins.host.as_str()).await {
                Ok(lookup) => lookup.iter().collect(),
                Err(e) => {
                    let host = &plugins.host;
                    warn!(
                        host,
                        "looking up the node plugins: {e}; trying again in {every}"
                    );
                    metrics.scrape(false);
                    Vec::new()
                }
            };
            let (client, timeout) = (&client, self.interval);
            let mut sent = std::mem::take(&mut etags);
            let reads = futures::stream::iter(addresses).map(|ip| {
                let url = format!("http://{}/store-paths", SocketAddr::new(ip, plugins.port));
                let etag = sent.remove(&url);
                async move {
                    let read = read(client, &url, etag.as_deref(), timeout).await;
                    (url, read)
                }
            });
            let mut reads = reads.buffer_unordered(READS);
            while let Some((url, read)) = reads.next().await {
                let Read { name, etag, node } = match read {
                    Ok(read) => read,
                    Err(e) => {
                        warn!(
                            url,
                            "reading the node store: {e:#}; trying again in {every}"
                        );
                        metrics.scrape(false);
                        continue;
                    }
                };
                metrics.scrape(true);
                let known = match node {
                    Some(node) => {
                        let store_paths = node.paths.len();
                        debug!(url, node = name, store_paths, "read the node store");
                        self.insert(name, node);
                        true
                    }
                    // A node forgotten since then needs a full read.
                    None => self.touch(&name),
                };
                if known {
                    etags.insert(url, etag);
                }
            }
            metrics.nodes(self.expire());
        }
    }
}

/// Where the node plugins serve `/store-paths`.
#[derive(Clone, Debug)]
pub struct Plugins {
    /// A name with an address for each plugin, or one address.
    pub host: String,
    pub port: u16,
}

impl std::str::FromStr for Plugins {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let (host, port) = s.rsplit_once(':').context("no port")?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        anyhow::ensure!(!host.is_empty(), "no host");
        Ok(Self {
            host: host.to_owned(),
            port: port
                .parse()
                .with_context(|| format!("invalid port {port:?}"))?,
        })
    }
}

/// What a read of a plugin's `/store-paths` found.
struct Read {
    name: String,
    /// The `ETag` to send next time.
    etag: String,
    /// The node store, or `None` if it hasn't changed since the listing with
    /// the `ETag` sent.
    node: Option<Node>,
}

/// Reads the store paths at `url`, a plugin's `/store-paths`, and the name
/// and system of its node. With the `etag` of an earlier listing, an
/// unchanged node store costs no listing.
async fn read(
    client: &reqwest::Client,
    url: &str,
    etag: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<Read> {
    let mut request = client.get(url).timeout(timeout);
    if let Some(etag) = etag {
        request = request.header(IF_NONE_MATCH, etag);
    }
    let mut response = request.send().await?.error_for_status()?;
    let header = |name: &str| {
        let value = response.headers().get(name).and_then(|v| v.to_str().ok());
        (value.map(str::to_owned)).with_context(|| format!("no valid {name} header"))
    };
    let (name, etag) = (header(NODE_HEADER)?, header(ETAG.as_str())?);
    if response.status() == StatusCode::NOT_MODIFIED {
        return Ok(Read {
            name,
            etag,
            node: None,
        });
    }
    let system = header(SYSTEM_HEADER)?;
    // Parsing as the body arrives keeps only the hashes, a fraction of the
    // listing's size.
    let lines = response.content_length().unwrap_or_default() / 64;
    let lines = usize::try_from(lines).unwrap_or_default();
    let mut paths = PathSet::with_capacity(lines);
    let mut rest = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        rest.extend_from_slice(&chunk);
        let end = rest.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        for line in rest[..end]
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
        {
            paths.insert(hash(line)?);
        }
        rest.drain(..end);
    }
    anyhow::ensure!(rest.is_empty(), "the listing ends mid-line");
    let node = Some(Node { system, paths });
    Ok(Read { name, etag, node })
}

/// The hash of the store path on a line of `/store-paths`.
fn hash(line: &[u8]) -> anyhow::Result<StorePathHash> {
    let line = std::str::from_utf8(line)?;
    Ok(*store_path::parse(&StoreDir::default(), line)?.hash())
}

#[cfg(test)]
pub mod tests {
    use harmonia_store_path::StorePath;
    use tokio::net::TcpListener;

    use super::*;
    use crate::store::Excess;
    use crate::store::test_utils::hello_store;

    /// A node of `system` whose store holds `paths`.
    pub fn node(nodes: &Nodes, name: &str, system: &str, paths: &[&StorePath]) {
        let paths = paths.iter().map(|p| *p.hash()).collect();
        let system = system.to_owned();
        nodes.insert(name.to_owned(), Node { system, paths });
    }

    #[tokio::test(start_paused = true)]
    async fn forgets_nodes_after_missed_reads() {
        let interval = Duration::from_secs(15);
        let nodes = Nodes::new(interval);
        node(&nodes, "a", "x86_64-linux", &[]);
        node(&nodes, "b", "x86_64-linux", &[]);
        tokio::time::advance(interval * 2).await;
        node(&nodes, "c", "x86_64-linux", &[]);
        assert!(nodes.touch("b"));
        tokio::time::advance(interval * 2).await;
        assert!(nodes.get("a").is_none());
        assert!(nodes.get("b").is_some());
        assert!(nodes.get("c").is_some());
        assert_eq!(nodes.expire(), 2);
        assert!(!nodes.touch("a"));
    }

    #[test]
    fn parses_plugins() {
        for (s, host, port) in [
            (
                "nix-store-csi-plugins.kube-system.svc:9809",
                "nix-store-csi-plugins.kube-system.svc",
                9809,
            ),
            ("127.0.0.1:9809", "127.0.0.1", 9809),
            ("[::1]:9809", "::1", 9809),
        ] {
            let plugins: Plugins = s.parse().unwrap();
            assert_eq!((plugins.host.as_str(), plugins.port), (host, port), "{s}");
        }
        for s in ["plugins", "plugins:x", ":9809"] {
            assert!(s.parse::<Plugins>().is_err(), "{s}");
        }
    }

    #[tokio::test]
    async fn reads_a_plugin_until_its_store_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let Some((store, _, closure)) = hello_store(tmp.path()).await else {
            return;
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/store-paths", listener.local_addr().unwrap());
        let paths = crate::metrics::Paths::new("node-a").unwrap();
        tokio::spawn(crate::metrics::serve(listener, store.clone(), Some(paths)));
        let (client, timeout) = (reqwest::Client::new(), Duration::from_secs(15));
        let read = |etag: Option<String>| {
            let (client, url) = (client.clone(), url.clone());
            async move { read(&client, &url, etag.as_deref(), timeout).await.unwrap() }
        };

        let listed = read(None).await;
        assert_eq!(listed.name, "node-a");
        let node = listed.node.unwrap();
        assert_eq!(node.system, store_path::system());
        assert_eq!(
            node.paths,
            closure.iter().map(|path| *path.hash()).collect()
        );
        let unchanged = read(Some(listed.etag.clone())).await;
        assert_eq!(
            (unchanged.name.as_str(), &unchanged.etag),
            ("node-a", &listed.etag)
        );
        assert!(unchanged.node.is_none());

        let collected = store.collect(|_, _| Ok(false), Duration::ZERO, Excess::default());
        assert_eq!(collected.await.unwrap(), closure.len());
        let changed = read(Some(listed.etag.clone())).await;
        assert_ne!(changed.etag, listed.etag);
        assert!(changed.node.unwrap().paths.is_empty());
    }
}
