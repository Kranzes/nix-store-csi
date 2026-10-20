use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use bytesize::ByteSize;
use clap::{Parser, Subcommand};
use harmonia_store_path::StoreDir;
use rustix::io::Errno;
use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
use tokio::net::{TcpListener, TcpSocket, UnixListener};
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{error, info, warn};

use crate::closure::Caches;
use crate::csi::Plugin;
use crate::csi::proto::identity_server::IdentityServer;
use crate::csi::proto::node_server::NodeServer;
use crate::narinfo::{NarInfo, PublicKey};
use crate::store::{Config, EnsureFree, Excess, Store};

mod closure;
mod csi;
mod metrics;
mod mounts;
mod narinfo;
mod prefetch;
mod store;
mod store_path;
mod transport;

const MINUTE: Duration = Duration::from_mins(1);

/// How often `unpack` logs its progress.
const UNPACK_PROGRESS: Duration = Duration::from_secs(5);

/// Like Nix's `--dry-run`: "5 store paths (12.4 MiB download, 36.7 MiB
/// unpacked)". The download size needs every narinfo's `FileSize`.
fn closure_size<'a>(infos: impl IntoIterator<Item = &'a NarInfo>) -> String {
    let (mut count, mut unpacked, mut download) = (0, 0, Some(0));
    for info in infos {
        count += 1;
        unpacked += info.nar_size();
        download = download.zip(info.file_size()).map(|(a, b)| a + b);
    }
    let s = if count == 1 { "" } else { "s" };
    let download = download.map_or_else(String::new, |d| format!("{} download, ", ByteSize(d)));
    format!(
        "{count} store path{s} ({download}{} unpacked)",
        ByteSize(unpacked)
    )
}

/// `duration` in whole milliseconds, for logs.
fn millis(duration: Duration) -> Duration {
    Duration::from_millis(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Binary caches.
    #[arg(
        long,
        value_name = "URLS",
        env = "NIX_STORE_CSI_SUBSTITUTERS",
        overrides_with = "substituters",
        default_value = "https://cache.nixos.org",
        global = true
    )]
    substituters: String,

    /// Keys that narinfos must be signed by.
    #[arg(
        long,
        value_name = "KEYS",
        env = "NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS",
        overrides_with = "trusted_public_keys",
        default_value = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=",
        global = true
    )]
    trusted_public_keys: String,

    /// Accept narinfos without a trusted signature.
    #[arg(long, env = "NIX_STORE_CSI_NO_REQUIRE_SIGS", global = true)]
    no_require_sigs: bool,

    /// Netrc file with credentials for caches, read on every request.
    #[arg(
        long,
        value_name = "FILE",
        env = "NIX_STORE_CSI_NETRC_FILE",
        global = true
    )]
    netrc_file: Option<PathBuf>,

    /// Directory that holds the node store in `store/`.
    #[arg(
        long,
        value_name = "DIR",
        env = "NIX_STORE_CSI_STATE_DIR",
        default_value = "/var/lib/nix-store-csi",
        global = true
    )]
    state_dir: PathBuf,

    /// off, error, warn, info, debug or trace.
    #[arg(
        long,
        value_name = "LEVEL",
        env = "NIX_STORE_CSI_LOG_LEVEL",
        default_value = "info",
        global = true
    )]
    log_level: tracing::level_filters::LevelFilter,

    /// text or json.
    #[arg(
        long,
        value_name = "FORMAT",
        env = "NIX_STORE_CSI_LOG_FORMAT",
        default_value = "text",
        global = true
    )]
    log_format: LogFormat,

    /// NARs to fetch at once.
    #[arg(
        long,
        value_name = "N",
        env = "NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS",
        default_value = "16",
        global = true
    )]
    max_substitution_jobs: std::num::NonZeroUsize,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the CSI node plugin on a unix socket.
    Csi {
        /// Socket path or `unix://` URL.
        #[arg(long, value_name = "SOCKET", env = "CSI_ENDPOINT")]
        endpoint: String,
        /// This node's name, as kubelet knows it.
        #[arg(long, value_name = "NAME", env = "NODE_NAME")]
        node_id: String,
        /// Keep store paths that a pod used in the last DURATION, like `24h` or
        /// `7d`, unless `--ensure-free` needs the space or inodes.
        #[arg(
            long,
            value_name = "DURATION",
            env = "NIX_STORE_CSI_KEEP_RECENT",
            default_value = "24h",
            value_parser = humantime::parse_duration
        )]
        keep_recent: Duration,
        /// Collect garbage every DURATION, like `10m` or `1h`. While
        /// `--ensure-free` needs the space or inodes, collection also runs
        /// every minute.
        #[arg(
            long,
            value_name = "DURATION",
            env = "NIX_STORE_CSI_GC_INTERVAL",
            default_value = "10m",
            value_parser = parse_interval
        )]
        gc_interval: Duration,
        /// Free space to keep on the state dir's filesystem, like `50G` or
        /// `20%`, by deleting unused store paths, least recently used first.
        /// It keeps the same share of inodes free, so `50G` of a 500G
        /// filesystem keeps 10% of its inodes free. `0` turns this off.
        #[arg(
            long,
            value_name = "SIZE",
            env = "NIX_STORE_CSI_ENSURE_FREE",
            default_value = "20%",
            value_parser = store::parse_ensure_free
        )]
        ensure_free: EnsureFree,
        /// Serve Prometheus metrics at `/metrics` on ADDRESS, like
        /// `[::]:9809`. Off unless set.
        #[arg(long, value_name = "ADDRESS", env = "NIX_STORE_CSI_METRICS_ADDRESS")]
        metrics_address: Option<SocketAddr>,
        /// Refuse a volume whose closure's NARs add up to more than SIZE, like
        /// `20G`, before fetching any of it. Prefetched closures don't count. Off
        /// unless set.
        #[arg(
            long,
            value_name = "SIZE",
            env = "NIX_STORE_CSI_MAX_CLOSURE_SIZE",
            value_parser = store::parse_size
        )]
        max_closure_size: Option<u64>,
        /// Keep the closures of the store paths listed in DIR fetched, even
        /// when `--ensure-free` needs the space. DIR, such as a mounted
        /// `ConfigMap`, holds a file for each volume attribute, `storePaths`
        /// or `storePaths.<system>`, and may hold a `revision` file, which the
        /// `prefetched` metric reports. The plugin reads it when it changes and
        /// every minute. Off unless set.
        #[arg(long, value_name = "DIR", env = "NIX_STORE_CSI_PREFETCH_DIR")]
        prefetch_dir: Option<PathBuf>,
    },
    /// Fetch the closures of store paths into the node store and print its
    /// directory, for use with `podman -v DIR:/nix/store:ro`.
    Unpack {
        /// Store paths, their base names, or paths inside them.
        #[arg(value_name = "STORE_PATH", required = true)]
        roots: Vec<String>,
        /// Only look up the closures in the caches and print their size, or
        /// fail if a store path is missing. This fetches no NARs and doesn't
        /// use the state dir.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let logs = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(cli.log_level);
    match cli.log_format {
        LogFormat::Text => logs.init(),
        LogFormat::Json => logs.json().init(),
    }
    if let Err(e) = run(cli).await {
        error!("{e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    raise_open_files_limit();
    // Any whitespace separates list items, since a value from a file or a YAML
    // block string can end in a newline.
    let trusted_keys = (cli.trusted_public_keys.split_whitespace())
        .map(|k| k.parse().with_context(|| format!("parsing public key {k}")))
        .collect::<anyhow::Result<Vec<PublicKey>>>()?;
    if trusted_keys.is_empty() && !cli.no_require_sigs {
        bail!("no trusted public keys given; pass --no-require-sigs to accept unsigned narinfos");
    }
    let stores: Vec<_> = (cli.substituters.split_whitespace())
        .map(str::to_owned)
        .collect();
    if stores.is_empty() {
        bail!("no substituters given");
    }
    let trusted_keys = (!cli.no_require_sigs).then_some(trusted_keys);
    if let Command::Unpack {
        roots,
        dry_run: true,
    } = &cli.command
    {
        let caches = Caches::new(
            &stores,
            StoreDir::default(),
            trusted_keys,
            None,
            cli.netrc_file.as_deref(),
        )?;
        let roots = store_path::roots(roots.iter().map(String::as_str))?;
        let closure = caches.resolve(&roots, &AtomicUsize::default()).await?;
        println!("{}", closure_size(closure.values().map(|e| &e.info)));
        return Ok(());
    }
    let (max_closure_size, ensure_free) = match cli.command {
        Command::Csi {
            max_closure_size,
            ensure_free,
            ..
        } => (max_closure_size, ensure_free),
        Command::Unpack { .. } => (None, EnsureFree::Bytes(0)),
    };
    let store = Arc::new(Store::new(Config {
        stores,
        trusted_keys,
        state_dir: cli.state_dir,
        jobs: cli.max_substitution_jobs.get(),
        netrc_file: cli.netrc_file,
        max_closure_size,
        ensure_free,
    })?);

    match cli.command {
        Command::Unpack { roots, .. } => {
            let roots = store_path::roots(roots.iter().map(String::as_str))?;
            let mut ensuring = store.ensure(&roots);
            loop {
                if let Ok(result) = tokio::time::timeout(UNPACK_PROGRESS, ensuring.done()).await {
                    result?;
                    break;
                }
                info!("{}", ensuring.progress());
            }
            store.sync_all().await?;
            println!("{}", store.dir().display());
            Ok(())
        }
        Command::Csi {
            endpoint,
            node_id,
            keep_recent,
            gc_interval,
            metrics_address,
            prefetch_dir,
            ..
        } => {
            store.prune_views(mounts::is_bound).await?;
            if let Some(address) = metrics_address {
                let listener = listen(address)
                    .await
                    .with_context(|| format!("listening on {address} for metrics"))?;
                let address = listener.local_addr()?;
                info!("serving metrics on http://{address}/metrics");
                tokio::spawn(metrics::serve(listener, metrics::registry(&store)));
            }
            tokio::spawn(collect_garbage(store.clone(), gc_interval, keep_recent));
            if let Some(dir) = prefetch_dir {
                tokio::spawn(prefetch::Prefetch::new(store.clone(), dir).run());
            }
            let plugin = Arc::new(Plugin::new(node_id, store));
            serve(&endpoint, plugin).await
        }
    }
}

/// Binds `address`. An IPv6 address takes IPv4 too, whatever
/// `net.ipv6.bindv6only` says. A kernel booted with `ipv6.disable=1` has no
/// IPv6 sockets, so there `[::]` falls back to `0.0.0.0`.
async fn listen(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => return TcpListener::bind(address).await,
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    };
    let socket = match socket {
        Err(e)
            if address.ip() == Ipv6Addr::UNSPECIFIED
                && e.raw_os_error() == Some(Errno::AFNOSUPPORT.raw_os_error()) =>
        {
            return TcpListener::bind((Ipv4Addr::UNSPECIFIED, address.port())).await;
        }
        socket => socket?,
    };
    socket2::SockRef::from(&socket).set_only_v6(false)?;
    // Like `TcpListener::bind`, so a restart can bind during TIME_WAIT.
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    socket.listen(1024)
}

/// Raises the soft limit on open files to the hard one. Containers often get
/// 1024, and each tree walk holds a directory open for each level, on every
/// core.
fn raise_open_files_limit() {
    let limit = getrlimit(Resource::Nofile);
    let raised = Rlimit {
        current: limit.maximum,
        ..limit
    };
    if let Err(e) = setrlimit(Resource::Nofile, raised) {
        warn!("raising the limit on open files: {e}");
    }
}

/// A nonzero duration, since a tokio interval can't have a zero period.
fn parse_interval(s: &str) -> anyhow::Result<Duration> {
    let interval = humantime::parse_duration(s)?;
    ensure!(!interval.is_zero(), "the interval can't be 0");
    Ok(interval)
}

/// Collects every `interval`, and also every minute while free space or
/// inodes are below `--ensure-free`. If a collection deletes nothing while they
/// are short, the per-minute collections pause for 10 minutes or until the
/// next sweep.
async fn collect_garbage(store: Arc<Store>, interval: Duration, keep_recent: Duration) {
    // `run` prunes the views right before serving, so the first tick waits a
    // full period.
    let every = |period| {
        let mut ticks = tokio::time::interval_at(Instant::now() + period, period);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticks
    };
    let (mut sweep, mut check) = (every(interval), every(MINUTE));
    let mut paused_until = Instant::now();
    loop {
        let measure = || {
            store.excess().unwrap_or_else(|e| {
                warn!("measuring the node store's filesystem: {e}");
                Excess::default()
            })
        };
        let excess = tokio::select! {
            _ = sweep.tick() => {
                paused_until = Instant::now();
                measure()
            }
            _ = check.tick() => match measure() {
                excess if excess != Excess::default() && Instant::now() >= paused_until => excess,
                _ => continue,
            },
        };
        match store.collect(mounts::is_bound, keep_recent, excess).await {
            Ok(0) if excess != Excess::default() => paused_until = Instant::now() + 10 * MINUTE,
            Ok(0) => {}
            Ok(n) => info!(deleted = n, "collected the node store"),
            Err(e) => warn!("collecting the node store: {e:#}"),
        }
        // Otherwise a check that is due would run right after a sweep.
        check.reset();
    }
}

async fn serve(endpoint: &str, plugin: Arc<Plugin>) -> anyhow::Result<()> {
    let socket = PathBuf::from(endpoint.strip_prefix("unix://").unwrap_or(endpoint));
    // An earlier run may have left the socket behind.
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("listening on {}", socket.display()))?;
    info!("serving {} on {}", csi::DRIVER, socket.display());
    let mut term = signal(SignalKind::terminate()).context("handling SIGTERM")?;
    let mut int = signal(SignalKind::interrupt()).context("handling SIGINT")?;
    let shutdown = {
        let plugin = plugin.clone();
        async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            info!("shutting down");
            plugin.shut_down();
        }
    };
    tonic::transport::Server::builder()
        .add_service(IdentityServer::from_arc(plugin.clone()))
        .add_service(NodeServer::from_arc(plugin))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .context("serving")
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    use crate::narinfo::tests::{fixture_narinfos, fixtures, without_lines};

    #[test]
    fn sizes_closures() {
        let Some(dir) = fixtures() else { return };
        let narinfos = fixture_narinfos(&dir, "cache-zstd");
        let infos: Vec<_> = narinfos.iter().map(|(_, _, info)| info).collect();
        let size = closure_size(infos.iter().copied());
        assert!(
            size.starts_with(&format!("{} store paths (", infos.len()))
                && size.contains(" download, "),
            "{size}"
        );
        // A narinfo without FileSize leaves out the download size.
        let (_, text, _) = &narinfos[0];
        let without = NarInfo::parse(&StoreDir::default(), &without_lines(text, "FileSize:"));
        let size = closure_size(std::iter::once(&without.unwrap()));
        assert!(
            size.starts_with("1 store path (") && !size.contains("download"),
            "{size}"
        );
    }

    /// The chart serves metrics on `[::]`, which has to take IPv4 too. The
    /// default `net.ipv6.bindv6only=0` allows that even without the socket
    /// option, so this catches a regression only on a host or in a network
    /// namespace with `net.ipv6.bindv6only=1`.
    #[tokio::test]
    async fn listens_on_ipv4_too() {
        let listener = listen("[::]:0".parse().unwrap()).await.unwrap();
        assert!(!socket2::SockRef::from(&listener).only_v6().unwrap());
        let port = listener.local_addr().unwrap().port();
        tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
    }

    #[test]
    fn cli() {
        Cli::command().debug_assert();
        // A later value replaces an earlier one.
        let cli = Cli::try_parse_from([
            "nix-store-csi",
            "--substituters",
            "https://a",
            "--substituters",
            "https://b https://c",
            "unpack",
            "hello",
        ])
        .unwrap();
        let stores: Vec<_> = cli.substituters.split_whitespace().collect();
        assert_eq!(stores, ["https://b", "https://c"]);
    }

    #[test]
    fn rejects_zero_gc_interval() {
        let csi = |interval| {
            Cli::try_parse_from([
                "nix-store-csi",
                "csi",
                "--endpoint=/csi.sock",
                "--node-id=node",
                "--gc-interval",
                interval,
            ])
        };
        assert!(csi("0s").is_err());
        assert!(csi("1h").is_ok());
    }
}
