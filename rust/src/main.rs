use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use harmonia_store_path::StoreDir;
use tokio::net::UnixListener;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{error, info, warn};

use crate::csi::Plugin;
use crate::csi::proto::identity_server::IdentityServer;
use crate::csi::proto::node_server::NodeServer;
use crate::narinfo::PublicKey;
use crate::store::{Config, Store};

mod cache_info;
mod closure;
mod csi;
mod mounts;
mod nar;
mod narinfo;
mod store;
mod store_path;
mod transport;

const MINUTE: Duration = Duration::from_mins(1);

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
    substituters: Vec<String>,

    /// Keys that narinfos must be signed by.
    #[arg(
        long,
        value_name = "KEYS",
        env = "NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS",
        overrides_with = "trusted_public_keys",
        default_value = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=",
        global = true
    )]
    trusted_public_keys: Vec<String>,

    /// Accept narinfos without a trusted signature.
    #[arg(long, global = true)]
    no_require_sigs: bool,

    /// Holds the node store, in `store/`.
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

    /// Store dir that store paths are named under.
    #[arg(
        long,
        value_name = "DIR",
        default_value = "/nix/store",
        value_parser = store_path::parse_store_dir,
        global = true
    )]
    store_dir: StoreDir,

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
        /// Name that pods give as the volume's `driver`.
        #[arg(long, value_name = "NAME", default_value = "nix-store-csi")]
        driver_name: String,
        /// Delete store paths no pod has used for this long, like `24h` or `7d`.
        /// `0s` keeps everything.
        #[arg(
            long,
            value_name = "DURATION",
            env = "NIX_STORE_CSI_GC_AFTER",
            default_value = "24h",
            value_parser = humantime::parse_duration
        )]
        gc_after: Duration,
        /// Collect when the state dir's filesystem is fuller than this percent,
        /// least recently used first. `100` turns this off.
        #[arg(
            long,
            value_name = "PERCENT",
            env = "NIX_STORE_CSI_GC_HIGH_THRESHOLD",
            default_value = "85",
            value_parser = clap::value_parser!(u8).range(1..=100)
        )]
        gc_high_threshold: u8,
        /// How full to collect down to.
        #[arg(
            long,
            value_name = "PERCENT",
            env = "NIX_STORE_CSI_GC_LOW_THRESHOLD",
            default_value = "80",
            value_parser = clap::value_parser!(u8).range(1..=100)
        )]
        gc_low_threshold: u8,
    },
    /// Fetch the closures of ROOTs into the node store and print its directory,
    /// for use with `podman -v DIR:/nix/store`.
    Unpack {
        /// Store paths or their base names.
        #[arg(value_name = "ROOT", required = true)]
        roots: Vec<String>,
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
    let trusted_keys = words(&cli.trusted_public_keys)
        .map(|k| k.parse().with_context(|| format!("parsing public key {k}")))
        .collect::<anyhow::Result<Vec<PublicKey>>>()?;
    if trusted_keys.is_empty() && !cli.no_require_sigs {
        bail!("no trusted public keys given; pass --no-require-sigs to accept unsigned paths");
    }
    let stores: Vec<_> = words(&cli.substituters).map(str::to_owned).collect();
    if stores.is_empty() {
        bail!("no substituters given");
    }
    let store = Arc::new(Store::new(Config {
        stores,
        trusted_keys: (!cli.no_require_sigs).then_some(trusted_keys),
        state_dir: cli.state_dir,
        store_dir: cli.store_dir,
        jobs: cli.max_substitution_jobs.get(),
    })?);

    match cli.command {
        Command::Unpack { roots } => {
            let roots = store.roots(roots.iter().map(String::as_str))?;
            let names = store.ensure(&roots).done().await?;
            info!(paths = names.len(), "unpacked");
            println!("{}", store.dir().display());
            Ok(())
        }
        Command::Csi {
            endpoint,
            node_id,
            driver_name,
            gc_after,
            gc_high_threshold,
            gc_low_threshold,
        } => {
            if gc_low_threshold > gc_high_threshold {
                bail!("--gc-low-threshold is above --gc-high-threshold");
            }
            store.prune_views(mounts::is_bound).await?;
            tokio::spawn(collect_garbage(
                store.clone(),
                gc_after,
                gc_high_threshold,
                gc_low_threshold,
            ));
            let plugin = Arc::new(Plugin::new(driver_name, node_id, store));
            serve(&endpoint, plugin).await
        }
    }
}

/// Lists are space-separated, as in Nix. Any whitespace counts, since one from
/// a file or a YAML block string can end in a newline.
fn words<'a>(values: impl IntoIterator<Item = &'a String>) -> impl Iterator<Item = &'a str> {
    values.into_iter().flat_map(|v| v.split_whitespace())
}

/// Collects every 10 minutes, and every minute that the disk is too full.
/// After a minute's collection frees nothing, the next waits for the 10.
async fn collect_garbage(store: Arc<Store>, unused_for: Duration, high: u8, low: u8) {
    // Views were pruned just before serving.
    let every = |period| {
        let mut every = tokio::time::interval_at(Instant::now() + period, period);
        every.set_missed_tick_behavior(MissedTickBehavior::Delay);
        every
    };
    let (mut sweep, mut check) = (every(10 * MINUTE), every(MINUTE));
    let mut futile = false;
    loop {
        let excess = || {
            store.excess(high, low).unwrap_or_else(|e| {
                warn!("measuring the node store's filesystem: {e}");
                0
            })
        };
        let free = tokio::select! {
            _ = sweep.tick() => {
                futile = false;
                excess()
            }
            _ = check.tick() => match excess() {
                free if free > 0 && !futile => free,
                _ => continue,
            },
        };
        match store.collect(mounts::is_bound, unused_for, free).await {
            Ok(0) => futile = free > 0,
            Ok(n) => info!(deleted = n, "collected the node store"),
            Err(e) => warn!("collecting the node store: {e:#}"),
        }
    }
}

async fn serve(endpoint: &str, plugin: Arc<Plugin>) -> anyhow::Result<()> {
    let socket = PathBuf::from(endpoint.strip_prefix("unix://").unwrap_or(endpoint));
    // An earlier run may have left the socket behind.
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("listening on {}", socket.display()))?;
    info!("serving {} on {}", plugin.name(), socket.display());
    tonic::transport::Server::builder()
        .add_service(IdentityServer::from_arc(plugin.clone()))
        .add_service(NodeServer::from_arc(plugin))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown())
        .await
        .context("serving")
}

async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing a SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("shutting down");
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn splits_lists() {
        let values = ["a b\n".to_owned(), "\tc  ".to_owned(), String::new()];
        assert_eq!(words(&values).collect::<Vec<_>>(), ["a", "b", "c"]);
    }

    #[test]
    fn cli() {
        Cli::command().debug_assert();
        let cli = Cli::try_parse_from([
            "nix-store-csi",
            "--substituters",
            "https://a https://b",
            "csi",
            "--endpoint",
            "unix:///csi/csi.sock",
            "--node-id",
            "n1",
        ])
        .unwrap();
        let stores: Vec<_> = words(&cli.substituters).collect();
        assert_eq!(stores, ["https://a", "https://b"]);
        assert_eq!(cli.store_dir.to_str(), "/nix/store");
        let Command::Csi {
            endpoint,
            node_id,
            driver_name,
            gc_after,
            gc_high_threshold,
            gc_low_threshold,
        } = cli.command
        else {
            panic!()
        };
        assert_eq!(endpoint, "unix:///csi/csi.sock");
        assert_eq!(node_id, "n1");
        assert_eq!(driver_name, "nix-store-csi");
        assert_eq!(gc_after, Duration::from_hours(24));
        assert_eq!((gc_high_threshold, gc_low_threshold), (85, 80));
        let cli = Cli::try_parse_from(["nix-store-csi", "unpack", "hello"]).unwrap();
        assert_eq!(cli.substituters, ["https://cache.nixos.org"]);
        assert_eq!(cli.trusted_public_keys.len(), 1);

        // As in Nix, a later value replaces an earlier one.
        let cli = Cli::try_parse_from([
            "nix-store-csi",
            "--substituters",
            "https://a",
            "--substituters",
            "https://b",
            "unpack",
            "hello",
        ])
        .unwrap();
        assert_eq!(cli.substituters, ["https://b"]);
    }
}
