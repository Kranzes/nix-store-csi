use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use anyhow::{Context, bail};
use bytesize::ByteSize;
use clap::Parser;
use rustix::fs::Mode;
use rustix::io::Errno;
use rustix::process::{Resource, Rlimit, getrlimit, setrlimit, umask};
use tokio::net::{TcpListener, TcpSocket, UnixListener};
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::{error, info, warn};

use crate::cli::{Cli, Command, LogFormat};
use crate::closure::Caches;
use crate::csi::Plugin;
use crate::csi::proto::identity_server::IdentityServer;
use crate::csi::proto::node_server::NodeServer;
use crate::narinfo::{NarInfo, PublicKey};
use crate::store::{Config, EnsureFree, Excess, Store};

mod cli;
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
    // Restore creates files with their canonical modes, and a stricter umask
    // would take bits off them. Nix sets the same one.
    umask(Mode::from_raw_mode(0o022));
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
    let config = Config {
        stores,
        trusted_keys: (!cli.no_require_sigs).then_some(trusted_keys),
        state_dir: cli.state_dir,
        jobs: cli.max_substitution_jobs.get(),
        netrc_file: cli.netrc_file,
        max_closure_size: None,
        ensure_free: EnsureFree::Bytes(0),
        measure: false,
    };
    match cli.command {
        Command::Unpack { roots, dry_run } => unpack(config, &roots, dry_run).await,
        Command::Csi {
            endpoint,
            node_id,
            keep_recent,
            gc_interval,
            ensure_free,
            metrics_address,
            max_closure_size,
            prefetch_dir,
        } => {
            let config = Config {
                max_closure_size,
                ensure_free,
                measure: metrics_address.is_some(),
                ..config
            };
            let store = Arc::new(Store::new(config)?);
            store.prune_views(mounts::is_bound).await?;
            if let Some(address) = metrics_address {
                let listener = listen(address)
                    .await
                    .with_context(|| format!("listening on {address} for metrics"))?;
                let address = listener.local_addr()?;
                let url = format!("http://{address}/metrics");
                info!(%url, "serving metrics");
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

/// Fetches the closures of `roots` and prints the node store's directory.
/// With `dry_run`, it only resolves them against the caches and prints their
/// size, without opening the state dir.
async fn unpack(config: Config, roots: &[String], dry_run: bool) -> anyhow::Result<()> {
    let roots = store_path::roots(roots.iter().map(String::as_str))?;
    if dry_run {
        let caches = Caches::new(
            &config.stores,
            config.trusted_keys,
            None,
            config.netrc_file.as_deref(),
        )?;
        let closure = caches.resolve(&roots, &AtomicUsize::default()).await?;
        println!("{}", closure_size(closure.values().map(|e| &e.info)));
        return Ok(());
    }
    let store = Arc::new(Store::new(config)?);
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
        warn!("raising the limit on open files: {e}; keeping the current limit");
    }
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
                warn!(
                    "measuring the node store's filesystem: {e}; collecting only by --keep-recent"
                );
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
            Err(e) => {
                warn!("collecting the node store: {e:#}; trying again at the next collection");
            }
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
    info!(socket = %socket.display(), "serving the CSI plugin");
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
    use harmonia_store_path::StoreDir;

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
}
