//! The command line, which `docs/CLI.md` copies.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, ensure};
use clap::{Parser, Subcommand};

use crate::store::EnsureFree;

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    /// Binary caches.
    #[arg(
        long,
        value_name = "URLS",
        env = "NIX_STORE_CSI_SUBSTITUTERS",
        overrides_with = "substituters",
        default_value = "https://cache.nixos.org",
        global = true
    )]
    pub substituters: String,

    /// Keys that narinfos must be signed by.
    #[arg(
        long,
        value_name = "KEYS",
        env = "NIX_STORE_CSI_TRUSTED_PUBLIC_KEYS",
        overrides_with = "trusted_public_keys",
        default_value = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=",
        global = true
    )]
    pub trusted_public_keys: String,

    /// Accept narinfos without a trusted signature.
    #[arg(long, env = "NIX_STORE_CSI_NO_REQUIRE_SIGS", global = true)]
    pub no_require_sigs: bool,

    /// Netrc file with credentials for caches, read on every request.
    #[arg(
        long,
        value_name = "FILE",
        env = "NIX_STORE_CSI_NETRC_FILE",
        global = true
    )]
    pub netrc_file: Option<PathBuf>,

    /// Directory that holds the node store in `store/`.
    #[arg(
        long,
        value_name = "DIR",
        env = "NIX_STORE_CSI_STATE_DIR",
        default_value = "/var/lib/nix-store-csi",
        global = true
    )]
    pub state_dir: PathBuf,

    /// off, error, warn, info, debug or trace.
    #[arg(
        long,
        value_name = "LEVEL",
        env = "NIX_STORE_CSI_LOG_LEVEL",
        default_value = "info",
        global = true
    )]
    pub log_level: tracing::level_filters::LevelFilter,

    /// text or json.
    #[arg(
        long,
        value_name = "FORMAT",
        env = "NIX_STORE_CSI_LOG_FORMAT",
        default_value = "text",
        global = true
    )]
    pub log_format: LogFormat,

    /// NARs to fetch at once.
    #[arg(
        long,
        value_name = "N",
        env = "NIX_STORE_CSI_MAX_SUBSTITUTION_JOBS",
        default_value = "16",
        global = true
    )]
    pub max_substitution_jobs: std::num::NonZeroUsize,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
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
            value_parser = parse_ensure_free
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
            value_parser = parse_size
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
pub enum LogFormat {
    Text,
    Json,
}

/// A nonzero duration, since a tokio interval can't have a zero period.
fn parse_interval(s: &str) -> anyhow::Result<Duration> {
    let interval = humantime::parse_duration(s)?;
    ensure!(!interval.is_zero(), "the interval cannot be 0");
    Ok(interval)
}

/// Parses `--ensure-free` like harmonia-gc does: a percentage like `20%`, or a
/// size for [`parse_size`].
fn parse_ensure_free(s: &str) -> anyhow::Result<EnsureFree> {
    let s = s.trim();
    if let Some(p) = s.strip_suffix('%') {
        let p: f64 = (p.trim().parse()).with_context(|| format!("invalid percentage {s:?}"))?;
        ensure!(
            (0.0..=100.0).contains(&p),
            "{s:?} is not between 0% and 100%"
        );
        return Ok(EnsureFree::Percent(p));
    }
    parse_size(s).map(EnsureFree::Bytes)
}

/// Parses a size like `50G`, where K, M, G and T are powers of 1024.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "the size is checked to fit"
)]
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (number, shift) = match s.chars().last().map(|c| c.to_ascii_uppercase()) {
        Some('K') => (&s[..s.len() - 1], 10),
        Some('M') => (&s[..s.len() - 1], 20),
        Some('G') => (&s[..s.len() - 1], 30),
        Some('T') => (&s[..s.len() - 1], 40),
        _ => (s, 0),
    };
    let n: f64 = number
        .parse()
        .with_context(|| format!("invalid size {s:?}"))?;
    let bytes = n * (1u64 << shift) as f64;
    ensure!(
        (0.0..=u64::MAX as f64).contains(&bytes),
        "{s:?} is out of range"
    );
    Ok(bytes as u64)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

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

    #[test]
    fn parses_ensure_free() {
        for (s, want) in [
            ("20%", EnsureFree::Percent(20.0)),
            ("12.5 %", EnsureFree::Percent(12.5)),
            ("512", EnsureFree::Bytes(512)),
            ("1K", EnsureFree::Bytes(1 << 10)),
            ("50G", EnsureFree::Bytes(50 << 30)),
            ("1.5g", EnsureFree::Bytes(3 << 29)),
            ("2T", EnsureFree::Bytes(2 << 40)),
        ] {
            assert_eq!(parse_ensure_free(s).unwrap(), want, "{s}");
        }
        for s in ["", "101%", "-1G", "x%", "50GB", "NaN%", "inf"] {
            assert!(parse_ensure_free(s).is_err(), "{s}");
        }
    }
}
