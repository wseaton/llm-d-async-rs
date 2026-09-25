use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::config::ConfigError;
use crate::config::duration::parse_duration;

/// Asynchronous dispatch processor for llm-d.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Directory holding the embedded store.
    #[arg(long, default_value = "data")]
    pub data_dir: PathBuf,

    /// Request payloads up to this many bytes are stored in the database;
    /// larger ones are streamed to blob files.
    #[arg(long, default_value_t = 64 * 1024)]
    pub inline_payload_limit: usize,
    /// Largest accepted request payload, in bytes.
    #[arg(long, default_value_t = 1 << 30)]
    pub max_payload_bytes: u64,
    /// Largest JSON submission body, in bytes. JSON bodies are buffered in
    /// memory; send larger payloads as multipart.
    #[arg(long, default_value_t = 64 << 20)]
    pub max_json_body_bytes: usize,
    /// How long a result body stays readable after a destructive pop.
    #[arg(long, default_value = "24h", value_parser = parse_duration)]
    pub result_blob_retention: Duration,

    /// Address of the producer/consumer HTTP API.
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub api_addr: SocketAddr,
    /// Port of the /healthz and /readyz probes.
    #[arg(long, default_value_t = 8081)]
    pub health_port: u16,
    /// Port of the Prometheus /metrics endpoint.
    #[arg(long, default_value_t = 9090)]
    pub metrics_port: u16,

    /// Workers in the default pool when no --pool-config-file is given.
    #[arg(long, default_value_t = 64)]
    pub concurrency: usize,
    /// Timeout of one inference attempt.
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub request_timeout: Duration,
    /// Maximum time a worker parks one request at a pool gate before
    /// re-enqueueing it (0 waits until the request deadline).
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub gate_wait_timeout: Duration,
    /// Maximum time to wait for in-flight requests after SIGTERM.
    #[arg(long, default_value = "2m", value_parser = parse_duration)]
    pub drain_timeout: Duration,
    /// Path to the worker pools JSON file.
    #[arg(long)]
    pub pool_config_file: Option<PathBuf>,

    /// Inline JSON transport configuration.
    #[arg(long, conflicts_with = "transport_config_file")]
    pub transport_config: Option<String>,
    /// Path to the transport configuration JSON file.
    #[arg(long)]
    pub transport_config_file: Option<PathBuf>,
    /// If positive, periodically reload the queues of --transport-config-file.
    #[arg(long, default_value = "0", value_parser = parse_duration)]
    pub transport_config_watch_interval: Duration,
    /// Path to the request merge policy JSON file (default random-robin).
    #[arg(long)]
    pub request_merge_policy_config_file: Option<PathBuf>,
    /// Interval of the queue backlog metrics poll (0 disables).
    #[arg(long, default_value = "15s", value_parser = parse_duration)]
    pub metrics_backlog_poll_interval: Duration,

    /// CA certificate (PEM) for verifying the inference gateway.
    #[arg(long)]
    pub tls_ca_cert: Option<PathBuf>,
    /// Client certificate (PEM) for mTLS.
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,
    /// Client key (PKCS#8 PEM) for mTLS.
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
    /// Skip TLS certificate verification (dev/test only).
    #[arg(long)]
    pub tls_insecure_skip_verify: bool,

    /// Prometheus server URL for metric-based gates.
    #[arg(long)]
    pub prometheus_url: Option<String>,
    /// TTL of cached metric-source reads (0 disables caching).
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    pub prometheus_cache_ttl: Duration,

    /// Log verbosity: 2 info, 3-4 debug, 5+ trace. RUST_LOG overrides it.
    #[arg(short = 'v', long = "v", default_value_t = 2)]
    pub verbosity: u8,
}

impl Cli {
    pub fn transport_config_bytes(&self) -> Result<Vec<u8>, ConfigError> {
        match (&self.transport_config, &self.transport_config_file) {
            (Some(inline), None) => Ok(inline.clone().into_bytes()),
            (None, Some(path)) => std::fs::read(path).map_err(|source| ConfigError::Read {
                path: path.clone(),
                source,
            }),
            _ => Err(ConfigError::Cli(
                "exactly one of --transport-config or --transport-config-file is required".into(),
            )),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.concurrency == 0 {
            return Err(ConfigError::Cli("--concurrency must be positive".into()));
        }
        if !self.transport_config_watch_interval.is_zero() && self.transport_config_file.is_none() {
            return Err(ConfigError::Cli(
                "--transport-config-watch-interval requires --transport-config-file".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use crate::config::cli::Cli;

    #[test]
    fn defaults_match_the_go_processor() {
        let cli = Cli::try_parse_from(["llm-d-async", "--transport-config", "{}"]).unwrap();
        assert_eq!(cli.concurrency, 64);
        assert_eq!(cli.request_timeout, Duration::from_secs(300));
        assert_eq!(cli.gate_wait_timeout, Duration::from_secs(300));
        assert_eq!(cli.drain_timeout, Duration::from_secs(120));
        assert_eq!(cli.metrics_backlog_poll_interval, Duration::from_secs(15));
        assert_eq!(cli.prometheus_cache_ttl, Duration::from_secs(5));
        assert_eq!(cli.verbosity, 2);
        assert_eq!(cli.inline_payload_limit, 65536);
        assert_eq!(cli.result_blob_retention, Duration::from_secs(86_400));
        cli.validate().unwrap();
    }

    #[test]
    fn rejects_conflicting_flags() {
        assert!(
            Cli::try_parse_from([
                "x",
                "--transport-config",
                "{}",
                "--transport-config-file",
                "f"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["x", "--tls-cert", "c"]).is_err());
        assert!(Cli::try_parse_from(["x", "--request-timeout", "5"]).is_err());
        let cli = Cli::try_parse_from([
            "x",
            "--transport-config",
            "{}",
            "--transport-config-watch-interval",
            "10s",
        ])
        .unwrap();
        assert!(cli.validate().is_err());
        let cli = Cli::try_parse_from(["x"]).unwrap();
        assert!(cli.transport_config_bytes().is_err());
    }
}
