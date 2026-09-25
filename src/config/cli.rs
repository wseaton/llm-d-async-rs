use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::config::ConfigError;
use crate::config::duration::parse_duration;
use crate::store::config::{Backend, BlobLocation, StoreConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    /// redb under --data-dir, owned by this process alone.
    Embedded,
    /// Postgres at --database-url, shared by every replica.
    Postgres,
}

/// Asynchronous dispatch processor for llm-d.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Where queues, results, and control values live.
    #[arg(long, value_enum, default_value_t = StoreKind::Embedded)]
    pub store: StoreKind,
    /// Directory holding the embedded store.
    #[arg(long, default_value = "data")]
    pub data_dir: PathBuf,
    /// Postgres connection URL (`postgres://...` or `key=value` form). TLS
    /// follows its `sslmode`.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    pub database_url: Option<String>,
    /// Connections each replica keeps to Postgres.
    #[arg(long, default_value_t = 32)]
    pub database_max_connections: usize,
    /// Extra CA certificate (PEM) for verifying Postgres.
    #[arg(long)]
    pub database_ca_cert: Option<PathBuf>,
    /// How long a replica's queue partitions, request claims and quota slots
    /// outlive its last heartbeat.
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub partition_lease_ttl: Duration,
    /// Where request bodies over --inline-payload-limit and binary results
    /// (audio, images) go. With --store postgres, an object store URL
    /// (`s3://bucket/prefix`, `gs://`, `az://`, `file://`) is required;
    /// `postgres` keeps them in the database, for development only. The
    /// embedded store uses `local` files under --data-dir.
    #[arg(long, env = "BLOB_STORE")]
    pub blob_store: Option<BlobLocation>,

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

    pub fn store_config(&self) -> Result<StoreConfig, ConfigError> {
        let backend = match self.store {
            StoreKind::Embedded => Backend::Embedded {
                dir: self.data_dir.clone(),
            },
            StoreKind::Postgres => Backend::Postgres {
                url: self
                    .database_url
                    .clone()
                    .filter(|u| !u.is_empty())
                    .ok_or_else(|| {
                        ConfigError::Cli("--store postgres requires --database-url".into())
                    })?,
                max_connections: self.database_max_connections,
                ca_cert: self.database_ca_cert.clone(),
                lease_ttl: self.partition_lease_ttl,
            },
        };
        let config = StoreConfig {
            backend,
            blobs: self.blob_store.clone(),
            result_blob_retention: self.result_blob_retention,
        };
        config
            .validate()
            .map_err(|e| ConfigError::Cli(e.to_string()))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.database_max_connections == 0 {
            return Err(ConfigError::Cli(
                "--database-max-connections must be positive".into(),
            ));
        }
        if self.partition_lease_ttl.is_zero() {
            return Err(ConfigError::Cli(
                "--partition-lease-ttl must be positive".into(),
            ));
        }
        self.store_config()?;
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

    use crate::config::cli::{Cli, StoreKind};

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
        assert_eq!(cli.store, StoreKind::Embedded);
        assert_eq!(cli.partition_lease_ttl, Duration::from_secs(30));
        assert_eq!(cli.blob_store, None);
        cli.validate().unwrap();
    }

    #[test]
    fn postgres_needs_a_database_and_a_shared_blob_store() {
        let parse = |args: &[&str]| {
            let mut all = vec!["x", "--transport-config", "{}"];
            all.extend_from_slice(args);
            Cli::try_parse_from(all).unwrap()
        };
        let no_url = Cli {
            database_url: None,
            ..parse(&["--store", "postgres"])
        };
        assert!(no_url.validate().is_err());
        let url = ["--store", "postgres", "--database-url", "postgres://db/x"];
        let no_blobs = Cli {
            blob_store: None,
            ..parse(&url)
        };
        assert!(no_blobs.validate().is_err(), "large bodies need a store");
        let mut rows = url.to_vec();
        rows.extend(["--blob-store", "postgres"]);
        parse(&rows).validate().unwrap();
        let mut local = url.to_vec();
        local.extend(["--blob-store", "local"]);
        assert!(parse(&local).validate().is_err());
        let mut s3 = url.to_vec();
        s3.extend(["--blob-store", "s3://bucket/p"]);
        parse(&s3).validate().unwrap();
        assert!(Cli::try_parse_from(["x", "--blob-store", "bucket"]).is_err());
        assert!(Cli::try_parse_from(["x", "--store", "redis"]).is_err());
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
