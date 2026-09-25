use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use postgres_native_tls::MakeTlsConnector;

use crate::store::error::StoreError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_WAIT: Duration = Duration::from_secs(30);
/// Partition heartbeats get connections of their own: waiting behind the
/// data path for longer than a lease would hand the partitions away.
const CONTROL_CONNECTIONS: usize = 2;

/// Connection pools plus what it takes to open connections of their own.
#[derive(Clone)]
pub struct Database {
    pub pool: Pool,
    /// For partition leases only.
    pub control: Pool,
    config: tokio_postgres::Config,
    tls: MakeTlsConnector,
}

impl Database {
    /// Connects to `url` (`postgres://...` or `key=value` form). TLS follows
    /// the URL's `sslmode` and verifies against the system roots plus
    /// `ca_cert`.
    pub async fn connect(
        url: &str,
        max_connections: usize,
        ca_cert: Option<&Path>,
    ) -> Result<Self, StoreError> {
        let mut config = tokio_postgres::Config::from_str(url)
            .map_err(|e| StoreError::Config(format!("database URL: {e}")))?;
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(CONNECT_TIMEOUT);
        }
        if config.get_application_name().is_none() {
            config.application_name("llm-d-async");
        }
        let mut tls = native_tls::TlsConnector::builder();
        if let Some(path) = ca_cert {
            let pem = std::fs::read(path)?;
            let cert = native_tls::Certificate::from_pem(&pem)
                .map_err(|e| StoreError::Config(format!("{}: {e}", path.display())))?;
            tls.add_root_certificate(cert);
        }
        let tls = MakeTlsConnector::new(
            tls.build()
                .map_err(|e| StoreError::Config(format!("TLS: {e}")))?,
        );
        let pool = |size: usize| {
            let manager = Manager::from_config(
                config.clone(),
                tls.clone(),
                ManagerConfig {
                    recycling_method: RecyclingMethod::Fast,
                },
            );
            Pool::builder(manager)
                .max_size(size.max(1))
                .runtime(Runtime::Tokio1)
                .create_timeout(Some(CONNECT_TIMEOUT))
                .wait_timeout(Some(POOL_WAIT))
                .build()
                .map_err(|e| StoreError::Config(format!("connection pool: {e}")))
        };
        let control = pool(CONTROL_CONNECTIONS)?;
        let pool = pool(max_connections)?;
        drop(pool.get().await?);
        Ok(Self {
            pool,
            control,
            config,
            tls,
        })
    }

    /// A connection outside the pool, for LISTEN. The caller drives
    /// `connection`.
    pub async fn dedicated(
        &self,
    ) -> Result<
        (
            tokio_postgres::Client,
            tokio_postgres::Connection<
                tokio_postgres::Socket,
                postgres_native_tls::TlsStream<tokio_postgres::Socket>,
            >,
        ),
        StoreError,
    > {
        Ok(self.config.connect(self.tls.clone()).await?)
    }
}
