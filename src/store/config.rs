//! Which store and blob backend a process runs on.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::gate::admission::counters::Counters;
use crate::gate::admission::counters::local::LocalCounters;
use crate::store::Store;
use crate::store::blob::BlobStore;
use crate::store::blob::local::LocalBlobs;
use crate::store::blob::object::ObjectBlobs;
use crate::store::blob::postgres::PostgresBlobs;
use crate::store::embedded::EmbeddedStore;
use crate::store::error::StoreError;
use crate::store::postgres::connect::Database;
use crate::store::postgres::{PgStore, PostgresOptions};
use crate::store::signal::ResultSignal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// redb in `dir`; one process.
    Embedded { dir: PathBuf },
    /// Shared by every replica on the database.
    Postgres {
        url: String,
        max_connections: usize,
        ca_cert: Option<PathBuf>,
        lease_ttl: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobLocation {
    /// Files under the embedded store's directory.
    Local,
    /// Chunked rows in the Postgres store's database. Every byte goes
    /// through Postgres's WAL and memory, so only for development and tests.
    Postgres,
    /// An object store URL: `s3://`, `gs://`, `az://`, `file://`. Postgres
    /// store only.
    Object(String),
}

impl FromStr for BlobLocation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(Self::Local),
            "postgres" => Ok(Self::Postgres),
            url if url.contains("://") => Ok(Self::Object(url.to_owned())),
            other => Err(format!(
                "want 'local', 'postgres', or an object store URL, got {other:?}"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub backend: Backend,
    /// `None` means local files for the embedded store. The Postgres store
    /// has no default: its large bodies belong in an object store.
    pub blobs: Option<BlobLocation>,
    pub result_blob_retention: Duration,
}

pub struct Opened {
    pub store: Store,
    pub counters: Arc<dyn Counters>,
}

fn url_of(location: &BlobLocation) -> Result<&str, StoreError> {
    match location {
        BlobLocation::Object(url) => Ok(url),
        other => Err(StoreError::Config(format!(
            "{other:?} blobs cannot be shared by Postgres replicas"
        ))),
    }
}

impl StoreConfig {
    fn blob_location(&self) -> Result<BlobLocation, StoreError> {
        match (&self.backend, &self.blobs) {
            (Backend::Embedded { .. }, None) => Ok(BlobLocation::Local),
            (Backend::Postgres { .. }, None) => Err(StoreError::Config(
                "--store postgres needs --blob-store: an object store URL (s3://bucket/prefix) \
                 for request and result bodies over --inline-payload-limit, or 'postgres' to \
                 keep them in the database (development only)"
                    .into(),
            )),
            (Backend::Embedded { .. }, Some(BlobLocation::Postgres)) => Err(StoreError::Config(
                "--blob-store postgres needs --store postgres".into(),
            )),
            (Backend::Postgres { .. }, Some(BlobLocation::Local)) => Err(StoreError::Config(
                "--blob-store local cannot be shared by replicas; use postgres or an object store URL"
                    .into(),
            )),
            (Backend::Embedded { .. }, Some(BlobLocation::Object(_))) => Err(StoreError::Config(
                "an embedded store deletes every blob it does not reference, so it cannot share an \
                 object store; use --blob-store local, or --store postgres"
                    .into(),
            )),
            (_, Some(location)) => Ok(location.clone()),
        }
    }

    /// Checks the combination without opening anything.
    pub fn validate(&self) -> Result<(), StoreError> {
        self.blob_location().map(|_| ())
    }

    /// Opens the store. `results` is notified whenever a result is written,
    /// by this process or, on a shared store, any other.
    pub async fn open(&self, results: Arc<ResultSignal>) -> Result<Opened, StoreError> {
        let location = self.blob_location()?;
        match &self.backend {
            Backend::Embedded { dir } => {
                let blobs = BlobStore::new(Arc::new(LocalBlobs::open(&dir.join("blobs"))?));
                let (store, recovery) =
                    EmbeddedStore::open(dir, blobs, self.result_blob_retention).await?;
                tracing::info!(
                    data_dir = %dir.display(),
                    recovered_claims = recovery.claims,
                    orphan_blobs = recovery.orphan_blobs,
                    "embedded store opened"
                );
                Ok(Opened {
                    store: Arc::new(store),
                    counters: Arc::new(LocalCounters::default()),
                })
            }
            Backend::Postgres {
                url,
                max_connections,
                ca_cert,
                lease_ttl,
            } => {
                let db = Database::connect(url, *max_connections, ca_cert.as_deref()).await?;
                let blobs = match &location {
                    BlobLocation::Postgres => {
                        tracing::warn!(
                            "large request and result bodies are kept in Postgres; use an \
                             object store (--blob-store s3://...) outside development"
                        );
                        BlobStore::new(Arc::new(PostgresBlobs::new(db.pool.clone())))
                    }
                    _ => BlobStore::new(Arc::new(ObjectBlobs::from_url(url_of(&location)?)?)),
                };
                let options = PostgresOptions {
                    lease_ttl: *lease_ttl,
                    result_blob_retention: self.result_blob_retention,
                };
                let store = PgStore::open(db, blobs, &options, results).await?;
                let counters = Arc::new(store.counters());
                Ok(Opened {
                    store: Arc::new(store),
                    counters,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use crate::store::config::{Backend, BlobLocation, StoreConfig};

    fn config(backend: Backend, blobs: Option<BlobLocation>) -> StoreConfig {
        StoreConfig {
            backend,
            blobs,
            result_blob_retention: Duration::from_secs(1),
        }
    }

    fn postgres() -> Backend {
        Backend::Postgres {
            url: "postgres://x".into(),
            max_connections: 1,
            ca_cert: None,
            lease_ttl: Duration::from_secs(30),
        }
    }

    fn embedded() -> Backend {
        Backend::Embedded {
            dir: PathBuf::from("d"),
        }
    }

    #[test]
    fn blob_locations_parse() {
        assert_eq!("local".parse(), Ok(BlobLocation::Local));
        assert_eq!("postgres".parse(), Ok(BlobLocation::Postgres));
        assert_eq!(
            "s3://bucket/prefix".parse(),
            Ok(BlobLocation::Object("s3://bucket/prefix".into()))
        );
        assert!("bucket".parse::<BlobLocation>().is_err());
    }

    #[test]
    fn replicas_cannot_share_local_blobs() {
        assert!(
            config(postgres(), Some(BlobLocation::Local))
                .validate()
                .is_err()
        );
        assert!(
            config(embedded(), Some(BlobLocation::Postgres))
                .validate()
                .is_err()
        );
        assert!(
            config(embedded(), Some(BlobLocation::Object("file:///x".into())))
                .validate()
                .is_err()
        );
        let no_blobs = config(postgres(), None).validate().unwrap_err().to_string();
        assert!(no_blobs.contains("s3://"), "{no_blobs}");
        for ok in [
            config(embedded(), None),
            config(embedded(), Some(BlobLocation::Local)),
            config(postgres(), Some(BlobLocation::Postgres)),
            config(postgres(), Some(BlobLocation::Object("s3://b".into()))),
        ] {
            ok.validate().unwrap();
        }
    }
}
