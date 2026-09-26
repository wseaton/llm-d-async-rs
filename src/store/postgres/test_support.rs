//! Postgres test databases. Tests that need one read `TEST_DATABASE_URL`, a
//! database where they may create schemas; each fixture gets a schema of its
//! own. Without the variable they are skipped, unless `REQUIRE_POSTGRES` is
//! set (as in CI), which makes a missing database a failure.

use std::sync::Arc;
use std::time::Duration;

use crate::store::Store;
use crate::store::blob::BlobStore;
use crate::store::blob::object::ObjectBlobs;
use crate::store::blob::postgres::PostgresBlobs;
use crate::store::postgres::connect::Database;
use crate::store::postgres::{PgStore, PostgresOptions};
use crate::store::signal::ResultSignal;

pub struct Fixture {
    pub store: Store,
    pub pg: Arc<PgStore>,
    pub db: Database,
    pub results: Arc<ResultSignal>,
    _blob_dir: Option<tempfile::TempDir>,
}

/// A fresh schema in the test database, as a connection URL. `None` when
/// no test database is configured.
pub async fn schema_url() -> Option<String> {
    let Ok(base) = std::env::var("TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_POSTGRES").is_none(),
            "REQUIRE_POSTGRES is set but TEST_DATABASE_URL is not"
        );
        eprintln!("TEST_DATABASE_URL not set; skipping a Postgres test");
        return None;
    };
    let schema = format!("t_{:016x}", rand::random::<u64>());
    let db = Database::connect(&base, 1, None).await.unwrap();
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let sep = if base.contains('?') { '&' } else { '?' };
    Some(format!("{base}{sep}options=-c%20search_path%3D{schema}"))
}

pub fn options(lease_ttl: Duration) -> PostgresOptions {
    PostgresOptions {
        lease_ttl,
        result_blob_retention: Duration::from_secs(3600),
    }
}

/// Another replica on the database of `url`.
pub async fn replica(
    url: &str,
    blobs: impl FnOnce(&Database) -> BlobStore,
    lease_ttl: Duration,
) -> Fixture {
    let db = Database::connect(url, 8, None).await.unwrap();
    let results = Arc::new(ResultSignal::default());
    let pg = Arc::new(
        PgStore::open(
            db.clone(),
            blobs(&db),
            &options(lease_ttl),
            Arc::clone(&results),
        )
        .await
        .unwrap(),
    );
    Fixture {
        store: pg.clone(),
        pg,
        db,
        results,
        _blob_dir: None,
    }
}

pub fn chunk_blobs(db: &Database) -> BlobStore {
    BlobStore::new(Arc::new(PostgresBlobs::new(db.pool.clone())))
}

pub async fn fixture() -> Option<Fixture> {
    let url = schema_url().await?;
    Some(replica(&url, chunk_blobs, Duration::from_secs(30)).await)
}

pub async fn object_fixture() -> Option<Fixture> {
    let url = schema_url().await?;
    let dir = tempfile::tempdir().unwrap();
    let blob_url = format!("file://{}", dir.path().display());
    let mut fixture = replica(
        &url,
        |_| BlobStore::new(Arc::new(ObjectBlobs::from_url(&blob_url).unwrap())),
        Duration::from_secs(30),
    )
    .await;
    fixture._blob_dir = Some(dir);
    Some(fixture)
}
