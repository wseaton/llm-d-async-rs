//! A real Redis per test: `redis-server` (or `REDIS_SERVER_BIN`) on a free
//! port with persistence off. Without the binary, Redis tests are skipped
//! unless `REQUIRE_REDIS` is set.

use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use crate::store::Store;
use crate::store::blob::BlobStore;
use crate::store::blob::local::LocalBlobs;
use crate::store::redis::counters::RedisCounters;
use crate::store::redis::{RedisOptions, RedisStore};
use crate::store::signal::ResultSignal;

pub struct Server {
    pub url: String,
    _child: Child,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap()
}

pub async fn server() -> Option<Server> {
    let bin = std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into());
    let port = free_port();
    let spawned = Command::new(&bin)
        .args(["--port", &port.to_string(), "--bind", "127.0.0.1"])
        .args(["--save", "", "--appendonly", "no"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn();
    let child = match spawned {
        Ok(child) => child,
        Err(e) => {
            assert!(
                std::env::var("REQUIRE_REDIS").is_err(),
                "REQUIRE_REDIS is set but {bin} cannot start: {e}"
            );
            eprintln!("skipping a Redis test: {bin}: {e}");
            return None;
        }
    };
    let url = format!("redis://127.0.0.1:{port}");
    let client = ::redis::Client::open(url.as_str()).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await
            && ::redis::cmd("PING")
                .query_async::<String>(&mut conn)
                .await
                .is_ok()
        {
            break;
        }
        assert!(Instant::now() < until, "redis-server never answered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Some(Server { url, _child: child })
}

pub fn options() -> RedisOptions {
    RedisOptions {
        claim_lease: Duration::from_secs(300),
        reclaim_interval: Duration::from_secs(3600),
        retry_queue: "retry-sortedset".into(),
        result_blob_retention: Duration::from_secs(3600),
    }
}

pub struct Fixture {
    pub store: Store,
    pub redis: RedisStore,
    pub server: Server,
    pub _dir: tempfile::TempDir,
}

pub async fn open_with(options: RedisOptions) -> Option<Fixture> {
    let server = server().await?;
    let dir = tempfile::tempdir().unwrap();
    let blobs = BlobStore::new(Arc::new(
        LocalBlobs::open(&dir.path().join("blobs")).unwrap(),
    ));
    let redis = RedisStore::open(
        &server.url,
        blobs,
        options,
        Arc::new(ResultSignal::default()),
    )
    .await
    .unwrap();
    Some(Fixture {
        store: Arc::new(redis.clone()),
        redis,
        server,
        _dir: dir,
    })
}

pub async fn fixture() -> Option<Fixture> {
    open_with(options()).await
}

/// Counters on their own Redis, kept alive by the returned server.
pub struct CountersFixture {
    pub counters: Arc<RedisCounters>,
    pub _server: Server,
}

pub async fn counters() -> Option<CountersFixture> {
    let fixture = fixture().await?;
    Some(CountersFixture {
        counters: Arc::new(fixture.redis.counters(Duration::from_secs(300))),
        _server: fixture.server,
    })
}
