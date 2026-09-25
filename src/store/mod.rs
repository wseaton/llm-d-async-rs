//! Durable queue state in an embedded redb database, with large bodies in
//! blob files beside it.
//!
//! One process owns the database file. That is what lets a claim be a plain
//! row instead of a lease: whoever opens the store is the only possible owner,
//! so every claim left behind by a previous process is returned to its queue.

pub mod blob;
pub mod error;
pub mod kv;
pub mod requests;
pub mod results;
pub mod staging;
mod tables;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};

use crate::api::payload::PayloadStorage;
use crate::api::request::InternalRequest;
use crate::store::blob::{BlobKey, BlobStore};
use crate::store::error::StoreError;
use crate::store::tables::{
    ACTIVE, BLOBS, CANCELLED, CLAIMED, ClaimedRecord, KV, META, PAYLOADS, PENDING, RESULT_CLAIMS,
    RESULTS, RETRY, TOMBSTONES,
};

const DB_FILE: &str = "llm-d-async.redb";
const BLOB_DIR: &str = "blobs";

#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// How long a result body blob stays readable after its result is
    /// taken by a destructive pop. Acknowledged claims delete it at once.
    pub result_blob_retention: Duration,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Claims of a previous process returned to their queues.
    pub claims: usize,
    /// Blob files nothing referenced, deleted.
    pub orphan_blobs: usize,
}

/// A request body opened for sending.
pub enum PayloadBody {
    Inline(Bytes),
    File { file: tokio::fs::File, size: u64 },
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    blobs: BlobStore,
    next_claim_id: Arc<AtomicU64>,
    result_blob_retention_ms: i64,
}

impl Store {
    /// Opens (or creates) the store, returns every claim a previous process
    /// left behind to its queue, and deletes unreferenced blob files.
    pub fn open(dir: &Path, options: &StoreOptions) -> Result<(Self, Recovery), StoreError> {
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join(DB_FILE))?;
        let blobs = BlobStore::open(&dir.join(BLOB_DIR))?;
        let mut recovery = Recovery::default();
        let referenced;
        let txn = db.begin_write()?;
        {
            for_each_table(&txn)?;
            recovery.claims = recover_claims(&txn)?;
            referenced = referenced_blobs(&txn)?;
        }
        txn.commit()?;
        for key in blobs.list()? {
            if !referenced.contains(&key) {
                blobs.remove(&key)?;
                recovery.orphan_blobs += 1;
            }
        }
        let result_blob_retention_ms =
            i64::try_from(options.result_blob_retention.as_millis()).unwrap_or(i64::MAX);
        Ok((
            Self {
                db: Arc::new(db),
                blobs,
                next_claim_id: Arc::new(AtomicU64::new(1)),
                result_blob_retention_ms,
            },
            recovery,
        ))
    }

    pub fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    /// Runs blocking store work off the async runtime.
    async fn run<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T, StoreError> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db)).await?
    }

    /// Readiness probe: a read transaction succeeds.
    pub async fn ping(&self) -> Result<(), StoreError> {
        self.run(|db| {
            let txn = db.begin_read()?;
            txn.open_table(META)?;
            Ok(())
        })
        .await
    }

    /// Opens the body of a claimed request. `None` when it is missing.
    pub async fn open_payload(
        &self,
        envelope: &InternalRequest,
    ) -> Result<Option<PayloadBody>, StoreError> {
        match envelope.payload.storage {
            PayloadStorage::Inline => {
                let generation = envelope.generation_key();
                self.run(move |db| {
                    let txn = db.begin_read()?;
                    let payloads = txn.open_table(PAYLOADS)?;
                    Ok(payloads
                        .get(generation.as_str())?
                        .map(|v| PayloadBody::Inline(Bytes::copy_from_slice(v.value()))))
                })
                .await
            }
            PayloadStorage::Blob => {
                let Some(key) = BlobKey::request(&envelope.routing.request_token) else {
                    return Ok(None);
                };
                Ok(self
                    .blobs
                    .open_file(&key)
                    .await?
                    .map(|(file, size)| PayloadBody::File { file, size }))
            }
        }
    }
}

fn for_each_table(txn: &WriteTransaction) -> Result<(), StoreError> {
    txn.open_table(PENDING)?;
    txn.open_table(PAYLOADS)?;
    txn.open_table(BLOBS)?;
    txn.open_table(CLAIMED)?;
    txn.open_table(RETRY)?;
    txn.open_table(ACTIVE)?;
    txn.open_table(CANCELLED)?;
    txn.open_table(RESULTS)?;
    txn.open_table(RESULT_CLAIMS)?;
    txn.open_table(TOMBSTONES)?;
    txn.open_table(KV)?;
    txn.open_table(META)?;
    Ok(())
}

fn recover_claims(txn: &WriteTransaction) -> Result<usize, StoreError> {
    let mut claimed = txn.open_table(CLAIMED)?;
    let mut pending = txn.open_table(PENDING)?;
    let mut records = Vec::new();
    for entry in claimed.iter()? {
        let (key, value) = entry?;
        match serde_json::from_str::<ClaimedRecord>(value.value()) {
            Ok(record) => records.push(record),
            Err(e) => {
                tracing::error!(generation = key.value(), error = %e, "dropping undecodable claim")
            }
        }
    }
    for r in &records {
        pending.insert((r.queue.as_str(), r.deadline, r.seq), r.envelope.as_str())?;
    }
    claimed.retain(|_, _| false)?;
    Ok(records.len())
}

fn referenced_blobs(txn: &WriteTransaction) -> Result<BTreeSet<BlobKey>, StoreError> {
    let blobs = txn.open_table(BLOBS)?;
    let mut keys = BTreeSet::new();
    for entry in blobs.iter()? {
        let (key, _) = entry?;
        if let Some(key) = BlobKey::parse(key.value()) {
            keys.insert(key);
        }
    }
    Ok(keys)
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::api::request::{InternalRequest, RequestMessage};
    use crate::api::routing::InternalRouting;
    use crate::store::requests::NewRequest;
    use crate::store::staging::StagedPayload;
    use crate::store::{Store, StoreOptions};

    pub fn options() -> StoreOptions {
        StoreOptions {
            result_blob_retention: Duration::from_secs(3600),
        }
    }

    pub fn open() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let (store, recovery) = Store::open(dir.path(), &options()).unwrap();
        assert_eq!(recovery.claims, 0);
        (dir, store)
    }

    pub fn envelope(id: &str, token: &str, queue: &str, deadline: i64) -> InternalRequest {
        InternalRequest {
            routing: InternalRouting {
                request_token: token.into(),
                request_queue_name: queue.into(),
                result_queue_name: "results".into(),
                ..Default::default()
            },
            request: RequestMessage {
                id: id.into(),
                created: 0,
                deadline,
                metadata: BTreeMap::new(),
                headers: BTreeMap::new(),
                endpoint: String::new(),
                model: String::new(),
            },
            payload: StagedPayload::Inline(Vec::new()).info("application/json"),
        }
    }

    /// An inline-payload request whose token is the hex of its ID's bytes.
    pub fn new_request(id: &str, queue: &str, deadline: i64) -> NewRequest {
        let token: String = id.bytes().map(|b| format!("{b:02x}")).collect();
        let payload = StagedPayload::Inline(format!(r#"{{"prompt":"{id}"}}"#).into_bytes());
        let mut envelope = envelope(id, &token, queue, deadline);
        envelope.payload = payload.info("application/json");
        NewRequest { envelope, payload }
    }
}
