//! Durable queue state in an embedded redb database.
//!
//! One process owns the database file. That is what lets a claim be a plain
//! row instead of a lease: whoever opens the store is the only possible owner,
//! so every claim left behind by a previous process is returned to its queue.
//! The blob store must belong to this database alone, since blobs nothing
//! here references are deleted.

mod kv;
mod requests;
mod results;
mod tables;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};

use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::clock::now_millis;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBody, BlobStore};
use crate::store::embedded::tables::{
    ACTIVE, BLOBS, CANCELLED, CLAIMED, ClaimedRecord, KV, META, PAYLOADS, PENDING, RESULT_CLAIMS,
    RESULTS, RETRY, TOMBSTONES,
};
use crate::store::error::StoreError;
use crate::store::queue::{
    AckOutcome, Admission, Admitted, Applied, Backlog, NewRequest, Outcome, PayloadBody, Peeked,
    RequestStatus, ResultClaim,
};
use crate::store::{QueueStore, Stamped};

const DB_FILE: &str = "llm-d-async.redb";

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Claims of a previous process returned to their queues.
    pub claims: usize,
    /// Blobs nothing referenced, deleted.
    pub orphan_blobs: usize,
}

#[derive(Clone)]
pub struct EmbeddedStore {
    db: Arc<Database>,
    blobs: BlobStore,
    next_claim_id: Arc<AtomicU64>,
    result_blob_retention_ms: i64,
}

impl EmbeddedStore {
    /// Opens (or creates) the store in `dir`, returns every claim a previous
    /// process left behind to its queue, and deletes unreferenced blobs.
    pub async fn open(
        dir: &Path,
        blobs: BlobStore,
        result_blob_retention: Duration,
    ) -> Result<(Self, Recovery), StoreError> {
        let path = dir.to_owned();
        let (db, claims) = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&path)?;
            let db = Database::create(path.join(DB_FILE))?;
            let txn = db.begin_write()?;
            for_each_table(&txn)?;
            let claims = recover_claims(&txn)?;
            txn.commit()?;
            Ok::<_, StoreError>((db, claims))
        })
        .await??;
        let store = Self {
            db: Arc::new(db),
            blobs,
            next_claim_id: Arc::new(AtomicU64::new(1)),
            result_blob_retention_ms: i64::try_from(result_blob_retention.as_millis())
                .unwrap_or(i64::MAX),
        };
        let orphan_blobs = store.collect_orphan_blobs(i64::MAX).await?;
        Ok((
            store,
            Recovery {
                claims,
                orphan_blobs,
            },
        ))
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

    async fn check(&self) -> Result<(), StoreError> {
        self.run(|db| {
            let txn = db.begin_read()?;
            txn.open_table(META)?;
            Ok(())
        })
        .await
    }

    async fn collect_orphan_blobs(&self, cutoff_ms: i64) -> Result<usize, StoreError> {
        let listed = self.blobs.list(cutoff_ms).await?;
        if listed.is_empty() {
            return Ok(0);
        }
        let referenced = self.run(referenced_blobs).await?;
        let orphans: Vec<BlobKey> = listed
            .into_iter()
            .map(|l| l.key)
            .filter(|key| !referenced.contains(key))
            .collect();
        for key in &orphans {
            self.blobs.remove(key).await?;
        }
        Ok(orphans.len())
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

fn referenced_blobs(db: &Database) -> Result<BTreeSet<BlobKey>, StoreError> {
    let txn = db.begin_read()?;
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

impl QueueStore for EmbeddedStore {
    fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    fn ping(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.check())
    }

    fn join(&self, _queue: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn leave(&self, _queue: &str) {}

    fn submit(&self, requests: Vec<NewRequest>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.submit_requests(requests))
    }

    fn peek(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Peeked>, StoreError>> {
        Box::pin(self.peek_queue(queue, limit, now_ms))
    }

    fn has_pending(&self, queue: String, _now_ms: i64) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.queue_has_pending(queue))
    }

    fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Admitted>, StoreError>> {
        Box::pin(self.admit_rows(queue, admissions, now_ms))
    }

    fn apply_outcomes(
        &self,
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Applied, StoreError>> {
        Box::pin(self.end_claims(outcomes, now_ms))
    }

    fn promote_due_retries(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.promote_retries(now_ms, limit))
    }

    fn open_payload<'a>(
        &'a self,
        envelope: &'a InternalRequest,
    ) -> BoxFuture<'a, Result<Option<PayloadBody>, StoreError>> {
        Box::pin(self.payload(envelope))
    }

    fn is_cancelled(
        &self,
        id: String,
        token: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.cancelled(id, token, now_ms))
    }

    fn request_status(
        &self,
        id: String,
        token: Option<String>,
    ) -> BoxFuture<'_, Result<RequestStatus, StoreError>> {
        Box::pin(self.status(id, token))
    }

    fn cancel(&self, ids: Vec<String>, now_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.cancel_ids(ids, now_ms))
    }

    fn backlog(
        &self,
        queue: String,
        now_ms: i64,
        bounds_s: Vec<i64>,
    ) -> BoxFuture<'_, Result<Backlog, StoreError>> {
        Box::pin(self.queue_backlog(queue, now_ms.div_euclid(1000), bounds_s))
    }

    fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
        Box::pin(self.pop(route, now_ms))
    }

    fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<ResultClaim>, StoreError>> {
        Box::pin(self.claim(route, owner, lease_ms, now_ms))
    }

    fn renew_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.renew(route, claim_id, owner, lease_ms, now_ms))
    }

    fn ack_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<AckOutcome, StoreError>> {
        Box::pin(self.ack(route, claim_id, owner, now_ms))
    }

    fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<(BlobBody, String)>, StoreError>> {
        Box::pin(self.result_blob(key, now_ms))
    }

    fn result_depth(&self, route: String, _now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.depth(route))
    }

    fn kv_get(&self, key: String) -> BoxFuture<'_, Result<Stamped<Option<Vec<u8>>>, StoreError>> {
        Box::pin(async move {
            Ok(Stamped {
                value: self.get_value(key).await?,
                now_ms: now_millis(),
            })
        })
    }

    fn kv_put(&self, key: String, value: Option<Vec<u8>>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.put_value(key, value))
    }

    fn sweep(&self, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(async move {
            let markers = self.sweep_request_markers(now_ms).await?;
            let results = self.sweep_results(now_ms).await?;
            Ok(markers + results)
        })
    }

    fn collect_orphans(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.collect_orphan_blobs(cutoff_ms))
    }

    fn close(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::store::Store;
    use crate::store::blob::BlobStore;
    use crate::store::blob::local::LocalBlobs;
    use crate::store::embedded::{EmbeddedStore, Recovery};

    pub struct Fixture {
        pub store: Store,
        pub dir: tempfile::TempDir,
    }

    pub async fn open_in(dir: &Path) -> (EmbeddedStore, Recovery) {
        let blobs = BlobStore::new(Arc::new(LocalBlobs::open(&dir.join("blobs")).unwrap()));
        EmbeddedStore::open(dir, blobs, Duration::from_secs(3600))
            .await
            .unwrap()
    }

    pub async fn open() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let (store, recovery) = open_in(dir.path()).await;
        assert_eq!(recovery.claims, 0);
        Fixture {
            store: Arc::new(store),
            dir,
        }
    }

    pub async fn fixture() -> Option<Fixture> {
        Some(open().await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::store::Store;
    use crate::store::blob::key::BlobKey;
    use crate::store::conformance::{NOW_MS, conformance_tests};
    use crate::store::embedded::tables::{CLAIMED, RETRY};
    use crate::store::embedded::test_support::{open, open_in};
    use crate::store::queue::{Admission, Admitted, ClaimRef, Outcome};
    use crate::store::staging::PayloadSink;
    use crate::store::test_support::new_request;

    conformance_tests!(crate::store::embedded::test_support::fixture);

    async fn claim_head(store: &Store) -> ClaimRef {
        let head = store.peek("q".into(), 1, NOW_MS).await.unwrap().remove(0);
        let generation = head.envelope.unwrap().generation_key();
        let out = store
            .admit(
                "q".into(),
                vec![Admission::Claim {
                    key: head.key,
                    generation,
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        let Some(Admitted::Claimed { claim, .. }) = out.into_iter().next() else {
            panic!("not claimed")
        };
        claim
    }

    #[tokio::test]
    async fn reopening_recovers_claims_and_deletes_orphan_blobs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = open_in(dir.path()).await;
            let store: Store = Arc::new(store);
            let mut sink = PayloadSink::new(store.blobs().clone(), "0a", 4, 1 << 20);
            sink.push(b"0123456789").await.unwrap();
            let payload = sink.finish().await.unwrap();
            let mut req = new_request("a", "q", 100);
            req.envelope.routing.request_token = "0a".into();
            req.envelope.payload = payload.info("audio/wav");
            req.payload = payload;
            store.submit(vec![req]).await.unwrap();
            claim_head(&store).await;
            let mut sink = PayloadSink::new(store.blobs().clone(), "0b", 4, 1 << 20);
            sink.push(b"9876543210").await.unwrap();
            sink.finish().await.unwrap();
            let mut w = store
                .blobs()
                .create(&BlobKey::result("0c", 1).unwrap())
                .await
                .unwrap();
            w.write(Bytes::from_static(b"result")).await.unwrap();
            w.commit().await.unwrap();
        }
        let (store, recovery) = open_in(dir.path()).await;
        assert_eq!(recovery.claims, 1);
        assert_eq!(recovery.orphan_blobs, 2);
        let store: Store = Arc::new(store);
        claim_head(&store).await;
        assert!(!dir.path().join("blobs/requests/0b").exists());
        assert!(dir.path().join("blobs/requests/0a").exists());
    }

    #[tokio::test]
    async fn undecodable_rows_are_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let (embedded, _) = open_in(dir.path()).await;
        embedded
            .run(|db| {
                let txn = db.begin_write()?;
                {
                    txn.open_table(RETRY)?.insert((0, 1), "garbage")?;
                    txn.open_table(CLAIMED)?.insert("x\u{0}ff", "garbage")?;
                }
                txn.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        let store: Store = Arc::new(embedded);
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let head = store.peek("q".into(), 1, NOW_MS).await.unwrap().remove(0);
        let envelope = head.envelope.clone().unwrap();
        let claim = claim_head(&store).await;
        store
            .apply_outcomes(
                vec![
                    Outcome::Release {
                        claim: ClaimRef {
                            generation: "x\u{0}ff".into(),
                            claim_id: 1,
                        },
                    },
                    Outcome::Retry {
                        claim,
                        envelope,
                        due_ms: NOW_MS,
                    },
                ]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(store.promote_due_retries(NOW_MS, 10).await.unwrap(), 1);
        assert!(store.has_pending("q".into(), NOW_MS).await.unwrap());
    }

    #[tokio::test]
    async fn fixture_store_is_empty() {
        let fixture = open().await;
        assert!(!fixture.store.has_pending("q".into(), NOW_MS).await.unwrap());
        assert!(fixture.dir.path().join("blobs").exists());
    }
}
