use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, Table};

use crate::store::Store;
use crate::store::blob::BlobKey;
use crate::store::error::StoreError;
use crate::store::tables::{
    BLOBS, BlobRecord, RESULT_CLAIMS, RESULTS, ResultClaimRecord, ResultRecord, TOMBSTONES,
};

/// Acknowledged claims are remembered this long so a retried ack succeeds.
const ACK_TOMBSTONE_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultClaim {
    pub claim_id: u64,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Acked,
    AlreadyAcked,
    OwnershipLost,
}

type RouteTable<'t> = Table<'t, (&'static str, u64), &'static str>;

fn route_range(route: &str) -> std::ops::RangeInclusive<(&str, u64)> {
    (route, 0)..=(route, u64::MAX)
}

/// Returns claims whose lease lapsed to the route at their original position.
fn requeue_expired_claims(
    claims: &mut RouteTable<'_>,
    results: &mut RouteTable<'_>,
    route: &str,
    now_ms: i64,
) -> Result<(), StoreError> {
    let mut expired = Vec::new();
    for entry in claims.range(route_range(route))? {
        let (key, value) = entry?;
        match serde_json::from_str::<ResultClaimRecord>(value.value()) {
            Ok(claim) if claim.lease_until_ms <= now_ms => {
                expired.push((key.value().1, Some(claim.record)));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(%route, error = %e, "removing undecodable result claim");
                expired.push((key.value().1, None));
            }
        }
    }
    for (seq, record) in expired {
        claims.remove((route, seq))?;
        if let Some(record) = record
            && !record.expired(now_ms)
        {
            results.insert((route, seq), serde_json::to_string(&record)?.as_str())?;
        }
    }
    Ok(())
}

/// Removes and returns the oldest unexpired result of `route`.
fn take_oldest(
    results: &mut RouteTable<'_>,
    route: &str,
    now_ms: i64,
) -> Result<Option<(u64, ResultRecord)>, StoreError> {
    loop {
        let head = results
            .range(route_range(route))?
            .next()
            .transpose()?
            .map(|(k, v)| (k.value().1, v.value().to_owned()));
        let Some((seq, value)) = head else {
            return Ok(None);
        };
        results.remove((route, seq))?;
        match serde_json::from_str::<ResultRecord>(&value) {
            Ok(record) if !record.expired(now_ms) => return Ok(Some((seq, record))),
            Ok(_) => {}
            Err(e) => tracing::error!(%route, error = %e, "dropping undecodable result"),
        }
    }
}

impl Store {
    /// Destructively takes the oldest result of `route`. A body blob it
    /// references stays readable for the result blob retention.
    pub async fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        let retention_ms = self.result_blob_retention_ms;
        self.run(move |db| {
            let txn = db.begin_write()?;
            let taken;
            {
                let mut results = txn.open_table(RESULTS)?;
                taken = take_oldest(&mut results, &route, now_ms)?;
                if let Some(blob) = taken.as_ref().and_then(|(_, r)| r.blob.as_deref()) {
                    let mut blobs = txn.open_table(BLOBS)?;
                    let current = blobs
                        .get(blob)?
                        .map(|v| serde_json::from_str::<BlobRecord>(v.value()))
                        .transpose()?;
                    if let Some(mut record) = current {
                        let retained = now_ms.saturating_add(retention_ms);
                        record.expires_at_ms =
                            Some(record.expires_at_ms.map_or(retained, |at| at.min(retained)));
                        blobs.insert(blob, serde_json::to_string(&record)?.as_str())?;
                    }
                }
            }
            txn.commit()?;
            Ok(taken.map(|(_, r)| r.body))
        })
        .await
    }

    /// Leases the oldest result of `route` to `owner`. The result stays
    /// durable until acknowledged and returns to the route if the lease lapses.
    pub async fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<Option<ResultClaim>, StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            let claimed;
            {
                let mut results = txn.open_table(RESULTS)?;
                let mut claims = txn.open_table(RESULT_CLAIMS)?;
                requeue_expired_claims(&mut claims, &mut results, &route, now_ms)?;
                claimed = take_oldest(&mut results, &route, now_ms)?;
                if let Some((seq, record)) = &claimed {
                    let claim = ResultClaimRecord {
                        owner,
                        lease_until_ms: now_ms.saturating_add(lease_ms),
                        record: record.clone(),
                    };
                    claims.insert(
                        (route.as_str(), *seq),
                        serde_json::to_string(&claim)?.as_str(),
                    )?;
                }
            }
            txn.commit()?;
            Ok(claimed.map(|(claim_id, record)| ResultClaim {
                claim_id,
                body: record.body,
            }))
        })
        .await
    }

    /// Extends a live lease. Returns false when `owner` no longer holds it.
    pub async fn renew_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            let renewed;
            {
                let mut claims = txn.open_table(RESULT_CLAIMS)?;
                let current = claims
                    .get((route.as_str(), claim_id))?
                    .map(|v| serde_json::from_str::<ResultClaimRecord>(v.value()))
                    .transpose()?;
                renewed = match current {
                    Some(mut claim) if claim.owner == owner && claim.lease_until_ms > now_ms => {
                        claim.lease_until_ms = now_ms.saturating_add(lease_ms);
                        claims.insert(
                            (route.as_str(), claim_id),
                            serde_json::to_string(&claim)?.as_str(),
                        )?;
                        true
                    }
                    _ => false,
                };
            }
            txn.commit()?;
            Ok(renewed)
        })
        .await
    }

    /// Acknowledges a claimed result and deletes its body blob: the consumer
    /// has taken what it needs. Repeating a successful ack is safe.
    pub async fn ack_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> Result<AckOutcome, StoreError> {
        let blobs = self.blobs.clone();
        self.run(move |db| {
            let txn = db.begin_write()?;
            let outcome;
            let mut dropped = Vec::new();
            {
                let mut claims = txn.open_table(RESULT_CLAIMS)?;
                let mut tombstones = txn.open_table(TOMBSTONES)?;
                let mut blob_table = txn.open_table(BLOBS)?;
                let key = (route.as_str(), claim_id);
                let current = claims
                    .get(key)?
                    .map(|v| serde_json::from_str::<ResultClaimRecord>(v.value()))
                    .transpose()?;
                outcome = match current {
                    Some(claim) if claim.owner == owner && claim.lease_until_ms > now_ms => {
                        claims.remove(key)?;
                        tombstones.insert(key, now_ms.saturating_add(ACK_TOMBSTONE_TTL_MS))?;
                        if let Some(blob) = claim.record.blob.as_deref().and_then(BlobKey::parse) {
                            blob_table.remove(blob.as_str())?;
                            dropped.push(blob);
                        }
                        AckOutcome::Acked
                    }
                    _ => match tombstones.get(key)?.map(|v| v.value()) {
                        Some(expires) if expires > now_ms => AckOutcome::AlreadyAcked,
                        _ => AckOutcome::OwnershipLost,
                    },
                };
            }
            txn.commit()?;
            blobs.remove_dropped(&dropped);
            Ok(outcome)
        })
        .await
    }

    /// Opens a result body blob for reading, with its content type. `None`
    /// when it is unknown, expired, or already deleted.
    pub async fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> Result<Option<(tokio::fs::File, u64, String)>, StoreError> {
        let lookup = key.clone();
        let record = self
            .run(move |db| {
                let txn = db.begin_read()?;
                let blobs = txn.open_table(BLOBS)?;
                blobs
                    .get(lookup.as_str())?
                    .map(|v| serde_json::from_str::<BlobRecord>(v.value()))
                    .transpose()
                    .map_err(StoreError::from)
            })
            .await?;
        let Some(record) = record.filter(|r| r.expires_at_ms.is_none_or(|at| at > now_ms)) else {
            return Ok(None);
        };
        Ok(self
            .blobs
            .open_file(&key)
            .await?
            .map(|(file, size)| (file, size, record.content_type)))
    }

    /// Results waiting on `route`, excluding leased ones.
    pub async fn result_depth(&self, route: String) -> Result<u64, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let results = txn.open_table(RESULTS)?;
            let mut depth = 0;
            for entry in results.range(route_range(&route))? {
                entry?;
                depth += 1;
            }
            Ok(depth)
        })
        .await
    }

    /// Deletes expired results and their claims, old ack tombstones, and
    /// blobs past their retention. Returns rows removed.
    pub async fn sweep_results(&self, now_ms: i64) -> Result<u64, StoreError> {
        let blobs = self.blobs.clone();
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut removed = 0;
            let mut dropped = Vec::new();
            {
                let mut results = txn.open_table(RESULTS)?;
                let before = results.len()?;
                results.retain(|_, v| {
                    serde_json::from_str::<ResultRecord>(v)
                        .map(|r| !r.expired(now_ms))
                        .unwrap_or(false)
                })?;
                removed += before - results.len()?;

                let mut claims = txn.open_table(RESULT_CLAIMS)?;
                let before = claims.len()?;
                claims.retain(|_, v| {
                    serde_json::from_str::<ResultClaimRecord>(v)
                        .map(|c| !c.record.expired(now_ms))
                        .unwrap_or(false)
                })?;
                removed += before - claims.len()?;

                let mut tombstones = txn.open_table(TOMBSTONES)?;
                let before = tombstones.len()?;
                tombstones.retain(|_, expires| expires > now_ms)?;
                removed += before - tombstones.len()?;

                let mut blob_table = txn.open_table(BLOBS)?;
                let mut expired = Vec::new();
                for entry in blob_table.iter()? {
                    let (key, value) = entry?;
                    match serde_json::from_str::<BlobRecord>(value.value()) {
                        Ok(record) if record.expires_at_ms.is_some_and(|at| at <= now_ms) => {
                            expired.push(key.value().to_owned());
                        }
                        Ok(_) => {}
                        // Keep it: the blob may still back a queued request.
                        Err(e) => tracing::error!(blob = key.value(), error = %e, "undecodable blob record"),
                    }
                }
                for key in expired {
                    blob_table.remove(key.as_str())?;
                    removed += 1;
                    if let Some(key) = BlobKey::parse(&key) {
                        dropped.push(key);
                    }
                }
            }
            txn.commit()?;
            blobs.remove_dropped(&dropped);
            Ok(removed)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use crate::api::result::{ResultMessage, StoredBody};
    use crate::store::Store;
    use crate::store::blob::BlobKey;
    use crate::store::requests::{Admission, Admitted, Outcome};
    use crate::store::results::{AckOutcome, ResultClaim};
    use crate::store::test_support::{new_request, open};

    const NOW: i64 = 1_000_000;

    /// Submits `ids` to queue "q" with the given result TTL and finishes each
    /// with `make_result`.
    async fn finish_with(
        store: &Store,
        ids: &[&str],
        ttl_s: u64,
        make_result: impl Fn(&crate::api::request::InternalRequest) -> ResultMessage,
    ) {
        let mut reqs = Vec::new();
        for id in ids {
            let mut r = new_request(id, "q", 100);
            r.envelope.routing.result_ttl_seconds = ttl_s;
            reqs.push(r);
        }
        store.submit(reqs).await.unwrap();
        for peeked in store.peek("q".into(), ids.len()).await.unwrap() {
            let env = peeked.envelope.unwrap();
            let out = store
                .admit(
                    "q".into(),
                    vec![Admission::Claim {
                        key: peeked.key,
                        generation: env.generation_key(),
                    }],
                    NOW,
                )
                .await
                .unwrap();
            let Some(Admitted::Claimed(claim)) = out.into_iter().next() else {
                panic!("not claimed")
            };
            let result = make_result(&env);
            store
                .apply_outcomes(
                    vec![Outcome::Finish {
                        claim,
                        envelope: env,
                        result,
                    }],
                    NOW,
                )
                .await
                .unwrap();
        }
    }

    async fn finish(store: &Store, ids: &[&str], ttl_s: u64) {
        finish_with(store, ids, ttl_s, |env| {
            ResultMessage::http(env, 200, env.request.id.as_bytes())
        })
        .await;
    }

    /// Finishes one request whose result body is a blob named after its token.
    async fn finish_by_reference(store: &Store, id: &str, ttl_s: u64) -> BlobKey {
        let token: String = id.bytes().map(|b| format!("{b:02x}")).collect();
        let key = BlobKey::result(&token).unwrap();
        let mut w = store.blobs().writer().await.unwrap();
        w.write(b"audio-bytes").await.unwrap();
        let digest = w.commit(&key).await.unwrap();
        let payload_ref = key.to_ref();
        finish_with(store, &[id], ttl_s, move |env| {
            ResultMessage::http_by_reference(
                env,
                200,
                StoredBody {
                    payload_ref: payload_ref.clone(),
                    content_type: "audio/wav".into(),
                    size: digest.size,
                    sha256: digest.sha256.clone(),
                },
            )
        })
        .await;
        key
    }

    fn id_of(body: &str) -> String {
        serde_json::from_str::<ResultMessage>(body).unwrap().id
    }

    #[tokio::test]
    async fn pop_is_fifo() {
        let (_dir, store) = open();
        finish(&store, &["a", "b"], 0).await;
        assert_eq!(store.result_depth("results".into()).await.unwrap(), 2);
        let first = store
            .pop_result("results".into(), NOW)
            .await
            .unwrap()
            .unwrap();
        let second = store
            .pop_result("results".into(), NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((id_of(&first), id_of(&second)), ("a".into(), "b".into()));
        assert_eq!(store.pop_result("results".into(), NOW).await.unwrap(), None);
        assert_eq!(
            store.pop_result("elsewhere".into(), NOW).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn expired_results_are_skipped_and_swept() {
        let (_dir, store) = open();
        finish(&store, &["a"], 10).await;
        assert_eq!(
            store
                .pop_result("results".into(), NOW + 10_000)
                .await
                .unwrap(),
            None
        );
        finish(&store, &["b"], 10).await;
        assert_eq!(store.sweep_results(NOW + 9_999).await.unwrap(), 0);
        assert_eq!(store.sweep_results(NOW + 10_000).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn claim_ack_is_idempotent_and_fenced() {
        let (_dir, store) = open();
        finish(&store, &["a"], 0).await;
        let route = || "results".to_string();
        let ResultClaim { claim_id, body } = store
            .claim_result(route(), "me".into(), 1_000, NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id_of(&body), "a");
        assert_eq!(
            store
                .claim_result(route(), "other".into(), 1_000, NOW)
                .await
                .unwrap(),
            None
        );
        assert!(
            !store
                .renew_result(route(), claim_id, "other".into(), 1_000, NOW)
                .await
                .unwrap()
        );
        assert!(
            store
                .renew_result(route(), claim_id, "me".into(), 1_000, NOW + 500)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .ack_result(route(), claim_id, "other".into(), NOW + 600)
                .await
                .unwrap(),
            AckOutcome::OwnershipLost
        );
        assert_eq!(
            store
                .ack_result(route(), claim_id, "me".into(), NOW + 600)
                .await
                .unwrap(),
            AckOutcome::Acked
        );
        assert_eq!(
            store
                .ack_result(route(), claim_id, "me".into(), NOW + 700)
                .await
                .unwrap(),
            AckOutcome::AlreadyAcked
        );
        assert_eq!(
            store
                .claim_result(route(), "me".into(), 1_000, NOW)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn lapsed_lease_is_redelivered_in_order() {
        let (_dir, store) = open();
        finish(&store, &["a", "b"], 0).await;
        let route = || "results".to_string();
        let first = store
            .claim_result(route(), "crashed".into(), 1_000, NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id_of(&first.body), "a");
        let again = store
            .claim_result(route(), "survivor".into(), 1_000, NOW + 1_000)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.claim_id, first.claim_id);
        assert_eq!(id_of(&again.body), "a");
        assert_eq!(
            store
                .ack_result(route(), first.claim_id, "crashed".into(), NOW + 1_000)
                .await
                .unwrap(),
            AckOutcome::OwnershipLost
        );
    }

    #[tokio::test]
    async fn ack_deletes_the_result_blob() {
        let (dir, store) = open();
        let key = finish_by_reference(&store, "a", 0).await;
        let (mut file, size, content_type) = store
            .open_result_blob(key.clone(), NOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((size, content_type.as_str()), (11, "audio/wav"));
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"audio-bytes");
        let claim = store
            .claim_result("results".into(), "me".into(), 1_000, NOW)
            .await
            .unwrap()
            .unwrap();
        let msg: ResultMessage = serde_json::from_str(&claim.body).unwrap();
        assert_eq!(msg.payload_ref, key.to_ref());
        assert_eq!(msg.payload_size, 11);
        store
            .ack_result("results".into(), claim.claim_id, "me".into(), NOW)
            .await
            .unwrap();
        assert!(store.open_result_blob(key, NOW).await.unwrap().is_none());
        assert!(!dir.path().join("blobs/results/61").exists());
    }

    #[tokio::test]
    async fn queued_result_keeps_its_blob_past_the_retention() {
        let (_dir, store) = open();
        let key = finish_by_reference(&store, "a", 0).await;
        store.sweep_results(NOW + 10 * 3_600_000).await.unwrap();
        assert!(
            store
                .open_result_blob(key, NOW + 10 * 3_600_000)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn popped_result_blob_expires_with_retention_or_ttl() {
        let (dir, store) = open();
        let retained = finish_by_reference(&store, "a", 0).await;
        let ttl = finish_by_reference(&store, "b", 5).await;
        store
            .pop_result("results".into(), NOW)
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .open_result_blob(retained.clone(), NOW)
                .await
                .unwrap()
                .is_some()
        );
        store.sweep_results(NOW + 5_000).await.unwrap();
        assert!(
            store
                .open_result_blob(ttl, NOW + 5_000)
                .await
                .unwrap()
                .is_none()
        );
        assert!(!dir.path().join("blobs/results/62").exists());
        assert!(
            store
                .open_result_blob(retained.clone(), NOW + 5_000)
                .await
                .unwrap()
                .is_some()
        );
        store.sweep_results(NOW + 3_600_000).await.unwrap();
        assert!(!dir.path().join("blobs/results/61").exists());
    }
}
