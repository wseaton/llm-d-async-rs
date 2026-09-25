use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, Table};

use crate::store::blob::BlobBody;
use crate::store::blob::key::BlobKey;
use crate::store::embedded::EmbeddedStore;
use crate::store::embedded::tables::{
    BLOBS, BlobRecord, RESULT_CLAIMS, RESULTS, ResultClaimRecord, ResultRecord, TOMBSTONES,
};
use crate::store::error::StoreError;
use crate::store::queue::{ACK_TOMBSTONE_TTL_MS, AckOutcome, ResultClaim};

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

impl EmbeddedStore {
    pub(crate) async fn pop(
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

    pub(crate) async fn claim(
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

    pub(crate) async fn renew(
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

    pub(crate) async fn ack(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> Result<AckOutcome, StoreError> {
        let (outcome, dropped) = self
            .run(move |db| {
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
                            if let Some(blob) =
                                claim.record.blob.as_deref().and_then(BlobKey::parse)
                            {
                                blob_table.remove(blob.to_string().as_str())?;
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
                Ok((outcome, dropped))
            })
            .await?;
        self.blobs.remove_dropped(&dropped).await;
        Ok(outcome)
    }

    pub(crate) async fn result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> Result<Option<(BlobBody, String)>, StoreError> {
        let lookup = key.to_string();
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
            .open(&key)
            .await?
            .map(|body| (body, record.content_type)))
    }

    pub(crate) async fn depth(&self, route: String) -> Result<u64, StoreError> {
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
    pub(crate) async fn sweep_results(&self, now_ms: i64) -> Result<u64, StoreError> {
        let (removed, dropped) = self.run(move |db| {
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
            Ok((removed, dropped))
        })
        .await?;
        self.blobs.remove_dropped(&dropped).await;
        Ok(removed)
    }
}
