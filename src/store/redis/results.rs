use crate::api::result::ResultMessage;
use crate::store::blob::BlobBody;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::queue::{ACK_TOMBSTONE_TTL_MS, AckOutcome, ResultClaim};
use crate::store::redis::{RedisStore, keys, millis};

/// Results a receive looks at per call, as upstream.
const RECEIVE_BATCH: i64 = 100;

impl RedisStore {
    /// Drops `route` if its expiry passed. True when it did.
    async fn expire_route(&self, route: &str, now_ms: i64) -> Result<bool, StoreError> {
        let dropped: i64 = self
            .inner
            .scripts
            .expire_check
            .key(route)
            .arg(now_ms)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(dropped == 1)
    }

    pub(crate) async fn pop(
        &self,
        route: String,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        self.expire_route(&route, now_ms).await?;
        let body: Option<String> = ::redis::cmd("RPOP")
            .arg(&route)
            .query_async(&mut self.conn())
            .await?;
        if let Some(blob) = body
            .as_deref()
            .and_then(|b| serde_json::from_str::<ResultMessage>(b).ok())
            .and_then(|r| BlobKey::from_ref(&r.payload_ref))
        {
            let retained = now_ms.saturating_add(millis(self.inner.options.result_blob_retention));
            let _: i64 = self
                .inner
                .scripts
                .retain_blob
                .key(keys::BLOB_TYPES)
                .key(keys::BLOB_EXPIRY)
                .arg(blob.to_string())
                .arg(retained)
                .invoke_async(&mut self.conn())
                .await?;
        }
        Ok(body)
    }

    pub(crate) async fn claim(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<Option<ResultClaim>, StoreError> {
        loop {
            let got: Vec<::redis::Value> = self
                .inner
                .scripts
                .receive
                .key(&route)
                .key(keys::result_claimed(&route))
                .key(keys::result_claim_owners(&route))
                .key(keys::result_claims_idx(&route))
                .key(keys::result_tombstones(&route))
                .arg(&owner)
                .arg(lease_ms)
                .arg(now_ms)
                .arg(RECEIVE_BATCH)
                .invoke_async(&mut self.conn())
                .await?;
            let mut got = got.into_iter();
            let status: i64 = match got.next() {
                None => return Ok(None),
                Some(v) => ::redis::from_redis_value(v)?,
            };
            if status != 1 {
                continue;
            }
            let (Some(body), Some(field)) = (got.next(), got.next()) else {
                return Err(StoreError::Config(
                    "result receive returned a short reply".into(),
                ));
            };
            let body: String = ::redis::from_redis_value(body)?;
            let field: String = ::redis::from_redis_value(field)?;
            let claim_id = keys::claim_id_of(&field);
            let _: i64 = ::redis::cmd("HSET")
                .arg(keys::result_claim_ids(&route))
                .arg(claim_id)
                .arg(&field)
                .query_async(&mut self.conn())
                .await?;
            return Ok(Some(ResultClaim { claim_id, body }));
        }
    }

    async fn claim_field(&self, route: &str, claim_id: u64) -> Result<Option<String>, StoreError> {
        Ok(::redis::cmd("HGET")
            .arg(keys::result_claim_ids(route))
            .arg(claim_id)
            .query_async(&mut self.conn())
            .await?)
    }

    pub(crate) async fn renew(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let Some(field) = self.claim_field(&route, claim_id).await? else {
            return Ok(false);
        };
        let renewed: i64 = self
            .inner
            .scripts
            .renew_result
            .key(keys::result_claim_owners(&route))
            .key(keys::result_claims_idx(&route))
            .arg(&field)
            .arg(&owner)
            .arg(lease_ms)
            .arg(now_ms)
            .invoke_async(&mut self.conn())
            .await?;
        Ok(renewed == 1)
    }

    pub(crate) async fn ack(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> Result<AckOutcome, StoreError> {
        let Some(field) = self.claim_field(&route, claim_id).await? else {
            return Ok(AckOutcome::OwnershipLost);
        };
        let reply: Vec<::redis::Value> = self
            .inner
            .scripts
            .ack_result
            .key(keys::result_claimed(&route))
            .key(keys::result_claim_owners(&route))
            .key(keys::result_claims_idx(&route))
            .key(keys::result_tombstones(&route))
            .key(keys::BLOB_TYPES)
            .key(keys::BLOB_EXPIRY)
            .arg(&field)
            .arg(&owner)
            .arg(ACK_TOMBSTONE_TTL_MS)
            .arg(now_ms)
            .invoke_async(&mut self.conn())
            .await?;
        let mut reply = reply.into_iter();
        let code: i64 = match reply.next() {
            Some(v) => ::redis::from_redis_value(v)?,
            None => 0,
        };
        Ok(match code {
            1 => {
                let blob: String = match reply.next() {
                    Some(v) => ::redis::from_redis_value(v)?,
                    None => String::new(),
                };
                if let Some(key) = BlobKey::parse(&blob)
                    && let Err(e) = self.inner.blobs.remove(&key).await
                {
                    tracing::warn!(blob = %key, error = %e, "deleting an acknowledged result's body failed; orphan collection will retry");
                }
                AckOutcome::Acked
            }
            2 => AckOutcome::AlreadyAcked,
            _ => AckOutcome::OwnershipLost,
        })
    }

    pub(crate) async fn result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> Result<Option<(BlobBody, String)>, StoreError> {
        let name = key.to_string();
        let (content_type, expires): (Option<String>, Option<f64>) = ::redis::pipe()
            .hget(keys::BLOB_TYPES, &name)
            .zscore(keys::BLOB_EXPIRY, &name)
            .query_async(&mut self.conn())
            .await?;
        let Some(content_type) = content_type else {
            return Ok(None);
        };
        if expires.is_some_and(|at| at as i64 <= now_ms) {
            return Ok(None);
        }
        Ok(self
            .inner
            .blobs
            .open(&key)
            .await?
            .map(|body| (body, content_type)))
    }

    pub(crate) async fn depth(&self, route: String, now_ms: i64) -> Result<u64, StoreError> {
        self.expire_route(&route, now_ms).await?;
        Ok(::redis::cmd("LLEN")
            .arg(&route)
            .query_async(&mut self.conn())
            .await?)
    }

    /// Drops expired result routes, lapsed ack tombstones and claim IDs no
    /// claim or tombstone needs, and blobs past their lifetime. Returns
    /// entries removed.
    pub(crate) async fn sweep_results(&self, now_ms: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn();
        let mut removed = 0;
        let routes: Vec<String> = ::redis::cmd("SMEMBERS")
            .arg(keys::RESULT_ROUTES)
            .query_async(&mut conn)
            .await?;
        for route in routes {
            if self.expire_route(&route, now_ms).await? {
                removed += 1;
            }
            let lapsed: u64 = ::redis::cmd("ZREMRANGEBYSCORE")
                .arg(keys::result_tombstones(&route))
                .arg("-inf")
                .arg(now_ms)
                .query_async(&mut conn)
                .await?;
            removed += lapsed;
            let ids: Vec<(String, String)> = ::redis::cmd("HGETALL")
                .arg(keys::result_claim_ids(&route))
                .query_async(&mut conn)
                .await?;
            for (claim_id, field) in ids {
                let (claimed, tombstone): (bool, Option<f64>) = ::redis::pipe()
                    .hexists(keys::result_claimed(&route), &field)
                    .zscore(keys::result_tombstones(&route), &field)
                    .query_async(&mut conn)
                    .await?;
                if !claimed && tombstone.is_none() {
                    let _: i64 = ::redis::cmd("HDEL")
                        .arg(keys::result_claim_ids(&route))
                        .arg(claim_id)
                        .query_async(&mut conn)
                        .await?;
                }
            }
            let (exists, claims): (bool, u64) = ::redis::pipe()
                .exists(&route)
                .hlen(keys::result_claimed(&route))
                .query_async(&mut conn)
                .await?;
            if !exists && claims == 0 {
                let _: i64 = ::redis::cmd("SREM")
                    .arg(keys::RESULT_ROUTES)
                    .arg(&route)
                    .query_async(&mut conn)
                    .await?;
            }
        }
        let expired: Vec<String> = ::redis::cmd("ZRANGEBYSCORE")
            .arg(keys::BLOB_EXPIRY)
            .arg("-inf")
            .arg(now_ms)
            .query_async(&mut conn)
            .await?;
        for name in expired {
            let _: () = ::redis::pipe()
                .hdel(keys::BLOB_TYPES, &name)
                .ignore()
                .zrem(keys::BLOB_EXPIRY, &name)
                .ignore()
                .query_async(&mut conn)
                .await?;
            removed += 1;
            if let Some(key) = BlobKey::parse(&name) {
                self.inner.blobs.remove(&key).await?;
            }
        }
        Ok(removed)
    }
}
