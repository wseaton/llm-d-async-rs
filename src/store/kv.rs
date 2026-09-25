//! Small control-plane values written by operators and controllers:
//! dispatch budgets and leased dispatch-rate limits.

use redb::ReadableDatabase;

use crate::api::dispatch_rate::DispatchRateLimit;
use crate::store::Store;
use crate::store::error::StoreError;
use crate::store::tables::KV;

fn budget_key(key: &str) -> String {
    format!("budget/{key}")
}

fn dispatch_rate_key(key: &str) -> String {
    format!("dispatch-rate/{key}")
}

impl Store {
    async fn kv_get(&self, key: String) -> Result<Option<Vec<u8>>, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let kv = txn.open_table(KV)?;
            Ok(kv.get(key.as_str())?.map(|v| v.value().to_vec()))
        })
        .await
    }

    async fn kv_put(&self, key: String, value: Option<Vec<u8>>) -> Result<(), StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            {
                let mut kv = txn.open_table(KV)?;
                match &value {
                    Some(v) => {
                        kv.insert(key.as_str(), v.as_slice())?;
                    }
                    None => {
                        kv.remove(key.as_str())?;
                    }
                }
            }
            txn.commit()?;
            Ok(())
        })
        .await
    }

    /// The raw budget value, as the operator wrote it.
    pub async fn budget(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.kv_get(budget_key(key)).await
    }

    /// Sets (`Some`) or clears (`None`) a budget value.
    pub async fn set_budget(&self, key: &str, value: Option<Vec<u8>>) -> Result<(), StoreError> {
        self.kv_put(budget_key(key), value).await
    }

    /// The stored dispatch-rate command. `Err` in the inner result means the
    /// stored bytes do not decode, which gates must treat as fail-closed.
    pub async fn dispatch_rate(
        &self,
        key: &str,
    ) -> Result<Option<Result<DispatchRateLimit, serde_json::Error>>, StoreError> {
        Ok(self
            .kv_get(dispatch_rate_key(key))
            .await?
            .map(|bytes| serde_json::from_slice(&bytes)))
    }

    pub async fn set_dispatch_rate(
        &self,
        key: &str,
        limit: Option<&DispatchRateLimit>,
    ) -> Result<(), StoreError> {
        let value = limit.map(serde_json::to_vec).transpose()?;
        self.kv_put(dispatch_rate_key(key), value).await
    }
}

#[cfg(test)]
mod tests {
    use crate::api::dispatch_rate::{API_VERSION, DispatchRateLimit};
    use crate::store::test_support::open;

    #[tokio::test]
    async fn budget_round_trip() {
        let (_dir, store) = open();
        assert_eq!(store.budget("b").await.unwrap(), None);
        store.set_budget("b", Some(b"0.5".to_vec())).await.unwrap();
        assert_eq!(
            store.budget("b").await.unwrap().as_deref(),
            Some(&b"0.5"[..])
        );
        store.set_budget("b", None).await.unwrap();
        assert_eq!(store.budget("b").await.unwrap(), None);
    }

    #[tokio::test]
    async fn dispatch_rate_round_trip() {
        let (_dir, store) = open();
        let limit = DispatchRateLimit {
            api_version: API_VERSION.into(),
            pool_id: "p".into(),
            max_admission_rps: 2.5,
            valid_until_unix_millis: 10,
            decision_id: "d".into(),
        };
        store.set_dispatch_rate("k", Some(&limit)).await.unwrap();
        assert_eq!(
            store.dispatch_rate("k").await.unwrap().unwrap().unwrap(),
            limit
        );
        assert!(store.dispatch_rate("other").await.unwrap().is_none());
    }
}
