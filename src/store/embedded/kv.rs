//! Small control-plane values written by operators and controllers.

use redb::ReadableDatabase;

use crate::store::embedded::EmbeddedStore;
use crate::store::embedded::tables::KV;
use crate::store::error::StoreError;

impl EmbeddedStore {
    pub(crate) async fn get_value(&self, key: String) -> Result<Option<Vec<u8>>, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let kv = txn.open_table(KV)?;
            Ok(kv.get(key.as_str())?.map(|v| v.value().to_vec()))
        })
        .await
    }

    pub(crate) async fn put_value(
        &self,
        key: String,
        value: Option<Vec<u8>>,
    ) -> Result<(), StoreError> {
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
}
