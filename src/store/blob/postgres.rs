//! Blobs as 1 MiB rows in Postgres, shared by every replica on the same
//! database. Chunks are written and read one statement at a time, so neither
//! a slow upload nor a slow reader holds a connection.

use bytes::{Bytes, BytesMut};
use deadpool_postgres::Pool;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::boxed::BoxFuture;
use crate::clock::now_millis;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBackend, BlobBody, BlobError, BlobUpload, Listed};

const CHUNK: usize = 1 << 20;
const READ_AHEAD: usize = 2;

#[derive(Clone)]
pub struct PostgresBlobs {
    pool: Pool,
}

impl PostgresBlobs {
    /// The tables come from the Postgres queue store's schema.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    async fn insert(&self, key: &str, idx: i32, data: &[u8]) -> Result<(), BlobError> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO lda_blob_chunks (key, idx, data, written_ms) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (key, idx) DO UPDATE SET data = EXCLUDED.data, written_ms = EXCLUDED.written_ms",
                &[&key, &idx, &data, &now_millis()],
            )
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), BlobError> {
        let client = self.pool.get().await?;
        client
            .execute("DELETE FROM lda_blob_chunks WHERE key = $1", &[&key])
            .await?;
        Ok(())
    }
}

impl BlobBackend for PostgresBlobs {
    fn create(&self, key: &BlobKey) -> BoxFuture<'_, Result<Box<dyn BlobUpload>, BlobError>> {
        let key = key.to_string();
        Box::pin(async move {
            Ok(Box::new(ChunkUpload {
                blobs: self.clone(),
                key,
                buf: BytesMut::new(),
                next: 0,
                finished: false,
            }) as Box<dyn BlobUpload>)
        })
    }

    fn open(&self, key: &BlobKey) -> BoxFuture<'_, Result<Option<BlobBody>, BlobError>> {
        let key = key.to_string();
        Box::pin(async move {
            let client = self.pool.get().await?;
            let row = client
                .query_one(
                    "SELECT count(*)::int4, coalesce(sum(octet_length(data)), 0)::int8
                     FROM lda_blob_chunks WHERE key = $1",
                    &[&key],
                )
                .await?;
            drop(client);
            let chunks: i32 = row.get(0);
            let size: i64 = row.get(1);
            if chunks == 0 {
                return Ok(None);
            }
            let (tx, rx) = mpsc::channel(READ_AHEAD);
            let pool = self.pool.clone();
            tokio::spawn(async move {
                for idx in 0..chunks {
                    let chunk = read_chunk(&pool, &key, idx).await;
                    let failed = chunk.is_err();
                    if tx.send(chunk).await.is_err() || failed {
                        return;
                    }
                }
            });
            Ok(Some(BlobBody {
                size: u64::try_from(size).unwrap_or(0),
                stream: Box::pin(ReceiverStream::new(rx)),
            }))
        })
    }

    fn remove<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>> {
        Box::pin(async move { self.delete(&key.to_string()).await })
    }

    fn list(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<Vec<Listed>, BlobError>> {
        Box::pin(async move {
            let client = self.pool.get().await?;
            let rows = client
                .query(
                    "SELECT key, max(written_ms) FROM lda_blob_chunks
                     GROUP BY key HAVING max(written_ms) <= $1",
                    &[&cutoff_ms],
                )
                .await?;
            Ok(rows
                .iter()
                .filter_map(|row| {
                    let key: &str = row.get(0);
                    Some(Listed {
                        key: BlobKey::parse(key)?,
                        modified_ms: row.get(1),
                    })
                })
                .collect())
        })
    }
}

async fn read_chunk(pool: &Pool, key: &str, idx: i32) -> Result<Bytes, BlobError> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "SELECT data FROM lda_blob_chunks WHERE key = $1 AND idx = $2",
            &[&key, &idx],
        )
        .await?;
    let data: Vec<u8> = row.get(0);
    Ok(Bytes::from(data))
}

struct ChunkUpload {
    blobs: PostgresBlobs,
    key: String,
    buf: BytesMut,
    next: i32,
    finished: bool,
}

impl ChunkUpload {
    async fn flush(&mut self, min: usize) -> Result<(), BlobError> {
        while self.buf.len() >= min.max(1) {
            let chunk = self.buf.split_to(self.buf.len().min(CHUNK));
            self.blobs.insert(&self.key, self.next, &chunk).await?;
            self.next += 1;
        }
        Ok(())
    }
}

impl BlobUpload for ChunkUpload {
    fn write(&mut self, chunk: Bytes) -> BoxFuture<'_, Result<(), BlobError>> {
        Box::pin(async move {
            if self.finished {
                return Err(BlobError::Finished);
            }
            self.buf.extend_from_slice(&chunk);
            self.flush(CHUNK).await
        })
    }

    fn commit(mut self: Box<Self>) -> BoxFuture<'static, Result<(), BlobError>> {
        Box::pin(async move {
            if self.finished {
                return Err(BlobError::Finished);
            }
            self.flush(1).await?;
            if self.next == 0 {
                self.blobs.insert(&self.key, 0, &[]).await?;
            }
            self.finished = true;
            Ok(())
        })
    }
}

impl Drop for ChunkUpload {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let blobs = self.blobs.clone();
        let key = std::mem::take(&mut self.key);
        runtime.spawn(async move {
            if let Err(e) = blobs.delete(&key).await {
                tracing::warn!(blob = %key, error = %e, "failed to discard blob upload");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::store::blob::conformance;
    use crate::store::postgres::test_support::{chunk_blobs, fixture};

    #[tokio::test]
    async fn keeps_the_blob_contract() {
        let Some(f) = fixture().await else {
            return;
        };
        conformance::check(chunk_blobs(&f.db)).await;
    }
}
