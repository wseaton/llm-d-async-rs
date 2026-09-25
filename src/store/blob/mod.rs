//! Large request and result bodies, apart from the queue.
//!
//! A blob is written in full and committed before the transaction that
//! references it commits, so a committed reference always names a complete
//! blob. The queue store owns the references; a blob nothing references is
//! an orphan, deleted by [`crate::store::QueueStore::collect_orphans`].

pub mod key;
pub mod local;
pub mod object;
pub mod postgres;

use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::boxed::{BoxFuture, BoxStream};
use crate::store::blob::key::BlobKey;

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("blob file: {0}")]
    Io(#[from] std::io::Error),
    #[error("object store: {0}")]
    Object(#[from] object_store::Error),
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("postgres pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("invalid blob store URL: {0}")]
    Url(String),
    #[error("blob writer already finished")]
    Finished,
}

/// A blob opened for reading.
pub struct BlobBody {
    pub size: u64,
    pub stream: BoxStream<'static, Result<Bytes, BlobError>>,
}

impl std::fmt::Debug for BlobBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobBody({} bytes)", self.size)
    }
}

/// A blob last written at `modified_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: BlobKey,
    pub modified_ms: i64,
}

/// Where blob bytes live.
pub trait BlobBackend: Send + Sync {
    /// Starts writing `key`. Readers see nothing under `key` that they can
    /// trust until the upload commits and a reference to it commits.
    fn create(&self, key: &BlobKey) -> BoxFuture<'_, Result<Box<dyn BlobUpload>, BlobError>>;

    /// `None` when there is no such blob.
    fn open(&self, key: &BlobKey) -> BoxFuture<'_, Result<Option<BlobBody>, BlobError>>;

    /// Deletes a blob. A missing blob is not an error.
    fn remove<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>>;

    /// Every blob last written at or before `cutoff_ms`, committed or not.
    fn list(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<Vec<Listed>, BlobError>>;
}

/// One blob being written. Dropped before [`BlobUpload::commit`], it
/// discards what was written.
pub trait BlobUpload: Send {
    fn write(&mut self, chunk: Bytes) -> BoxFuture<'_, Result<(), BlobError>>;

    fn commit(self: Box<Self>) -> BoxFuture<'static, Result<(), BlobError>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDigest {
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone)]
pub struct BlobStore(Arc<dyn BlobBackend>);

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl BlobStore {
    pub fn new(backend: Arc<dyn BlobBackend>) -> Self {
        Self(backend)
    }

    pub async fn create(&self, key: &BlobKey) -> Result<BlobWriter, BlobError> {
        Ok(BlobWriter {
            upload: Some(self.0.create(key).await?),
            size: 0,
            hasher: Sha256::new(),
        })
    }

    pub async fn open(&self, key: &BlobKey) -> Result<Option<BlobBody>, BlobError> {
        self.0.open(key).await
    }

    pub async fn remove(&self, key: &BlobKey) -> Result<(), BlobError> {
        self.0.remove(key).await
    }

    /// Deletes blobs whose references a committed transaction dropped. A
    /// failure leaves an orphan for [`crate::store::QueueStore::collect_orphans`].
    pub async fn remove_dropped(&self, keys: &[BlobKey]) {
        for key in keys {
            if let Err(e) = self.0.remove(key).await {
                tracing::warn!(blob = %key, error = %e, "failed to delete blob");
            }
        }
    }

    /// [`BlobStore::remove_dropped`] in a task of its own, for callers that
    /// cannot wait (destructors).
    pub fn spawn_remove(&self, keys: Vec<BlobKey>) {
        if keys.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                blobs = keys.len(),
                "no runtime to delete blobs; leaving orphans"
            );
            return;
        };
        let blobs = self.clone();
        runtime.spawn(async move { blobs.remove_dropped(&keys).await });
    }

    pub async fn list(&self, cutoff_ms: i64) -> Result<Vec<Listed>, BlobError> {
        self.0.list(cutoff_ms).await
    }
}

/// Streams one blob, hashing as it goes. Dropped without
/// [`BlobWriter::commit`], what was written is discarded.
pub struct BlobWriter {
    upload: Option<Box<dyn BlobUpload>>,
    size: u64,
    hasher: Sha256,
}

impl BlobWriter {
    pub async fn write(&mut self, chunk: Bytes) -> Result<(), BlobError> {
        let upload = self.upload.as_mut().ok_or(BlobError::Finished)?;
        self.hasher.update(&chunk);
        self.size += chunk.len() as u64;
        upload.write(chunk).await
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Makes the blob durable.
    pub async fn commit(mut self) -> Result<BlobDigest, BlobError> {
        let upload = self.upload.take().ok_or(BlobError::Finished)?;
        upload.commit().await?;
        Ok(BlobDigest {
            size: self.size,
            sha256: hex(&std::mem::take(&mut self.hasher).finalize()),
        })
    }
}

/// Reads a whole blob into memory. For tests and small bodies only.
pub async fn read_all(body: BlobBody) -> Result<Vec<u8>, BlobError> {
    use tokio_stream::StreamExt;
    let mut stream = body.stream;
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod conformance {
    use bytes::Bytes;

    use crate::clock::now_millis;
    use crate::store::blob::key::BlobKey;
    use crate::store::blob::{BlobStore, read_all};

    /// The contract every backend keeps.
    pub async fn check(store: BlobStore) {
        let key = BlobKey::result("abc", 3).unwrap();
        assert!(store.open(&key).await.unwrap().is_none());

        let mut w = store.create(&key).await.unwrap();
        w.write(Bytes::from_static(b"hello ")).await.unwrap();
        w.write(Bytes::from_static(b"world")).await.unwrap();
        let digest = w.commit().await.unwrap();
        assert_eq!(digest.size, 11);
        assert_eq!(
            digest.sha256,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        let body = store.open(&key).await.unwrap().unwrap();
        assert_eq!(body.size, 11);
        assert_eq!(read_all(body).await.unwrap(), b"hello world");

        let big_key = BlobKey::request("0123").unwrap();
        let chunk = Bytes::from(vec![7u8; 300 * 1024]);
        let mut w = store.create(&big_key).await.unwrap();
        for _ in 0..10 {
            w.write(chunk.clone()).await.unwrap();
        }
        let digest = w.commit().await.unwrap();
        assert_eq!(digest.size, 3000 * 1024);
        let body = store.open(&big_key).await.unwrap().unwrap();
        assert_eq!(body.size, 3000 * 1024);
        let bytes = read_all(body).await.unwrap();
        assert_eq!(bytes.len(), 3000 * 1024);
        assert!(bytes.iter().all(|b| *b == 7));

        let empty = BlobKey::request("e0").unwrap();
        let w = store.create(&empty).await.unwrap();
        assert_eq!(w.commit().await.unwrap().size, 0);
        let body = store.open(&empty).await.unwrap().unwrap();
        assert_eq!(body.size, 0);
        assert!(read_all(body).await.unwrap().is_empty());

        let mut listed: Vec<BlobKey> = store
            .list(now_millis() + 60_000)
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.key)
            .collect();
        listed.sort();
        let mut want = vec![key.clone(), big_key.clone(), empty.clone()];
        want.sort();
        assert_eq!(listed, want);
        assert!(store.list(0).await.unwrap().is_empty());

        store.remove(&key).await.unwrap();
        store.remove(&key).await.unwrap();
        assert!(store.open(&key).await.unwrap().is_none());
        store
            .remove_dropped(&[big_key.clone(), empty.clone()])
            .await;
        assert!(store.open(&big_key).await.unwrap().is_none());

        let abandoned = BlobKey::request("dead").unwrap();
        let mut w = store.create(&abandoned).await.unwrap();
        w.write(Bytes::from_static(b"partial")).await.unwrap();
        drop(w);
        for _ in 0..50 {
            if store.list(now_millis() + 60_000).await.unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(store.list(now_millis() + 60_000).await.unwrap().is_empty());
        assert!(store.open(&abandoned).await.unwrap().is_none());
    }
}
