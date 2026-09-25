use bytes::Bytes;

use crate::api::payload::{PayloadInfo, PayloadStorage};
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobError, BlobStore, BlobWriter};

/// A request body received and made durable, ready to be referenced by a
/// submission.
#[derive(Debug, PartialEq, Eq)]
pub enum StagedPayload {
    Inline(Vec<u8>),
    Blob { key: BlobKey, size: u64 },
}

impl StagedPayload {
    pub fn info(&self, content_type: &str) -> PayloadInfo {
        let (size, storage) = match self {
            Self::Inline(bytes) => (bytes.len() as u64, PayloadStorage::Inline),
            Self::Blob { size, .. } => (*size, PayloadStorage::Blob),
        };
        PayloadInfo {
            content_type: content_type.to_owned(),
            size,
            storage,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StageError {
    #[error("payload exceeds {max} bytes")]
    TooLarge { max: u64 },
    #[error("request token {0:?} cannot name a blob")]
    Token(String),
    #[error("write payload: {0}")]
    Blob(#[from] BlobError),
}

/// Receives a request body chunk by chunk. Bodies up to `inline_limit`
/// stay in memory and are stored inline; larger ones spill to the blob of
/// the submission's token as they arrive, so memory use is bounded by the
/// limit.
pub struct PayloadSink {
    blobs: BlobStore,
    token: String,
    inline_limit: usize,
    max: u64,
    buf: Vec<u8>,
    writer: Option<(BlobKey, BlobWriter)>,
}

impl PayloadSink {
    pub fn new(blobs: BlobStore, token: &str, inline_limit: usize, max: u64) -> Self {
        Self {
            blobs,
            token: token.to_owned(),
            inline_limit,
            max,
            buf: Vec::new(),
            writer: None,
        }
    }

    pub async fn push(&mut self, chunk: &[u8]) -> Result<(), StageError> {
        let written = match &self.writer {
            Some((_, w)) => w.size(),
            None => self.buf.len() as u64,
        };
        if written + chunk.len() as u64 > self.max {
            return Err(StageError::TooLarge { max: self.max });
        }
        if let Some((_, writer)) = &mut self.writer {
            writer.write(Bytes::copy_from_slice(chunk)).await?;
        } else if self.buf.len() + chunk.len() <= self.inline_limit {
            self.buf.extend_from_slice(chunk);
        } else {
            let key = BlobKey::request(&self.token)
                .ok_or_else(|| StageError::Token(self.token.clone()))?;
            let mut writer = self.blobs.create(&key).await?;
            writer
                .write(Bytes::from(std::mem::take(&mut self.buf)))
                .await?;
            writer.write(Bytes::copy_from_slice(chunk)).await?;
            self.writer = Some((key, writer));
        }
        Ok(())
    }

    /// Makes the body durable.
    pub async fn finish(self) -> Result<StagedPayload, StageError> {
        match self.writer {
            None => Ok(StagedPayload::Inline(self.buf)),
            Some((key, writer)) => {
                let digest = writer.commit().await?;
                Ok(StagedPayload::Blob {
                    key,
                    size: digest.size,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::store::blob::key::BlobKey;
    use crate::store::blob::local::LocalBlobs;
    use crate::store::blob::{BlobStore, read_all};
    use crate::store::staging::{PayloadSink, StageError, StagedPayload};

    fn blobs(dir: &std::path::Path) -> BlobStore {
        BlobStore::new(Arc::new(LocalBlobs::open(dir).unwrap()))
    }

    #[tokio::test]
    async fn small_bodies_stay_inline() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = blobs(dir.path());
        let mut sink = PayloadSink::new(blobs.clone(), "ab", 8, 100);
        sink.push(b"1234").await.unwrap();
        sink.push(b"5678").await.unwrap();
        assert_eq!(
            sink.finish().await.unwrap(),
            StagedPayload::Inline(b"12345678".to_vec())
        );
        assert!(blobs.list(i64::MAX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn large_bodies_spill_to_a_blob() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = blobs(dir.path());
        let mut sink = PayloadSink::new(blobs.clone(), "ab", 8, 100);
        sink.push(b"12345").await.unwrap();
        sink.push(b"67890").await.unwrap();
        sink.push(b"abc").await.unwrap();
        let staged = sink.finish().await.unwrap();
        let key = BlobKey::request("ab").unwrap();
        assert_eq!(
            staged,
            StagedPayload::Blob {
                key: key.clone(),
                size: 13
            }
        );
        let body = blobs.open(&key).await.unwrap().unwrap();
        assert_eq!(body.size, 13);
        assert_eq!(read_all(body).await.unwrap(), b"1234567890abc");
    }

    #[tokio::test]
    async fn oversized_bodies_are_rejected_and_leave_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = blobs(dir.path());
        let mut sink = PayloadSink::new(blobs.clone(), "ab", 4, 10);
        sink.push(b"123456").await.unwrap();
        assert!(matches!(
            sink.push(b"78901").await,
            Err(StageError::TooLarge { max: 10 })
        ));
        drop(sink);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
        assert!(blobs.list(i64::MAX).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_token_that_cannot_name_a_blob_is_rejected_on_spill() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = PayloadSink::new(blobs(dir.path()), "../x", 2, 10);
        sink.push(b"1").await.unwrap();
        assert!(matches!(sink.push(b"234").await, Err(StageError::Token(_))));
    }
}
