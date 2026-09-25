use crate::api::payload::{PayloadInfo, PayloadStorage};
use crate::store::blob::{BlobKey, BlobStore, BlobWriter};

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
    Io(#[from] std::io::Error),
}

/// Receives a request body chunk by chunk. Bodies up to `inline_limit`
/// stay in memory and are stored inline; larger ones spill to a blob file
/// as they arrive, so memory use is bounded by the limit.
pub struct PayloadSink {
    blobs: BlobStore,
    inline_limit: usize,
    max: u64,
    buf: Vec<u8>,
    writer: Option<BlobWriter>,
}

impl PayloadSink {
    pub fn new(blobs: BlobStore, inline_limit: usize, max: u64) -> Self {
        Self {
            blobs,
            inline_limit,
            max,
            buf: Vec::new(),
            writer: None,
        }
    }

    pub async fn push(&mut self, chunk: &[u8]) -> Result<(), StageError> {
        let written = match &self.writer {
            Some(w) => w.size(),
            None => self.buf.len() as u64,
        };
        if written + chunk.len() as u64 > self.max {
            return Err(StageError::TooLarge { max: self.max });
        }
        if let Some(writer) = &mut self.writer {
            writer.write(chunk).await?;
        } else if self.buf.len() + chunk.len() <= self.inline_limit {
            self.buf.extend_from_slice(chunk);
        } else {
            let mut writer = self.blobs.writer().await?;
            writer.write(&std::mem::take(&mut self.buf)).await?;
            writer.write(chunk).await?;
            self.writer = Some(writer);
        }
        Ok(())
    }

    /// Makes the body durable. A blob is named after the generation's token.
    pub async fn finish(self, token: &str) -> Result<StagedPayload, StageError> {
        match self.writer {
            None => Ok(StagedPayload::Inline(self.buf)),
            Some(writer) => {
                let key =
                    BlobKey::request(token).ok_or_else(|| StageError::Token(token.to_owned()))?;
                let digest = writer.commit(&key).await?;
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
    use crate::store::blob::{BlobKey, BlobStore};
    use crate::store::staging::{PayloadSink, StageError, StagedPayload};

    #[tokio::test]
    async fn small_bodies_stay_inline() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::open(dir.path()).unwrap();
        let mut sink = PayloadSink::new(blobs.clone(), 8, 100);
        sink.push(b"1234").await.unwrap();
        sink.push(b"5678").await.unwrap();
        assert_eq!(
            sink.finish("ab").await.unwrap(),
            StagedPayload::Inline(b"12345678".to_vec())
        );
        assert!(blobs.list().unwrap().is_empty());
    }

    #[tokio::test]
    async fn large_bodies_spill_to_a_blob() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::open(dir.path()).unwrap();
        let mut sink = PayloadSink::new(blobs.clone(), 8, 100);
        sink.push(b"12345").await.unwrap();
        sink.push(b"67890").await.unwrap();
        sink.push(b"abc").await.unwrap();
        let staged = sink.finish("ab").await.unwrap();
        let key = BlobKey::request("ab").unwrap();
        assert_eq!(
            staged,
            StagedPayload::Blob {
                key: key.clone(),
                size: 13
            }
        );
        let (_, size) = blobs.open_file(&key).await.unwrap().unwrap();
        assert_eq!(size, 13);
        let bytes = std::fs::read(dir.path().join("requests/ab")).unwrap();
        assert_eq!(bytes, b"1234567890abc");
    }

    #[tokio::test]
    async fn oversized_bodies_are_rejected_and_leave_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = BlobStore::open(dir.path()).unwrap();
        let mut sink = PayloadSink::new(blobs.clone(), 4, 10);
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
    }
}
