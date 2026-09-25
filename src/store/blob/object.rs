//! Blobs in an object store (`s3://`, `gs://`, `az://`, `file://`), shared by
//! every replica. Credentials and endpoints come from the provider's usual
//! environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`).
//!
//! Uploads are buffered up to [`PART_SIZE`] and sent as one PUT; larger ones
//! become multipart uploads whose object appears only on commit.

use std::sync::Arc;

use bytes::Bytes;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::boxed::BoxFuture;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBackend, BlobBody, BlobError, BlobUpload, Listed};

const PART_SIZE: usize = 8 << 20;
const PART_CONCURRENCY: usize = 4;

#[derive(Clone)]
pub struct ObjectBlobs {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectBlobs {
    /// Opens the store a URL names; its path is the prefix blobs go under.
    pub fn from_url(url: &str) -> Result<Self, BlobError> {
        let url = reqwest::Url::parse(url).map_err(|e| BlobError::Url(e.to_string()))?;
        let (store, prefix) = object_store::parse_url_opts(&url, std::env::vars())?;
        Ok(Self {
            store: Arc::from(store),
            prefix,
        })
    }

    fn path(&self, key: &BlobKey) -> Path {
        self.prefix
            .clone()
            .join(key.dir())
            .join(key.name().as_str())
    }

    fn key_of(&self, location: &Path) -> Option<BlobKey> {
        let parts: Vec<String> = location
            .prefix_match(&self.prefix)?
            .map(|p| p.as_ref().to_owned())
            .collect();
        match parts.as_slice() {
            [dir, name] => BlobKey::parse(&format!("{dir}/{name}")),
            _ => None,
        }
    }
}

fn not_found(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::NotFound { .. })
}

impl BlobBackend for ObjectBlobs {
    fn create(&self, key: &BlobKey) -> BoxFuture<'_, Result<Box<dyn BlobUpload>, BlobError>> {
        let writer = BufWriter::with_capacity(Arc::clone(&self.store), self.path(key), PART_SIZE)
            .with_max_concurrency(PART_CONCURRENCY);
        Box::pin(async move {
            Ok(Box::new(ObjectUpload {
                writer: Some(writer),
            }) as Box<dyn BlobUpload>)
        })
    }

    fn open(&self, key: &BlobKey) -> BoxFuture<'_, Result<Option<BlobBody>, BlobError>> {
        let path = self.path(key);
        Box::pin(async move {
            let got = match self.store.get(&path).await {
                Ok(got) => got,
                Err(e) if not_found(&e) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let size = got.meta.size;
            let stream = got
                .into_stream()
                .map(|chunk| chunk.map_err(BlobError::from));
            Ok(Some(BlobBody {
                size,
                stream: Box::pin(stream),
            }))
        })
    }

    fn remove<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>> {
        let path = self.path(key);
        Box::pin(async move {
            match self.store.delete(&path).await {
                Err(e) if !not_found(&e) => Err(e.into()),
                _ => Ok(()),
            }
        })
    }

    fn list(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<Vec<Listed>, BlobError>> {
        Box::pin(async move {
            let mut listing = self.store.list(Some(&self.prefix));
            let mut out = Vec::new();
            while let Some(meta) = listing.next().await {
                let meta = meta?;
                let modified_ms = meta.last_modified.timestamp_millis();
                if modified_ms > cutoff_ms {
                    continue;
                }
                if let Some(key) = self.key_of(&meta.location) {
                    out.push(Listed { key, modified_ms });
                }
            }
            Ok(out)
        })
    }
}

struct ObjectUpload {
    writer: Option<BufWriter>,
}

impl BlobUpload for ObjectUpload {
    fn write(&mut self, chunk: Bytes) -> BoxFuture<'_, Result<(), BlobError>> {
        Box::pin(async move {
            let writer = self.writer.as_mut().ok_or(BlobError::Finished)?;
            writer.write_all(&chunk).await?;
            Ok(())
        })
    }

    fn commit(mut self: Box<Self>) -> BoxFuture<'static, Result<(), BlobError>> {
        let writer = self.writer.take();
        Box::pin(async move {
            let mut writer = writer.ok_or(BlobError::Finished)?;
            writer.shutdown().await?;
            Ok(())
        })
    }
}

impl Drop for ObjectUpload {
    fn drop(&mut self) {
        let Some(mut writer) = self.writer.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(e) = writer.abort().await {
                    tracing::warn!(error = %e, "failed to abort blob upload");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::store::blob::object::ObjectBlobs;
    use crate::store::blob::{BlobStore, conformance};
    use crate::store::conformance::conformance_tests;

    conformance_tests!(crate::store::postgres::test_support::object_fixture);

    #[tokio::test]
    async fn keeps_the_blob_contract_on_a_file_url() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}/bucket/prefix", dir.path().display());
        std::fs::create_dir_all(dir.path().join("bucket/prefix")).unwrap();
        let blobs = ObjectBlobs::from_url(&url).unwrap();
        conformance::check(BlobStore::new(Arc::new(blobs))).await;
    }

    #[tokio::test]
    async fn list_ignores_foreign_objects() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("p/requests")).unwrap();
        std::fs::write(dir.path().join("p/requests/not-hex"), b"x").unwrap();
        std::fs::write(dir.path().join("p/other"), b"x").unwrap();
        let blobs = ObjectBlobs::from_url(&format!("file://{}/p", dir.path().display())).unwrap();
        let store = BlobStore::new(Arc::new(blobs));
        assert!(store.list(i64::MAX).await.unwrap().is_empty());
        assert!(dir.path().join("p/requests/not-hex").exists());
    }

    #[test]
    fn rejects_unparseable_urls() {
        assert!(ObjectBlobs::from_url("not a url").is_err());
        assert!(ObjectBlobs::from_url("ftp://host/x").is_err());
    }
}
