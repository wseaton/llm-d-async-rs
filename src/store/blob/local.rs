//! Blobs as files in a local directory, for a single process.
//!
//! A blob is written to `tmp/`, fsynced, and renamed into place on commit.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use crate::boxed::BoxFuture;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBackend, BlobBody, BlobError, BlobUpload, Listed};

const TMP: &str = "tmp";
const DIRS: [&str; 2] = ["requests", "results"];

#[derive(Clone)]
pub struct LocalBlobs {
    root: Arc<PathBuf>,
}

impl LocalBlobs {
    /// Opens `root`, creating it, and deletes uploads a previous process left
    /// unfinished.
    pub fn open(root: &Path) -> io::Result<Self> {
        for dir in DIRS.iter().chain([&TMP]) {
            std::fs::create_dir_all(root.join(dir))?;
        }
        for entry in std::fs::read_dir(root.join(TMP))? {
            std::fs::remove_file(entry?.path())?;
        }
        Ok(Self {
            root: Arc::new(root.to_owned()),
        })
    }

    fn path(&self, key: &BlobKey) -> PathBuf {
        self.root.join(key.dir()).join(key.name())
    }

    fn list_sync(&self, cutoff_ms: i64) -> io::Result<Vec<Listed>> {
        let mut out = Vec::new();
        for dir in DIRS {
            for entry in std::fs::read_dir(self.root.join(dir))? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(key) = name
                    .to_str()
                    .and_then(|n| BlobKey::parse(&format!("{dir}/{n}")))
                else {
                    std::fs::remove_file(entry.path())?;
                    continue;
                };
                let modified_ms = entry
                    .metadata()?
                    .modified()?
                    .duration_since(UNIX_EPOCH)
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                if modified_ms <= cutoff_ms {
                    out.push(Listed { key, modified_ms });
                }
            }
        }
        Ok(out)
    }
}

impl BlobBackend for LocalBlobs {
    fn create(&self, key: &BlobKey) -> BoxFuture<'_, Result<Box<dyn BlobUpload>, BlobError>> {
        let dest = self.path(key);
        Box::pin(async move {
            let tmp = self
                .root
                .join(TMP)
                .join(format!("{:032x}", rand::random::<u128>()));
            let file = tokio::fs::File::create(&tmp).await?;
            Ok(Box::new(FileUpload {
                file: Some(file),
                tmp,
                dest,
            }) as Box<dyn BlobUpload>)
        })
    }

    fn open(&self, key: &BlobKey) -> BoxFuture<'_, Result<Option<BlobBody>, BlobError>> {
        let path = self.path(key);
        Box::pin(async move {
            let file = match tokio::fs::File::open(path).await {
                Ok(file) => file,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let size = file.metadata().await?.len();
            let stream =
                tokio_stream::StreamExt::map(tokio_util::io::ReaderStream::new(file), |chunk| {
                    chunk.map_err(BlobError::from)
                });
            Ok(Some(BlobBody {
                size,
                stream: Box::pin(stream),
            }))
        })
    }

    fn remove<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>> {
        let path = self.path(key);
        Box::pin(async move {
            match tokio::fs::remove_file(path).await {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
                _ => Ok(()),
            }
        })
    }

    fn list(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<Vec<Listed>, BlobError>> {
        let this = self.clone();
        Box::pin(async move {
            Ok(
                tokio::task::spawn_blocking(move || this.list_sync(cutoff_ms))
                    .await
                    .map_err(io::Error::other)??,
            )
        })
    }
}

struct FileUpload {
    file: Option<tokio::fs::File>,
    tmp: PathBuf,
    dest: PathBuf,
}

impl BlobUpload for FileUpload {
    fn write(&mut self, chunk: Bytes) -> BoxFuture<'_, Result<(), BlobError>> {
        Box::pin(async move {
            let file = self.file.as_mut().ok_or(BlobError::Finished)?;
            file.write_all(&chunk).await?;
            Ok(())
        })
    }

    fn commit(mut self: Box<Self>) -> BoxFuture<'static, Result<(), BlobError>> {
        Box::pin(async move {
            let mut file = self.file.take().ok_or(BlobError::Finished)?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&self.tmp, &self.dest).await?;
            let dir = self
                .dest
                .parent()
                .map(Path::to_owned)
                .ok_or_else(|| io::Error::other("blob path has no parent"))?;
            tokio::task::spawn_blocking(move || std::fs::File::open(dir)?.sync_all())
                .await
                .map_err(io::Error::other)??;
            Ok(())
        })
    }
}

impl Drop for FileUpload {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::store::blob::local::LocalBlobs;
    use crate::store::blob::{BlobStore, conformance};

    #[tokio::test]
    async fn keeps_the_blob_contract() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = LocalBlobs::open(dir.path()).unwrap();
        conformance::check(BlobStore::new(Arc::new(blobs))).await;
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn open_clears_unfinished_uploads_and_list_drops_strays() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("tmp")).unwrap();
        std::fs::write(dir.path().join("tmp/half"), b"x").unwrap();
        let blobs = BlobStore::new(Arc::new(LocalBlobs::open(dir.path()).unwrap()));
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
        std::fs::write(dir.path().join("requests/not-hex"), b"x").unwrap();
        assert!(blobs.list(i64::MAX).await.unwrap().is_empty());
        assert!(!dir.path().join("requests/not-hex").exists());
    }
}
