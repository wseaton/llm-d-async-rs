//! Large bodies as files next to the database.
//!
//! A blob is written to `tmp/`, fsynced, and renamed into place before the
//! transaction that references it commits, so a committed reference always
//! names a complete file. Files nothing references (a crash between rename
//! and commit, or between commit and delete) are removed when the store opens.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

const REQUESTS: &str = "requests";
const RESULTS: &str = "results";
const TMP: &str = "tmp";
const REF_SCHEME: &str = "blob://";

/// Names one blob: `requests/<token>` or `results/<token>`, where the token is
/// the hex request token of the generation it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey(String);

fn valid_token(token: &str) -> bool {
    !token.is_empty() && token.len() <= 128 && token.bytes().all(|b| b.is_ascii_hexdigit())
}

impl BlobKey {
    pub fn request(token: &str) -> Option<Self> {
        valid_token(token).then(|| Self(format!("{REQUESTS}/{token}")))
    }

    pub fn result(token: &str) -> Option<Self> {
        valid_token(token).then(|| Self(format!("{RESULTS}/{token}")))
    }

    pub fn parse(key: &str) -> Option<Self> {
        let (dir, token) = key.split_once('/')?;
        match dir {
            REQUESTS => Self::request(token),
            RESULTS => Self::result(token),
            _ => None,
        }
    }

    /// The `payload_ref` a result carries for this blob.
    pub fn to_ref(&self) -> String {
        format!("{REF_SCHEME}{}", self.0)
    }

    pub fn from_ref(payload_ref: &str) -> Option<Self> {
        Self::parse(payload_ref.strip_prefix(REF_SCHEME)?)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDigest {
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone)]
pub struct BlobStore {
    root: Arc<PathBuf>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl BlobStore {
    pub fn open(root: &Path) -> io::Result<Self> {
        for dir in [REQUESTS, RESULTS, TMP] {
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
        self.root.join(&key.0)
    }

    pub async fn writer(&self) -> io::Result<BlobWriter> {
        let tmp = self
            .root
            .join(TMP)
            .join(format!("{:032x}", rand::random::<u128>()));
        let file = tokio::fs::File::create(&tmp).await?;
        Ok(BlobWriter {
            store: self.clone(),
            file: Some(file),
            tmp,
            size: 0,
            hasher: Sha256::new(),
        })
    }

    /// Opens a blob for reading. `None` when it does not exist.
    pub async fn open_file(&self, key: &BlobKey) -> io::Result<Option<(tokio::fs::File, u64)>> {
        match tokio::fs::File::open(self.path(key)).await {
            Ok(file) => {
                let size = file.metadata().await?.len();
                Ok(Some((file, size)))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Deletes a blob. A missing blob is not an error.
    pub fn remove(&self, key: &BlobKey) -> io::Result<()> {
        match std::fs::remove_file(self.path(key)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Deletes blobs whose references a committed transaction dropped. A
    /// failure leaves an orphan the next open removes.
    pub fn remove_dropped(&self, keys: &[BlobKey]) {
        for key in keys {
            if let Err(e) = self.remove(key) {
                tracing::warn!(blob = key.as_str(), error = %e, "failed to delete blob");
            }
        }
    }

    /// Every blob on disk.
    pub fn list(&self) -> io::Result<Vec<BlobKey>> {
        let mut keys = Vec::new();
        for dir in [REQUESTS, RESULTS] {
            for entry in std::fs::read_dir(self.root.join(dir))? {
                let name = entry?.file_name();
                match name
                    .to_str()
                    .and_then(|n| BlobKey::parse(&format!("{dir}/{n}")))
                {
                    Some(key) => keys.push(key),
                    None => std::fs::remove_file(self.root.join(dir).join(&name))?,
                }
            }
        }
        Ok(keys)
    }
}

/// Streams one blob to disk, hashing as it goes. Dropped without
/// [`BlobWriter::commit`], the partial file is removed.
pub struct BlobWriter {
    store: BlobStore,
    file: Option<tokio::fs::File>,
    tmp: PathBuf,
    size: u64,
    hasher: Sha256,
}

impl BlobWriter {
    pub async fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("blob writer already committed"))?;
        file.write_all(chunk).await?;
        self.hasher.update(chunk);
        self.size += chunk.len() as u64;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Makes the blob durable under `key`, replacing any blob already there.
    pub async fn commit(mut self, key: &BlobKey) -> io::Result<BlobDigest> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| io::Error::other("blob writer already committed"))?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        let dest = self.store.path(key);
        tokio::fs::rename(&self.tmp, &dest).await?;
        let dir = dest
            .parent()
            .map(Path::to_owned)
            .ok_or_else(|| io::Error::other("blob path has no parent"))?;
        tokio::task::spawn_blocking(move || std::fs::File::open(dir)?.sync_all())
            .await
            .map_err(io::Error::other)??;
        Ok(BlobDigest {
            size: self.size,
            sha256: hex(&std::mem::take(&mut self.hasher).finalize()),
        })
    }
}

impl Drop for BlobWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::store::blob::{BlobKey, BlobStore};

    #[test]
    fn keys_reject_path_tricks() {
        assert!(BlobKey::request("ab12").is_some());
        for bad in ["", "../x", "a/b", "zz", "ab.cd"] {
            assert!(BlobKey::request(bad).is_none(), "{bad}");
        }
        assert!(BlobKey::parse("requests/ab").is_some());
        assert!(BlobKey::parse("other/ab").is_none());
        let key = BlobKey::result("ff").unwrap();
        assert_eq!(key.to_ref(), "blob://results/ff");
        assert_eq!(BlobKey::from_ref("blob://results/ff"), Some(key));
        assert_eq!(BlobKey::from_ref("s3://results/ff"), None);
    }

    #[tokio::test]
    async fn commit_hashes_and_places_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        let mut w = store.writer().await.unwrap();
        w.write(b"hello ").await.unwrap();
        w.write(b"world").await.unwrap();
        let key = BlobKey::result("abc").unwrap();
        let digest = w.commit(&key).await.unwrap();
        assert_eq!(digest.size, 11);
        assert_eq!(
            digest.sha256,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        let (_, size) = store.open_file(&key).await.unwrap().unwrap();
        assert_eq!(size, 11);
        assert_eq!(store.list().unwrap(), std::slice::from_ref(&key));
        store.remove(&key).unwrap();
        store.remove(&key).unwrap();
        assert!(store.open_file(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropped_writer_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        let mut w = store.writer().await.unwrap();
        w.write(b"partial").await.unwrap();
        drop(w);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
        assert!(store.list().unwrap().is_empty());
    }
}
