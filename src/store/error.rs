use crate::store::blob::BlobError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("create data dir: {0}")]
    Io(#[from] std::io::Error),
    #[error("open database: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("begin transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("open table: {0}")]
    Table(#[from] redb::TableError),
    #[error("storage: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("commit: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("encode record: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("store task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("blob: {0}")]
    Blob(#[from] BlobError),
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("postgres pool: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("postgres config: {0}")]
    Config(String),
    #[error("request store is closed")]
    Closed,
    #[error(transparent)]
    Shared(std::sync::Arc<StoreError>),
}
