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
}
