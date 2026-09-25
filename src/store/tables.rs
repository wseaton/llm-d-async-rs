//! Table layout of the embedded store.
//!
//! ```text
//!  submit ──► PENDING (queue, deadline, seq) ──admit──► CLAIMED (generation)
//!                 ▲          │                             │   │   │
//!                 │          └─ terminal (expired, ...) ─┐ │   │   └─ release ─► PENDING
//!                 │                                      ▼ ▼   │
//!                 └──── promote ◄── RETRY (due_ms, seq) ◄──────┘ retry
//!                                                 RESULTS (route, seq) ──claim──► RESULT_CLAIMS
//! ```
//!
//! Request bodies are never in PENDING: small ones live in PAYLOADS keyed by
//! generation, large ones in blob files registered in BLOBS. CLAIMED rows only
//! exist while this process holds the request in memory; opening the store
//! moves every one back to PENDING.

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

pub const PENDING: TableDefinition<(&str, i64, u64), &str> = TableDefinition::new("pending");
pub const PAYLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("payloads");
pub const BLOBS: TableDefinition<&str, &str> = TableDefinition::new("blobs");
pub const CLAIMED: TableDefinition<&str, &str> = TableDefinition::new("claimed");
pub const RETRY: TableDefinition<(i64, u64), &str> = TableDefinition::new("retry");
pub const ACTIVE: TableDefinition<&str, &str> = TableDefinition::new("active");
pub const CANCELLED: TableDefinition<&str, &str> = TableDefinition::new("cancelled");
pub const RESULTS: TableDefinition<(&str, u64), &str> = TableDefinition::new("results");
pub const RESULT_CLAIMS: TableDefinition<(&str, u64), &str> = TableDefinition::new("result_claims");
pub const TOMBSTONES: TableDefinition<(&str, u64), i64> = TableDefinition::new("result_tombstones");
pub const KV: TableDefinition<&str, &[u8]> = TableDefinition::new("kv");
pub const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

pub const META_SEQ: &str = "seq";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedRecord {
    pub queue: String,
    pub deadline: i64,
    pub seq: u64,
    pub claim_id: u64,
    /// The PENDING value, restored verbatim on release.
    pub envelope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryRecord {
    pub queue: String,
    pub deadline: i64,
    pub envelope: String,
}

/// The generation currently live for a request ID, or the one marked cancelled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    /// The result's body blob, deleted with the result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    pub body: String,
}

impl ResultRecord {
    pub fn expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|at| at <= now_ms)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultClaimRecord {
    pub owner: String,
    pub lease_until_ms: i64,
    pub record: ResultRecord,
}

/// A blob file the store references. Unreferenced files are deleted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobRecord {
    pub content_type: String,
    /// `None` while something still needs the blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}
