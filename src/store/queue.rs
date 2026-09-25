//! Request lifecycle types shared by every store backend.

use bytes::Bytes;

use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::blob::BlobBody;
use crate::store::staging::StagedPayload;

pub struct NewRequest {
    /// Must carry `request_token`, `request_queue_name`, and the `payload`
    /// info of `payload`.
    pub envelope: InternalRequest,
    pub payload: StagedPayload,
}

/// Position of a request within its queue: earliest deadline first, then
/// submission order. `seq` is unique within the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingKey {
    pub deadline: i64,
    pub seq: u64,
}

pub struct Peeked {
    pub key: PendingKey,
    /// `Err` holds the decode error of a row that can never be dispatched.
    pub envelope: Result<InternalRequest, String>,
    /// Cancelled while queued.
    pub cancelled: bool,
}

/// Proof that this process holds a request. Outcomes for a claim that is no
/// longer current are discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRef {
    pub generation: String,
    /// Unique per claim: two claims of one request never share it.
    pub claim_id: u64,
}

/// What a queue consumer decided for one peeked request.
pub enum Admission {
    /// Take the request for dispatch.
    Claim { key: PendingKey, generation: String },
    /// Finish the request without dispatching it.
    Finish {
        key: PendingKey,
        envelope: Box<InternalRequest>,
        result: Box<ResultMessage>,
    },
    /// Remove an undecodable row.
    Discard { key: PendingKey },
}

impl Admission {
    pub fn key(&self) -> PendingKey {
        match self {
            Self::Claim { key, .. } | Self::Finish { key, .. } | Self::Discard { key } => *key,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Admitted {
    /// `payload` is the request body when it is stored inline; a body in a
    /// blob is opened at dispatch with `open_payload`.
    Claimed {
        claim: ClaimRef,
        payload: Option<Bytes>,
    },
    Finished,
    /// The row left the queue, or this process lost the right to take it,
    /// before this admission ran.
    Gone,
}

/// The end of one claim.
#[derive(Debug, Clone)]
pub enum Outcome {
    Finish {
        claim: ClaimRef,
        envelope: InternalRequest,
        result: ResultMessage,
    },
    /// Park the request until `due_ms`, then return it to its queue.
    Retry {
        claim: ClaimRef,
        envelope: InternalRequest,
        due_ms: i64,
    },
    /// Return the request to its queue unchanged.
    Release { claim: ClaimRef },
}

impl Outcome {
    pub fn claim(&self) -> &ClaimRef {
        match self {
            Self::Finish { claim, .. } | Self::Retry { claim, .. } | Self::Release { claim } => {
                claim
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub results_written: usize,
    /// Outcomes dropped because their claim was no longer current.
    pub fenced: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Backlog {
    pub depth: u64,
    /// Requests with deadline <= now + bound, one per bound.
    pub cumulative: Vec<u64>,
}

/// A request body opened for sending.
pub enum PayloadBody {
    Inline(Bytes),
    Blob(BlobBody),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultClaim {
    pub claim_id: u64,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Acked,
    AlreadyAcked,
    OwnershipLost,
}

/// Cancellation markers outlive their request so a late cancel is idempotent.
pub const CANCEL_MARKER_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

/// Acknowledged claims are remembered this long so a retried ack succeeds.
pub const ACK_TOMBSTONE_TTL_MS: i64 = 7 * 24 * 3600 * 1000;
