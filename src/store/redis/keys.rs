//! Key names. Everything outside the `llm-d-async:` namespace is upstream
//! llm-d-async's `redis-sortedset` layout, shared with its producers,
//! dispatchers, and the llm-d-router coordinator's async-broker.

pub fn claimed(queue: &str) -> String {
    format!("{queue}:claimed")
}

pub fn claim_owners(queue: &str) -> String {
    format!("{queue}:claim-owners")
}

pub fn claims_idx(queue: &str) -> String {
    format!("{queue}:claims-idx")
}

/// A request's claim field: its ID, or its ID and token for a request that
/// carries one.
pub fn claim_key(id: &str, token: &str) -> String {
    if token.is_empty() {
        id.to_owned()
    } else {
        format!("{id}\u{0}{token}")
    }
}

pub fn active(id: &str) -> String {
    format!("request-active:{id}")
}

pub fn cancel(id: &str) -> String {
    format!("request-cancel:{id}")
}

/// A payload stored apart from its envelope, as llm-d-async#458 writes it.
pub fn is_payload_key(payload_ref: &str) -> bool {
    payload_ref.starts_with("request-payload:")
}

/// Output saved by an interrupted generation, apart from the envelope so a
/// Go dispatcher re-serializing the envelope cannot drop it.
pub fn progress(id: &str, token: &str) -> String {
    format!("request-progress:{id}:{token}")
}

pub fn result_claimed(route: &str) -> String {
    format!("{route}:result-claimed")
}

pub fn result_claim_owners(route: &str) -> String {
    format!("{route}:result-claim-owners")
}

pub fn result_claims_idx(route: &str) -> String {
    format!("{route}:result-claims-idx")
}

pub fn result_tombstones(route: &str) -> String {
    format!("{route}:result-ack-tombstones")
}

/// Maps the numeric claim IDs this processor hands out to upstream's result
/// claim fields.
pub fn result_claim_ids(route: &str) -> String {
    format!("llm-d-async:result-claim-ids:{route}")
}

/// Every result route this processor wrote, for sweeping expired ones.
pub const RESULT_ROUTES: &str = "llm-d-async:result-routes";

/// The blobs the store references, with their content types.
pub const BLOB_TYPES: &str = "llm-d-async:blob-types";

/// When referenced blobs that have a lifetime expire (ms).
pub const BLOB_EXPIRY: &str = "llm-d-async:blob-expiry";

/// Where result writes are announced to every replica.
pub const RESULTS_CHANNEL: &str = "llm-d-async:results";

pub fn kv(key: &str) -> String {
    format!("llm-d-async:kv:{key}")
}

/// FNV-1a: a claim ID every replica derives alike from the same field.
pub fn claim_id_of(field: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in field.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash.max(1)
}

#[cfg(test)]
mod tests {
    use crate::store::redis::keys::{claim_id_of, claim_key, is_payload_key, progress};

    #[test]
    fn claim_keys_follow_upstream() {
        assert_eq!(claim_key("a", ""), "a");
        assert_eq!(claim_key("a", "0f"), "a\u{0}0f");
    }

    #[test]
    fn payload_keys_are_told_from_blob_refs() {
        assert!(is_payload_key("request-payload:a:0f"));
        assert!(!is_payload_key("blob://requests/0f"));
        assert_eq!(progress("a", "0f"), "request-progress:a:0f");
    }

    #[test]
    fn claim_ids_are_stable_and_nonzero() {
        assert_eq!(claim_id_of("a\u{0}0f"), claim_id_of("a\u{0}0f"));
        assert_ne!(claim_id_of("a\u{0}0f"), claim_id_of("a\u{0}10"));
        assert_eq!(claim_id_of(""), 0xcbf2_9ce4_8422_2325);
    }
}
