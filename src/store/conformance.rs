//! The behaviour every [`crate::store::QueueStore`] backend must have. Each
//! backend runs every case through [`conformance_tests`].

use bytes::Bytes;

use crate::api::dispatch_rate::{API_VERSION, DispatchRateLimit};
use crate::api::request::InternalRequest;
use crate::api::result::{ResultMessage, StoredBody};
use crate::clock::now_millis;
use crate::store::Store;
use crate::store::blob::key::BlobKey;
use crate::store::blob::read_all;
use crate::store::queue::{
    AckOutcome, Admission, Admitted, ClaimRef, NewRequest, Outcome, PayloadBody, Peeked,
    ResultClaim,
};
use crate::store::staging::PayloadSink;
use crate::store::test_support::{envelope, new_request};

pub const NOW_MS: i64 = 1_000_000;

macro_rules! conformance_tests {
    ($fixture:path) => {
        conformance_tests!(@cases $fixture;
            peek_orders_by_deadline_then_submission,
            peek_reports_cancellation,
            claim_removes_from_queue_and_payload_stays_readable,
            admitting_a_row_twice_reports_gone,
            finish_at_admission_writes_a_result,
            discard_removes_the_row_and_its_blob,
            finish_writes_result_and_clears_request_state,
            blob_payload_lives_until_the_result_is_written,
            blob_payload_survives_retry_and_release,
            fenced_result_blob_is_deleted,
            replayed_finish_keeps_its_result_blob,
            stale_claim_id_is_fenced,
            release_restores_the_original_position,
            retry_waits_until_due,
            cancel_marks_only_the_live_generation,
            cancel_after_deadline_is_a_noop,
            backlog_counts_deadline_buckets,
            pop_is_fifo,
            expired_results_are_skipped_and_swept,
            claim_ack_is_idempotent_and_fenced,
            lapsed_lease_is_redelivered_in_order,
            ack_deletes_the_result_blob,
            queued_result_keeps_its_blob_past_the_retention,
            popped_result_blob_expires_with_retention_or_ttl,
            orphans_are_collected_and_referenced_blobs_kept,
            budget_round_trip,
            dispatch_rate_round_trip,
            ping_succeeds,
        );
    };
    (@cases $fixture:path; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(fixture) = $fixture().await else {
                    return;
                };
                crate::store::conformance::$name(fixture.store.clone()).await;
            }
        )*
    };
}

pub(crate) use conformance_tests;

async fn claim_head(store: &Store, queue: &str) -> (Peeked, ClaimRef) {
    store.join(queue).await.unwrap();
    let mut peeked = store.peek(queue.into(), 1, NOW_MS).await.unwrap();
    let head = peeked.remove(0);
    let env = head.envelope.clone().unwrap();
    let admitted = store
        .admit(
            queue.into(),
            vec![Admission::Claim {
                key: head.key,
                generation: env.generation_key(),
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    let Some(Admitted::Claimed(claim)) = admitted.into_iter().next() else {
        panic!("not claimed")
    };
    (head, claim)
}

async fn peek_ids(store: &Store, queue: &str, limit: usize) -> Vec<String> {
    store.join(queue).await.unwrap();
    store
        .peek(queue.into(), limit, NOW_MS)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.envelope.unwrap().request.id)
        .collect()
}

async fn payload_bytes(store: &Store, env: &InternalRequest) -> Option<Vec<u8>> {
    match store.open_payload(env).await.unwrap()? {
        PayloadBody::Inline(bytes) => Some(bytes.to_vec()),
        PayloadBody::Blob(body) => Some(read_all(body).await.unwrap()),
    }
}

async fn blob_exists(store: &Store, key: &BlobKey) -> bool {
    store.blobs().open(key).await.unwrap().is_some()
}

/// Stages `body` through a sink with an 8-byte inline limit.
async fn blob_request(store: &Store, id: &str, token: &str, body: &[u8]) -> NewRequest {
    let mut sink = PayloadSink::new(store.blobs().clone(), token, 8, 1 << 20);
    sink.push(body).await.unwrap();
    let payload = sink.finish().await.unwrap();
    let mut env = envelope(id, token, "q", 100);
    env.payload = payload.info("audio/wav");
    NewRequest {
        envelope: env,
        payload,
    }
}

async fn write_blob(store: &Store, key: &BlobKey, body: &'static [u8]) -> StoredBody {
    let mut w = store.blobs().create(key).await.unwrap();
    w.write(Bytes::from_static(body)).await.unwrap();
    let digest = w.commit().await.unwrap();
    StoredBody {
        payload_ref: key.to_ref(),
        location: store.blobs().location(key),
        content_type: "audio/wav".into(),
        size: digest.size,
        sha256: digest.sha256,
    }
}

pub async fn peek_orders_by_deadline_then_submission(store: Store) {
    store
        .submit(vec![
            new_request("late", "q", 300),
            new_request("early", "q", 100),
            new_request("other-queue", "r", 1),
            new_request("early2", "q", 100),
        ])
        .await
        .unwrap();
    assert_eq!(peek_ids(&store, "q", 10).await, ["early", "early2", "late"]);
    assert_eq!(peek_ids(&store, "q", 2).await, ["early", "early2"]);
    assert!(store.has_pending("r".into(), NOW_MS).await.unwrap());
    assert!(!store.has_pending("nope".into(), NOW_MS).await.unwrap());
}

pub async fn peek_reports_cancellation(store: Store) {
    store
        .submit(vec![
            new_request("a", "q", 2_000),
            new_request("b", "q", 2_000),
        ])
        .await
        .unwrap();
    assert_eq!(store.cancel(vec!["b".into()], NOW_MS).await.unwrap(), 1);
    store.join("q").await.unwrap();
    let flags: Vec<(String, bool)> = store
        .peek("q".into(), 10, NOW_MS)
        .await
        .unwrap()
        .into_iter()
        .map(|p| (p.envelope.unwrap().request.id, p.cancelled))
        .collect();
    assert_eq!(flags, [("a".into(), false), ("b".into(), true)]);
}

pub async fn claim_removes_from_queue_and_payload_stays_readable(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (head, _) = claim_head(&store, "q").await;
    assert!(!store.has_pending("q".into(), NOW_MS).await.unwrap());
    assert!(peek_ids(&store, "q", 10).await.is_empty());
    let env = head.envelope.unwrap();
    assert_eq!(
        payload_bytes(&store, &env).await.unwrap(),
        br#"{"prompt":"a"}"#
    );
}

pub async fn admitting_a_row_twice_reports_gone(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    store.join("q").await.unwrap();
    let head = store.peek("q".into(), 1, NOW_MS).await.unwrap().remove(0);
    let generation = head.envelope.unwrap().generation_key();
    let out = store
        .admit(
            "q".into(),
            vec![
                Admission::Discard { key: head.key },
                Admission::Discard { key: head.key },
            ],
            NOW_MS,
        )
        .await
        .unwrap();
    assert_eq!(out, [Admitted::Finished, Admitted::Gone]);
    let out = store
        .admit(
            "q".into(),
            vec![Admission::Claim {
                key: head.key,
                generation,
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    assert_eq!(out, [Admitted::Gone]);
}

pub async fn finish_at_admission_writes_a_result(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100), new_request("b", "q", 200)])
        .await
        .unwrap();
    store.join("q").await.unwrap();
    let peeked = store.peek("q".into(), 2, NOW_MS).await.unwrap();
    let mut admissions = Vec::new();
    for p in peeked {
        let env = p.envelope.unwrap();
        admissions.push(Admission::Finish {
            key: p.key,
            result: Box::new(ResultMessage::deadline_exceeded(&env)),
            envelope: Box::new(env),
        });
    }
    let out = store.admit("q".into(), admissions, NOW_MS).await.unwrap();
    assert_eq!(out, [Admitted::Finished, Admitted::Finished]);
    assert!(!store.has_pending("q".into(), NOW_MS).await.unwrap());
    assert_eq!(
        store.result_depth("results".into(), NOW_MS).await.unwrap(),
        2
    );
    let first = store
        .pop_result("results".into(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id_of(&first), "a");
}

pub async fn discard_removes_the_row_and_its_blob(store: Store) {
    let req = blob_request(&store, "a", "0a", b"0123456789").await;
    let key = BlobKey::request("0a").unwrap();
    store.submit(vec![req]).await.unwrap();
    store.join("q").await.unwrap();
    let head = store.peek("q".into(), 1, NOW_MS).await.unwrap().remove(0);
    let out = store
        .admit(
            "q".into(),
            vec![Admission::Discard { key: head.key }],
            NOW_MS,
        )
        .await
        .unwrap();
    assert_eq!(out, [Admitted::Finished]);
    assert!(!store.has_pending("q".into(), NOW_MS).await.unwrap());
    assert!(!blob_exists(&store, &key).await);
    assert_eq!(
        store.result_depth("results".into(), NOW_MS).await.unwrap(),
        0
    );
}

pub async fn finish_writes_result_and_clears_request_state(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    let env = head.envelope.unwrap();
    let result = ResultMessage::http(&env, 200, b"ok");
    let applied = store
        .apply_outcomes(
            vec![Outcome::Finish {
                claim: claim.clone(),
                envelope: env.clone(),
                result: result.clone(),
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    assert_eq!(applied.results_written, 1);
    let body = store
        .pop_result("results".into(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<ResultMessage>(&body).unwrap(),
        result
    );
    assert!(payload_bytes(&store, &env).await.is_none());
    let applied = store
        .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
        .await
        .unwrap();
    assert_eq!(applied.fenced, 1);
    assert!(!store.has_pending("q".into(), NOW_MS).await.unwrap());
    assert_eq!(store.cancel(vec!["a".into()], NOW_MS).await.unwrap(), 0);
}

pub async fn blob_payload_lives_until_the_result_is_written(store: Store) {
    let req = blob_request(&store, "a", "0a", b"RIFF-large-wav-bytes").await;
    assert_eq!(req.envelope.payload.size, 20);
    store.submit(vec![req]).await.unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    let env = head.envelope.unwrap();
    assert_eq!(env.payload.content_type, "audio/wav");
    assert_eq!(
        payload_bytes(&store, &env).await.unwrap(),
        b"RIFF-large-wav-bytes"
    );
    store
        .apply_outcomes(
            vec![Outcome::Finish {
                claim,
                result: ResultMessage::http(&env, 200, b"{}"),
                envelope: env.clone(),
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    assert!(!blob_exists(&store, &BlobKey::request("0a").unwrap()).await);
    assert!(payload_bytes(&store, &env).await.is_none());
}

pub async fn blob_payload_survives_retry_and_release(store: Store) {
    store
        .submit(vec![blob_request(&store, "a", "0a", b"0123456789").await])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    store
        .apply_outcomes(
            vec![Outcome::Retry {
                claim,
                envelope: head.envelope.unwrap(),
                due_ms: NOW_MS,
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    store.promote_due_retries(NOW_MS, 10).await.unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    store
        .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
        .await
        .unwrap();
    let env = head.envelope.unwrap();
    assert_eq!(payload_bytes(&store, &env).await.unwrap(), b"0123456789");
}

pub async fn fenced_result_blob_is_deleted(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    let env = head.envelope.unwrap();
    let stale = ClaimRef {
        claim_id: claim.claim_id + 1,
        ..claim
    };
    let key = BlobKey::result("61", stale.claim_id).unwrap();
    let stored = write_blob(&store, &key, b"audio").await;
    let result = ResultMessage::http_by_reference(&env, 200, stored);
    let applied = store
        .apply_outcomes(
            vec![Outcome::Finish {
                claim: stale,
                envelope: env,
                result,
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    assert_eq!(applied.fenced, 1);
    assert!(!blob_exists(&store, &key).await);
}

pub async fn replayed_finish_keeps_its_result_blob(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    let env = head.envelope.unwrap();
    let key = BlobKey::result("61", claim.claim_id).unwrap();
    let stored = write_blob(&store, &key, b"audio").await;
    let finish = Outcome::Finish {
        claim,
        result: ResultMessage::http_by_reference(&env, 200, stored),
        envelope: env,
    };
    let first = store
        .apply_outcomes(vec![finish.clone()], NOW_MS)
        .await
        .unwrap();
    assert_eq!(first.results_written, 1);
    let replay = store.apply_outcomes(vec![finish], NOW_MS).await.unwrap();
    assert_eq!(replay.fenced, 1);
    assert!(blob_exists(&store, &key).await);
    assert!(store.open_result_blob(key, NOW_MS).await.unwrap().is_some());
}

pub async fn stale_claim_id_is_fenced(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (_, claim) = claim_head(&store, "q").await;
    let stale = ClaimRef {
        claim_id: claim.claim_id + 1,
        ..claim
    };
    let applied = store
        .apply_outcomes(vec![Outcome::Release { claim: stale }], NOW_MS)
        .await
        .unwrap();
    assert_eq!(applied.fenced, 1);
    assert!(!store.has_pending("q".into(), NOW_MS).await.unwrap());
}

pub async fn release_restores_the_original_position(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100), new_request("b", "q", 200)])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    store
        .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
        .await
        .unwrap();
    let again = store.peek("q".into(), 1, NOW_MS).await.unwrap().remove(0);
    assert_eq!(again.key, head.key);
    let (_, second) = claim_head(&store, "q").await;
    assert_ne!(second.claim_id, 0);
}

pub async fn retry_waits_until_due(store: Store) {
    store
        .submit(vec![new_request("a", "q", 100)])
        .await
        .unwrap();
    let (head, claim) = claim_head(&store, "q").await;
    let mut env = head.envelope.unwrap();
    env.routing.retry_count = 1;
    store
        .apply_outcomes(
            vec![Outcome::Retry {
                claim,
                envelope: env,
                due_ms: NOW_MS + 500,
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    store.promote_due_retries(NOW_MS + 499, 10).await.unwrap();
    assert!(!store.has_pending("q".into(), NOW_MS + 499).await.unwrap());
    assert!(
        store
            .peek("q".into(), 10, NOW_MS + 499)
            .await
            .unwrap()
            .is_empty()
    );
    store.promote_due_retries(NOW_MS + 500, 10).await.unwrap();
    assert!(store.has_pending("q".into(), NOW_MS + 500).await.unwrap());
    let head = store
        .peek("q".into(), 10, NOW_MS + 500)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(head.envelope.unwrap().routing.retry_count, 1);
}

pub async fn cancel_marks_only_the_live_generation(store: Store) {
    store
        .submit(vec![new_request("a", "q", 2_000)])
        .await
        .unwrap();
    assert_eq!(
        store
            .cancel(vec!["a".into(), "missing".into(), "".into()], NOW_MS)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .is_cancelled("a".into(), "61".into(), NOW_MS)
            .await
            .unwrap()
    );
    assert!(
        !store
            .is_cancelled("a".into(), "other".into(), NOW_MS)
            .await
            .unwrap()
    );
    let (_, claim) = claim_head(&store, "q").await;
    store
        .apply_outcomes(
            vec![Outcome::Finish {
                claim,
                envelope: envelope("a", "61", "q", 2_000),
                result: ResultMessage::cancelled(&envelope("a", "61", "q", 2_000)),
            }],
            NOW_MS,
        )
        .await
        .unwrap();
    let mut again = new_request("a", "q", 2_000);
    again.envelope.routing.request_token = "62".into();
    store.submit(vec![again]).await.unwrap();
    assert!(
        !store
            .is_cancelled("a".into(), "62".into(), NOW_MS)
            .await
            .unwrap()
    );
}

pub async fn cancel_after_deadline_is_a_noop(store: Store) {
    store.submit(vec![new_request("a", "q", 10)]).await.unwrap();
    assert_eq!(store.cancel(vec!["a".into()], 10_000).await.unwrap(), 0);
    assert!(
        !store
            .is_cancelled("a".into(), "61".into(), 10_000)
            .await
            .unwrap()
    );
}

pub async fn backlog_counts_deadline_buckets(store: Store) {
    store
        .submit(vec![
            new_request("expired", "q", 90),
            new_request("now", "q", 100),
            new_request("soon", "q", 105),
            new_request("later", "q", 1000),
            new_request("elsewhere", "r", 100),
        ])
        .await
        .unwrap();
    let b = store
        .backlog("q".into(), 100_000, vec![-1, 0, 5, 60])
        .await
        .unwrap();
    assert_eq!(b.depth, 4);
    assert_eq!(b.cumulative, [1, 2, 3, 3]);
    let (_, _claim) = claim_head(&store, "q").await;
    let b = store.backlog("q".into(), 100_000, vec![]).await.unwrap();
    assert_eq!(b.depth, 3);
}

/// Submits `ids` to queue "q" with the given result TTL and finishes each
/// with `make_result`, which may write a blob under the claim's attempt.
async fn finish_with<F, Fut>(store: &Store, ids: &[&str], ttl_s: u64, make_result: F)
where
    F: Fn(InternalRequest, ClaimRef) -> Fut,
    Fut: std::future::Future<Output = ResultMessage>,
{
    let mut reqs = Vec::new();
    for id in ids {
        let mut r = new_request(id, "q", 100);
        r.envelope.routing.result_ttl_seconds = ttl_s;
        reqs.push(r);
    }
    store.submit(reqs).await.unwrap();
    for _ in ids {
        let (head, claim) = claim_head(store, "q").await;
        let env = head.envelope.unwrap();
        let result = make_result(env.clone(), claim.clone()).await;
        store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    envelope: env,
                    result,
                }],
                NOW_MS,
            )
            .await
            .unwrap();
    }
}

async fn finish(store: &Store, ids: &[&str], ttl_s: u64) {
    finish_with(store, ids, ttl_s, |env, _| async move {
        ResultMessage::http(&env, 200, env.request.id.as_bytes())
    })
    .await;
}

/// Finishes one request whose result body is a blob.
async fn finish_by_reference(store: &Store, id: &str, ttl_s: u64) -> BlobKey {
    let key = std::sync::Arc::new(std::sync::Mutex::new(None));
    let seen = std::sync::Arc::clone(&key);
    finish_with(store, &[id], ttl_s, move |env, claim| {
        let seen = std::sync::Arc::clone(&seen);
        let store = store.clone();
        async move {
            let key = BlobKey::result(&env.routing.request_token, claim.claim_id).unwrap();
            let stored = write_blob(&store, &key, b"audio-bytes").await;
            *seen.lock().unwrap() = Some(key);
            ResultMessage::http_by_reference(&env, 200, stored)
        }
    })
    .await;
    key.lock().unwrap().clone().unwrap()
}

fn id_of(body: &str) -> String {
    serde_json::from_str::<ResultMessage>(body).unwrap().id
}

pub async fn pop_is_fifo(store: Store) {
    finish(&store, &["a", "b"], 0).await;
    assert_eq!(
        store.result_depth("results".into(), NOW_MS).await.unwrap(),
        2
    );
    let first = store
        .pop_result("results".into(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    let second = store
        .pop_result("results".into(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((id_of(&first), id_of(&second)), ("a".into(), "b".into()));
    assert_eq!(
        store.pop_result("results".into(), NOW_MS).await.unwrap(),
        None
    );
    assert_eq!(
        store.pop_result("elsewhere".into(), NOW_MS).await.unwrap(),
        None
    );
}

pub async fn expired_results_are_skipped_and_swept(store: Store) {
    finish(&store, &["a"], 10).await;
    assert_eq!(
        store
            .pop_result("results".into(), NOW_MS + 10_000)
            .await
            .unwrap(),
        None
    );
    finish(&store, &["b"], 10).await;
    assert_eq!(store.sweep(NOW_MS + 9_999).await.unwrap(), 0);
    assert!(store.sweep(NOW_MS + 10_000).await.unwrap() >= 1);
    assert_eq!(
        store.pop_result("results".into(), NOW_MS).await.unwrap(),
        None
    );
}

pub async fn claim_ack_is_idempotent_and_fenced(store: Store) {
    finish(&store, &["a"], 0).await;
    let route = || "results".to_string();
    let ResultClaim { claim_id, body } = store
        .claim_result(route(), "me".into(), 1_000, NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id_of(&body), "a");
    assert_eq!(store.result_depth(route(), NOW_MS).await.unwrap(), 0);
    assert_eq!(
        store
            .claim_result(route(), "other".into(), 1_000, NOW_MS)
            .await
            .unwrap(),
        None
    );
    assert!(
        !store
            .renew_result(route(), claim_id, "other".into(), 1_000, NOW_MS)
            .await
            .unwrap()
    );
    assert!(
        store
            .renew_result(route(), claim_id, "me".into(), 1_000, NOW_MS + 500)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .ack_result(route(), claim_id, "other".into(), NOW_MS + 600)
            .await
            .unwrap(),
        AckOutcome::OwnershipLost
    );
    assert_eq!(
        store
            .ack_result(route(), claim_id, "me".into(), NOW_MS + 600)
            .await
            .unwrap(),
        AckOutcome::Acked
    );
    assert_eq!(
        store
            .ack_result(route(), claim_id, "me".into(), NOW_MS + 700)
            .await
            .unwrap(),
        AckOutcome::AlreadyAcked
    );
    assert_eq!(
        store
            .claim_result(route(), "me".into(), 1_000, NOW_MS)
            .await
            .unwrap(),
        None
    );
}

pub async fn lapsed_lease_is_redelivered_in_order(store: Store) {
    finish(&store, &["a", "b"], 0).await;
    let route = || "results".to_string();
    let first = store
        .claim_result(route(), "crashed".into(), 1_000, NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id_of(&first.body), "a");
    let again = store
        .claim_result(route(), "survivor".into(), 1_000, NOW_MS + 1_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.claim_id, first.claim_id);
    assert_eq!(id_of(&again.body), "a");
    assert!(
        !store
            .renew_result(
                route(),
                first.claim_id,
                "crashed".into(),
                1_000,
                NOW_MS + 1_000
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .ack_result(route(), first.claim_id, "crashed".into(), NOW_MS + 1_000)
            .await
            .unwrap(),
        AckOutcome::OwnershipLost
    );
    let next = store
        .claim_result(route(), "survivor".into(), 1_000, NOW_MS + 1_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id_of(&next.body), "b");
}

pub async fn ack_deletes_the_result_blob(store: Store) {
    let key = finish_by_reference(&store, "a", 0).await;
    let (body, content_type) = store
        .open_result_blob(key.clone(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((body.size, content_type.as_str()), (11, "audio/wav"));
    assert_eq!(read_all(body).await.unwrap(), b"audio-bytes");
    let claim = store
        .claim_result("results".into(), "me".into(), 1_000, NOW_MS)
        .await
        .unwrap()
        .unwrap();
    let msg: ResultMessage = serde_json::from_str(&claim.body).unwrap();
    assert_eq!(msg.payload_ref, key.to_ref());
    assert_eq!(msg.payload_size, 11);
    store
        .ack_result("results".into(), claim.claim_id, "me".into(), NOW_MS)
        .await
        .unwrap();
    assert!(
        store
            .open_result_blob(key.clone(), NOW_MS)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!blob_exists(&store, &key).await);
}

pub async fn queued_result_keeps_its_blob_past_the_retention(store: Store) {
    let key = finish_by_reference(&store, "a", 0).await;
    store.sweep(NOW_MS + 10 * 3_600_000).await.unwrap();
    assert!(
        store
            .open_result_blob(key, NOW_MS + 10 * 3_600_000)
            .await
            .unwrap()
            .is_some()
    );
}

pub async fn popped_result_blob_expires_with_retention_or_ttl(store: Store) {
    let retained = finish_by_reference(&store, "a", 0).await;
    let ttl = finish_by_reference(&store, "b", 5).await;
    store
        .pop_result("results".into(), NOW_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .open_result_blob(retained.clone(), NOW_MS)
            .await
            .unwrap()
            .is_some()
    );
    store.sweep(NOW_MS + 5_000).await.unwrap();
    assert!(
        store
            .open_result_blob(ttl.clone(), NOW_MS + 5_000)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!blob_exists(&store, &ttl).await);
    assert!(
        store
            .open_result_blob(retained.clone(), NOW_MS + 5_000)
            .await
            .unwrap()
            .is_some()
    );
    store.sweep(NOW_MS + 3_600_000).await.unwrap();
    assert!(!blob_exists(&store, &retained).await);
}

pub async fn orphans_are_collected_and_referenced_blobs_kept(store: Store) {
    let kept = blob_request(&store, "a", "0a", b"0123456789").await;
    store.submit(vec![kept]).await.unwrap();
    let _never_submitted = blob_request(&store, "b", "0b", b"9876543210").await;
    let result_orphan = BlobKey::result("0c", 1).unwrap();
    write_blob(&store, &result_orphan, b"late").await;
    let cutoff = now_millis() + 60_000;
    assert_eq!(store.collect_orphans(0).await.unwrap(), 0);
    assert_eq!(store.collect_orphans(cutoff).await.unwrap(), 2);
    assert!(blob_exists(&store, &BlobKey::request("0a").unwrap()).await);
    assert!(!blob_exists(&store, &BlobKey::request("0b").unwrap()).await);
    assert!(!blob_exists(&store, &result_orphan).await);
    assert_eq!(store.collect_orphans(cutoff).await.unwrap(), 0);
}

pub async fn budget_round_trip(store: Store) {
    assert_eq!(store.budget("b").await.unwrap(), None);
    store.set_budget("b", Some(b"0.5".to_vec())).await.unwrap();
    assert_eq!(
        store.budget("b").await.unwrap().as_deref(),
        Some(&b"0.5"[..])
    );
    store.set_budget("b", Some(b"0.25".to_vec())).await.unwrap();
    assert_eq!(
        store.budget("b").await.unwrap().as_deref(),
        Some(&b"0.25"[..])
    );
    store.set_budget("b", None).await.unwrap();
    assert_eq!(store.budget("b").await.unwrap(), None);
    store.set_budget("b", None).await.unwrap();
}

pub async fn dispatch_rate_round_trip(store: Store) {
    let limit = DispatchRateLimit {
        api_version: API_VERSION.into(),
        pool_id: "p".into(),
        max_admission_rps: 2.5,
        valid_until_unix_millis: 10,
        decision_id: "d".into(),
    };
    store.set_dispatch_rate("k", Some(&limit)).await.unwrap();
    let before = crate::clock::now_millis();
    let read = store.dispatch_rate("k").await.unwrap();
    let after = crate::clock::now_millis();
    assert_eq!(read.value.unwrap().unwrap(), limit);
    assert!(
        (before - 1_000..=after + 1_000).contains(&read.now_ms),
        "store clock {} outside [{before}, {after}]",
        read.now_ms
    );
    assert!(store.dispatch_rate("other").await.unwrap().value.is_none());
    store
        .kv_put("dispatch-rate/bad".into(), Some(b"{".to_vec()))
        .await
        .unwrap();
    assert!(
        store
            .dispatch_rate("bad")
            .await
            .unwrap()
            .value
            .unwrap()
            .is_err()
    );
}

pub async fn ping_succeeds(store: Store) {
    store.ping().await.unwrap();
}
