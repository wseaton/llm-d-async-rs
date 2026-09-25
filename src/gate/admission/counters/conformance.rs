//! The behaviour every [`crate::gate::admission::counters::Counters`]
//! implementation must have.

use std::time::Duration;

use crate::clock::now_millis;
use crate::gate::admission::counters::{BucketSpec, Counters};

macro_rules! counters_conformance {
    ($fixture:path) => {
        counters_conformance!(@cases $fixture;
            slots_are_limited_per_key_and_given_back,
            concurrent_acquires_never_exceed_the_limit,
            rate_windows_count_per_window_and_slide,
            buckets_drain_and_reset_after_expiry,
        );
    };
    (@cases $fixture:path; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                let Some(counters) = $fixture().await else {
                    return;
                };
                crate::gate::admission::counters::conformance::$name(counters).await;
            }
        )*
    };
}

pub(crate) use counters_conformance;

/// Retries a check that depends on a release landing asynchronously.
async fn eventually<F, Fut>(mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("condition never held");
}

pub async fn slots_are_limited_per_key_and_given_back<C: Counters + 'static>(c: std::sync::Arc<C>) {
    let a1 = c.acquire_slot("t:a".into(), 2).await.unwrap().unwrap();
    let _a2 = c.acquire_slot("t:a".into(), 2).await.unwrap().unwrap();
    assert!(c.acquire_slot("t:a".into(), 2).await.unwrap().is_none());
    let _b = c.acquire_slot("t:b".into(), 2).await.unwrap().unwrap();
    drop(a1);
    let again = std::sync::Arc::new(tokio::sync::Mutex::new(None));
    eventually(|| {
        let c = std::sync::Arc::clone(&c);
        let again = std::sync::Arc::clone(&again);
        async move {
            match c.acquire_slot("t:a".into(), 2).await.unwrap() {
                Some(slot) => {
                    *again.lock().await = Some(slot);
                    true
                }
                None => false,
            }
        }
    })
    .await;
    assert!(c.acquire_slot("t:a".into(), 3).await.unwrap().is_some());
}

pub async fn concurrent_acquires_never_exceed_the_limit<C: Counters + 'static>(
    c: std::sync::Arc<C>,
) {
    let mut tasks = Vec::new();
    for _ in 0..40 {
        let c = std::sync::Arc::clone(&c);
        tasks.push(tokio::spawn(async move {
            c.acquire_slot("t:burst".into(), 7).await.unwrap()
        }));
    }
    let mut held = Vec::new();
    for t in tasks {
        if let Some(slot) = t.await.unwrap() {
            held.push(slot);
        }
    }
    assert_eq!(held.len(), 7);
}

pub async fn rate_windows_count_per_window_and_slide<C: Counters + 'static>(c: std::sync::Arc<C>) {
    let short = Duration::from_millis(400);
    let long = Duration::from_secs(60);
    assert!(c.admit("r:a".into(), 2, short).await.unwrap());
    assert!(c.admit("r:a".into(), 2, short).await.unwrap());
    assert!(!c.admit("r:a".into(), 2, short).await.unwrap());
    assert!(c.admit("r:a".into(), 1, long).await.unwrap());
    assert!(!c.admit("r:a".into(), 1, long).await.unwrap());
    assert!(c.admit("r:b".into(), 2, short).await.unwrap());
    tokio::time::sleep(short + Duration::from_millis(50)).await;
    assert!(c.admit("r:a".into(), 2, short).await.unwrap());
    assert!(!c.admit("r:a".into(), 1, long).await.unwrap());
}

pub async fn buckets_drain_and_reset_after_expiry<C: Counters + 'static>(c: std::sync::Arc<C>) {
    let live = BucketSpec {
        rate: 0.0,
        capacity: 3.0,
        expires_ms: now_millis() + 60_000,
    };
    for _ in 0..3 {
        assert!(c.take_token("b:a".into(), live).await.unwrap());
    }
    assert!(!c.take_token("b:a".into(), live).await.unwrap());
    assert!(c.take_token("b:b".into(), live).await.unwrap());
    let lapsed = BucketSpec {
        expires_ms: now_millis() - 1,
        ..live
    };
    assert!(
        !c.take_token("b:a".into(), lapsed).await.unwrap(),
        "the stored expiry, not the new one, decides the reset"
    );
    for _ in 0..5 {
        assert!(c.take_token("b:a".into(), lapsed).await.unwrap());
    }
    let refilling = BucketSpec {
        rate: 20.0,
        capacity: 1.0,
        expires_ms: now_millis() + 60_000,
    };
    assert!(c.take_token("b:c".into(), refilling).await.unwrap());
    assert!(!c.take_token("b:c".into(), refilling).await.unwrap());
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(c.take_token("b:c".into(), refilling).await.unwrap());
}
