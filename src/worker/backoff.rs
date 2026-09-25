use std::time::Duration;

const BASE_DELAY_SECS: f64 = 2.0;
const MAX_DELAY_SECS: f64 = 60.0;
const GATE_WAIT_INITIAL: Duration = Duration::from_millis(100);
const GATE_WAIT_MAX: Duration = Duration::from_secs(1);

/// Exponential backoff with equal jitter, capped at 60s and at the time left
/// before the deadline. `jitter` is uniform in [0, 1).
pub fn retry_backoff_secs(retry_count: u32, secs_to_deadline: i64, jitter: f64) -> f64 {
    if secs_to_deadline <= 0 {
        return 0.0;
    }
    let cap = MAX_DELAY_SECS.min(secs_to_deadline as f64);
    let exp = BASE_DELAY_SECS * 2f64.powi(i32::try_from(retry_count).unwrap_or(i32::MAX));
    let t = cap.min(exp);
    t / 2.0 + jitter * t / 2.0
}

/// Honors a server Retry-After longer than the computed backoff, clamped to
/// half the time left so one server estimate cannot spend the whole deadline,
/// and jittered up to 25% so requests shed together do not return together.
pub fn with_retry_after(
    backoff_secs: f64,
    retry_after: Option<Duration>,
    secs_to_deadline: i64,
    jitter: f64,
) -> f64 {
    let Some(retry_after) = retry_after.map(|d| d.as_secs_f64()) else {
        return backoff_secs;
    };
    if retry_after <= backoff_secs {
        return backoff_secs;
    }
    let clamped = retry_after.min(secs_to_deadline as f64 / 2.0);
    backoff_secs.max(clamped * (1.0 + jitter / 4.0))
}

pub fn first_gate_wait() -> Duration {
    GATE_WAIT_INITIAL
}

pub fn next_gate_wait(current: Duration) -> Duration {
    (current * 2).min(GATE_WAIT_MAX)
}

/// A wait in [backoff/2, backoff).
pub fn jittered(backoff: Duration, jitter: f64) -> Duration {
    let half = backoff / 2;
    half + (backoff - half).mul_f64(jitter)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::worker::backoff::{jittered, next_gate_wait, retry_backoff_secs, with_retry_after};

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(retry_backoff_secs(1, 1000, 0.0), 2.0);
        assert!((retry_backoff_secs(1, 1000, 0.999_999) - 3.999_998).abs() < 1e-9);
        assert_eq!(retry_backoff_secs(3, 1000, 0.0), 8.0);
        assert_eq!(retry_backoff_secs(10, 1000, 0.0), 30.0);
        assert_eq!(retry_backoff_secs(10, 10, 0.0), 5.0);
        assert_eq!(retry_backoff_secs(1, 0, 0.5), 0.0);
        assert_eq!(retry_backoff_secs(u32::MAX, 100, 0.0), 30.0);
    }

    #[test]
    fn backoff_stays_before_the_deadline() {
        for secs in 1..200 {
            for jitter in [0.0, 0.5, 0.999_999] {
                for retry in 1..10 {
                    let b = retry_backoff_secs(retry, secs, jitter);
                    assert!(b < secs as f64, "{retry} {secs} {jitter}");
                    let r = with_retry_after(b, Some(Duration::from_secs(10_000)), secs, jitter);
                    assert!(r < secs as f64, "{retry} {secs} {jitter} {r}");
                }
            }
        }
    }

    #[test]
    fn retry_after_only_lengthens() {
        assert_eq!(with_retry_after(4.0, None, 100, 0.0), 4.0);
        assert_eq!(
            with_retry_after(4.0, Some(Duration::from_secs(2)), 100, 0.0),
            4.0
        );
        assert_eq!(
            with_retry_after(4.0, Some(Duration::from_secs(10)), 100, 0.0),
            10.0
        );
        assert_eq!(
            with_retry_after(4.0, Some(Duration::from_secs(10)), 100, 1.0),
            12.5
        );
        assert_eq!(
            with_retry_after(4.0, Some(Duration::from_secs(90)), 100, 0.0),
            50.0
        );
        assert_eq!(
            with_retry_after(4.0, Some(Duration::from_secs(90)), 6, 0.0),
            4.0
        );
    }

    #[test]
    fn gate_waits() {
        assert_eq!(
            next_gate_wait(Duration::from_millis(100)),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_gate_wait(Duration::from_millis(800)),
            Duration::from_secs(1)
        );
        assert_eq!(
            jittered(Duration::from_millis(100), 0.0),
            Duration::from_millis(50)
        );
        assert!(jittered(Duration::from_millis(100), 0.999) < Duration::from_millis(100));
    }
}
