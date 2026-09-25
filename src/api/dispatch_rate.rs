use serde::{Deserialize, Serialize};

pub const API_VERSION: &str = "llm-d.ai/v1alpha1";

/// A leased, pool-scoped ceiling on new inference dispatches, written by an
/// external controller. `max_admission_rps == 0` is an explicit pause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DispatchRateLimit {
    pub api_version: String,
    pub pool_id: String,
    pub max_admission_rps: f64,
    #[serde(rename = "valid_until_unix_ms")]
    pub valid_until_unix_millis: i64,
    pub decision_id: String,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DispatchRateError {
    #[error("unsupported api_version {0:?}")]
    ApiVersion(String),
    #[error("pool_id is required")]
    MissingPool,
    #[error("max_admission_rps must be finite and non-negative")]
    Rate,
    #[error("dispatch-rate lease has expired")]
    Expired,
    #[error("decision_id is required")]
    MissingDecision,
}

impl DispatchRateLimit {
    pub fn validate_at(&self, now_millis: i64) -> Result<(), DispatchRateError> {
        if self.api_version != API_VERSION {
            return Err(DispatchRateError::ApiVersion(self.api_version.clone()));
        }
        if self.pool_id.is_empty() {
            return Err(DispatchRateError::MissingPool);
        }
        if !self.max_admission_rps.is_finite() || self.max_admission_rps < 0.0 {
            return Err(DispatchRateError::Rate);
        }
        if self.valid_until_unix_millis <= now_millis {
            return Err(DispatchRateError::Expired);
        }
        if self.decision_id.is_empty() {
            return Err(DispatchRateError::MissingDecision);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::api::dispatch_rate::{API_VERSION, DispatchRateError, DispatchRateLimit};

    fn valid() -> DispatchRateLimit {
        DispatchRateLimit {
            api_version: API_VERSION.into(),
            pool_id: "p".into(),
            max_admission_rps: 5.0,
            valid_until_unix_millis: 2_000,
            decision_id: "d".into(),
        }
    }

    #[test]
    fn validation() {
        assert_eq!(valid().validate_at(1_000), Ok(()));
        let zero = DispatchRateLimit {
            max_admission_rps: 0.0,
            ..valid()
        };
        assert_eq!(zero.validate_at(1_000), Ok(()));
        assert_eq!(valid().validate_at(2_000), Err(DispatchRateError::Expired));
        let cases = [
            (
                DispatchRateLimit {
                    api_version: "v0".into(),
                    ..valid()
                },
                DispatchRateError::ApiVersion("v0".into()),
            ),
            (
                DispatchRateLimit {
                    pool_id: String::new(),
                    ..valid()
                },
                DispatchRateError::MissingPool,
            ),
            (
                DispatchRateLimit {
                    max_admission_rps: -1.0,
                    ..valid()
                },
                DispatchRateError::Rate,
            ),
            (
                DispatchRateLimit {
                    max_admission_rps: f64::INFINITY,
                    ..valid()
                },
                DispatchRateError::Rate,
            ),
            (
                DispatchRateLimit {
                    max_admission_rps: f64::NAN,
                    ..valid()
                },
                DispatchRateError::Rate,
            ),
            (
                DispatchRateLimit {
                    decision_id: String::new(),
                    ..valid()
                },
                DispatchRateError::MissingDecision,
            ),
        ];
        for (limit, want) in cases {
            assert_eq!(limit.validate_at(1_000), Err(want));
        }
    }

    #[test]
    fn wire_names_match_the_go_contract() {
        let v = serde_json::to_value(valid()).unwrap();
        assert_eq!(v["valid_until_unix_ms"], 2_000);
        assert_eq!(v["max_admission_rps"], 5.0);
    }
}
