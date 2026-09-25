use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::api::routing::{Classification, Tier};
use crate::boxed::BoxFuture;
use crate::gate::release::Releases;
use crate::gate::{Gate, GateError, SharedGate, Verdict};

/// Admits everything while the saturation gate admits. Once it refuses:
/// reserved traffic waits, interactive overflow is dropped with a 429-style
/// result, and everything else is returned to its queue.
pub struct TierAdmissionGate {
    saturation: SharedGate,
    tier_label: String,
}

impl TierAdmissionGate {
    pub fn new(saturation: SharedGate, tier_label: String) -> Self {
        Self {
            saturation,
            tier_label,
        }
    }
}

impl Gate for TierAdmissionGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        self.saturation.budget()
    }

    fn apply<'a>(
        &'a self,
        msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Result<Verdict, GateError>> {
        Box::pin(async move {
            if self.saturation.apply(msg, releases).await? != Verdict::Refuse {
                return Ok(Verdict::Continue);
            }
            Ok(match msg.routing.classification() {
                Some(Classification::Reserved) => Verdict::Wait,
                Some(Classification::Overflow)
                    if msg.routing.labels.get(&self.tier_label).map(String::as_str)
                        == Some(Tier::Interactive.as_str()) =>
                {
                    let mut result = ResultMessage::http(msg, 0, b"");
                    result.payload = r#"{"error": "Too Many Requests", "code": 429}"#.to_owned();
                    Verdict::Drop(Some(Box::new(result)))
                }
                _ => Verdict::Refuse,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::api::routing::Classification;
    use crate::gate::combinator::tier_admission::TierAdmissionGate;
    use crate::gate::release::Releases;
    use crate::gate::test_support::{FixedGate, request};
    use crate::gate::{Gate, Verdict};

    async fn decide(saturated: bool, class: Option<Classification>, tier: Option<&str>) -> Verdict {
        let inner = if saturated {
            Verdict::Refuse
        } else {
            Verdict::Continue
        };
        let g = TierAdmissionGate::new(Arc::new(FixedGate::new(1.0, inner)), "sla".into());
        let mut msg = request(&[]);
        msg.routing.set_classification(class);
        if let Some(t) = tier {
            msg.routing.labels.insert("sla".into(), t.into());
        }
        g.apply(&mut msg, &mut Releases::default()).await.unwrap()
    }

    #[tokio::test]
    async fn admits_when_not_saturated() {
        assert_eq!(
            decide(false, Some(Classification::Overflow), Some("interactive")).await,
            Verdict::Continue
        );
    }

    #[tokio::test]
    async fn saturated_decisions() {
        assert_eq!(
            decide(true, Some(Classification::Reserved), Some("batch")).await,
            Verdict::Wait
        );
        assert_eq!(
            decide(true, Some(Classification::Overflow), Some("batch")).await,
            Verdict::Refuse
        );
        assert_eq!(
            decide(true, None, Some("interactive")).await,
            Verdict::Refuse
        );
        let Verdict::Drop(Some(result)) =
            decide(true, Some(Classification::Overflow), Some("interactive")).await
        else {
            panic!("interactive overflow must drop")
        };
        assert_eq!(
            result.payload,
            r#"{"error": "Too Many Requests", "code": 429}"#
        );
        assert_eq!(result.status_code, 0);
        assert_eq!(result.error_code, None);
    }
}
