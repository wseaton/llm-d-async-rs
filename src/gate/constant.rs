use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::release::Releases;
use crate::gate::{Gate, GateError, Verdict};

/// Always open, full budget.
pub struct OpenGate;

impl Gate for OpenGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async { 1.0 })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        _releases: &'a mut Releases,
    ) -> BoxFuture<'a, Result<Verdict, GateError>> {
        Box::pin(async { Ok(Verdict::Continue) })
    }
}
