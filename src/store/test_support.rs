use std::collections::BTreeMap;

use crate::api::request::{InternalRequest, RequestMessage};
use crate::api::routing::InternalRouting;
use crate::store::queue::NewRequest;
use crate::store::staging::StagedPayload;

pub fn envelope(id: &str, token: &str, queue: &str, deadline: i64) -> InternalRequest {
    InternalRequest {
        routing: InternalRouting {
            request_token: token.into(),
            request_queue_name: queue.into(),
            result_queue_name: "results".into(),
            ..Default::default()
        },
        request: RequestMessage {
            id: id.into(),
            created: 0,
            deadline,
            metadata: BTreeMap::new(),
            headers: BTreeMap::new(),
            endpoint: String::new(),
            model: String::new(),
        },
        payload: StagedPayload::Inline(Vec::new()).info("application/json"),
    }
}

/// An inline-payload request whose token is the hex of its ID's bytes.
pub fn new_request(id: &str, queue: &str, deadline: i64) -> NewRequest {
    let token: String = id.bytes().map(|b| format!("{b:02x}")).collect();
    let payload = StagedPayload::Inline(format!(r#"{{"prompt":"{id}"}}"#).into_bytes());
    let mut envelope = envelope(id, &token, queue, deadline);
    envelope.payload = payload.info("application/json");
    NewRequest { envelope, payload }
}
