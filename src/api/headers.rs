/// Header llm-d-router's flow control reads to arbitrate fairness between flows.
pub const FAIRNESS_ID: &str = "x-llm-d-inference-fairness-id";

/// Header llm-d-router reads to resolve a request's InferenceObjective.
pub const OBJECTIVE: &str = "x-llm-d-inference-objective";

/// Header carrying the queue-level inference objective.
pub const GATEWAY_OBJECTIVE: &str = "x-gateway-inference-objective";

/// Response header llm-d-router sets on 429/503 to say why a request was dropped.
pub const DROPPED_REASON: &str = "x-llm-d-request-dropped-reason";
