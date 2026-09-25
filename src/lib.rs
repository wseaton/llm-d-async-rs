//! Asynchronous dispatch processor for llm-d: pulls requests from durable
//! queues in an embedded store, gates dispatch on system capacity, and
//! forwards requests to an inference gateway.

pub mod api;
pub mod boxed;
pub mod clock;
pub mod config;
pub mod dispatch;
pub mod gate;
pub mod merge;
pub mod runner;
pub mod server;
pub mod store;
pub mod telemetry;
pub mod tls;
pub mod worker;
