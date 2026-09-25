//! Moving requests from queues to workers and outcomes back to the store.
//!
//! ```text
//!  consumer (per queue) ──Claimed──► merge (per pool) ──Dispatch──► workers
//!        │ peek, gate, admit                                   │
//!        ▼                                                     ▼ Outcome
//!      store ◄────────────── writer ◄───────────────── ClaimGuard
//! ```
//!
//! Every claimed request travels with a [`claim::ClaimGuard`]. Whatever drops
//! it without finishing it (shutdown, a closed channel, a panic) returns the
//! request to its queue.

pub mod backlog;
pub mod claim;
pub mod consumer;
pub mod message;
pub mod queues;
pub mod reload;
pub mod upkeep;
pub mod writer;
