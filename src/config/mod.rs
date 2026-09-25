//! Command line flags and the JSON configuration files they point at.

pub mod cli;
pub mod duration;
pub mod merge_policy;
pub mod pools;
pub mod transport;

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}")]
    Cli(String),
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse transport config: {0}")]
    TransportJson(serde_json::Error),
    #[error("invalid transport config: {0}")]
    Transport(String),
    #[error("invalid pool config: {0}")]
    Pools(String),
    #[error("invalid merge policy config: {0}")]
    MergePolicy(String),
}
