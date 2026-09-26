use serde::{Deserialize, Serialize};

/// Output a generation produced before it was interrupted, saved with the
/// request so the next attempt continues it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// The rendered prompt, as the engine saw it.
    pub prompt_token_ids: Vec<u32>,
    pub token_ids: Vec<u32>,
    /// Set once the generation finished: only derendering is left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    /// Attempts that continued from saved output.
    pub resumes: u32,
}
