use serde::{Deserialize, Serialize};

/// Output a streamed completion produced before it was interrupted, saved
/// with the request so the next attempt continues it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// The original prompt, as the engine saw it.
    pub prompt_token_ids: Vec<u32>,
    pub token_ids: Vec<u32>,
    /// The text of `token_ids`.
    pub text: String,
    /// Attempts that continued from saved output.
    pub resumes: u32,
}
