use serde::{Deserialize, Serialize};

pub const JSON_CONTENT_TYPE: &str = "application/json";

/// Where a request body lives and how to send it. The body itself is never
/// part of the envelope: it is opaque bytes, stored apart, read only at
/// dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadInfo {
    /// Sent upstream as the request's Content-Type.
    pub content_type: String,
    pub size: u64,
    pub storage: PayloadStorage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadStorage {
    /// In the store, for bodies at or under the inline limit.
    Inline,
    /// In a file under the store's blob directory.
    Blob,
}

/// Whether a media type is JSON (`application/json` or `*/*+json`).
/// Parameters such as `charset` are ignored.
pub fn is_json_media_type(content_type: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type == JSON_CONTENT_TYPE || media_type.ends_with("+json")
}

#[cfg(test)]
mod tests {
    use crate::api::payload::is_json_media_type;

    #[test]
    fn json_media_types() {
        for ct in [
            "application/json",
            "Application/JSON; charset=utf-8",
            "application/problem+json",
            " application/json ",
        ] {
            assert!(is_json_media_type(ct), "{ct}");
        }
        for ct in [
            "audio/wav",
            "text/plain",
            "",
            "application/jsonl",
            "application/octet-stream",
        ] {
            assert!(!is_json_media_type(ct), "{ct}");
        }
    }
}
