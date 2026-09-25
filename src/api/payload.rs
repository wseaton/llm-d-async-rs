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

/// Whether a media type names a binary body: `audio/*`, `image/*`,
/// `video/*`, `font/*`, `model/*`, or a known binary `application/*` type.
/// Parameters such as `charset` are ignored.
pub fn is_binary_media_type(content_type: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let Some((kind, subtype)) = media_type.split_once('/') else {
        return false;
    };
    match kind {
        "audio" | "image" | "video" | "font" | "model" => true,
        "application" => {
            matches!(
                subtype,
                "octet-stream"
                    | "pdf"
                    | "zip"
                    | "gzip"
                    | "zstd"
                    | "x-tar"
                    | "protobuf"
                    | "x-protobuf"
                    | "msgpack"
                    | "x-msgpack"
                    | "cbor"
                    | "wasm"
            ) || subtype.ends_with("+zip")
                || subtype.ends_with("+cbor")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::api::payload::is_binary_media_type;

    #[test]
    fn binary_media_types() {
        for ct in [
            "audio/wav",
            "Audio/MPEG; rate=44100",
            " image/png ",
            "image/svg+xml",
            "video/mp4",
            "font/woff2",
            "model/gltf-binary",
            "application/octet-stream",
            "application/pdf",
            "application/zip",
            "application/gzip",
            "application/x-protobuf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document+zip",
            "application/vnd.example+cbor",
        ] {
            assert!(is_binary_media_type(ct), "{ct}");
        }
        for ct in [
            "",
            "garbage",
            "application/json",
            "Application/JSON; charset=utf-8",
            "application/problem+json",
            "application/jsonl",
            "application/x-ndjson",
            "application/xml",
            "text/plain",
            "text/event-stream",
            "text/html; charset=utf-8",
        ] {
            assert!(!is_binary_media_type(ct), "{ct}");
        }
    }
}
