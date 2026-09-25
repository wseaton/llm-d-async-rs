use serde::Deserialize;

use crate::worker::vllm::Api;

#[derive(Deserialize)]
struct Body {
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct Usage {
    #[serde(default)]
    prompt_tokens: i64,
    #[serde(default)]
    completion_tokens: i64,
}

/// Prompt and completion tokens from an OpenAI completions response.
/// `None` for other endpoints, unparsable bodies, or no usage object.
pub fn parse_usage(body: &[u8], request_url: &str) -> Option<(u64, u64)> {
    Api::from_url(request_url)?;
    let usage = serde_json::from_slice::<Body>(body).ok()?.usage?;
    Some((
        u64::try_from(usage.prompt_tokens).unwrap_or(0),
        u64::try_from(usage.completion_tokens).unwrap_or(0),
    ))
}

#[cfg(test)]
mod tests {
    use crate::worker::usage::parse_usage;

    const BODY: &[u8] = br#"{"usage":{"prompt_tokens":7,"completion_tokens":3}}"#;

    #[test]
    fn parses_completions_usage() {
        assert_eq!(parse_usage(BODY, "http://gw/v1/completions"), Some((7, 3)));
        assert_eq!(
            parse_usage(BODY, "http://gw/base/v1/chat/completions/"),
            Some((7, 3))
        );
    }

    #[test]
    fn ignores_everything_else() {
        assert_eq!(parse_usage(BODY, "http://gw/v1/embeddings"), None);
        assert_eq!(parse_usage(b"not json", "http://gw/v1/completions"), None);
        assert_eq!(parse_usage(b"{}", "http://gw/v1/completions"), None);
        assert_eq!(parse_usage(BODY, "not a url"), None);
        assert_eq!(
            parse_usage(
                br#"{"usage":{"prompt_tokens":-5,"completion_tokens":2}}"#,
                "http://gw/v1/completions"
            ),
            Some((0, 2))
        );
    }
}
