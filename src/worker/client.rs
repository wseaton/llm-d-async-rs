use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};

use crate::api::headers::DROPPED_REASON;
use crate::api::payload::is_json_media_type;
use crate::api::result::StoredBody;
use crate::store::PayloadBody;
use crate::store::blob::{BlobKey, BlobStore};

/// How an inference failure is handled: retried, shed, or final.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    RateLimit,
    Server,
    InvalidRequest,
    Unknown,
}

impl ErrorCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "RATE_LIMIT",
            Self::Server => "SERVER_ERROR",
            Self::InvalidRequest => "INVALID_REQ",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Not worth retrying.
    pub fn fatal(self) -> bool {
        !matches!(self, Self::RateLimit | Self::Server)
    }

    /// The backend refused for capacity.
    pub fn sheddable(self) -> bool {
        self == Self::RateLimit
    }
}

#[derive(Debug)]
pub struct ClientError {
    pub category: ErrorCategory,
    pub message: String,
    pub cause: Option<String>,
    /// 0 when no HTTP response was received.
    pub status: u16,
    pub body: Bytes,
    pub retry_after: Option<Duration>,
    pub dropped_reason: Option<String>,
}

impl ClientError {
    fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
            cause: None,
            status: 0,
            body: Bytes::new(),
            retry_after: None,
            dropped_reason: None,
        }
    }

    fn caused_by(mut self, cause: impl std::fmt::Display) -> Self {
        self.cause = Some(cause.to_string());
        self
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.category.as_str(), self.message)?;
        if let Some(reason) = &self.dropped_reason {
            write!(f, " (dropped: {reason})")?;
        }
        if let Some(cause) = &self.cause {
            write!(f, " (caused by: {cause})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClientError {}

pub enum ResponseBody {
    Inline(Bytes),
    /// A 2xx non-JSON body, streamed into a result blob as it arrived.
    Stored(StoredBody),
}

pub struct InferenceResponse {
    pub status: u16,
    pub body: ResponseBody,
}

#[derive(Debug, thiserror::Error)]
enum StoreBodyError {
    #[error("read response: {0}")]
    Read(#[from] reqwest::Error),
    #[error("write blob: {0}")]
    Write(#[from] std::io::Error),
}

pub struct InferenceClient {
    http: reqwest::Client,
    blobs: BlobStore,
}

/// Seconds (`120`) or an HTTP date. Dates in the past are zero.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

impl InferenceClient {
    pub fn new(http: reqwest::Client, blobs: BlobStore) -> Self {
        Self { http, blobs }
    }

    /// POSTs `payload` to `url`. The payload is streamed, never buffered. A
    /// 2xx response that is not JSON is streamed into the blob `result_key`.
    pub async fn send(
        &self,
        url: &str,
        mut headers: HeaderMap,
        payload: PayloadBody,
        result_key: &BlobKey,
    ) -> Result<InferenceResponse, Box<ClientError>> {
        let request = match payload {
            PayloadBody::Inline(bytes) => self.http.post(url).headers(headers).body(bytes),
            PayloadBody::File { file, size } => {
                headers.insert(CONTENT_LENGTH, HeaderValue::from(size));
                let stream = tokio_util::io::ReaderStream::new(file);
                self.http
                    .post(url)
                    .headers(headers)
                    .body(reqwest::Body::wrap_stream(stream))
            }
        };
        let mut response = request.send().await.map_err(|e| {
            let category = if e.is_builder() {
                ErrorCategory::InvalidRequest
            } else {
                ErrorCategory::Unknown
            };
            Box::new(ClientError::new(category, "failed to send request").caused_by(e))
        })?;

        let status = response.status().as_u16();
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
                .map(str::to_owned)
        };
        let dropped_reason = header(DROPPED_REASON).filter(|r| !r.is_empty());
        let retry_after = header(RETRY_AFTER.as_str()).and_then(|v| parse_retry_after(&v));
        let content_type = header(CONTENT_TYPE.as_str()).unwrap_or_default();

        if (200..300).contains(&status) && !is_json_media_type(&content_type) {
            let stored = self
                .store_body(&mut response, &content_type, result_key)
                .await
                .map_err(|e| {
                    Box::new(ClientError {
                        status,
                        ..ClientError::new(ErrorCategory::Server, "failed to store response body")
                            .caused_by(e)
                    })
                })?;
            return Ok(InferenceResponse {
                status,
                body: ResponseBody::Stored(stored),
            });
        }

        let body = response.bytes().await.map_err(|e| {
            Box::new(ClientError {
                status,
                retry_after,
                dropped_reason: dropped_reason.clone(),
                ..ClientError::new(ErrorCategory::Server, "failed to read response").caused_by(e)
            })
        })?;
        let category = match status {
            429 => Some((ErrorCategory::RateLimit, "rate limited")),
            400..=499 => Some((ErrorCategory::InvalidRequest, "client error")),
            500..=599 => Some((ErrorCategory::Server, "server error")),
            _ => None,
        };
        if let Some((category, what)) = category {
            return Err(Box::new(ClientError {
                category,
                message: format!("{what}: status code {status}"),
                cause: None,
                status,
                body,
                retry_after: (category != ErrorCategory::InvalidRequest)
                    .then_some(retry_after)
                    .flatten(),
                dropped_reason,
            }));
        }
        Ok(InferenceResponse {
            status,
            body: ResponseBody::Inline(body),
        })
    }

    async fn store_body(
        &self,
        response: &mut reqwest::Response,
        content_type: &str,
        key: &BlobKey,
    ) -> Result<StoredBody, StoreBodyError> {
        let mut writer = self.blobs.writer().await?;
        while let Some(chunk) = response.chunk().await? {
            writer.write(&chunk).await?;
        }
        let digest = writer.commit(key).await?;
        Ok(StoredBody {
            payload_ref: key.to_ref(),
            content_type: content_type.to_owned(),
            size: digest.size,
            sha256: digest.sha256,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::worker::client::{ClientError, ErrorCategory, parse_retry_after};

    #[test]
    fn retry_after_forms() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after(" 0 "), Some(Duration::ZERO));
        assert_eq!(
            parse_retry_after("Thu, 01 Dec 1994 16:00:00 GMT"),
            Some(Duration::ZERO)
        );
        let future = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(30));
        let d = parse_retry_after(&future).unwrap();
        assert!(
            d > Duration::from_secs(28) && d <= Duration::from_secs(30),
            "{d:?}"
        );
        assert_eq!(parse_retry_after("soon"), None);
        assert_eq!(parse_retry_after("-5"), None);
    }

    #[test]
    fn categories() {
        assert!(!ErrorCategory::RateLimit.fatal() && ErrorCategory::RateLimit.sheddable());
        assert!(!ErrorCategory::Server.fatal() && !ErrorCategory::Server.sheddable());
        assert!(ErrorCategory::InvalidRequest.fatal());
        assert!(ErrorCategory::Unknown.fatal());
    }

    #[test]
    fn display_matches_go() {
        let mut e = ClientError::new(ErrorCategory::RateLimit, "rate limited: status code 429");
        e.dropped_reason = Some("queue-ttl".into());
        assert_eq!(
            e.to_string(),
            "RATE_LIMIT: rate limited: status code 429 (dropped: queue-ttl)"
        );
        let e =
            ClientError::new(ErrorCategory::Unknown, "failed to send request").caused_by("refused");
        assert_eq!(
            e.to_string(),
            "UNKNOWN: failed to send request (caused by: refused)"
        );
    }
}
