use std::time::{Duration, SystemTime};

use bytes::Bytes;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};

use crate::api::headers::DROPPED_REASON;
use crate::api::payload::is_binary_media_type;
use crate::api::result::StoredBody;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobError, BlobStore, BlobWriter};
use crate::store::queue::PayloadBody;
use crate::worker::vllm::{Reassembly, StreamError};

const EVENT_STREAM: &str = "text/event-stream";

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
    pub(crate) fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
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
    Inline(String),
    /// A binary body, in a result blob.
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
    Write(#[from] BlobError),
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

/// Classifies a failure before any response headers arrived: a refused,
/// reset or timed-out connection is retryable.
fn send_failure(e: &reqwest::Error) -> ErrorCategory {
    if e.is_builder() {
        ErrorCategory::InvalidRequest
    } else if e.is_redirect() {
        ErrorCategory::Unknown
    } else {
        ErrorCategory::Server
    }
}

/// What a response's status line and headers say about it.
struct Received {
    status: u16,
    dropped_reason: Option<String>,
    retry_after: Option<Duration>,
    content_type: String,
}

impl Received {
    fn of(response: &reqwest::Response) -> Self {
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
                .map(str::to_owned)
        };
        Self {
            status: response.status().as_u16(),
            dropped_reason: header(DROPPED_REASON).filter(|r| !r.is_empty()),
            retry_after: header(RETRY_AFTER.as_str()).and_then(|v| parse_retry_after(&v)),
            content_type: header(CONTENT_TYPE.as_str()).unwrap_or_default(),
        }
    }
}

/// The error an HTTP error status maps to; `None` for other statuses.
fn status_error(
    status: u16,
    body: Bytes,
    retry_after: Option<Duration>,
    dropped_reason: Option<String>,
) -> Option<ClientError> {
    let (category, what) = match status {
        429 => (ErrorCategory::RateLimit, "rate limited"),
        400..=499 => (ErrorCategory::InvalidRequest, "client error"),
        500..=599 => (ErrorCategory::Server, "server error"),
        _ => return None,
    };
    Some(ClientError {
        category,
        message: format!("{what}: status code {status}"),
        cause: None,
        status,
        body,
        retry_after: (category != ErrorCategory::InvalidRequest)
            .then_some(retry_after)
            .flatten(),
        dropped_reason,
    })
}

/// A stream that failed after a 2xx response. An error event carries the
/// status and body the non-streamed request would have failed with.
pub fn stream_failed(e: StreamError) -> ClientError {
    match e {
        StreamError::Upstream(error) => {
            let status = error.error.code;
            let body = serde_json::to_vec(&error)
                .map(Bytes::from)
                .unwrap_or_default();
            let mut failed =
                status_error(status, body.clone(), None, None).unwrap_or_else(|| ClientError {
                    status,
                    body,
                    ..ClientError::new(ErrorCategory::Server, "error event")
                });
            failed.cause = Some(error.error.message);
            failed
        }
        e if e.interrupted() => {
            ClientError::new(ErrorCategory::Server, "stream interrupted").caused_by(e)
        }
        e => ClientError::new(ErrorCategory::Unknown, "unusable stream").caused_by(e),
    }
}

impl InferenceClient {
    pub fn new(http: reqwest::Client, blobs: BlobStore) -> Self {
        Self { http, blobs }
    }

    /// POSTs `payload` to `url`. The payload is streamed, never buffered. A
    /// 2xx response with a binary media type is streamed into the blob
    /// `result_key`; any other successful response is inlined unless it is
    /// not UTF-8, in which case it goes to that blob too.
    pub async fn send(
        &self,
        url: &str,
        mut headers: HeaderMap,
        payload: PayloadBody,
        result_key: &BlobKey,
    ) -> Result<InferenceResponse, Box<ClientError>> {
        let request = match payload {
            PayloadBody::Inline(bytes) => self.http.post(url).headers(headers).body(bytes),
            PayloadBody::Blob(body) => {
                headers.insert(CONTENT_LENGTH, HeaderValue::from(body.size));
                self.http
                    .post(url)
                    .headers(headers)
                    .body(reqwest::Body::wrap_stream(body.stream))
            }
        };
        let mut response = request.send().await.map_err(|e| {
            Box::new(ClientError::new(send_failure(&e), "failed to send request").caused_by(e))
        })?;

        let Received {
            status,
            dropped_reason,
            retry_after,
            content_type,
        } = Received::of(&response);

        let store_failed = |e: StoreBodyError| {
            Box::new(ClientError {
                status,
                ..ClientError::new(ErrorCategory::Server, "failed to store response body")
                    .caused_by(e)
            })
        };

        if (200..300).contains(&status) && is_binary_media_type(&content_type) {
            let stored = self
                .stream_body(&mut response, &content_type, result_key)
                .await
                .map_err(store_failed)?;
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
        if let Some(e) = status_error(status, body.clone(), retry_after, dropped_reason) {
            return Err(Box::new(e));
        }
        let text = match String::from_utf8(Vec::from(body)) {
            Ok(text) => text,
            Err(e) => {
                let stored = self
                    .write_body(Bytes::from(e.into_bytes()), &content_type, result_key)
                    .await
                    .map_err(store_failed)?;
                return Ok(InferenceResponse {
                    status,
                    body: ResponseBody::Stored(stored),
                });
            }
        };
        Ok(InferenceResponse {
            status,
            body: ResponseBody::Inline(text),
        })
    }

    /// POSTs a JSON `body` and returns the body of a 2xx reply.
    pub async fn post_json(
        &self,
        url: &str,
        mut headers: HeaderMap,
        body: Vec<u8>,
    ) -> Result<Bytes, Box<ClientError>> {
        headers.remove(CONTENT_LENGTH);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = self
            .http
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                Box::new(ClientError::new(send_failure(&e), "failed to send request").caused_by(e))
            })?;
        let Received {
            status,
            dropped_reason,
            retry_after,
            ..
        } = Received::of(&response);
        let body = response.bytes().await.map_err(|e| {
            Box::new(ClientError {
                status,
                ..ClientError::new(ErrorCategory::Server, "failed to read response").caused_by(e)
            })
        })?;
        if (200..300).contains(&status) {
            return Ok(body);
        }
        let unexpected = || ClientError {
            status,
            body: body.clone(),
            ..ClientError::new(
                ErrorCategory::Unknown,
                format!("unexpected status {status}"),
            )
        };
        Err(Box::new(
            status_error(status, body.clone(), retry_after, dropped_reason)
                .unwrap_or_else(unexpected),
        ))
    }

    /// POSTs `body`, which asks for an event stream, and feeds the stream to
    /// `reassembly`, which keeps the output so far when this fails or is
    /// cancelled. Returns the response status once the stream has ended.
    pub async fn send_streamed(
        &self,
        url: &str,
        mut headers: HeaderMap,
        body: Bytes,
        reassembly: &mut Reassembly,
    ) -> Result<u16, Box<ClientError>> {
        headers.remove(CONTENT_LENGTH);
        let mut response = self
            .http
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                Box::new(ClientError::new(send_failure(&e), "failed to send request").caused_by(e))
            })?;

        let Received {
            status,
            dropped_reason,
            retry_after,
            content_type,
        } = Received::of(&response);

        if !(200..300).contains(&status) || !content_type.starts_with(EVENT_STREAM) {
            let body = response.bytes().await.map_err(|e| {
                Box::new(ClientError {
                    status,
                    retry_after,
                    dropped_reason: dropped_reason.clone(),
                    ..ClientError::new(ErrorCategory::Server, "failed to read response")
                        .caused_by(e)
                })
            })?;
            let unexpected = || ClientError {
                status,
                body: body.clone(),
                ..ClientError::new(
                    ErrorCategory::Unknown,
                    format!("expected an event stream, got status {status} {content_type:?}"),
                )
            };
            return Err(Box::new(
                status_error(status, body.clone(), retry_after, dropped_reason)
                    .unwrap_or_else(unexpected),
            ));
        }

        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => reassembly
                    .feed(&chunk)
                    .map_err(|e| Box::new(stream_failed(e)))?,
                Ok(None) => break,
                Err(e) => {
                    return Err(Box::new(
                        ClientError::new(ErrorCategory::Server, "stream interrupted").caused_by(e),
                    ));
                }
            }
        }
        Ok(status)
    }

    async fn stream_body(
        &self,
        response: &mut reqwest::Response,
        content_type: &str,
        key: &BlobKey,
    ) -> Result<StoredBody, StoreBodyError> {
        let mut writer = self.blobs.create(key).await?;
        while let Some(chunk) = response.chunk().await? {
            writer.write(chunk).await?;
        }
        Ok(self.commit(writer, content_type, key).await?)
    }

    async fn write_body(
        &self,
        body: Bytes,
        content_type: &str,
        key: &BlobKey,
    ) -> Result<StoredBody, StoreBodyError> {
        let mut writer = self.blobs.create(key).await?;
        writer.write(body).await?;
        Ok(self.commit(writer, content_type, key).await?)
    }

    async fn commit(
        &self,
        writer: BlobWriter,
        content_type: &str,
        key: &BlobKey,
    ) -> Result<StoredBody, BlobError> {
        let digest = writer.commit().await?;
        Ok(StoredBody {
            payload_ref: key.to_ref(),
            location: self.blobs.location(key),
            content_type: content_type.to_owned(),
            size: digest.size,
            sha256: digest.sha256,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use axum::response::Redirect;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    use crate::worker::client::{ClientError, ErrorCategory, parse_retry_after, send_failure};

    async fn send_error(url: &str) -> reqwest::Error {
        reqwest::Client::new()
            .post(url)
            .body("{}")
            .send()
            .await
            .unwrap_err()
    }

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

    #[tokio::test]
    async fn refused_connection_is_retryable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let e = send_error(&format!("http://{addr}/v1/completions")).await;
        assert_eq!(send_failure(&e), ErrorCategory::Server, "{e:?}");
    }

    #[tokio::test]
    async fn connection_dropped_before_headers_is_retryable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await.unwrap();
        });
        let e = send_error(&format!("http://{addr}/v1/completions")).await;
        assert_eq!(send_failure(&e), ErrorCategory::Server, "{e:?}");
    }

    #[tokio::test]
    async fn invalid_url_is_an_invalid_request() {
        let e = send_error("http://[::1/v1/completions").await;
        assert_eq!(send_failure(&e), ErrorCategory::InvalidRequest, "{e:?}");
    }

    #[tokio::test]
    async fn redirect_loop_is_fatal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().fallback(|| async { Redirect::temporary("/again") });
        tokio::spawn(async move { axum::serve(listener, router).await });
        let e = send_error(&format!("http://{addr}/v1/completions")).await;
        let category = send_failure(&e);
        assert_eq!(category, ErrorCategory::Unknown, "{e:?}");
        assert!(category.fatal());
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
