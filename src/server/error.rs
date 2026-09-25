use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::store::error::StoreError;
use crate::store::staging::StageError;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    TooLarge(String),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("{0}")]
    Internal(String),
}

impl From<StageError> for ApiError {
    fn from(e: StageError) -> Self {
        match e {
            StageError::TooLarge { .. } => Self::TooLarge(e.to_string()),
            StageError::Token(_) | StageError::Blob(_) => Self::Internal(e.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Store(_) | Self::Internal(_) => {
                tracing::error!(error = %self, "request failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}
