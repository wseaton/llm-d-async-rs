use std::fmt;

const REQUESTS: &str = "requests";
const RESULTS: &str = "results";
const REF_SCHEME: &str = "blob://";

/// Names one blob. A request body is `requests/<token>`; a result body is
/// `results/<token>-<attempt>`, one per claim attempt, so an attempt that
/// lost its claim can never overwrite the body a later attempt committed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobKey {
    Request { token: String },
    Result { token: String, attempt: u64 },
}

fn valid_token(token: &str) -> bool {
    !token.is_empty() && token.len() <= 128 && token.bytes().all(|b| b.is_ascii_hexdigit())
}

impl BlobKey {
    pub fn request(token: &str) -> Option<Self> {
        valid_token(token).then(|| Self::Request {
            token: token.to_owned(),
        })
    }

    pub fn result(token: &str, attempt: u64) -> Option<Self> {
        valid_token(token).then(|| Self::Result {
            token: token.to_owned(),
            attempt,
        })
    }

    /// Parses the name of a result body as it appears in its download URL.
    pub fn parse_result_name(name: &str) -> Option<Self> {
        let (token, attempt) = name.rsplit_once('-')?;
        if attempt.is_empty() || !attempt.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Self::result(token, attempt.parse().ok()?)
    }

    pub fn parse(key: &str) -> Option<Self> {
        match key.split_once('/')? {
            (REQUESTS, token) => Self::request(token),
            (RESULTS, name) => Self::parse_result_name(name),
            _ => None,
        }
    }

    /// The `payload_ref` a result carries for this blob.
    pub fn to_ref(&self) -> String {
        format!("{REF_SCHEME}{self}")
    }

    pub fn from_ref(payload_ref: &str) -> Option<Self> {
        Self::parse(payload_ref.strip_prefix(REF_SCHEME)?)
    }

    pub fn dir(&self) -> &'static str {
        match self {
            Self::Request { .. } => REQUESTS,
            Self::Result { .. } => RESULTS,
        }
    }

    /// The key without its directory.
    pub fn name(&self) -> String {
        match self {
            Self::Request { token } => token.clone(),
            Self::Result { token, attempt } => format!("{token}-{attempt}"),
        }
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.dir(), self.name())
    }
}

#[cfg(test)]
mod tests {
    use crate::store::blob::key::BlobKey;

    #[test]
    fn keys_reject_path_tricks() {
        assert!(BlobKey::request("ab12").is_some());
        for bad in ["", "../x", "a/b", "zz", "ab.cd"] {
            assert!(BlobKey::request(bad).is_none(), "{bad}");
            assert!(BlobKey::result(bad, 1).is_none(), "{bad}");
        }
        for bad in [
            "requests/",
            "other/ab",
            "results/ab",
            "results/ab-",
            "results/ab--1",
            "results/ab-1x",
            "results/-1",
            "results/ab-99999999999999999999999",
        ] {
            assert!(BlobKey::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn keys_round_trip() {
        let request = BlobKey::request("ab").unwrap();
        assert_eq!(request.to_string(), "requests/ab");
        assert_eq!(BlobKey::parse("requests/ab"), Some(request));
        let result = BlobKey::result("ff", 7).unwrap();
        assert_eq!(result.to_string(), "results/ff-7");
        assert_eq!(result.name(), "ff-7");
        assert_eq!(result.to_ref(), "blob://results/ff-7");
        assert_eq!(
            BlobKey::from_ref("blob://results/ff-7"),
            Some(result.clone())
        );
        assert_eq!(BlobKey::parse_result_name("ff-7"), Some(result));
        assert_eq!(BlobKey::from_ref("s3://results/ff-7"), None);
    }
}
