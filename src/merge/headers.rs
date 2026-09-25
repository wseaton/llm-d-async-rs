use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

/// Outgoing request headers. Names are case-insensitive: setting one replaces
/// every case variant, so the wire never carries two values for one name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<(String, String)>);

#[derive(Debug, thiserror::Error)]
#[error("invalid header {name:?}")]
pub struct InvalidHeader {
    pub name: String,
}

impl Headers {
    pub fn set(&mut self, name: &str, value: &str) {
        self.0.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.0.push((name.to_owned(), value.to_owned()));
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn to_header_map(&self) -> Result<HeaderMap, InvalidHeader> {
        let mut map = HeaderMap::with_capacity(self.0.len());
        for (k, v) in &self.0 {
            let invalid = || InvalidHeader { name: k.clone() };
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|_| invalid())?;
            let value = HeaderValue::from_str(v).map_err(|_| invalid())?;
            map.insert(name, value);
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use crate::merge::headers::Headers;

    #[test]
    fn set_replaces_case_variants() {
        let mut h = Headers::default();
        h.set("Content-Type", "application/json");
        h.set("content-type", "audio/wav");
        assert_eq!(h.get("CONTENT-TYPE"), Some("audio/wav"));
        let map = h.to_header_map().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["content-type"], "audio/wav");
    }

    #[test]
    fn invalid_headers_are_reported() {
        let mut h = Headers::default();
        h.set("bad header", "x");
        assert_eq!(h.to_header_map().unwrap_err().name, "bad header");
        let mut h = Headers::default();
        h.set("x", "line\nbreak");
        assert!(h.to_header_map().is_err());
    }
}
