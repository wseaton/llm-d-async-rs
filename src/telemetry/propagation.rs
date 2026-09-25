use std::collections::BTreeMap;

use opentelemetry::propagation::{Extractor, Injector};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataExtractor<'a>(&'a BTreeMap<String, String>);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// Continues a trace whose context the producer placed in request metadata
/// (e.g. a `traceparent` entry).
pub fn set_parent_from_metadata(span: &tracing::Span, metadata: &BTreeMap<String, String>) {
    if metadata.is_empty() {
        return;
    }
    let parent =
        opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(metadata)));
    let _ = span.set_parent(parent);
}

/// Writes the current span's context into outgoing request headers.
pub fn inject_current(headers: &mut HeaderMap) {
    let cx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&cx, &mut HeaderInjector(headers))
    });
}
