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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use reqwest::header::HeaderMap;
    use tracing_subscriber::layer::SubscriberExt;

    use crate::telemetry::propagation::{inject_current, set_parent_from_metadata};

    const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN: &str = "00f067aa0ba902b7";

    /// The headers a request sent under a `process-request` span carries,
    /// given the request's metadata.
    fn outgoing(metadata: &[(&str, &str)]) -> HeaderMap {
        crate::telemetry::tracing::set_propagator();
        let provider = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        let metadata: BTreeMap<String, String> = metadata
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("process-request");
            set_parent_from_metadata(&span, &metadata);
            let mut headers = HeaderMap::new();
            span.in_scope(|| inject_current(&mut headers));
            headers
        })
    }

    fn traceparent(headers: &HeaderMap) -> Vec<String> {
        headers["traceparent"]
            .to_str()
            .unwrap()
            .split('-')
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn a_traceparent_in_metadata_continues_the_producers_trace() {
        let headers = outgoing(&[("traceparent", &format!("00-{TRACE}-{SPAN}-01"))]);
        let parts = traceparent(&headers);
        assert_eq!(parts[1], TRACE, "same trace");
        assert_ne!(parts[2], SPAN, "a child span of its own");
        assert_eq!(parts[3], "01", "still sampled");
    }

    #[test]
    fn without_a_traceparent_the_request_starts_a_trace() {
        let headers = outgoing(&[("userid", "t")]);
        let parts = traceparent(&headers);
        assert_eq!(parts[1].len(), 32);
        assert_ne!(parts[1], TRACE);
        assert_ne!(parts[1], "0".repeat(32));
    }

    #[test]
    fn baggage_in_metadata_travels_with_the_request() {
        let headers = outgoing(&[
            ("traceparent", &format!("00-{TRACE}-{SPAN}-01")),
            ("baggage", "tenant=a"),
        ]);
        assert_eq!(headers["baggage"], "tenant=a");
    }

    #[test]
    fn a_malformed_traceparent_starts_a_fresh_trace() {
        let headers = outgoing(&[("traceparent", "garbage")]);
        let parts = traceparent(&headers);
        assert_eq!(parts[1].len(), 32);
        assert_ne!(parts[1], TRACE);
    }
}
