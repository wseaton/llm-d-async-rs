use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const DEFAULT_SERVICE_NAME: &str = "llm-d-async";

#[derive(Debug, thiserror::Error)]
pub enum TracingError {
    #[error("build OTLP exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    #[error("install subscriber: {0}")]
    Subscriber(#[from] tracing_subscriber::util::TryInitError),
}

/// Flushes spans on shutdown.
pub struct TracingGuard {
    provider: Option<SdkTracerProvider>,
}

impl TracingGuard {
    pub fn shutdown(self) {
        if let Some(provider) = self.provider
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!(error = %e, "failed to flush traces");
        }
    }
}

fn verbosity_filter(verbosity: u8) -> &'static str {
    match verbosity {
        0..=2 => "info",
        3..=4 => "debug",
        _ => "trace",
    }
}

/// Installs the W3C trace-context and baggage propagators.
pub fn set_propagator() {
    opentelemetry::global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
}

/// Installs the log subscriber and, when `OTEL_EXPORTER_OTLP_ENDPOINT` is
/// set, an OTLP/gRPC span exporter with W3C trace-context propagation.
pub fn init(verbosity: u8) -> Result<TracingGuard, TracingError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(verbosity_filter(verbosity)));
    let fmt = tracing_subscriber::fmt::layer();

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    if endpoint.is_empty() {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .try_init()?;
        tracing::info!("OTEL_EXPORTER_OTLP_ENDPOINT not set, tracing export disabled");
        return Ok(TracingGuard { provider: None });
    }

    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned());
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .build(),
        )
        .build();
    set_propagator();
    let otel = tracing_opentelemetry::layer().with_tracer(provider.tracer(DEFAULT_SERVICE_NAME));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .try_init()?;
    tracing::info!(%endpoint, service = %service_name, "OpenTelemetry tracing initialized");
    Ok(TracingGuard {
        provider: Some(provider),
    })
}

#[cfg(test)]
mod tests {
    use crate::telemetry::tracing::verbosity_filter;

    #[test]
    fn verbosity_maps_to_levels() {
        assert_eq!(verbosity_filter(0), "info");
        assert_eq!(verbosity_filter(2), "info");
        assert_eq!(verbosity_filter(3), "debug");
        assert_eq!(verbosity_filter(4), "debug");
        assert_eq!(verbosity_filter(9), "trace");
    }
}
