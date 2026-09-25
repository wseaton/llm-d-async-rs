use std::collections::BTreeMap;

use reqwest::Url;
use serde::Deserialize;

use crate::clock::now_millis;
use crate::gate::BoxFuture;
use crate::gate::metric::source::{MetricSource, Sample, SourceError, parse_value};

/// Evaluates one PromQL expression as an instant query.
pub struct PromQlSource {
    http: reqwest::Client,
    endpoint: Url,
    expr: String,
}

#[derive(Deserialize)]
struct Response {
    status: String,
    #[serde(default)]
    data: Option<Data>,
    #[serde(default, rename = "errorType")]
    error_type: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Data {
    result_type: String,
    result: serde_json::Value,
}

#[derive(Deserialize)]
struct VectorSample {
    #[serde(default)]
    metric: BTreeMap<String, String>,
    value: (f64, String),
}

impl PromQlSource {
    pub fn new(http: reqwest::Client, prometheus: &Url, expr: String) -> Result<Self, SourceError> {
        let mut base = prometheus.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let endpoint = base
            .join("api/v1/query")
            .map_err(|e| SourceError::Invalid(format!("prometheus url: {e}")))?;
        Ok(Self {
            http,
            endpoint,
            expr,
        })
    }

    pub fn expr(&self) -> &str {
        &self.expr
    }

    async fn run(&self) -> Result<Vec<Sample>, SourceError> {
        let url = self.endpoint.to_string();
        let time = format!("{:.3}", now_millis() as f64 / 1000.0);
        let mut request_url = self.endpoint.clone();
        request_url
            .query_pairs_mut()
            .append_pair("query", &self.expr)
            .append_pair("time", &time);
        let response = self
            .http
            .get(request_url)
            .send()
            .await
            .map_err(|e| SourceError::Http {
                url: url.clone(),
                message: e.to_string(),
            })?;
        let status = response.status();
        let body = response.bytes().await.map_err(|e| SourceError::Http {
            url: url.clone(),
            message: e.to_string(),
        })?;
        let parsed: Response = serde_json::from_slice(&body).map_err(|e| {
            if status.is_success() {
                SourceError::Parse(e.to_string())
            } else {
                SourceError::Status {
                    url: url.clone(),
                    status: status.as_u16(),
                }
            }
        })?;
        parse_response(parsed)
    }
}

fn parse_response(parsed: Response) -> Result<Vec<Sample>, SourceError> {
    if parsed.status != "success" {
        return Err(SourceError::Query {
            error_type: parsed.error_type,
            message: parsed.error,
        });
    }
    if !parsed.warnings.is_empty() {
        tracing::info!(warnings = ?parsed.warnings, "Prometheus query returned warnings");
    }
    let data = parsed
        .data
        .ok_or_else(|| SourceError::Parse("missing data".into()))?;
    if data.result_type != "vector" {
        return Err(SourceError::Parse(format!(
            "expected vector result, got {}",
            data.result_type
        )));
    }
    let samples: Vec<VectorSample> =
        serde_json::from_value(data.result).map_err(|e| SourceError::Parse(e.to_string()))?;
    samples
        .into_iter()
        .map(|s| {
            let value = parse_value(&s.value.1)
                .ok_or_else(|| SourceError::Parse(format!("sample value {:?}", s.value.1)))?;
            Ok(Sample {
                labels: s.metric,
                value,
            })
        })
        .collect()
}

impl MetricSource for PromQlSource {
    fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>> {
        Box::pin(self.run())
    }
}

#[cfg(test)]
mod tests {
    use crate::gate::metric::source::SourceError;
    use crate::gate::metric::source::promql::{PromQlSource, Response, parse_response};

    fn parse(body: &str) -> Result<Vec<(String, f64)>, SourceError> {
        let r: Response = serde_json::from_str(body).unwrap();
        parse_response(r).map(|samples| {
            samples
                .into_iter()
                .map(|s| (s.labels.get("pool").cloned().unwrap_or_default(), s.value))
                .collect()
        })
    }

    #[test]
    fn endpoint_keeps_the_base_path() {
        let http = reqwest::Client::new();
        for (base, want) in [
            ("http://prom:9090", "http://prom:9090/api/v1/query"),
            ("http://gw/prometheus", "http://gw/prometheus/api/v1/query"),
            ("http://gw/prometheus/", "http://gw/prometheus/api/v1/query"),
        ] {
            let src = PromQlSource::new(http.clone(), &base.parse().unwrap(), "up".into()).unwrap();
            assert_eq!(src.endpoint.as_str(), want);
        }
    }

    #[test]
    fn parses_vectors() {
        let got = parse(
            r#"{"status":"success","data":{"resultType":"vector","result":[
                {"metric":{"pool":"a"},"value":[1700000000.1,"0.25"]},
                {"metric":{},"value":[1700000000.1,"+Inf"]}]}}"#,
        )
        .unwrap();
        assert_eq!(got, [("a".into(), 0.25), (String::new(), f64::INFINITY)]);
        assert_eq!(
            parse(r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#).unwrap(),
            []
        );
    }

    #[test]
    fn rejects_errors_and_non_vectors() {
        assert_eq!(
            parse(r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#),
            Err(SourceError::Query {
                error_type: "bad_data".into(),
                message: "parse error".into()
            })
        );
        assert!(matches!(
            parse(r#"{"status":"success","data":{"resultType":"scalar","result":[1,"1"]}}"#),
            Err(SourceError::Parse(_))
        ));
        assert!(matches!(
            parse(
                r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"x"]}]}}"#
            ),
            Err(SourceError::Parse(_))
        ));
    }
}
