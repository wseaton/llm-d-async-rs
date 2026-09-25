use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::boxed::BoxFuture;
use crate::gate::metric::source::{MetricSource, Sample, SourceError, parse_value};

const SCRAPE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default)]
pub struct ScrapeConfig {
    pub url: String,
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    /// 0 means the metric is already normalized to [0, 1].
    pub max_count_per_pod: f64,
    /// Return the normalized value as the budget instead of 1 − value.
    pub direct_budget: bool,
    /// When set with `pods_metric`, capacity is ready pods × max_count_per_pod.
    pub pods_url: String,
    pub pods_metric: String,
    pub pods_labels: BTreeMap<String, String>,
}

/// Scrapes a Prometheus text-format endpoint and turns one metric into a
/// budget in [0, 1].
pub struct ScrapeSource {
    http: reqwest::Client,
    cfg: ScrapeConfig,
}

impl ScrapeSource {
    pub fn new(http: reqwest::Client, cfg: ScrapeConfig) -> Self {
        Self { http, cfg }
    }

    async fn fetch(
        &self,
        url: &str,
        metric: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<Sample>, SourceError> {
        let http_err = |e: reqwest::Error| SourceError::Http {
            url: url.to_owned(),
            message: e.to_string(),
        };
        let response = self
            .http
            .get(url)
            .timeout(SCRAPE_TIMEOUT)
            .send()
            .await
            .map_err(http_err)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(SourceError::Status {
                url: url.to_owned(),
                status: response.status().as_u16(),
            });
        }
        let body = response.text().await.map_err(http_err)?;
        Ok(parse_text(&body, metric)?
            .into_iter()
            .filter(|s| labels.iter().all(|(k, v)| s.labels.get(k) == Some(v)))
            .collect())
    }

    async fn run(&self) -> Result<Vec<Sample>, SourceError> {
        let samples = self
            .fetch(&self.cfg.url, &self.cfg.metric, &self.cfg.labels)
            .await?;
        let mut max_count = self.cfg.max_count_per_pod;
        if !self.cfg.pods_url.is_empty() && !self.cfg.pods_metric.is_empty() {
            let pods = self
                .fetch(
                    &self.cfg.pods_url,
                    &self.cfg.pods_metric,
                    &self.cfg.pods_labels,
                )
                .await?;
            let ready = pods.first().map(|s| s.value).ok_or_else(|| {
                SourceError::Invalid(format!(
                    "pods metric {} not found at {}",
                    self.cfg.pods_metric, self.cfg.pods_url
                ))
            })?;
            if ready <= 0.0 || ready.is_nan() {
                return Err(SourceError::Invalid(format!(
                    "ready pods is {ready}, cannot compute capacity"
                )));
            }
            max_count = ready * self.cfg.max_count_per_pod;
        }
        Ok(samples
            .into_iter()
            .map(|s| {
                let normalized = if max_count > 0.0 {
                    s.value / max_count
                } else {
                    s.value
                };
                let normalized = normalized.clamp(0.0, 1.0);
                Sample {
                    labels: s.labels,
                    value: if self.cfg.direct_budget {
                        normalized
                    } else {
                        1.0 - normalized
                    },
                }
            })
            .collect())
    }
}

impl MetricSource for ScrapeSource {
    fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>> {
        Box::pin(self.run())
    }
}

/// Samples named exactly `metric` from a Prometheus text exposition. Only
/// gauge, counter and untyped families count; histograms and summaries have
/// no single value and yield nothing.
pub fn parse_text(body: &str, metric: &str) -> Result<Vec<Sample>, SourceError> {
    let mut types: HashMap<&str, &str> = HashMap::new();
    let mut samples = Vec::new();
    for (n, raw) in body.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(comment) = line.strip_prefix('#') {
            let mut words = comment.split_whitespace();
            if words.next() == Some("TYPE")
                && let (Some(name), Some(kind)) = (words.next(), words.next())
            {
                types.insert(name, kind);
            }
            continue;
        }
        let err = |what: &str| SourceError::Parse(format!("line {}: {what}: {raw}", n + 1));
        let name_end = line
            .find(|c: char| c == '{' || c.is_whitespace())
            .ok_or_else(|| err("missing value"))?;
        let (name, rest) = line.split_at(name_end);
        let (labels, rest) = if rest.starts_with('{') {
            parse_labels(rest).map_err(|e| err(&e))?
        } else {
            (BTreeMap::new(), rest)
        };
        let value_text = rest
            .split_whitespace()
            .next()
            .ok_or_else(|| err("missing value"))?;
        let value = parse_value(value_text).ok_or_else(|| err("bad value"))?;
        if name == metric {
            samples.push(Sample { labels, value });
        }
    }
    match types.get(metric) {
        Some(&"histogram") | Some(&"summary") | Some(&"gaugehistogram") => Ok(Vec::new()),
        _ => Ok(samples),
    }
}

/// Parses `{k="v",...}` at the start of `s`, returning the labels and the rest.
fn parse_labels(s: &str) -> Result<(BTreeMap<String, String>, &str), String> {
    let mut labels = BTreeMap::new();
    let mut chars = s.char_indices().skip(1).peekable();
    loop {
        while chars
            .peek()
            .is_some_and(|(_, c)| c.is_whitespace() || *c == ',')
        {
            chars.next();
        }
        match chars.peek() {
            Some((i, '}')) => {
                let end = *i + 1;
                return Ok((labels, &s[end..]));
            }
            None => return Err("unterminated labels".into()),
            _ => {}
        }
        let mut key = String::new();
        while let Some((_, c)) = chars.peek() {
            if *c == '=' || c.is_whitespace() {
                break;
            }
            key.push(*c);
            chars.next();
        }
        while chars.peek().is_some_and(|(_, c)| c.is_whitespace()) {
            chars.next();
        }
        if chars.next().map(|(_, c)| c) != Some('=') {
            return Err(format!("label {key:?} missing '='"));
        }
        while chars.peek().is_some_and(|(_, c)| c.is_whitespace()) {
            chars.next();
        }
        if chars.next().map(|(_, c)| c) != Some('"') {
            return Err(format!("label {key:?} value not quoted"));
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                Some((_, '"')) => break,
                Some((_, '\\')) => match chars.next() {
                    Some((_, 'n')) => value.push('\n'),
                    Some((_, c)) => value.push(c),
                    None => return Err("unterminated escape".into()),
                },
                Some((_, c)) => value.push(c),
                None => return Err(format!("label {key:?} value unterminated")),
            }
        }
        labels.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use crate::gate::metric::source::scrape::parse_text;

    const BODY: &str = r#"
# HELP vllm:num_requests_running Running requests.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="m",pod="a"} 3
vllm:num_requests_running{model_name="m", pod="b",} 5 1700000000000
vllm:num_requests_running_other 9
# TYPE latency histogram
latency_bucket{le="1"} 2
latency_sum 3
# TYPE plain untyped
plain 1.5e1
escaped{v="a\"b\\c\nd"} +Inf
"#;

    #[test]
    fn parses_gauges_with_labels() {
        let s = parse_text(BODY, "vllm:num_requests_running").unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s[1].labels["pod"], "b");
        assert_eq!(s[1].value, 5.0);
        assert_eq!(parse_text(BODY, "plain").unwrap()[0].value, 15.0);
        let e = parse_text(BODY, "escaped").unwrap();
        assert_eq!(e[0].labels["v"], "a\"b\\c\nd");
        assert_eq!(e[0].value, f64::INFINITY);
    }

    #[test]
    fn histograms_and_missing_families_yield_nothing() {
        assert!(parse_text(BODY, "latency").unwrap().is_empty());
        assert!(parse_text(BODY, "absent").unwrap().is_empty());
    }

    #[test]
    fn malformed_lines_error() {
        for bad in ["m{a=\"1\"", "m{a=1} 2", "m", "m{a=\"1\"} x"] {
            assert!(parse_text(bad, "m").is_err(), "{bad}");
        }
    }
}
