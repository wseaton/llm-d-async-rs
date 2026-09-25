//! The PromQL the Prometheus gates build from their params.

use std::collections::BTreeMap;

/// `name{k="v",...}` with labels sorted and values quoted.
pub fn selector(name: &str, labels: &BTreeMap<&str, &str>) -> String {
    if labels.is_empty() {
        return name.to_owned();
    }
    let matchers: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}={}", quote(v)))
        .collect();
    format!("{name}{{{}}}", matchers.join(","))
}

/// A PromQL double-quoted string literal. JSON escaping is a subset of the
/// Go escapes PromQL accepts.
fn quote(value: &str) -> String {
    serde_json::Value::String(value.to_owned()).to_string()
}

fn with_namespace<'a>(
    mut labels: BTreeMap<&'a str, &'a str>,
    namespace: &'a str,
) -> BTreeMap<&'a str, &'a str> {
    if !namespace.is_empty() {
        labels.insert("namespace", namespace);
    }
    labels
}

/// Budget = 1 − EPP flow-control pool saturation.
pub fn saturation(pool: &str, namespace: &str) -> String {
    let labels = with_namespace(BTreeMap::from([("inference_pool", pool)]), namespace);
    format!(
        "1 - {}",
        selector("llm_d_epp_flow_control_pool_saturation", &labels)
    )
}

/// D = 1 − (flow-control queue size / (ready pods × max concurrency)).
/// Needs the flow control plugin, which the llm-d router does not enable.
pub fn flow_control_queue(pool: &str, max_concurrency: f64, namespace: &str) -> String {
    let queue = with_namespace(BTreeMap::from([("inference_pool", pool)]), namespace);
    let pods = with_namespace(BTreeMap::from([("name", pool)]), namespace);
    format!(
        "1 - (sum by(inference_pool)({}) / on() ({} * {max_concurrency}))",
        selector("inference_extension_flow_control_queue_size", &queue),
        selector("inference_pool_ready_pods", &pods),
    )
}

/// D = 1 − (mean per-pod queue depth / max concurrency). Part of EPP's base
/// metrics, so a stock install resolves it; it stops reporting (rather than
/// freezing) when the pool drains.
pub fn pool_queue(pool: &str, max_concurrency: f64, namespace: &str) -> String {
    let queue = with_namespace(BTreeMap::from([("name", pool)]), namespace);
    format!(
        "1 - (avg by(name)({}) / {max_concurrency})",
        selector("inference_pool_per_pod_queue_size", &queue),
    )
}

/// D = 1 − (running vLLM requests / (ready pods × max concurrency)). Needs an
/// `inference_pool` label relabeled onto vLLM metrics.
pub fn vllm_running(pool: &str, max_concurrency: f64, namespace: &str) -> String {
    let vllm = with_namespace(BTreeMap::from([("inference_pool", pool)]), namespace);
    let pods = with_namespace(BTreeMap::from([("name", pool)]), namespace);
    format!(
        "1 - (sum({}) / on() ({} * {max_concurrency}))",
        selector("vllm:num_requests_running", &vllm),
        selector("inference_pool_ready_pods", &pods),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::gate::metric::source::queries::{
        flow_control_queue, pool_queue, saturation, selector, vllm_running,
    };

    #[test]
    fn selectors_quote_and_sort() {
        assert_eq!(selector("m", &BTreeMap::new()), "m");
        assert_eq!(
            selector("m", &BTreeMap::from([("z", "1"), ("a", "x\"y\\")])),
            r#"m{a="x\"y\\",z="1"}"#
        );
    }

    #[test]
    fn queries_match_the_go_gates() {
        assert_eq!(
            saturation("p", "ns"),
            r#"1 - llm_d_epp_flow_control_pool_saturation{inference_pool="p",namespace="ns"}"#
        );
        assert_eq!(
            flow_control_queue("p", 100.0, ""),
            r#"1 - (sum by(inference_pool)(inference_extension_flow_control_queue_size{inference_pool="p"}) / on() (inference_pool_ready_pods{name="p"} * 100))"#
        );
        assert_eq!(
            pool_queue("p", 12.5, "ns"),
            r#"1 - (avg by(name)(inference_pool_per_pod_queue_size{name="p",namespace="ns"}) / 12.5)"#
        );
        assert_eq!(
            vllm_running("p", 64.0, ""),
            r#"1 - (sum(vllm:num_requests_running{inference_pool="p"}) / on() (inference_pool_ready_pods{name="p"} * 64))"#
        );
    }
}
