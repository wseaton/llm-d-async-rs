use std::collections::{BTreeMap, HashSet};

use serde::Deserialize;

use crate::config::ConfigError;
use crate::config::pools::WorkerPools;

pub type GateParams = serde_json::Map<String, serde_json::Value>;

/// Queue topology, parsed from `--transport-config[-file]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(default = "default_result_queue")]
    pub result_queue_name: String,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub queues: Vec<QueueConfig>,
}

fn default_result_queue() -> String {
    "result-list".to_owned()
}
fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_batch_size() -> usize {
    10
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    #[serde(default)]
    pub id: String,
    pub queue_name: String,
    /// Overrides the request's and the transport's result queue.
    #[serde(default)]
    pub result_queue_name: String,
    /// When > 0, each result written for this queue expires after this long.
    #[serde(default)]
    pub result_ttl_seconds: u64,
    #[serde(default)]
    pub worker_pool_id: String,
    #[serde(default)]
    pub inference_objective: String,
    #[serde(default)]
    pub request_path_url: String,
    pub igw_base_url: String,
    #[serde(default)]
    pub gate_type: String,
    #[serde(default)]
    pub gate_params: GateParams,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl QueueConfig {
    fn normalize(&mut self) {
        if self.worker_pool_id.is_empty() {
            self.worker_pool_id = "default".to_owned();
        }
        if self.request_path_url.is_empty() {
            self.request_path_url = "/v1/completions".to_owned();
        }
        if self.id.is_empty() {
            self.id = self.queue_name.clone();
        }
    }
}

impl TransportConfig {
    /// Parses and validates a config. Startup requires at least one queue;
    /// hot reload may drain every queue, so it passes `allow_empty`.
    pub fn parse(data: &[u8], pools: &WorkerPools, allow_empty: bool) -> Result<Self, ConfigError> {
        let mut cfg: Self = serde_json::from_slice(data).map_err(ConfigError::TransportJson)?;
        for q in &mut cfg.queues {
            q.normalize();
        }
        cfg.validate(pools, allow_empty)?;
        Ok(cfg)
    }

    fn validate(&self, pools: &WorkerPools, allow_empty: bool) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Transport(msg));
        if self.poll_interval_ms == 0 {
            return invalid("poll_interval_ms must be positive".into());
        }
        if self.batch_size == 0 {
            return invalid("batch_size must be positive".into());
        }
        if self.result_queue_name.is_empty() {
            return invalid("result_queue_name must not be empty".into());
        }
        if !allow_empty && self.queues.is_empty() {
            return invalid("at least one queue must be configured".into());
        }
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for q in &self.queues {
            if q.queue_name.is_empty() {
                return invalid("queue_name is required for each queue".into());
            }
            if !ids.insert(q.id.as_str()) {
                return invalid(format!("duplicate queue id {:?}", q.id));
            }
            if !names.insert(q.queue_name.as_str()) {
                return invalid(format!("duplicate queue_name {:?}", q.queue_name));
            }
            if q.igw_base_url.is_empty() {
                return invalid(format!(
                    "queue {:?}: igw_base_url must be specified",
                    q.queue_name
                ));
            }
            if let Err(e) = reqwest::Url::parse(&q.igw_base_url) {
                return invalid(format!("queue {:?}: igw_base_url: {e}", q.queue_name));
            }
            if pools.get(&q.worker_pool_id).is_none() {
                return invalid(format!(
                    "queue {:?}: worker pool {:?} not found in pool configuration",
                    q.queue_name, q.worker_pool_id
                ));
            }
        }
        Ok(())
    }

    pub fn queue_by_name(&self, name: &str) -> Option<&QueueConfig> {
        self.queues.iter().find(|q| q.queue_name == name)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::pools::{WorkerPoolConfig, WorkerPools};
    use crate::config::transport::TransportConfig;

    fn pools() -> WorkerPools {
        WorkerPools::new(vec![
            WorkerPoolConfig::new("default", 2),
            WorkerPoolConfig::new("p", 1),
        ])
        .unwrap()
    }

    #[test]
    fn applies_defaults() {
        let cfg = TransportConfig::parse(
            br#"{"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}"#,
            &pools(),
            false,
        )
        .unwrap();
        assert_eq!(cfg.result_queue_name, "result-list");
        assert_eq!(cfg.poll_interval_ms, 1000);
        assert_eq!(cfg.batch_size, 10);
        let q = &cfg.queues[0];
        assert_eq!(q.id, "q");
        assert_eq!(q.worker_pool_id, "default");
        assert_eq!(q.request_path_url, "/v1/completions");
    }

    #[test]
    fn rejects_invalid_configs() {
        let cases: &[(&str, &str)] = &[
            (r#"{"queues":[]}"#, "at least one queue"),
            (
                r#"{"queues":[{"queue_name":"","igw_base_url":"http://gw"}]}"#,
                "queue_name is required",
            ),
            (
                r#"{"queues":[{"queue_name":"q","igw_base_url":""}]}"#,
                "igw_base_url must be specified",
            ),
            (
                r#"{"queues":[{"queue_name":"q","igw_base_url":"not a url"}]}"#,
                "igw_base_url",
            ),
            (
                r#"{"queues":[{"queue_name":"q","igw_base_url":"http://gw"},{"queue_name":"q","id":"x","igw_base_url":"http://gw"}]}"#,
                "duplicate queue_name",
            ),
            (
                r#"{"queues":[{"queue_name":"a","id":"x","igw_base_url":"http://gw"},{"queue_name":"b","id":"x","igw_base_url":"http://gw"}]}"#,
                "duplicate queue id",
            ),
            (
                r#"{"queues":[{"queue_name":"q","igw_base_url":"http://gw","worker_pool_id":"nope"}]}"#,
                "worker pool \"nope\" not found",
            ),
            (
                r#"{"batch_size":0,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}"#,
                "batch_size",
            ),
            (
                r#"{"poll_interval_ms":0,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}"#,
                "poll_interval_ms",
            ),
            (
                r#"{"url":"redis://x","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}"#,
                "unknown field",
            ),
            (
                r#"{"queues":[{"queue_name":"q","igw_base_url":"http://gw","gate":"x"}]}"#,
                "unknown field",
            ),
        ];
        for (input, want) in cases {
            let err = TransportConfig::parse(input.as_bytes(), &pools(), false).unwrap_err();
            assert!(err.to_string().contains(want), "{input}: {err}");
        }
    }

    #[test]
    fn reload_may_drain_every_queue() {
        let cfg = TransportConfig::parse(br#"{"queues":[]}"#, &pools(), true).unwrap();
        assert!(cfg.queues.is_empty());
    }
}
