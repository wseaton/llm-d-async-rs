use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::config::ConfigError;
use crate::config::transport::GateParams;

/// A named worker pool: a fixed number of workers, optionally gated.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPoolConfig {
    pub id: String,
    pub workers: usize,
    #[serde(default)]
    pub gate_type: String,
    #[serde(default)]
    pub gate_params: GateParams,
}

impl WorkerPoolConfig {
    pub fn new(id: &str, workers: usize) -> Self {
        Self {
            id: id.to_owned(),
            workers,
            gate_type: String::new(),
            gate_params: GateParams::new(),
        }
    }
}

/// Validated set of worker pools, keyed by ID.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerPools(BTreeMap<String, WorkerPoolConfig>);

impl WorkerPools {
    pub fn new(pools: Vec<WorkerPoolConfig>) -> Result<Self, ConfigError> {
        let mut map = BTreeMap::new();
        for (i, pool) in pools.into_iter().enumerate() {
            if pool.id.is_empty() {
                return Err(ConfigError::Pools(format!(
                    "pool at index {i} has an empty ID"
                )));
            }
            if pool.workers == 0 {
                return Err(ConfigError::Pools(format!(
                    "pool {:?} must have at least 1 worker",
                    pool.id
                )));
            }
            if map.contains_key(&pool.id) {
                return Err(ConfigError::Pools(format!(
                    "duplicate pool ID {:?}",
                    pool.id
                )));
            }
            map.insert(pool.id.clone(), pool);
        }
        if map.is_empty() {
            return Err(ConfigError::Pools(
                "at least one pool must be configured".into(),
            ));
        }
        Ok(Self(map))
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let data = std::fs::read(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let pools: Vec<WorkerPoolConfig> =
            serde_json::from_slice(&data).map_err(|e| ConfigError::Pools(e.to_string()))?;
        Self::new(pools)
    }

    pub fn get(&self, id: &str) -> Option<&WorkerPoolConfig> {
        self.0.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &WorkerPoolConfig> {
        self.0.values()
    }

    pub fn total_workers(&self) -> usize {
        self.0.values().map(|p| p.workers).sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::pools::{WorkerPoolConfig, WorkerPools};

    #[test]
    fn validates_pools() {
        assert!(WorkerPools::new(vec![]).is_err());
        assert!(WorkerPools::new(vec![WorkerPoolConfig::new("", 1)]).is_err());
        assert!(WorkerPools::new(vec![WorkerPoolConfig::new("a", 0)]).is_err());
        assert!(
            WorkerPools::new(vec![
                WorkerPoolConfig::new("a", 1),
                WorkerPoolConfig::new("a", 2)
            ])
            .is_err()
        );
        let pools = WorkerPools::new(vec![
            WorkerPoolConfig::new("a", 1),
            WorkerPoolConfig::new("b", 2),
        ])
        .unwrap();
        assert_eq!(pools.total_workers(), 3);
        assert_eq!(pools.get("b").map(|p| p.workers), Some(2));
    }

    #[test]
    fn parses_gate_params() {
        let pools: Vec<WorkerPoolConfig> = serde_json::from_str(
            r#"[{"id":"a","workers":4,"gate_type":"wait-on-refuse","gate_params":{"gate":{"gate_type":"constant"}}}]"#,
        )
        .unwrap();
        assert_eq!(pools[0].gate_type, "wait-on-refuse");
        assert!(pools[0].gate_params.contains_key("gate"));
    }
}
