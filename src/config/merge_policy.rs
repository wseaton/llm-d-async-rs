use std::collections::BTreeMap;
use std::path::Path;

use reqwest::header::HeaderName;
use serde::Deserialize;

use crate::api::headers;
use crate::config::ConfigError;

/// Metadata attribute stamped as the fairness identity when none is configured.
pub const DEFAULT_FAIRNESS_ATTRIBUTE: &str = "userid";

pub const LANE_KEYS: [&str; 6] = [
    "reserved-interactive",
    "reserved-async",
    "reserved-batch",
    "overflow-interactive",
    "overflow-async",
    "overflow-batch",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fairness {
    /// `None` disables stamping.
    pub header: Option<HeaderName>,
    pub attribute: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergePolicyConfig {
    RandomRobin {
        fairness: Fairness,
    },
    TierPriority {
        priority_header: Option<HeaderName>,
        tier_label: String,
        objective_header: HeaderName,
        lane_objectives: BTreeMap<String, String>,
        fairness: Fairness,
    },
}

impl Default for MergePolicyConfig {
    fn default() -> Self {
        Self::RandomRobin {
            fairness: Fairness {
                header: Some(HeaderName::from_static(headers::FAIRNESS_ID)),
                attribute: DEFAULT_FAIRNESS_ATTRIBUTE.to_owned(),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RandomRobinParams {
    /// Absent means the default header; an explicit "" disables stamping.
    #[serde(default)]
    fairness_header: Option<String>,
    #[serde(default)]
    fairness_attribute: String,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TierPriorityParams {
    #[serde(default)]
    priority_header: String,
    #[serde(default)]
    tier_label: String,
    #[serde(default)]
    objective_header: String,
    #[serde(default)]
    lane_objectives: BTreeMap<String, String>,
    #[serde(default)]
    fairness_header: Option<String>,
    #[serde(default)]
    fairness_attribute: String,
}

fn header(field: &str, value: &str) -> Result<HeaderName, ConfigError> {
    HeaderName::from_bytes(value.as_bytes()).map_err(|_| {
        ConfigError::MergePolicy(format!("{field} {value:?} is not a legal HTTP header name"))
    })
}

fn fairness(header_param: Option<String>, attribute: String) -> Result<Fairness, ConfigError> {
    let header = match header_param.as_deref() {
        None => Some(HeaderName::from_static(headers::FAIRNESS_ID)),
        Some("") => None,
        Some(h) => Some(header("fairness_header", h)?),
    };
    let attribute = if attribute.is_empty() {
        DEFAULT_FAIRNESS_ATTRIBUTE.to_owned()
    } else {
        attribute
    };
    Ok(Fairness { header, attribute })
}

fn params<T: for<'de> Deserialize<'de> + Default>(
    value: Option<serde_json::Value>,
) -> Result<T, ConfigError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(T::default()),
        Some(v) => serde_json::from_value(v).map_err(|e| ConfigError::MergePolicy(e.to_string())),
    }
}

impl MergePolicyConfig {
    pub fn parse(data: &[u8]) -> Result<Self, ConfigError> {
        let spec: Spec =
            serde_json::from_slice(data).map_err(|e| ConfigError::MergePolicy(e.to_string()))?;
        match spec.kind.as_str() {
            "random-robin" => {
                let p: RandomRobinParams = params(spec.parameters)?;
                Ok(Self::RandomRobin {
                    fairness: fairness(p.fairness_header, p.fairness_attribute)?,
                })
            }
            "tier-priority" => {
                let p: TierPriorityParams = params(spec.parameters)?;
                let priority_header = if p.priority_header.is_empty() {
                    None
                } else {
                    Some(header("priority_header", &p.priority_header)?)
                };
                let objective_header = if p.objective_header.is_empty() {
                    HeaderName::from_static(headers::OBJECTIVE)
                } else {
                    header("objective_header", &p.objective_header)?
                };
                if let Some(bad) = p
                    .lane_objectives
                    .keys()
                    .find(|k| !LANE_KEYS.contains(&k.as_str()))
                {
                    return Err(ConfigError::MergePolicy(format!(
                        "lane_objectives key {bad:?} is not one of {LANE_KEYS:?}"
                    )));
                }
                Ok(Self::TierPriority {
                    priority_header,
                    tier_label: if p.tier_label.is_empty() {
                        "tier".to_owned()
                    } else {
                        p.tier_label
                    },
                    objective_header,
                    lane_objectives: p.lane_objectives,
                    fairness: fairness(p.fairness_header, p.fairness_attribute)?,
                })
            }
            other => Err(ConfigError::MergePolicy(format!(
                "unknown request merge policy type {other:?}"
            ))),
        }
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let data = std::fs::read(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&data)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::merge_policy::MergePolicyConfig;

    #[test]
    fn random_robin_fairness_defaults_and_disable() {
        let MergePolicyConfig::RandomRobin { fairness } =
            MergePolicyConfig::parse(br#"{"type":"random-robin"}"#).unwrap()
        else {
            panic!("wrong policy")
        };
        assert_eq!(
            fairness.header.unwrap().as_str(),
            "x-llm-d-inference-fairness-id"
        );
        assert_eq!(fairness.attribute, "userid");

        let MergePolicyConfig::RandomRobin { fairness } = MergePolicyConfig::parse(
            br#"{"type":"random-robin","parameters":{"fairness_header":"","fairness_attribute":"tenant"}}"#,
        )
        .unwrap() else {
            panic!("wrong policy")
        };
        assert!(fairness.header.is_none());
        assert_eq!(fairness.attribute, "tenant");
    }

    #[test]
    fn tier_priority_defaults() {
        let MergePolicyConfig::TierPriority {
            priority_header,
            tier_label,
            objective_header,
            lane_objectives,
            ..
        } = MergePolicyConfig::parse(br#"{"type":"tier-priority","parameters":{}}"#).unwrap()
        else {
            panic!("wrong policy")
        };
        assert!(priority_header.is_none());
        assert_eq!(tier_label, "tier");
        assert_eq!(objective_header.as_str(), "x-llm-d-inference-objective");
        assert!(lane_objectives.is_empty());
    }

    #[test]
    fn rejects_bad_configs() {
        let cases: &[(&str, &str)] = &[
            (r#"{"type":"fifo"}"#, "unknown request merge policy"),
            (r#"{"type":"random-robin","params":{}}"#, "unknown field"),
            (
                r#"{"type":"random-robin","parameters":{"fairness_header":"bad header"}}"#,
                "legal HTTP header",
            ),
            (
                r#"{"type":"tier-priority","parameters":{"priority_header":"x y"}}"#,
                "legal HTTP header",
            ),
            (
                r#"{"type":"tier-priority","parameters":{"objective_header":"x:y"}}"#,
                "legal HTTP header",
            ),
            (
                r#"{"type":"tier-priority","parameters":{"lane_objectives":{"gold":"x"}}}"#,
                "lane_objectives key",
            ),
            (
                r#"{"type":"tier-priority","parameters":{"tierlabel":"x"}}"#,
                "unknown field",
            ),
        ];
        for (input, want) in cases {
            let err = MergePolicyConfig::parse(input.as_bytes()).unwrap_err();
            assert!(err.to_string().contains(want), "{input}: {err}");
        }
    }
}
