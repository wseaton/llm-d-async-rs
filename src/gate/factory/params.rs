//! Typed reads of the loosely typed `gate_params` object. Like the Go
//! processor, scalars may be given as JSON numbers or as strings, and nested
//! objects either inline or as a JSON-encoded string.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::config::duration::parse_duration;
use crate::config::transport::GateParams;

/// A gate nested inside another gate's params.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NestedGate {
    pub gate_type: String,
    #[serde(default)]
    pub gate_params: GateParams,
}

pub struct Params<'a>(pub &'a GateParams);

impl Params<'_> {
    fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key).filter(|v| !v.is_null())
    }

    pub fn string(&self, key: &str, default: &str) -> String {
        match self.get(key) {
            Some(Value::String(s)) if !s.is_empty() => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            _ => default.to_owned(),
        }
    }

    pub fn float(&self, key: &str, default: f64) -> Result<f64, String> {
        match self.get(key) {
            None => Ok(default),
            Some(Value::Number(n)) => n.as_f64().ok_or_else(|| format!("invalid {key} value {n}")),
            Some(Value::String(s)) if s.is_empty() => Ok(default),
            Some(Value::String(s)) => s
                .parse()
                .map_err(|e| format!("invalid {key} value {s:?}: {e}")),
            Some(other) => Err(format!("invalid {key} value: unsupported type {other}")),
        }
    }

    pub fn int(&self, key: &str, default: i64) -> Result<i64, String> {
        match self.get(key) {
            None => Ok(default),
            Some(Value::Number(n)) => n
                .as_i64()
                .or_else(|| n.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64))
                .ok_or_else(|| format!("invalid {key} value: {n} is not an integer")),
            Some(Value::String(s)) if s.is_empty() => Ok(default),
            Some(Value::String(s)) => s
                .parse()
                .map_err(|e| format!("invalid {key} value {s:?}: {e}")),
            Some(other) => Err(format!("invalid {key} value: unsupported type {other}")),
        }
    }

    pub fn duration(&self, key: &str, default: Duration) -> Result<Duration, String> {
        let text = match self.get(key) {
            None => return Ok(default),
            Some(Value::String(s)) if s.is_empty() => return Ok(default),
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        };
        parse_duration(&text).map_err(|e| format!("invalid {key}: {e}"))
    }

    pub fn string_map(&self, key: &str) -> Result<BTreeMap<String, String>, String> {
        let object = match self.get(key) {
            None => return Ok(BTreeMap::new()),
            Some(Value::String(s)) if s.is_empty() => return Ok(BTreeMap::new()),
            Some(Value::String(s)) => serde_json::from_str::<serde_json::Map<String, Value>>(s)
                .map_err(|e| format!("failed to parse {key}: {e}"))?,
            Some(Value::Object(o)) => o.clone(),
            Some(other) => return Err(format!("unsupported type {other} for key {key:?}")),
        };
        Ok(object
            .into_iter()
            .map(|(k, v)| {
                let v = match v {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                (k, v)
            })
            .collect())
    }

    pub fn object(&self, key: &str) -> Result<GateParams, String> {
        match self.get(key) {
            None => Ok(GateParams::new()),
            Some(Value::String(s)) if s.is_empty() => Ok(GateParams::new()),
            Some(Value::String(s)) => {
                serde_json::from_str(s).map_err(|e| format!("failed to parse {key}: {e}"))
            }
            Some(Value::Object(o)) => Ok(o.clone()),
            Some(other) => Err(format!("unsupported type {other} for key {key:?}")),
        }
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<T, String> {
        let value = match self.get(key) {
            None => return Err(format!("requires a {key:?} parameter")),
            Some(Value::String(s)) => {
                serde_json::from_str(s).map_err(|e| format!("failed to parse {key}: {e}"))?
            }
            Some(v) => v.clone(),
        };
        serde_json::from_value(value).map_err(|e| format!("failed to parse {key}: {e}"))
    }

    pub fn gate(&self, key: &str) -> Result<NestedGate, String> {
        self.decode(key)
    }

    pub fn gates(&self, key: &str) -> Result<Vec<NestedGate>, String> {
        self.decode(key)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use crate::config::transport::GateParams;
    use crate::gate::factory::params::Params;

    fn params(v: serde_json::Value) -> GateParams {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn scalars_accept_numbers_and_strings() {
        let p = params(
            json!({"a": 1.5, "b": "2.5", "c": "", "d": 3, "e": "4", "f": 4.5, "g": true, "h": null}),
        );
        let p = Params(&p);
        assert_eq!(p.float("a", 0.0), Ok(1.5));
        assert_eq!(p.float("b", 0.0), Ok(2.5));
        assert_eq!(p.float("c", 9.0), Ok(9.0));
        assert_eq!(p.float("h", 9.0), Ok(9.0));
        assert_eq!(p.int("d", 0), Ok(3));
        assert_eq!(p.int("e", 0), Ok(4));
        assert!(p.int("f", 0).is_err());
        assert!(p.float("g", 0.0).is_err());
        assert_eq!(p.string("a", "x"), "1.5");
        assert_eq!(p.string("g", "x"), "true");
        assert_eq!(p.string("c", "x"), "x");
        assert_eq!(p.string("missing", "x"), "x");
    }

    #[test]
    fn durations_and_maps() {
        let p =
            params(json!({"w": "90s", "bad": "5", "m": {"a": 1, "b": "x"}, "s": "{\"k\":\"v\"}"}));
        let p = Params(&p);
        assert_eq!(p.duration("w", Duration::ZERO), Ok(Duration::from_secs(90)));
        assert_eq!(
            p.duration("none", Duration::from_secs(60)),
            Ok(Duration::from_secs(60))
        );
        assert!(p.duration("bad", Duration::ZERO).is_err());
        let m = p.string_map("m").unwrap();
        assert_eq!((m["a"].as_str(), m["b"].as_str()), ("1", "x"));
        assert_eq!(p.string_map("s").unwrap()["k"], "v");
    }

    #[test]
    fn nested_gates_inline_or_encoded() {
        let p = params(json!({
            "gate": {"gate_type": "constant"},
            "encoded": "{\"gate_type\":\"quota\",\"gate_params\":{\"limit\":2}}",
            "gates": [{"gate_type": "a"}, {"gate_type": "b", "gate_params": {}}],
            "typo": {"gate_typ": "a"}
        }));
        let p = Params(&p);
        assert_eq!(p.gate("gate").unwrap().gate_type, "constant");
        assert_eq!(p.gate("encoded").unwrap().gate_params["limit"], 2);
        assert_eq!(p.gates("gates").unwrap().len(), 2);
        assert!(p.gate("typo").is_err());
        assert!(p.gate("missing").unwrap_err().contains("requires"));
    }
}
