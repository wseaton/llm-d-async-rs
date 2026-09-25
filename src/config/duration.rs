use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurationError {
    #[error("invalid duration {0:?}")]
    Invalid(String),
    #[error("unknown unit {unit:?} in duration {input:?}")]
    UnknownUnit { input: String, unit: String },
    #[error("duration {0:?} must not be negative")]
    Negative(String),
    #[error("duration {0:?} overflows")]
    Overflow(String),
}

/// Parses Go's `time.ParseDuration` syntax ("1h30m", "1.5s", "250ms", "0"),
/// so flag values and gate params written for the Go processor still load.
pub fn parse_duration(input: &str) -> Result<Duration, DurationError> {
    let invalid = || DurationError::Invalid(input.to_owned());
    let mut s = input;
    if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    } else if s.starts_with('-') {
        return Err(DurationError::Negative(input.to_owned()));
    }
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.is_empty() {
        return Err(invalid());
    }

    let mut total_nanos: f64 = 0.0;
    while !s.is_empty() {
        let number_len = s
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(invalid)?;
        let (number, rest) = s.split_at(number_len);
        if number.is_empty() || number == "." || number.matches('.').count() > 1 {
            return Err(invalid());
        }
        let value: f64 = number.parse().map_err(|_| invalid())?;
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let (unit, rest) = rest.split_at(unit_len);
        let scale = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => {
                return Err(DurationError::UnknownUnit {
                    input: input.to_owned(),
                    unit: unit.to_owned(),
                });
            }
        };
        total_nanos += value * scale;
        s = rest;
    }
    if !total_nanos.is_finite() || total_nanos > u64::MAX as f64 {
        return Err(DurationError::Overflow(input.to_owned()));
    }
    Ok(Duration::from_nanos(total_nanos.round() as u64))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::duration::{DurationError, parse_duration};

    #[test]
    fn parses_go_durations() {
        let cases = [
            ("0", Duration::ZERO),
            ("0s", Duration::ZERO),
            ("5s", Duration::from_secs(5)),
            ("+5s", Duration::from_secs(5)),
            ("1.5s", Duration::from_millis(1500)),
            ("250ms", Duration::from_millis(250)),
            ("1h30m", Duration::from_secs(5400)),
            ("2m3s4ms", Duration::from_millis(123_004)),
            ("10us", Duration::from_micros(10)),
            ("10µs", Duration::from_micros(10)),
            ("7ns", Duration::from_nanos(7)),
            (".5h", Duration::from_secs(1800)),
        ];
        for (input, want) in cases {
            assert_eq!(parse_duration(input), Ok(want), "{input}");
        }
    }

    #[test]
    fn rejects_bad_durations() {
        for input in ["", "5", "s", "1..5s", "abc", "5 s", "."] {
            assert!(parse_duration(input).is_err(), "{input}");
        }
        assert_eq!(
            parse_duration("5d"),
            Err(DurationError::UnknownUnit {
                input: "5d".into(),
                unit: "d".into()
            })
        );
        assert_eq!(
            parse_duration("-5s"),
            Err(DurationError::Negative("-5s".into()))
        );
    }
}
