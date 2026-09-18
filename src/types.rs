//! Core metric primitives shared by every backend.
//!
//! [`MetricEntry`] is the single unit flowing through the non-blocking
//! pipeline: hot-path macros build one, [`crate::engine`] dispatches it to
//! the Prometheus registry (lock-free atomics) and to the Fjall writer
//! thread (bounded `mpsc` + drop counter).

use std::time::SystemTime;

/// Metric flavour. Serialized into the Fjall value payload's `metric_type`
/// field and used to pick the matching Prometheus collector
/// (`CounterVec` / `GaugeVec` / `HistogramVec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Monotonically increasing value (`inc_by` semantics).
    Counter,
    /// Arbitrarily settable value.
    Gauge,
    /// Sampled observation (Prometheus histogram + raw sample in Fjall).
    Histogram,
}

impl MetricType {
    /// Canonical lowercase identifier stored in the Fjall value payload
    /// (`counter|gauge|histogram`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MetricType {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            other => Err(crate::Error::InvalidConfig(format!(
                "unknown metric type `{other}`, expected counter|gauge|histogram"
            ))),
        }
    }
}

/// A single metric observation.
#[derive(Debug, Clone)]
pub struct MetricEntry {
    /// Capture time (`SystemTime` keeps core free of `chrono`; storage
    /// converts to micros-since-epoch keys only when `fjall-backend` is enabled).
    pub timestamp: SystemTime,
    /// Sanitized Prometheus-compatible metric name.
    pub name: String,
    /// Sample value.
    pub value: f64,
    /// Flavour selecting the Prometheus collector.
    pub metric_type: MetricType,
    /// Ordered `(key, value)` label pairs (already sanitized/truncated).
    pub labels: Vec<(String, String)>,
}

impl MetricEntry {
    /// Build an entry, sanitizing name/labels and stamping `now`.
    #[must_use]
    pub fn new(
        name: &str,
        value: f64,
        metric_type: MetricType,
        labels: Vec<(String, String)>,
    ) -> Self {
        Self {
            timestamp: SystemTime::now(),
            name: sanitize_metric_name(name),
            value,
            metric_type,
            labels: sanitize_labels(labels),
        }
    }
}

/// Internal pipeline message. `Shutdown` lets [`crate::shutdown`] flush and
/// join the writer thread deterministically.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum PipelineMsg {
    Metric(MetricEntry),
    Shutdown,
}

/// Prometheus exposition requires
/// `[a-zA-Z_:][a-zA-Z0-9_:]*`. Anything else becomes `_`; an empty or
/// digit-leading name gets an underscore prefix.
#[must_use]
pub fn sanitize_metric_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        let ok = if i == 0 {
            c.is_ascii_alphabetic() || c == '_' || c == ':'
        } else {
            c.is_ascii_alphanumeric() || c == '_' || c == ':'
        };
        out.push(if ok { c } else { '_' });
    }
    if out.is_empty() {
        out.push_str("_unnamed");
    }
    // Avoid colliding with the internal drop-counter unless intended.
    out
}

/// Sanitize label keys like metric names, truncate keys to 128 chars and
/// values to 256 chars, drop empty keys, cap at 16 pairs to bound
/// cardinality/memory per observation.
#[must_use]
pub fn sanitize_labels(labels: Vec<(String, String)>) -> Vec<(String, String)> {
    const MAX_PAIRS: usize = 16;
    const MAX_KEY: usize = 128;
    const MAX_VAL: usize = 256;

    labels
        .into_iter()
        .filter_map(|(k, v)| {
            let mut key = sanitize_metric_name(k.trim());
            if key.is_empty() || key == "_unnamed" && k.trim().is_empty() {
                return None;
            }
            if key.len() > MAX_KEY {
                key.truncate(MAX_KEY);
            }
            let mut val = v;
            if val.len() > MAX_VAL {
                val.truncate(MAX_VAL);
            }
            Some((key, val))
        })
        .take(MAX_PAIRS)
        .collect()
}

/// Split borrowed label pairs into the `&[&str]` slices the `prometheus` crate
/// expects, preserving declaration order.
#[must_use]
pub fn label_names_and_values(labels: &[(String, String)]) -> (Vec<String>, Vec<String>) {
    labels.iter().cloned().unzip()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_names() {
        assert_eq!(
            sanitize_metric_name("http.requests-total"),
            "http_requests_total"
        );
        assert_eq!(sanitize_metric_name("9lives"), "_lives");
        assert_eq!(sanitize_metric_name(""), "_unnamed");
    }

    #[test]
    fn truncates_and_caps_labels() {
        let labels = vec![("k".to_string(), "x".repeat(300))];
        let out = sanitize_labels(labels);
        assert_eq!(out[0].1.len(), 256);
    }
}
