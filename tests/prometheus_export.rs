//! Prometheus-only integration: init → record → scrape text contains series.
//!
//! Each `tests/*.rs` binary is its own process, so one global `init` per file.

#![cfg(feature = "prometheus-exporter")]

use tonggeret::Config;

fn ensure_init() {
    let _ = tonggeret::init(Config::default_light());
}

#[test]
fn counter_gauge_histogram_exported() {
    ensure_init();

    tonggeret::counter!("it_requests_total", 3.0, method = "GET", path = "/");
    tonggeret::gauge!("it_queue_depth", 9.0);
    tonggeret::histogram!("it_latency_ms", 42.0, route = "/");

    let text = tonggeret::prometheus_text().expect("gather");
    assert!(
        text.contains("it_requests_total"),
        "missing counter:\n{text}"
    );
    assert!(text.contains("it_queue_depth"), "missing gauge:\n{text}");
    assert!(text.contains("it_latency_ms"), "missing histogram:\n{text}");
    // Internal drop counter is always registered.
    assert!(
        text.contains("tonggeret_dropped_total"),
        "missing drop counter:\n{text}"
    );
}

#[test]
fn direct_api_matches_macros() {
    ensure_init();
    tonggeret::record_counter(
        "it_direct_total",
        1.0,
        vec![("a".to_string(), "b".to_string())],
    );
    let text = tonggeret::prometheus_text().unwrap();
    assert!(text.contains("it_direct_total"));
}
