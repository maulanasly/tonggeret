//! Macros must be silent no-ops before `init` (libraries emit unconditionally).

#[test]
fn macros_noop_before_init() {
    duckmetrics::counter!("noop_total", 1.0);
    duckmetrics::counter!("noop_total", 2.0, method = "GET");
    duckmetrics::gauge!("noop_gauge", 3.0, worker = "a");
    duckmetrics::histogram!("noop_hist_ms", 12.5, table = "t");
    duckmetrics::counter!("noop_total", 1.0, "method" => "POST");

    assert!(!duckmetrics::is_initialized() || duckmetrics::handle().is_some());
}
