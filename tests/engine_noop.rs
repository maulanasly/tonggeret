//! Macros must be silent no-ops before `init` (libraries emit unconditionally).

#[test]
fn macros_noop_before_init() {
    tonggeret::counter!("noop_total", 1.0);
    tonggeret::counter!("noop_total", 2.0, method = "GET");
    tonggeret::gauge!("noop_gauge", 3.0, worker = "a");
    tonggeret::histogram!("noop_hist_ms", 12.5, table = "t");
    tonggeret::counter!("noop_total", 1.0, "method" => "POST");

    assert!(!tonggeret::is_initialized() || tonggeret::handle().is_some());
}
