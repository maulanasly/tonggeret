//! Visitor tracking integration: observe → snapshot → scrape text contains series.
//!
//! Each `tests/*.rs` binary is its own process, so one global `init` per file.
//! Region codes below use the `Q*` block to avoid colliding with any other
//! test process sharing nothing (and with real country codes).

#![cfg(feature = "prometheus-exporter")]

use tonggeret::{
    Config,
    visitors::{REGION_UNKNOWN, observe_visitor, parse_region, snapshot_visitors},
};

fn ensure_init() {
    let _ = tonggeret::init(Config::default_light());
}

#[test]
fn totals_and_unique_estimates_exported_per_region() {
    ensure_init();

    assert_eq!(parse_region(Some("qx"), None, None), "QX");
    for i in 0..50 {
        assert!(observe_visitor(&format!("qx-visitor-{i}"), "QX"));
        assert!(observe_visitor(&format!("qz-visitor-{i}"), "QZ"));
    }
    // Duplicates must not inflate uniques.
    for _ in 0..10 {
        assert!(observe_visitor("qx-visitor-0", "QX"));
    }
    // Invalid region normalizes to unknown.
    assert!(observe_visitor("anon-0", "!!!"));

    let regions = snapshot_visitors();
    assert!(regions >= 3, "expected QX, QZ, unknown; got {regions}");

    let text = tonggeret::prometheus_text().expect("gather");
    assert!(text.contains("visitors_total"), "missing totals:\n{text}");
    assert!(
        text.contains("unique_visitors_estimate"),
        "missing estimates:\n{text}"
    );
    assert!(text.contains(r#"region="QX""#), "missing QX:\n{text}");
    assert!(text.contains(r#"region="QZ""#), "missing QZ:\n{text}");
    assert!(
        text.contains(&format!(r#"region="{REGION_UNKNOWN}""#)),
        "missing unknown:\n{text}"
    );
}
