//! Fjall ingestion + Parquet compaction roundtrips (requires `fjall-backend`).

#![cfg(feature = "fjall-backend")]

use std::time::Duration;

use tonggeret::{
    MetricEntry, MetricType,
    config::FjallConfig,
    storage::{
        decode_key, encode_key, encode_value, latest_cold_file, run_compaction_once_blocking,
    },
};

fn test_config(dir: &std::path::Path) -> FjallConfig {
    FjallConfig {
        dir: dir.join("fjall"),
        cache_size_bytes: 8 * 1024 * 1024,
        memtable_bytes: 1024 * 1024,
        cold_storage_dir: Some(dir.to_path_buf()),
        enable_compaction: true,
        compaction_interval: Duration::from_secs(3600),
        retention: Duration::from_secs(24 * 3600),
        batch_rows: 100,
    }
}

#[test]
fn key_codec_roundtrips_and_orders_chronologically() {
    let old = encode_key(1_000, 7, "http_requests_total");
    let new = encode_key(2_000, 3, "http_requests_total");
    assert!(old < new, "byte order must match chronological order");

    let (ts, name) = decode_key(&new).expect("decodable");
    assert_eq!(ts, 2_000);
    assert_eq!(name, "http_requests_total");

    // Sequence disambiguates identical timestamps.
    let a = encode_key(5_000, 0, "m");
    let b = encode_key(5_000, 1, "m");
    assert_ne!(a, b);
    assert!(a < b);
    assert!(decode_key(b"not-a-key").is_none());
}

#[test]
fn default_memory_budget_stays_under_10mb() {
    let budget = FjallConfig::default().memory_budget_bytes();
    assert!(
        budget <= 10 * 1024 * 1024,
        "budget {budget} exceeds 10 MiB target"
    );
}

#[test]
fn insert_scan_compact_purge_and_read_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path());

    let handles = tonggeret::storage::fjall_engine::open_handles(&cfg).unwrap();

    // One fresh row + one stale row (epoch micros ⇒ older than any retention).
    let fresh = MetricEntry::new("fresh_total", 1.0, MetricType::Counter, vec![]);
    let stale = MetricEntry::new("stale_total", 2.0, MetricType::Counter, vec![]);
    let now_micros = tonggeret::storage::fjall_engine::entry_micros(&fresh);
    handles
        .partition
        .insert(encode_key(now_micros, 0, &fresh.name), encode_value(&fresh))
        .unwrap();
    let stale_key = encode_key(0, 0, &stale.name);
    handles
        .partition
        .insert(stale_key.clone(), encode_value(&stale))
        .unwrap();

    let moved = run_compaction_once_blocking(&cfg, &handles).unwrap();
    assert_eq!(moved, 1, "expected exactly the stale row to move");

    // Exported key purged, fresh key retained.
    assert!(handles.partition.get(&stale_key).unwrap().is_none());
    let fresh_key = encode_key(now_micros, 0, &fresh.name);
    assert!(handles.partition.get(&fresh_key).unwrap().is_some());

    // A cold Parquet file exists and reads back with the contract schema.
    let cold = latest_cold_file(&cfg.cold_dir()).expect("cold parquet file");
    let file = std::fs::File::open(&cold).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let fields: Vec<&str> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(fields, ["ts", "name", "value", "metric_type", "labels"]);
    let reader = builder.build().unwrap();
    let mut rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "stale_total");
    }
    assert_eq!(rows, 1);
}
