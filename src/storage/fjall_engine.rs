//! Fjall LSM-tree ingestion engine.
//!
//! # Key format
//!
//! Composite big-endian timestamp key, so lexicographic byte order ==
//! chronological order and expiry is a prefix-bounded range scan:
//!
//! ```text
//! {:016x}:{:08x}:<metric_name>   (micros since epoch : seq : name)
//! ```
//!
//! The 32-bit wrapping sequence disambiguates entries sharing the same
//! microsecond and metric name.
//!
//! # Value format
//!
//! JSON payload: `{"value": f64, "metric_type": "counter|gauge|histogram",
//! "labels": {k: v}}`.
//!
//! # Memory budget
//!
//! [`open_handles`] configures Fjall's unified cache
//! ([`FjallConfig::cache_size_bytes`](crate::FjallConfig::cache_size_bytes),
//! default 8 MiB) plus a per-partition memtable cap
//! ([`FjallConfig::memtable_bytes`](crate::FjallConfig::memtable_bytes),
//! default 2 MiB). KV separation stays off — values (~10² B) sit far below
//! the 1 KiB separation threshold, so everything stays inline.

use std::time::UNIX_EPOCH;

use fjall::{
    Config as FjallKeyspaceConfig, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode,
};
use tokio::sync::mpsc;

use crate::{
    config::FjallConfig,
    error::{Error, Result},
    types::{MetricEntry, PipelineMsg},
};

/// Partition holding all metric samples.
pub const PARTITION_METRICS: &str = "metrics";

/// Cloned (`Arc`-backed) handles to the single open Fjall keyspace.
///
/// Both [`Keyspace`] and [`PartitionHandle`] are cheap to clone and thread-safe;
/// open once in [`crate::init`] and share — never open the same directory twice.
#[derive(Clone)]
pub struct FjallHandles {
    /// The open keyspace (persistence, flushes).
    pub keyspace: Keyspace,
    /// The `metrics` partition (inserts, scans, removes).
    pub partition: PartitionHandle,
}

impl std::fmt::Debug for FjallHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallHandles")
            .field("partition", &PARTITION_METRICS)
            .finish_non_exhaustive()
    }
}

/// Open (creating if needed) the keyspace + `metrics` partition with strict
/// memory limits.
pub fn open_handles(cfg: &FjallConfig) -> Result<FjallHandles> {
    std::fs::create_dir_all(&cfg.dir)?;
    std::fs::create_dir_all(cfg.cold_dir())?;

    let keyspace = FjallKeyspaceConfig::new(&cfg.dir)
        .cache_size(cfg.cache_size_bytes)
        // Total write-buffer budget: 2x one memtable (active + flushing).
        .max_write_buffer_size(u64::from(cfg.memtable_bytes).saturating_mul(2))
        .open()
        .map_err(|e| Error::Storage(format!("fjall open {}: {e}", cfg.dir.display())))?;

    let partition = keyspace
        .open_partition(
            PARTITION_METRICS,
            PartitionCreateOptions::default().max_memtable_size(cfg.memtable_bytes),
        )
        .map_err(|e| Error::Storage(format!("fjall open partition: {e}")))?;

    Ok(FjallHandles {
        keyspace,
        partition,
    })
}

/// Encode a composite sort key: `{micros:016x}:{seq:08x}:{name}`.
#[must_use]
pub fn encode_key(ts_micros: u64, seq: u32, name: &str) -> Vec<u8> {
    format!("{ts_micros:016x}:{seq:08x}:{name}").into_bytes()
}

/// Upper-bound prefix for "older than `cutoff_micros`": every key with a
/// smaller timestamp sorts strictly below `<hex>:...`, and the `:` separator
/// (`0x3a`) sorts below hex digits, so `..prefix` excludes the cutoff itself.
#[must_use]
pub fn cutoff_prefix(cutoff_micros: u64) -> String {
    format!("{cutoff_micros:016x}")
}

/// Decode `(timestamp_micros, metric_name)` from a composite key.
#[must_use]
pub fn decode_key(key: &[u8]) -> Option<(u64, String)> {
    let mut parts = key.splitn(3, |b| *b == b':');
    let ts = std::str::from_utf8(parts.next()?).ok()?;
    let _seq = parts.next()?;
    let name = std::str::from_utf8(parts.next()?).ok()?;
    let ts_micros = u64::from_str_radix(ts, 16).ok()?;
    Some((ts_micros, name.to_string()))
}

/// Encode the JSON value payload for one observation.
#[must_use]
pub fn encode_value(entry: &MetricEntry) -> Vec<u8> {
    let labels: serde_json::Map<String, serde_json::Value> = entry
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::json!({
        "value": entry.value,
        "metric_type": entry.metric_type.as_str(),
        "labels": labels,
    })
    .to_string()
    .into_bytes()
}

/// Micros since the Unix epoch for an entry (saturates at 0 on clock skew).
#[must_use]
pub fn entry_micros(entry: &MetricEntry) -> u64 {
    entry
        .timestamp
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Dedicated writer-thread main loop.
///
/// First message uses `blocking_recv` (parks when idle — zero CPU spin), the
/// rest drains via `try_recv` up to `batch_rows` per burst. Fjall inserts are
/// synchronous, so this thread needs no Tokio runtime.
// `FjallHandles` is intentionally owned: it moves into the dedicated thread.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn writer_loop(
    mut rx: mpsc::Receiver<PipelineMsg>,
    handles: FjallHandles,
    batch_rows: usize,
) {
    tracing::info!("duckmetrics fjall writer thread started");
    let mut batch: Vec<MetricEntry> = Vec::with_capacity(batch_rows);
    let mut seq: u32 = 0;

    loop {
        batch.clear();
        match rx.blocking_recv() {
            Some(PipelineMsg::Metric(e)) => batch.push(e),
            Some(PipelineMsg::Shutdown) => {
                tracing::info!("writer: shutdown requested");
                break;
            }
            None => {
                tracing::info!("writer: channel closed");
                break;
            }
        }
        while batch.len() < batch_rows {
            match rx.try_recv() {
                Ok(PipelineMsg::Metric(e)) => batch.push(e),
                Ok(PipelineMsg::Shutdown) => {
                    flush_batch(&handles.partition, &batch, &mut seq);
                    persist_best_effort(&handles.keyspace);
                    tracing::info!("writer: shutdown requested (with backlog)");
                    return;
                }
                Err(_) => break,
            }
        }
        flush_batch(&handles.partition, &batch, &mut seq);
    }
    persist_best_effort(&handles.keyspace);
    tracing::info!("writer thread exited");
}

fn flush_batch(partition: &PartitionHandle, batch: &[MetricEntry], seq: &mut u32) {
    for entry in batch {
        let key = encode_key(entry_micros(entry), *seq, &entry.name);
        *seq = seq.wrapping_add(1);
        let value = encode_value(entry);
        if let Err(e) = partition.insert(key, value) {
            tracing::error!(error = %e, metric = %entry.name, "writer: fjall insert failed");
        }
    }
}

fn persist_best_effort(keyspace: &Keyspace) {
    if let Err(e) = keyspace.persist(PersistMode::SyncAll) {
        tracing::warn!(error = %e, "writer: final persist failed");
    }
}
