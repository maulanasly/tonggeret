//! Parquet compaction: range-scan expired Fjall keys into ZSTD Parquet files.
//!
//! # Export schema (Arrow / Parquet consumption contract)
//!
//! | Column | Arrow type | Source |
//! |--------|-----------|--------|
//! | `ts` | `Timestamp(Microsecond)` | composite key (authoritative) |
//! | `name` | `Utf8` | composite key |
//! | `value` | `Float64` | value JSON `value` |
//! | `metric_type` | `Utf8` | value JSON `metric_type` |
//! | `labels` | `Utf8` (JSON object) | value JSON `labels` |
//!
//! Files are date-stamped (`metrics_cold_<UTC %Y%m%dT%H%M%S>.parquet`),
//! ZSTD-compressed, written to a temp file + atomically renamed. Exported
//! keys are removed from Fjall afterwards (at-least-once cold delivery: a
//! crash between Parquet rename and key removal re-exports those rows on the
//! next pass).
//!
//! Memory is chunk-bounded: at most `ROWS_PER_BATCH` rows plus their keys are
//! held at once, regardless of how many keys expired.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayBuilder as _, Float64Builder, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use fjall::PersistMode;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::{
    config::FjallConfig,
    error::{Error, Result},
    storage::fjall_engine::{FjallHandles, cutoff_prefix, decode_key},
};

/// Filename prefix for cold exports (`<prefix><UTC stamp>.parquet`).
pub const PARQUET_FILE_PREFIX: &str = "metrics_cold_";

/// Rows buffered per Arrow `RecordBatch` (bounds compaction memory).
const ROWS_PER_BATCH: usize = 5_000;

/// Arrow schema of every cold export (see module docs).
#[must_use]
pub fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("labels", DataType::Utf8, false),
    ]))
}

/// Newest `metrics_cold_*.parquet` in `cold_dir`, if any.
#[must_use]
pub fn latest_cold_file(cold_dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(cold_dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(PARQUET_FILE_PREFIX))
                && p.extension().is_some_and(|x| x == "parquet")
        })
        .max()
}

/// Run a single hot→cold pass synchronously. Returns rows exported.
pub fn run_compaction_once_blocking(cfg: &FjallConfig, handles: &FjallHandles) -> Result<u64> {
    let retention = chrono::Duration::from_std(cfg.retention)
        .map_err(|e| Error::Storage(format!("invalid retention: {e}")))?;
    let cutoff_micros =
        u64::try_from((chrono::Utc::now() - retention).timestamp_micros()).unwrap_or(0);
    let bound = cutoff_prefix(cutoff_micros);

    std::fs::create_dir_all(cfg.cold_dir())?;

    let schema = arrow_schema();
    let mut export: Option<(PathBuf, ArrowWriter<File>)> = None;
    let mut pending = PendingBatch::new();
    let mut moved: u64 = 0;

    for item in handles.partition.range(..bound) {
        let (key, value) = item.map_err(|e| Error::Storage(format!("fjall scan: {e}")))?;
        let key_bytes = key.to_vec();
        let Some((row_ts, row_name)) = decode_key(&key_bytes) else {
            tracing::debug!("compaction: dropping undecodable key");
            pending.removals.push(key_bytes);
            continue;
        };
        let (row_value, row_kind, row_labels) = decode_value(&value);

        if export.is_none() {
            export = Some(open_export_writer(cfg, &schema)?);
        }
        pending.push(row_ts, row_name, row_value, row_kind, row_labels, key_bytes);

        if pending.len() >= ROWS_PER_BATCH {
            moved += pending.flush(&mut export, &handles.partition)?;
        }
    }
    moved += pending.flush(&mut export, &handles.partition)?;

    if let Some((tmp, writer)) = export {
        finalize_export(cfg, &tmp, writer)?;
        if let Err(e) = handles.keyspace.persist(PersistMode::SyncAll) {
            tracing::warn!(error = %e, "compaction: persist failed");
        }
    }
    Ok(moved)
}

/// Chunk-bounded column builders plus the keys to remove once flushed.
struct PendingBatch {
    ts: TimestampMicrosecondBuilder,
    names: StringBuilder,
    values: Float64Builder,
    kinds: StringBuilder,
    labels: StringBuilder,
    removals: Vec<Vec<u8>>,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            ts: TimestampMicrosecondBuilder::new(),
            names: StringBuilder::new(),
            values: Float64Builder::new(),
            kinds: StringBuilder::new(),
            labels: StringBuilder::new(),
            removals: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.ts.len()
    }

    fn push(
        &mut self,
        row_ts: u64,
        name: String,
        value: f64,
        kind: String,
        labels: String,
        key: Vec<u8>,
    ) {
        self.ts
            .append_value(i64::try_from(row_ts).unwrap_or(i64::MAX));
        self.names.append_value(name);
        self.values.append_value(value);
        self.kinds.append_value(kind);
        self.labels.append_value(labels);
        self.removals.push(key);
    }

    /// Write buffered rows as one `RecordBatch` and remove their keys.
    /// Returns rows exported (0 when empty).
    fn flush(
        &mut self,
        export: &mut Option<(PathBuf, ArrowWriter<File>)>,
        partition: &fjall::PartitionHandle,
    ) -> Result<u64> {
        if self.len() == 0 {
            self.removals.clear();
            return Ok(0);
        }
        let batch = RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(self.ts.finish()),
                Arc::new(self.names.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.kinds.finish()),
                Arc::new(self.labels.finish()),
            ],
        )
        .map_err(|e| Error::Storage(format!("arrow batch: {e}")))?;
        let Some((_, writer)) = export else {
            return Err(Error::Storage("parquet writer missing".to_string()));
        };
        writer
            .write(&batch)
            .map_err(|e| Error::Storage(format!("parquet write: {e}")))?;
        let n = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        for key in self.removals.drain(..) {
            if let Err(e) = partition.remove(key) {
                tracing::warn!(error = %e, "compaction: key removal failed");
            }
        }
        Ok(n)
    }
}

/// Create the temp-file ZSTD `ArrowWriter` for one export pass.
fn open_export_writer(
    cfg: &FjallConfig,
    schema: &Arc<Schema>,
) -> Result<(PathBuf, ArrowWriter<File>)> {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
    let tmp = cfg
        .cold_dir()
        .join(format!(".{PARQUET_FILE_PREFIX}{stamp}.parquet.tmp"));
    let file =
        File::create(&tmp).map_err(|e| Error::Storage(format!("create tmp parquet: {e}")))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let writer = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
        .map_err(|e| Error::Storage(format!("parquet writer: {e}")))?;
    Ok((tmp, writer))
}

/// Close the writer and atomically publish the export.
fn finalize_export(cfg: &FjallConfig, tmp: &Path, writer: ArrowWriter<File>) -> Result<()> {
    let _ = writer
        .close()
        .map_err(|e| Error::Storage(format!("parquet close: {e}")))?;
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
    let final_path = cfg
        .cold_dir()
        .join(format!("{PARQUET_FILE_PREFIX}{stamp}.parquet"));
    std::fs::rename(tmp, &final_path)
        .map_err(|e| Error::Storage(format!("parquet rename: {e}")))?;
    Ok(())
}

/// Decode `(value, metric_type, labels_json)` from a value payload.
fn decode_value(raw: &[u8]) -> (f64, String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return (0.0, "unknown".to_string(), "{}".to_string());
    };
    let value = v
        .get("value")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    let kind = v
        .get("metric_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let labels = v.get("labels").map_or_else(
        || "{}".to_string(),
        |l| {
            if l.is_object() {
                l.to_string()
            } else {
                "{}".to_string()
            }
        },
    );
    (value, kind, labels)
}

/// Tokio background loop. Sleeps `compaction_interval`, then runs one pass in
/// `spawn_blocking` (Fjall scans + Parquet writes are blocking).
pub(crate) async fn compaction_loop(cfg: FjallConfig, handles: FjallHandles) {
    if !cfg.enable_compaction {
        tracing::info!("compaction disabled");
        return;
    }
    tracing::info!(interval = ?cfg.compaction_interval, "compaction task started");
    loop {
        tokio::time::sleep(cfg.compaction_interval).await;
        let cfg_clone = cfg.clone();
        let handles_clone = handles.clone();
        let res = tokio::task::spawn_blocking(move || {
            run_compaction_once_blocking(&cfg_clone, &handles_clone)
        })
        .await;
        match res {
            Ok(Ok(moved)) => {
                if moved > 0 {
                    tracing::info!(moved, "compaction pass completed");
                }
            }
            Ok(Err(e)) => tracing::error!(error = %e, "compaction pass failed"),
            Err(e) => tracing::error!(error = %e, "compaction task join failed"),
        }
    }
}
