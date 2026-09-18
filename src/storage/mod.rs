//! Embedded Fjall LSM-tree backend: high-throughput, low-memory ingestion +
//! periodic Parquet cold-storage compaction.
//!
//! Layout:
//! * [`fjall_engine`] — keyspace lifecycle, composite key codec, writer loop.
//! * [`parquet_exporter`] — range-scan compaction into ZSTD Parquet + latest-file lookup.
//!
//! Threading contract (see `engine.rs`):
//! * Exactly one `Arc`-shared [`fjall_engine::FjallHandles`] is opened in
//!   [`crate::init`] and cloned into the writer thread and the compaction
//!   task. Never open the same directory twice — separate handles do not
//!   share snapshots.
//! * The writer thread performs synchronous Fjall inserts (no runtime needed).
//! * Compaction runs blocking scans/writes inside `spawn_blocking`.
//! * No `unsafe` in this module — Fjall and Parquet are 100% safe Rust.

/// On-disk/wire format version (key codec + value JSON + Parquet schema).
/// Bump on any breaking change and document it in `CHANGELOG.md`.
pub const SCHEMA_VERSION: u32 = 1;

pub mod fjall_engine;
pub mod parquet_exporter;

pub use fjall_engine::{
    FjallHandles, cutoff_prefix, decode_key, encode_key, encode_value, entry_micros, open_handles,
};
pub use parquet_exporter::{
    PARQUET_FILE_PREFIX, arrow_schema, latest_cold_file, run_compaction_once_blocking,
};
