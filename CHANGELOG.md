# Changelog

All notable changes to `duckmetrics` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.2.0] - 2026-09-18

### Changed (breaking)
- Storage backend swapped from DuckDB to **Fjall LSM-tree** (`fjall-backend`
  replaces `duckdb-backend`; pure Rust, no C toolchain, <10 MiB RAM target).
- `DuckDbConfig` → `FjallConfig` (`dir`, `cache_size_bytes`,
  `memtable_bytes`, `retention`, `compaction_interval`, `batch_rows`);
  `Config::duckdb` → `Config::fjall`, `with_duckdb` → `with_fjall`,
  `EngineHandle::has_duckdb` → `has_fjall`.
- Hot store is now composite-key KV (`{micros:016x}:{seq:08x}:{name}` →
  JSON payload) instead of SQL rows; cold storage is date-stamped ZSTD
  Parquet (`metrics_cold_*.parquet`, Arrow contract in README) with exported
  keys purged from Fjall.

### Added
- `GET /telemetry/parquet` Axum route helper (`parquet_route`) serving the
  newest cold export with HTTP Range-Request support.
- `SCHEMA_VERSION` export; `FjallConfig::memory_budget_bytes()`.

## [0.1.0] - 2026-09-17

### Added
- Dual-mode engine: `OnceLock` global, bounded `mpsc` channel (default 16_384) + drop counter, dedicated DuckDB writer OS thread with C-API `Appender` batching.
- Prometheus sync: lazy `CounterVec` / `GaugeVec` / `HistogramVec` registry, `duckmetrics_dropped_total`, text exposition.
- DuckDB storage: WAL + `threads=2` + memory-limit pragmas, `metrics(ts, name, value, metric_type, labels JSON)` schema, hourly compaction of `>24h` rows into ZSTD Parquet + `metrics_all` unified view.
- Middleware: Axum `from_fn` tracker + `/metrics` handler; Actix-web `Transform` + handler (same series: `http_requests_total`, `http_request_duration_ms`).
- Macros `counter!` / `gauge!` / `histogram!` (`ident = value` and `"key" => value` forms), direct `record_*` functions, `thiserror` errors, `///` docs + doctests.
- Examples: `axum_server.rs`, `embedded_dashboard.html` (DuckDB-WASM), integration tests, README.
