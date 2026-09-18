# Changelog

All notable changes to `duckmetrics` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.1.0] - 2026-09-18

> Restart: prior 0.1.0/0.2.0 DuckDB → Fjall iteration was discarded and
> history rewritten. This is the first release of the restarted line.

### Added
- Dual-mode engine: `OnceLock` global, bounded `mpsc` channel (default 16_384) + drop counter, dedicated Fjall writer OS thread with batched inserts.
- Prometheus sync: lazy `CounterVec` / `GaugeVec` / `HistogramVec` registry, `duckmetrics_dropped_total`, text exposition.
- Embedded Fjall LSM-tree backend (`fjall-backend`): composite-key hot store (`{micros:016x}:{seq:08x}:{name}` → JSON payload), 8 MiB cache + 2 MiB memtable (<10 MiB steady-state), hourly compaction of keys older than `retention` (default 24h) into ZSTD Parquet (`metrics_cold_*.parquet`) with rename-then-purge ordering.
- `GET /telemetry/parquet` Axum route helper (`parquet_route`) serving the newest cold export with HTTP Range-Request support.
- Middleware: Axum `from_fn` tracker + `/metrics` handler; Actix-web `Transform` + handler (same series: `http_requests_total`, `http_request_duration_ms`).
- Macros `counter!` / `gauge!` / `histogram!` (`ident = value` and `"key" => value` forms), direct `record_*` functions, `thiserror` errors, `///` docs + doctests.
- `SCHEMA_VERSION` export; `FjallConfig::memory_budget_bytes()`.
- Examples: `axum_server.rs`, `embedded_dashboard.html` (DuckDB-WASM), integration tests.
