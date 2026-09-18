# Changelog

All notable changes to `tonggeret` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.0] - 2026-09-18

### Added
- Visitor tracking (`visitors` module + `track_visitors` Axum middleware /
  `TrackVisitors` Actix middleware): `visitors_total{region}` counter and
  `unique_visitors_estimate{region}` gauge via a std-only HyperLogLog
  (~4 KiB/region, ~1.6% error). Regions come from CDN country headers;
  visitor keys are hashed, never stored.

## [0.2.0] - 2026-09-18

### Changed (breaking)
- `axum` integration (`axum` feature, `middleware::axum`, example) now
  requires Axum 0.8 (`axum 0.7.9` → `0.8.9`, `axum-core 0.4` → `0.5`).
  No source changes were needed: `track`, `prometheus_handler`, and
  `parquet_route` use APIs identical in 0.8 (verified: full test suite +
  example smoke test, including bare `/orders` with no query string).
  Axum 0.7 users should pin `tonggeret 0.1.x`.

## [0.1.1] - 2026-09-18

### Fixed
- CI `audit` job: replaced `rustsec/audit-check@v2` (Node20 bundle →
  deprecation warning; Checks API → `Resource not accessible by
  integration` on fork PRs) with direct `cargo install cargo-audit` +
  `cargo audit` (no Node runtime, fork-safe, gated instead of
  `continue-on-error`). Policy lives in `.cargo/audit.toml`.
- Bumped `actions/checkout@v4` → `@v5` in all jobs (`v4` is also a Node20
  action; `v5` runs on Node24). `dtolnay/rust-toolchain` is composite
  (no Node runtime) and `Swatinem/rust-cache@v2` already targets Node24,
  so no Node20 warnings should remain.
- Fixed `RUSTSEC-2024-0437` (`protobuf 2.28` → `3.7.2`) via
  `prometheus 0.13` → `0.14` (no API change for the registry usage here).
- Documented time-boxed ignores in `.cargo/audit.toml` for the two
  advisories with no MSRV-compatible fix, both reachable only via the
  optional `actix` feature: `RUSTSEC-2026-0258` (`h2 0.3`, no fixed
  `0.3.x` line; revisit when `actix-http` migrates to `h2 0.4`) and
  `RUSTSEC-2026-0009` (`time 0.3.45`; fix `>=0.3.47` needs Rust 1.88 >
  MSRV 1.85; revisit on MSRV bump). `SCHEMA_VERSION` unchanged (1).

## [0.1.0] - 2026-09-18

> Restart: prior 0.1.0/0.2.0 DuckDB → Fjall iteration was discarded and
> history rewritten. This is the first release of the restarted line.

### Added
- Dual-mode engine: `OnceLock` global, bounded `mpsc` channel (default 16_384) + drop counter, dedicated Fjall writer OS thread with batched inserts.
- Prometheus sync: lazy `CounterVec` / `GaugeVec` / `HistogramVec` registry, `tonggeret_dropped_total`, text exposition.
- Embedded Fjall LSM-tree backend (`fjall-backend`): composite-key hot store (`{micros:016x}:{seq:08x}:{name}` → JSON payload), 8 MiB cache + 2 MiB memtable (<10 MiB steady-state), hourly compaction of keys older than `retention` (default 24h) into ZSTD Parquet (`metrics_cold_*.parquet`) with rename-then-purge ordering.
- `GET /telemetry/parquet` Axum route helper (`parquet_route`) serving the newest cold export with HTTP Range-Request support.
- Middleware: Axum `from_fn` tracker + `/metrics` handler; Actix-web `Transform` + handler (same series: `http_requests_total`, `http_request_duration_ms`).
- Macros `counter!` / `gauge!` / `histogram!` (`ident = value` and `"key" => value` forms), direct `record_*` functions, `thiserror` errors, `///` docs + doctests.
- `SCHEMA_VERSION` export; `FjallConfig::memory_budget_bytes()`.
- Examples: `axum_server.rs`, `embedded_dashboard.html` (DuckDB-WASM), integration tests.
