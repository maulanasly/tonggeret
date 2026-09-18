# duckmetrics

Ultra-low-memory Rust telemetry: **embedded Fjall LSM-tree** (<10 MiB RAM footprint) for high-throughput local ingestion, standard **Prometheus exposition**, and periodic **Parquet cold storage** — behind a non-blocking, lock-free hot path.

* Dual-mode dispatch: every `counter!` / `gauge!` / `histogram!` updates Prometheus atomics **and** streams to Fjall via a bounded channel → dedicated writer thread → LSM memtable.
* Pure Rust, no SQL engine, no C++ toolchain: `fjall` + `parquet` + `arrow` only.
* Plug-and-play Axum (`from_fn`) and Actix-web (`Transform`) middleware recording `http_requests_total` + `http_request_duration_ms`.

```rust
duckmetrics::init(duckmetrics::Config::default_light())?; // or ::default_full("./data/fjall")

duckmetrics::counter!("orders_total", 1.0, method = "POST", route = "/orders");
duckmetrics::gauge!("queue_depth", 42.0);
duckmetrics::histogram!("db_query_ms", 12.4, table = "orders");
```

## Installation

```toml
# Prometheus + Axum (defaults, fast build)
duckmetrics = "0.2"

# Dual-mode with embedded Fjall + Parquet (pure Rust, no system deps)
duckmetrics = { version = "0.2", features = ["fjall-backend"] }

# Actix-web instead of / in addition to Axum
duckmetrics = { version = "0.2", default-features = false, features = ["actix", "prometheus-exporter"] }
```

Requires Rust **1.85+** (edition 2024). License: **MIT**.

## Feature flags

| Flag | Enables | Default |
|------|---------|---------|
| `prometheus-exporter` | In-memory registry + text exposition | ✅ |
| `fjall-backend` | Fjall LSM-tree ingestion + Parquet compaction (`fjall`, `parquet`, `arrow`, `chrono`) | ❌ |
| `axum` | `middleware::axum::track` + `/metrics` + `/telemetry/parquet` route | ✅ |
| `actix` | `middleware::actix::DuckMetrics` + scrape handler | ❌ |

## Quickstart — Axum

```rust
use axum::{Router, routing::get, middleware};
use duckmetrics::middleware::axum as dm_axum;

duckmetrics::init(duckmetrics::Config::default_full("./data/fjall"))?;

let app = Router::new()
    .route("/", get(|| async { "hi" }))
    .route("/metrics", get(dm_axum::prometheus_handler))
    .merge(dm_axum::parquet_route("./data".into())) // GET /telemetry/parquet (Range-capable)
    .layer(middleware::from_fn(dm_axum::track));
```

Full runnable server: [`examples/axum_server.rs`](examples/axum_server.rs)
(`cargo run --example axum_server [--features fjall-backend]`).

Actix-web:

```rust
use duckmetrics::middleware::actix::{DuckMetrics, prometheus_handler};
let app = actix_web::App::new()
    .wrap(DuckMetrics)
    .route("/metrics", actix_web::web::get().to(prometheus_handler));
```

## Configuration

```rust
let mut fjall = duckmetrics::FjallConfig::new("./data/fjall");
fjall.cache_size_bytes = 8 * 1024 * 1024;  // unified block cache (default 8 MiB)
fjall.memtable_bytes = 2 * 1024 * 1024;    // per-partition memtable cap (default 2 MiB)
fjall.retention = std::time::Duration::from_secs(24 * 3600);
fjall.compaction_interval = std::time::Duration::from_secs(3600);
// fjall.cold_storage_dir = Some("./data/cold".into());
let cfg = duckmetrics::Config::default_light()
    .with_channel_capacity(16_384)
    .with_fjall(fjall);
duckmetrics::init(cfg)?;
```

* Channel full ⇒ sample dropped for Fjall, still recorded in Prometheus, `duckmetrics_dropped_total` incremented. Never blocks serving threads.
* Steady-state RAM ≈ `cache_size_bytes` + `memtable_bytes` (<10 MiB by default) plus transient batch/compaction buffers.
* If `init` runs before the Tokio runtime exists, call `duckmetrics::spawn_compaction_task(&fjall_cfg)` once inside the runtime.
* `duckmetrics::shutdown()` persists + joins the writer at process exit.

## Storage schema specification

### Fjall key format (hot store)

Composite big-endian timestamp key — lexicographic byte order **is** chronological order, so expiry is a prefix-bounded range scan:

```text
{:016x}:{:08x}:<metric_name>   (micros since epoch : seq : name)
```

* 16 lowercase hex digits, microseconds since epoch (order-preserving for all real timestamps).
* 8 hex digits wrapping sequence disambiguating identical micros + name.
* Cutoff scan: range `..{cutoff:016x}`.

### Fjall value format

JSON payload:

```json
{"value": 1.0, "metric_type": "counter", "labels": {"method": "GET"}}
```

`metric_type` ∈ `counter|gauge|histogram`.

### Parquet consumption contract (cold store)

Hourly compaction exports keys older than `retention` (default 24h) into
`metrics_cold_<UTC %Y%m%dT%H%M%S>.parquet` (ZSTD), then purges exported keys from Fjall.

| Column | Arrow type | Source |
|--------|-----------|--------|
| `ts` | `Timestamp(Microsecond)` | composite key (authoritative) |
| `name` | `Utf8` | composite key |
| `value` | `Float64` | value JSON |
| `metric_type` | `Utf8` | value JSON |
| `labels` | `Utf8` (JSON object) | value JSON |

```sql
-- DuckDB / DataFusion / Polars over the exports:
SELECT name, quantile_cont(value, 0.99) AS p99, count(*) AS n
FROM 'metrics_cold_*.parquet'
WHERE name = 'http_request_duration_ms'
GROUP BY 1;
```

`GET /telemetry/parquet` serves the newest export with HTTP Range-Request support (404 until the first compaction exports a row). Delivery is at-least-once per cold row across crashes (rename-then-purge ordering).

For interactive exploration, open [`examples/embedded_dashboard.html`](examples/embedded_dashboard.html) against a running server — it fetches `/telemetry/parquet` and runs SQL in the browser (via DuckDB-WASM, client-side only).

## Label cardinality

* Metric/label names are sanitized to `[a-zA-Z_:][a-zA-Z0-9_:]*`; values truncated to 256 chars; max 16 pairs/observation.
* Middleware uses route templates (`MatchedPath` / `match_pattern`), not raw IDs. Never put user IDs, tokens, or unbounded paths in labels.

## API guidelines

* `#![forbid(unsafe_code)]` — Fjall and Parquet are 100% safe Rust.
* `OnceLock` global, `try_send`-only hot path, `thiserror` errors, `///` docs + doctests on every public API.

## Troubleshooting

* `fjall config supplied but fjall-backend feature is not enabled` → add `features = ["fjall-backend"]`.
* `/metrics` returns 503 → `init` not called (macros no-op until then by design).
* `/telemetry/parquet` returns 404 → no export yet (compaction runs hourly; only keys older than `retention` are exported).
