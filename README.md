# tonggeret

[![CI](https://github.com/maulanasly/tonggeret/actions/workflows/ci.yml/badge.svg)](https://github.com/maulanasly/tonggeret/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tonggeret.svg)](https://crates.io/crates/tonggeret)
[![docs.rs](https://img.shields.io/docsrs/tonggeret.svg)](https://docs.rs/tonggeret)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE-MIT)
[![edition](https://img.shields.io/badge/edition-2024-blueviolet.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)

Ultra-low-memory Rust telemetry: **embedded Fjall LSM-tree** (<10 MiB RAM footprint) for high-throughput local ingestion, standard **Prometheus exposition**, and periodic **Parquet cold storage** — behind a non-blocking, lock-free hot path.

> Crate name is `tonggeret`; the GitHub remote is [`maulanasly/tonggeret`](https://github.com/maulanasly/tonggeret).

* Dual-mode dispatch: every `counter!` / `gauge!` / `histogram!` updates Prometheus atomics **and** streams to Fjall via a bounded channel → dedicated writer thread → LSM memtable.
* Pure Rust, no SQL engine, no C++ toolchain: `fjall` + `parquet` + `arrow` only. `#![forbid(unsafe_code)]`.
* Plug-and-play Axum (`from_fn`) and Actix-web (`Transform`) middleware recording `http_requests_total` + `http_request_duration_ms`.

```rust
tonggeret::init(tonggeret::Config::default_light())?; // or ::default_full("./data/fjall")

tonggeret::counter!("orders_total", 1.0, method = "POST", route = "/orders");
tonggeret::gauge!("queue_depth", 42.0);
tonggeret::histogram!("db_query_ms", 12.4, table = "orders");
```

## Status

Pre-1.0 (`0.1.0` is the first release of the restarted line; prior DuckDB-based iteration was discarded). Expect additive API evolution; any breaking storage change bumps `SCHEMA_VERSION` and is noted in [`CHANGELOG.md`](CHANGELOG.md).

Docs: [`docs.rs/tonggeret`](https://docs.rs/tonggeret) · Changelog: [`CHANGELOG.md`](CHANGELOG.md) · Contributing: [`AGENTS.md`](AGENTS.md).

## Installation

```toml
# Prometheus + Axum (defaults, fast build)
tonggeret = "0.1"

# Dual-mode with embedded Fjall + Parquet (pure Rust, no system deps)
tonggeret = { version = "0.1", features = ["fjall-backend"] }

# Actix-web instead of / in addition to Axum
tonggeret = { version = "0.1", default-features = false, features = ["actix", "prometheus-exporter"] }
```

Requires Rust **1.85+** (edition 2024, MSRV enforced in CI). License: **MIT** ([`LICENSE-MIT`](LICENSE-MIT)).

## Feature flags

| Flag | Enables | Default |
|------|---------|---------|
| `prometheus-exporter` | In-memory registry + text exposition | ✅ |
| `fjall-backend` | Fjall LSM-tree ingestion + Parquet compaction (`fjall`, `parquet`, `arrow`, `chrono`) | ❌ |
| `axum` | `middleware::axum::track` + `/metrics` + `/telemetry/parquet` route | ✅ |
| `actix` | `middleware::actix::Tonggeret` + scrape handler | ❌ |

## Quickstart — Axum

```rust
use axum::{Router, routing::get, middleware};
use tonggeret::middleware::axum as dm_axum;

tonggeret::init(tonggeret::Config::default_full("./data/fjall"))?;

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
use tonggeret::middleware::actix::{Tonggeret, prometheus_handler};
let app = actix_web::App::new()
    .wrap(Tonggeret)
    .route("/metrics", actix_web::web::get().to(prometheus_handler));
```

## Configuration

```rust
let mut fjall = tonggeret::FjallConfig::new("./data/fjall");
fjall.cache_size_bytes = 8 * 1024 * 1024;  // unified block cache (default 8 MiB)
fjall.memtable_bytes = 2 * 1024 * 1024;    // per-partition memtable cap (default 2 MiB)
fjall.retention = std::time::Duration::from_secs(24 * 3600);
fjall.compaction_interval = std::time::Duration::from_secs(3600);
// fjall.cold_storage_dir = Some("./data/cold".into());
let cfg = tonggeret::Config::default_light()
    .with_channel_capacity(16_384)
    .with_fjall(fjall);
tonggeret::init(cfg)?;
```

* Channel full ⇒ sample dropped for Fjall, still recorded in Prometheus, `tonggeret_dropped_total` incremented. Never blocks serving threads.
* Steady-state RAM ≈ `cache_size_bytes` + `memtable_bytes` (<10 MiB by default) plus transient batch/compaction buffers.
* If `init` runs before the Tokio runtime exists, call `tonggeret::spawn_compaction_task(&fjall_cfg)` once inside the runtime.
* `tonggeret::shutdown()` persists + joins the writer at process exit.

## Visitor tracking

Count total visits and estimate unique visitors per region without a
per-visitor label (which would explode Prometheus cardinality):

```rust
use axum::{Router, routing::get, middleware};
use tonggeret::middleware::axum as dm_axum;
use tonggeret::visitors::DEFAULT_SNAPSHOT_INTERVAL;

# async fn hello() -> &'static str { "hi" }
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
tonggeret::init(tonggeret::Config::default_light())?;
tonggeret::visitors::spawn_snapshot_task(DEFAULT_SNAPSHOT_INTERVAL);

let app: Router = Router::new()
    .route("/", get(hello))
    .route("/metrics", get(dm_axum::prometheus_handler))
    .layer(middleware::from_fn(dm_axum::track_visitors));
Ok(())
}
```

* Series: `visitors_total{region}` (counter, every visit) +
  `unique_visitors_estimate{region}` (gauge, HyperLogLog ~1.6% error,
  refreshed each snapshot).
* Region comes from `CF-IPCountry` → `X-Vercel-IP-Country` →
  `CloudFront-Viewer-Country` (validated 2-letter, else `unknown`;
  region count capped, overflow → `other`).
* Identity is first `X-Forwarded-For` (else peer) + `User-Agent`, hashed
  into a 4 KiB-per-region sketch — raw identifiers are never stored.
* Trust boundary: enable only behind a proxy/CDN that sets and sanitizes
  these headers; spoofed headers can skew totals.

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

## Development

```bash
cargo build
cargo test                                        # default features
cargo test --features fjall-backend               # full dual-mode
cargo test --all-features                         # includes actix
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

CI ([`ci.yml`](.github/workflows/ci.yml)) runs fmt, clippy, the test matrix above, `cargo doc`, an MSRV 1.85 gate, and `cargo audit` on every push to `main` and every PR. See [`AGENTS.md`](AGENTS.md) for the mandatory contributor workflow (branches, context memory, tests, pre-commit, PRs, releases).

## Troubleshooting

* `fjall config supplied but fjall-backend feature is not enabled` → add `features = ["fjall-backend"]`.
* `/metrics` returns 503 → `init` not called (macros no-op until then by design).
* `/telemetry/parquet` returns 404 → no export yet (compaction runs hourly; only keys older than `retention` are exported).

## License

MIT — see [`LICENSE-MIT`](LICENSE-MIT).
