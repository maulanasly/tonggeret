//! `tonggeret` — ultra-low-memory telemetry for Rust web services.
//!
//! Combines an **embedded Fjall LSM-tree** (<10 MiB RAM footprint) for
//! high-throughput local ingestion with standard **Prometheus exposition**
//! and periodic **Parquet cold storage**, behind a non-blocking, lock-free
//! hot path.
//!
//! # Quickstart (Axum, 2 lines + middleware)
//!
//! ```rust,no_run
//! use axum::{Router, routing::get, middleware};
//! use tonggeret::middleware::axum as dm_axum;
//!
//! # async fn hello() -> &'static str { "hi" }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 1. Initialize (Prometheus-only shown; use `Config::default_full(dir)`
//! //    for dual-mode with Fjall).
//! tonggeret::init(tonggeret::Config::default_light())?;
//!
//! // 2. Wrap routes + expose scrapers.
//! let app: Router = Router::new()
//!     .route("/", get(hello))
//!     .route("/metrics", get(dm_axum::prometheus_handler))
//!     .layer(middleware::from_fn(dm_axum::track));
//!
//! // Cold Parquet exports (requires `fjall-backend`):
//! // app.merge(dm_axum::parquet_route("./data".into()));
//!
//! // Custom business metrics anywhere (non-blocking, no-op if uninitialized):
//! tonggeret::counter!("orders_total", 1.0, method = "POST", route = "/orders");
//! tonggeret::gauge!("queue_depth", 42.0);
//! tonggeret::histogram!("db_query_ms", 12.4, table = "orders");
//! # Ok(())
//! # }
//! ```
//!
//! # Modes: Prometheus-only vs dual-mode
//!
//! * [`Config::default_light()`](config::Config::default_light) —
//!   Prometheus-only. Spawns **no** background threads; ideal for
//!   sidecars and libraries that only need `/metrics`.
//! * [`Config::default_full(dir)`](config::Config::default_full) —
//!   dual-mode (requires the `fjall-backend` feature). Adds one writer OS
//!   thread plus an optional Tokio compaction task that rolls hot rows
//!   older than `retention` into ZSTD Parquet under `cold_dir()`.
//!
//! # Lifecycle
//!
//! [`init`] once at startup, [`record_counter`]/[`record_gauge`]/
//! [`record_histogram`] (or the [`counter!`]/[`gauge!`]/[`histogram!`]
//! macros) anywhere, [`shutdown`] once at process exit to persist and join
//! the writer. If [`init`] runs before the Tokio runtime exists, call
//! [`engine::spawn_compaction_task`] from inside the runtime (requires
//! `fjall-backend`) — otherwise hourly compaction never starts.
//!
//! # Modules
//!
//! * [`engine`] — global [`engine::EngineHandle`] lifecycle.
//! * [`config`] — [`config::Config`] and [`config::FjallConfig`] tuning.
//! * [`types`] — [`types::MetricEntry`] and [`types::MetricType`].
//! * [`visitors`] — visitor totals + HyperLogLog uniques by region.
//! * [`crate::prometheus`] — registry sync (feature `prometheus-exporter`).
//! * [`crate::storage`] — Fjall hot store + Parquet compaction
//!   (feature `fjall-backend`; see [`crate::SCHEMA_VERSION`]).
//! * [`middleware`] — Axum / Actix-web request tracking.
//! * [`error`] — [`error::Error`] and [`error::Result`].
//!
//! # Feature flags (pay only for what you use)
//!
//! | Flag | Enables | Default |
//! |------|---------|---------|
//! | `prometheus-exporter` | In-memory registry + text exposition | ✅ on |
//! | `fjall-backend` | Fjall LSM-tree ingestion + Parquet compaction | ❌ off |
//! | `axum` | `from_fn` middleware + `/metrics` + `/telemetry/parquet` handlers (implies `prometheus-exporter`) | ✅ on |
//! | `actix` | `Transform` middleware + scrape handler (implies `prometheus-exporter`) | ❌ off |
//!
//! # Hot-path guarantees
//!
//! * [`counter!`]/[`gauge!`]/[`histogram!`] never `.await` and never block:
//!   Prometheus updates are atomics; Fjall ingestion is `try_send` on a
//!   bounded channel (default 16_384). Full ⇒ sample dropped for Fjall and
//!   counted in `tonggeret_dropped_total`.
//! * Uninitialized ⇒ silent no-op, so libraries can emit unconditionally.
//!
//! # Label cardinality
//!
//! Keep labels bounded: the middleware records route *templates*
//! (`MatchedPath`, e.g. `/users/:id`), never raw IDs, user IDs, or tokens.
//! Names are sanitized to `[a-zA-Z_:][a-zA-Z0-9_:]*`, values truncated to
//! 256 chars, at most 16 pairs per observation (see [`types`]).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod engine;
pub mod error;
pub mod middleware;
pub mod types;
pub mod visitors;

#[cfg(feature = "prometheus-exporter")]
pub mod prometheus;

#[cfg(feature = "fjall-backend")]
pub mod storage;

// -- re-exports -------------------------------------------------------------
pub use config::{Config, FjallConfig, PrometheusConfig};
#[cfg(feature = "prometheus-exporter")]
pub use engine::prometheus_text;
#[cfg(feature = "fjall-backend")]
pub use engine::spawn_compaction_task;
pub use engine::{
    EngineHandle, handle, init, is_initialized, record_counter, record_gauge, record_histogram,
    shutdown,
};
pub use error::{Error, Result};
#[cfg(feature = "fjall-backend")]
pub use storage::SCHEMA_VERSION;
pub use types::{MetricEntry, MetricType};

// ---------------------------------------------------------------------------
// Low-overhead direct API macros
// ---------------------------------------------------------------------------

/// Increment a counter (monotonically increasing value).
///
/// Non-blocking and safe to call before [`init`](crate::init) (then a
/// silent no-op). Three label forms are accepted:
///
/// ```rust
/// # tonggeret::counter!("orders_total", 1.0);
/// # tonggeret::counter!("orders_total", 1.0, method = "POST");
/// # tonggeret::counter!("orders_total", 1.0, "method" => "POST");
/// ```
#[macro_export]
macro_rules! counter {
    ($name:expr, $value:expr) => {
        $crate::engine::record_counter($name, ($value) as f64, ::std::vec::Vec::new())
    };
    ($name:expr, $value:expr, $($key:ident = $val:expr),* $(,)?) => {
        $crate::engine::record_counter(
            $name,
            ($value) as f64,
            ::std::vec![$( (::std::string::ToString::to_string(::std::stringify!($key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
    ($name:expr, $value:expr, $($key:expr => $val:expr),* $(,)?) => {
        $crate::engine::record_counter(
            $name,
            ($value) as f64,
            ::std::vec![$( ((::std::string::ToString::to_string(&$key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
}

/// Set a gauge (arbitrarily settable value, e.g. queue depth).
///
/// Same non-blocking / no-op-when-uninitialized contract and label forms
/// as [`counter!`].
///
/// ```rust
/// # tonggeret::gauge!("queue_depth", 42.0);
/// # tonggeret::gauge!("queue_depth", 42.0, worker = "a");
/// ```
#[macro_export]
macro_rules! gauge {
    ($name:expr, $value:expr) => {
        $crate::engine::record_gauge($name, ($value) as f64, ::std::vec::Vec::new())
    };
    ($name:expr, $value:expr, $($key:ident = $val:expr),* $(,)?) => {
        $crate::engine::record_gauge(
            $name,
            ($value) as f64,
            ::std::vec![$( (::std::string::ToString::to_string(::std::stringify!($key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
    ($name:expr, $value:expr, $($key:expr => $val:expr),* $(,)?) => {
        $crate::engine::record_gauge(
            $name,
            ($value) as f64,
            ::std::vec![$( ((::std::string::ToString::to_string(&$key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
}

/// Observe a histogram sample (e.g. latency in milliseconds).
///
/// Feeds the Prometheus histogram (see
/// [`PrometheusConfig::default_histogram_buckets`](config::PrometheusConfig::default_histogram_buckets))
/// and stores the raw sample in Fjall when `fjall-backend` is enabled.
/// Same non-blocking / no-op-when-uninitialized contract and label forms
/// as [`counter!`].
///
/// ```rust
/// # tonggeret::histogram!("db_query_ms", 12.4);
/// # tonggeret::histogram!("db_query_ms", 12.4, table = "orders");
/// ```
#[macro_export]
macro_rules! histogram {
    ($name:expr, $value:expr) => {
        $crate::engine::record_histogram($name, ($value) as f64, ::std::vec::Vec::new())
    };
    ($name:expr, $value:expr, $($key:ident = $val:expr),* $(,)?) => {
        $crate::engine::record_histogram(
            $name,
            ($value) as f64,
            ::std::vec![$( (::std::string::ToString::to_string(::std::stringify!($key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
    ($name:expr, $value:expr, $($key:expr => $val:expr),* $(,)?) => {
        $crate::engine::record_histogram(
            $name,
            ($value) as f64,
            ::std::vec![$( ((::std::string::ToString::to_string(&$key)), (::std::string::ToString::to_string(&$val)) ) ),*],
        )
    };
}
