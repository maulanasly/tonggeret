//! `duckmetrics` — ultra-low-memory telemetry for Rust web services.
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
//! use duckmetrics::middleware::axum as dm_axum;
//!
//! # async fn hello() -> &'static str { "hi" }
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 1. Initialize (Prometheus-only shown; use `Config::default_full(dir)`
//! //    for dual-mode with Fjall).
//! duckmetrics::init(duckmetrics::Config::default_light())?;
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
//! duckmetrics::counter!("orders_total", 1.0, method = "POST", route = "/orders");
//! duckmetrics::gauge!("queue_depth", 42.0);
//! duckmetrics::histogram!("db_query_ms", 12.4, table = "orders");
//! # Ok(())
//! # }
//! ```
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
//! Prometheus-only mode spawns **no** background threads. Enabling
//! `fjall-backend` adds one writer OS thread plus an optional Tokio
//! compaction task.
//!
//! # Hot-path guarantees
//!
//! * [`counter!`]/[`gauge!`]/[`histogram!`] never `.await` and never block:
//!   Prometheus updates are atomics; Fjall ingestion is `try_send` on a
//!   bounded channel (default 16_384). Full ⇒ sample dropped for Fjall and
//!   counted in `duckmetrics_dropped_total`.
//! * Uninitialized ⇒ silent no-op, so libraries can emit unconditionally.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod engine;
pub mod error;
pub mod middleware;
pub mod types;

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

/// Increment a counter.
///
/// ```rust
/// # duckmetrics::counter!("orders_total", 1.0);
/// # duckmetrics::counter!("orders_total", 1.0, method = "POST");
/// # duckmetrics::counter!("orders_total", 1.0, "method" => "POST");
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

/// Set a gauge.
///
/// ```rust
/// # duckmetrics::gauge!("queue_depth", 42.0);
/// # duckmetrics::gauge!("queue_depth", 42.0, worker = "a");
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

/// Observe a histogram sample.
///
/// ```rust
/// # duckmetrics::histogram!("db_query_ms", 12.4);
/// # duckmetrics::histogram!("db_query_ms", 12.4, table = "orders");
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
