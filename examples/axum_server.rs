//! Minimal Axum server showcasing `duckmetrics` in ~30 lines.
//!
//! Run with:
//! ```sh
//! # Prometheus-only (fast build):
//! cargo run --example axum_server
//! # Dual-mode with embedded Fjall (pure Rust, fast build):
//! cargo run --example axum_server --features fjall-backend
//! ```
//! Then:
//! * `curl localhost:3000/` — hello + custom metrics
//! * `curl localhost:3000/metrics` — Prometheus scrape
//! * `curl localhost:3000/telemetry/parquet -O -J` — latest cold Parquet export
//! * `curl "localhost:3000/orders?fail=1"` — error-path labels

use std::net::SocketAddr;

use axum::{Router, extract::Query, middleware, response::IntoResponse, routing::get};
use duckmetrics::middleware::axum as dm_axum;
use serde::Deserialize;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Deserialize)]
struct OrderParams {
    fail: Option<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_server=debug,duckmetrics=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // --- 2-line initialization -------------------------------------------
    #[cfg(feature = "fjall-backend")]
    let fjall_cfg = duckmetrics::FjallConfig::new("./data/fjall");
    #[cfg(feature = "fjall-backend")]
    let config = duckmetrics::Config::default_light().with_fjall(fjall_cfg.clone());
    #[cfg(not(feature = "fjall-backend"))]
    let config = duckmetrics::Config::default_light();
    duckmetrics::init(config)?;
    // ----------------------------------------------------------------------

    let app = Router::new()
        .route("/", get(hello))
        .route("/orders", get(create_order))
        .route("/metrics", get(dm_axum::prometheus_handler))
        .layer(middleware::from_fn(dm_axum::track));

    // Cold Parquet exports with Range-Request support (fjall-backend only).
    #[cfg(feature = "fjall-backend")]
    let app = app.merge(dm_axum::parquet_route(fjall_cfg.cold_dir()));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn hello() -> impl IntoResponse {
    // Custom business metrics — non-blocking, lock-free.
    duckmetrics::counter!("greetings_total", 1.0, route = "/");
    duckmetrics::gauge!("active_sessions", 7.0, region = "eu");
    "hello from duckmetrics"
}

async fn create_order(Query(q): Query<OrderParams>) -> impl IntoResponse {
    let start = std::time::Instant::now();
    // Simulate work.
    tokio::time::sleep(std::time::Duration::from_millis(12)).await;

    if q.fail.unwrap_or(0) == 1 {
        duckmetrics::counter!("orders_total", 1.0, route = "/orders", status = "error");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "order failed",
        );
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
    duckmetrics::counter!("orders_total", 1.0, route = "/orders", status = "ok");
    duckmetrics::histogram!("db_query_ms", elapsed_ms, table = "orders");
    (axum::http::StatusCode::OK, "order created")
}
