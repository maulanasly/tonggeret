//! Axum middleware (`axum::middleware::from_fn`) + `/metrics` scrape handler.
//!
//! ```rust,no_run
//! use axum::{Router, routing::get, middleware};
//! use tonggeret::middleware::axum as dm_axum;
//!
//! # async fn hello() -> &'static str { "hi" }
//! let app: Router = Router::new()
//!     .route("/", get(hello))
//!     .route("/metrics", get(dm_axum::prometheus_handler))
//!     .layer(middleware::from_fn(dm_axum::track));
//! ```

use std::{net::SocketAddr, time::Instant};

use axum::{
    extract::{ConnectInfo, MatchedPath, Request},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Record `http_requests_total` + `http_request_duration_ms` for each request.
///
/// Uses the route template (`MatchedPath`, e.g. `/users/:id`) when available
/// to keep label cardinality bounded; falls back to the raw path (truncated
/// to 128 chars).
pub async fn track(req: Request, next: Next) -> Response {
    let method = req.method().as_str().to_owned();
    let path = req.extensions().get::<MatchedPath>().map_or_else(
        || truncate_path(req.uri().path()),
        |m| m.as_str().to_owned(),
    );
    let start = Instant::now();

    let response = next.run(req).await;

    let status = response.status().as_u16().to_string();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;

    crate::engine::record_counter(
        "http_requests_total",
        1.0,
        vec![
            ("method".to_string(), method.clone()),
            ("path".to_string(), path.clone()),
            ("status".to_string(), status),
        ],
    );
    crate::engine::record_histogram(
        "http_request_duration_ms",
        elapsed_ms,
        vec![("method".to_string(), method), ("path".to_string(), path)],
    );
    response
}

/// Record `visitors_total` + HyperLogLog uniques per region for each request.
///
/// Region comes from CDN country headers (see
/// [`crate::visitors::parse_region`]); identity is the first
/// `X-Forwarded-For` entry (else the connection peer) plus `User-Agent`,
/// hashed only — raw identifiers are never stored. Counts `unknown` region
/// and `direct` peer when the proxy headers / [`ConnectInfo`] are absent.
/// Enable only behind a proxy/CDN that sets and sanitizes these headers.
///
/// ```rust,no_run
/// use axum::{Router, routing::get, middleware};
/// use tonggeret::middleware::axum as dm_axum;
///
/// # async fn hello() -> &'static str { "hi" }
/// let app: Router = Router::new()
///     .route("/", get(hello))
///     .route("/metrics", get(dm_axum::prometheus_handler))
///     .layer(middleware::from_fn(dm_axum::track_visitors));
/// ```
pub async fn track_visitors(req: Request, next: Next) -> Response {
    let headers = req.headers();
    let region = crate::visitors::parse_region(
        headers.get("cf-ipcountry").and_then(|v| v.to_str().ok()),
        headers
            .get("x-vercel-ip-country")
            .and_then(|v| v.to_str().ok()),
        headers
            .get("cloudfront-viewer-country")
            .and_then(|v| v.to_str().ok()),
    );
    let forwarded = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or_else(|| "direct".to_string(), |c| c.0.to_string());
    let key = crate::visitors::visitor_key(forwarded, &peer, user_agent);
    let _ = crate::visitors::observe_visitor(&key, &region);
    next.run(req).await
}

/// `GET /metrics` handler returning Prometheus text exposition.
///
/// `async` is required by Axum's handler trait even though the body is ready.
#[allow(clippy::unused_async)]
pub async fn prometheus_handler() -> impl IntoResponse {
    match crate::engine::prometheus_text() {
        Ok(body) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("metrics unavailable: {e}"),
        )
            .into_response(),
    }
}

fn truncate_path(p: &str) -> String {
    const MAX: usize = 128;
    if p.len() > MAX {
        p[..MAX].to_string()
    } else {
        p.to_string()
    }
}

// ---------------------------------------------------------------------------
// Parquet file exposer (`fjall-backend`)
// ---------------------------------------------------------------------------

/// Build a `GET /telemetry/parquet` route serving the newest cold export
/// found in `cold_dir`, with full HTTP Range-Request support.
///
/// The latest file is resolved per request, so newly compacted exports are
/// picked up without a restart. Responds `404` when no export exists yet.
///
/// ```rust,no_run
/// use axum::Router;
/// use tonggeret::middleware::axum as dm_axum;
/// use std::path::PathBuf;
///
/// let app: Router = Router::new()
///     .route("/metrics", axum::routing::get(dm_axum::prometheus_handler))
///     .merge(dm_axum::parquet_route(PathBuf::from("./data")));
/// ```
#[cfg(feature = "fjall-backend")]
pub fn parquet_route(cold_dir: std::path::PathBuf) -> axum::Router {
    use axum::extract::{Request, State};
    use tower::ServiceExt as _;

    async fn serve_latest(
        State(dir): State<std::sync::Arc<std::path::PathBuf>>,
        req: Request,
    ) -> Response {
        match crate::storage::latest_cold_file(&dir) {
            Some(path) => tower_http::services::ServeFile::new(path)
                .oneshot(req)
                .await
                .into_response(),
            None => (
                StatusCode::NOT_FOUND,
                "no parquet export available yet (compaction has not exported any keys)",
            )
                .into_response(),
        }
    }

    axum::Router::new()
        .route("/telemetry/parquet", axum::routing::get(serve_latest))
        .with_state(std::sync::Arc::new(cold_dir))
}
