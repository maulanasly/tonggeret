//! Plug-and-play framework middleware.
//!
//! All middlewares record through [`crate::engine`] (dual-dispatched to
//! Prometheus + Fjall automatically):
//! * `http_requests_total{method,path,status}` (counter) via `track`
//! * `http_request_duration_ms{method,path}` (histogram, milliseconds) via `track`
//! * `visitors_total{region}` (counter) via `track_visitors` / `TrackVisitors`
//! * `unique_visitors_estimate{region}` (gauge) via
//!   [`crate::visitors::VisitorsTracker::snapshot`] (see [`crate::visitors`])

#[cfg(feature = "axum")]
pub mod axum;

#[cfg(feature = "actix")]
pub mod actix;
