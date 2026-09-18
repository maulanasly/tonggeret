//! Plug-and-play framework middleware.
//!
//! Both middlewares record the same two series (dual-dispatched to Prometheus
//! + Fjall automatically):
//! * `http_requests_total{method,path,status}` (counter)
//! * `http_request_duration_ms{method,path}` (histogram, milliseconds)

#[cfg(feature = "axum")]
pub mod axum;

#[cfg(feature = "actix")]
pub mod actix;
