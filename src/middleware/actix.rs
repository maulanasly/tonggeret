//! Actix-web `Transform` middleware capturing the same series as the Axum one:
//! `http_requests_total{method,path,status}` + `http_request_duration_ms{method,path}`.
//!
//! ```rust,no_run
//! use actix_web::{App, web, HttpResponse};
//! use tonggeret::middleware::actix::Tonggeret;
//!
//! async fn hello() -> HttpResponse { HttpResponse::Ok().body("hi") }
//! let app = App::new()
//!     .wrap(Tonggeret)
//!     .route("/", web::get().to(hello))
//!     .route("/metrics", web::get().to(tonggeret::middleware::actix::prometheus_handler));
//! ```

use std::{
    future::{Ready, ready},
    pin::Pin,
    rc::Rc,
    time::Instant,
};

use actix_web::{
    Error, HttpResponse,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
};

/// Actix-web middleware. Attach with `.wrap(Tonggeret)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Tonggeret;

impl<S, B> Transform<S, ServiceRequest> for Tonggeret
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = TonggeretService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(TonggeretService {
            service: Rc::new(service),
        }))
    }
}

/// Inner service produced by [`Tonggeret`].
#[derive(Debug)]
pub struct TonggeretService<S> {
    service: Rc<S>,
}

impl<S, B> Service<ServiceRequest> for TonggeretService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = Rc::clone(&self.service);
        let method = req.method().as_str().to_owned();
        // Prefer the registered resource pattern (`/users/{id}`) to bound cardinality.
        let path = req
            .match_pattern()
            .unwrap_or_else(|| truncate_path(req.path()));
        let start = Instant::now();

        Box::pin(async move {
            match service.call(req).await {
                Ok(response) => {
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
                    Ok(response)
                }
                Err(e) => {
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
                    let status = e.error_response().status().as_u16().to_string();
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
                    Err(e)
                }
            }
        })
    }
}

/// `GET /metrics` handler for Actix-web.
///
/// `async` is required by Actix's `Handler` trait even though the body is ready.
#[allow(clippy::unused_async)]
pub async fn prometheus_handler() -> HttpResponse {
    match crate::engine::prometheus_text() {
        Ok(body) => HttpResponse::Ok()
            .content_type("text/plain; version=0.0.4; charset=utf-8")
            .body(body),
        Err(e) => HttpResponse::ServiceUnavailable().body(format!("metrics unavailable: {e}")),
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
