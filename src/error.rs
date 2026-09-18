//! Typed errors for `tonggeret`.
//!
//! All backend-specific failures are stringified so the public API surface
//! stays stable regardless of which Cargo features are enabled.

use thiserror::Error;

/// Crate-wide [`Result`](std::result::Result) alias.
pub type Result<T> = std::result::Result<T, Error>;

/// All errors producible by `tonggeret`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [`crate::init`] was called more than once in this process.
    #[error("tonggeret already initialized")]
    AlreadyInitialized,

    /// A metric macro / API was used before [`crate::init`].
    ///
    /// Note: hot-path macros intentionally **do not** return this error —
    /// they no-op when uninitialized to keep request paths non-blocking.
    /// This variant is returned by fallible APIs such as
    /// [`crate::prometheus_text`] or [`crate::shutdown`].
    #[error("tonggeret not initialized; call tonggeret::init() first")]
    NotInitialized,

    /// Invalid user configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Fjall backend failure (open, insert, range scan, compaction, Parquet export, …).
    #[error("storage error: {0}")]
    Storage(String),

    /// Prometheus registry / encoding failure.
    #[error("prometheus error: {0}")]
    Prometheus(String),

    /// I/O failure (creating DB directories, writing Parquet exports, …).
    #[error("io error: {0}")]
    Io(String),

    /// Internal pipeline is closed (receiver dropped, shutting down).
    #[error("metrics pipeline closed")]
    ChannelClosed,
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
