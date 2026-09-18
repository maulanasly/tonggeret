//! User-facing configuration with pay-for-what-you-use defaults.
//!
//! ```rust
//! use tonggeret::Config;
//! // Prometheus-only, no storage thread spawned:
//! let light = Config::default_light();
//! // Full dual-mode with embedded Fjall LSM-tree at `./data/fjall`:
//! let full = Config::default_full("./data/fjall");
//! ```

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

/// Embedded Fjall LSM-tree backend settings.
///
/// Requires the `fjall-backend` Cargo feature; otherwise [`Config::fjall`]
/// must stay `None` (validated in [`crate::init`]).
///
/// Memory budget: [`FjallConfig::cache_size_bytes`] (unified block cache,
/// default 8 MiB) + [`FjallConfig::memtable_bytes`] (per-partition write
/// buffer, default 2 MiB) bound steady-state RAM to <10 MiB plus transient
/// batch/compaction buffers.
#[derive(Debug, Clone)]
pub struct FjallConfig {
    /// Keyspace directory, e.g. `./data/fjall`. Created if missing.
    pub dir: PathBuf,
    /// Unified Fjall cache capacity in bytes (index blocks + blobs).
    /// Default 8 MiB (`8 * 1024 * 1024`).
    pub cache_size_bytes: u64,
    /// Per-partition memtable cap in bytes. Default 2 MiB.
    pub memtable_bytes: u32,
    /// Directory for `metrics_cold_*.parquet` exports (default: parent of `dir`).
    pub cold_storage_dir: Option<PathBuf>,
    /// Run periodic hot→cold compaction (default true).
    pub enable_compaction: bool,
    /// How often the background task scans for expired keys (default 1h).
    pub compaction_interval: Duration,
    /// Age after which keys are moved to Parquet (default 24h).
    pub retention: Duration,
    /// Writer batch: max entries drained per insert burst (default 1_000).
    pub batch_rows: usize,
}

impl Default for FjallConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/fjall"),
            cache_size_bytes: 8 * 1024 * 1024,
            memtable_bytes: 2 * 1024 * 1024,
            cold_storage_dir: None,
            enable_compaction: true,
            compaction_interval: Duration::from_secs(3600),
            retention: Duration::from_secs(24 * 3600),
            batch_rows: 1_000,
        }
    }
}

impl FjallConfig {
    /// New config pointing at `dir`.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            ..Self::default()
        }
    }

    /// Effective directory for cold Parquet files.
    #[must_use]
    pub fn cold_dir(&self) -> PathBuf {
        if let Some(d) = &self.cold_storage_dir {
            return d.clone();
        }
        self.dir
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
    }

    /// Total configured steady-state memory budget (cache + memtable).
    #[must_use]
    pub fn memory_budget_bytes(&self) -> u64 {
        self.cache_size_bytes
            .saturating_add(u64::from(self.memtable_bytes))
    }

    #[allow(dead_code)]
    fn validate(&self) -> Result<(), crate::Error> {
        if self.cache_size_bytes < 1024 * 1024 {
            return Err(crate::Error::InvalidConfig(
                "fjall.cache_size_bytes must be >= 1 MiB".to_string(),
            ));
        }
        if self.memtable_bytes < 256 * 1024 {
            return Err(crate::Error::InvalidConfig(
                "fjall.memtable_bytes must be >= 256 KiB".to_string(),
            ));
        }
        if self.batch_rows == 0 {
            return Err(crate::Error::InvalidConfig(
                "fjall.batch_rows must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Prometheus in-memory registry settings (feature `prometheus-exporter`).
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// Default histogram buckets for `histogram!` and HTTP latency.
    /// The HTTP middleware records **milliseconds**, so buckets are
    /// in ms: 5, 10, 25, 50, 100, 250, 500, 1000, 2500.
    pub default_histogram_buckets: Vec<f64>,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            default_histogram_buckets: vec![
                5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0,
            ],
        }
    }
}

/// Top-level configuration passed to [`crate::init`].
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Bounded channel capacity between hot path and Fjall writer.
    /// Default `16_384`. When full, samples are **dropped** and counted in
    /// `tonggeret_dropped_total` (never blocks serving threads).
    pub channel_capacity: usize,
    /// `None` ⇒ Prometheus-only mode (no writer thread spawned).
    pub fjall: Option<FjallConfig>,
    /// Prometheus registry tuning.
    pub prometheus: PrometheusConfig,
}

impl Config {
    /// Prometheus-only mode (works even without `fjall-backend` compiled in).
    #[must_use]
    pub fn default_light() -> Self {
        Self {
            channel_capacity: 16_384,
            fjall: None,
            prometheus: PrometheusConfig::default(),
        }
    }

    /// Dual-mode: Prometheus + embedded Fjall LSM-tree at `dir`.
    #[must_use]
    pub fn default_full(dir: impl AsRef<Path>) -> Self {
        Self {
            channel_capacity: 16_384,
            fjall: Some(FjallConfig::new(dir)),
            prometheus: PrometheusConfig::default(),
        }
    }

    /// Override channel capacity (must be ≥ 16).
    #[must_use]
    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Override Fjall config.
    #[must_use]
    pub fn with_fjall(mut self, cfg: FjallConfig) -> Self {
        self.fjall = Some(cfg);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if self.channel_capacity < 16 {
            return Err(crate::Error::InvalidConfig(
                "channel_capacity must be >= 16".to_string(),
            ));
        }
        if self.prometheus.default_histogram_buckets.is_empty() {
            return Err(crate::Error::InvalidConfig(
                "prometheus.default_histogram_buckets must not be empty".to_string(),
            ));
        }
        #[cfg(not(feature = "fjall-backend"))]
        if self.fjall.is_some() {
            return Err(crate::Error::InvalidConfig(
                "fjall config supplied but `fjall-backend` feature is not enabled".to_string(),
            ));
        }
        #[cfg(feature = "fjall-backend")]
        if let Some(db) = &self.fjall {
            db.validate()?;
        }
        Ok(())
    }
}
