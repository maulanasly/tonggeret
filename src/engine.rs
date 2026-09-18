//! Global telemetry engine: lock-free dispatch to Prometheus + bounded
//! channel to the Fjall writer thread.
//!
//! Hot path contract:
//! * never `.await`, never blocks — [`EngineHandle::record`] uses
//!   `try_send` only;
//! * uninitialized ⇒ silent no-op (so libraries can emit metrics
//!   unconditionally);
//! * channel full ⇒ sample dropped for Fjall, still counted in Prometheus,
//!   `tonggeret_dropped_total` incremented.

use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use tokio::sync::mpsc;

use crate::{
    config::Config,
    error::{Error, Result},
    types::{MetricEntry, MetricType, PipelineMsg},
};

#[cfg(feature = "fjall-backend")]
use crate::storage::FjallHandles;

static GLOBAL: OnceLock<Arc<EngineHandle>> = OnceLock::new();
static WRITER_JOIN: Mutex<Option<std::thread::JoinHandle<()>>> = Mutex::new(None);
static COMPACTION_TASK: Mutex<Option<tokio::task::JoinHandle<()>>> = Mutex::new(None);
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Handle to the initialized telemetry engine.
///
/// Clonable + `Send + Sync`; cheap to share across handlers.
#[derive(Debug)]
pub struct EngineHandle {
    tx: Option<mpsc::Sender<PipelineMsg>>,
    #[cfg(feature = "prometheus-exporter")]
    registry: Option<Arc<crate::prometheus::PromRegistry>>,
    /// Shared Fjall keyspace handle (single open per process).
    #[cfg(feature = "fjall-backend")]
    fjall: Option<Arc<FjallHandles>>,
    dropped_total: AtomicU64,
    has_fjall: bool,
}

impl EngineHandle {
    // Fallible under `prometheus-exporter` (registry creation); infallible
    // otherwise. Keep `Result` for a stable signature across feature sets.
    #[allow(clippy::unnecessary_wraps)]
    fn new(config: &Config) -> Result<(Self, Option<mpsc::Receiver<PipelineMsg>>)> {
        let _ = config.channel_capacity;
        #[cfg(feature = "prometheus-exporter")]
        let registry = {
            let reg = crate::prometheus::PromRegistry::new(
                config.prometheus.default_histogram_buckets.clone(),
            )?;
            Some(Arc::new(reg))
        };

        #[cfg(feature = "fjall-backend")]
        let (tx_opt, rx_opt) = if config.fjall.is_some() {
            let (tx, rx) = mpsc::channel::<PipelineMsg>(config.channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        #[cfg(not(feature = "fjall-backend"))]
        let (tx_opt, rx_opt): (
            Option<mpsc::Sender<PipelineMsg>>,
            Option<mpsc::Receiver<PipelineMsg>>,
        ) = (None, None);

        Ok((
            Self {
                tx: tx_opt,
                #[cfg(feature = "prometheus-exporter")]
                registry,
                #[cfg(feature = "fjall-backend")]
                fjall: None,
                dropped_total: AtomicU64::new(0),
                has_fjall: false,
            },
            rx_opt,
        ))
    }

    /// Record one observation (non-blocking, no-op on shutdown).
    pub fn record(&self, entry: MetricEntry) {
        if SHUTDOWN.load(Ordering::Relaxed) {
            return;
        }
        #[cfg(feature = "prometheus-exporter")]
        if let Some(reg) = &self.registry {
            reg.update(&entry);
        }
        if let Some(tx) = &self.tx {
            if tx.try_send(PipelineMsg::Metric(entry)).is_err() {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "prometheus-exporter")]
                if let Some(reg) = &self.registry {
                    reg.inc_dropped();
                }
            }
        }
    }

    /// Total Fjall samples dropped due to a full/closed channel.
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }

    /// Whether a Fjall writer thread is attached.
    #[must_use]
    pub fn has_fjall(&self) -> bool {
        self.has_fjall
    }

    /// Whether Prometheus sync is active.
    #[must_use]
    pub fn has_prometheus(&self) -> bool {
        #[cfg(feature = "prometheus-exporter")]
        {
            self.registry.is_some()
        }
        #[cfg(not(feature = "prometheus-exporter"))]
        {
            false
        }
    }

    /// Render Prometheus exposition text.
    #[cfg(feature = "prometheus-exporter")]
    pub fn prometheus_text(&self) -> Result<String> {
        self.registry
            .as_ref()
            .map_or_else(|| Ok(String::new()), |r| r.gather_text())
    }
}

// ---------------------------------------------------------------------------
// Global lifecycle
// ---------------------------------------------------------------------------

/// Initialize global telemetry once per process.
///
/// * Opens the Fjall keyspace **once** (when `config.fjall` is `Some`; fails
///   fast on open errors so misconfiguration is loud) and spawns the writer
///   OS thread sharing that handle.
/// * Spawns the compaction background task when a Tokio runtime is already
///   running; otherwise compaction is skipped with a warning — call
///   [`spawn_compaction_task`] after your runtime starts.
///
/// ```rust,no_run
/// # #[cfg(all(feature = "prometheus-exporter"))] {
/// tonggeret::init(tonggeret::Config::default_light()).unwrap();
/// # }
/// ```
// `Config` is intentionally consumed: `fjall` moves into the background tasks.
#[allow(clippy::needless_pass_by_value)]
pub fn init(config: Config) -> Result<()> {
    config.validate()?;

    // `mut` only needed when patching in the Fjall handle below.
    #[cfg_attr(not(feature = "fjall-backend"), allow(unused_mut))]
    let (mut handle, rx) = EngineHandle::new(&config)?;

    // Open the single shared keyspace *before* publishing the global handle,
    // so a failed open leaves global state untouched (init stays retryable).
    #[cfg(feature = "fjall-backend")]
    let fjall_handles: Option<Arc<FjallHandles>> = match &config.fjall {
        Some(fcfg) => {
            let shared = Arc::new(crate::storage::open_handles(fcfg)?);
            handle.fjall = Some(shared.clone());
            handle.has_fjall = true;
            Some(shared)
        }
        None => None,
    };

    let handle = Arc::new(handle);
    GLOBAL
        .set(handle.clone())
        .map_err(|_| Error::AlreadyInitialized)?;

    // Spawn Fjall writer thread (shares the single open keyspace handle).
    #[cfg(feature = "fjall-backend")]
    if let (Some(fcfg), Some(receiver), Some(handles)) = (config.fjall.clone(), rx, fjall_handles) {
        let batch_rows = fcfg.batch_rows;
        let writer_handles = (*handles).clone();
        let join = std::thread::Builder::new()
            .name("tonggeret-writer".to_string())
            .spawn(move || {
                crate::storage::fjall_engine::writer_loop(receiver, writer_handles, batch_rows);
            })
            .map_err(|e| Error::Storage(format!("spawn writer thread: {e}")))?;
        if let Ok(mut guard) = WRITER_JOIN.lock() {
            *guard = Some(join);
        }
        // Opportunistically start compaction if we're inside a Tokio runtime.
        try_spawn_compaction(&config);
    }

    #[cfg(not(feature = "fjall-backend"))]
    let _ = rx;

    tracing::info!(
        has_fjall = handle.has_fjall(),
        has_prometheus = handle.has_prometheus(),
        "tonggeret initialized"
    );
    Ok(())
}

/// Global handle, if [`init`] succeeded.
#[must_use]
pub fn handle() -> Option<Arc<EngineHandle>> {
    GLOBAL.get().cloned()
}

/// Whether [`init`] has been called.
#[must_use]
pub fn is_initialized() -> bool {
    GLOBAL.get().is_some()
}

/// Flush + join the writer thread and stop compaction.
///
/// Safe to call once at shutdown; hot-path macros become silent no-ops after.
pub fn shutdown() -> Result<()> {
    let Some(h) = GLOBAL.get() else {
        return Err(Error::NotInitialized);
    };
    if SHUTDOWN.swap(true, Ordering::SeqCst) {
        return Ok(());
    }

    // Best-effort synchronous delivery of the sentinel (retry; never block forever).
    if let Some(tx) = &h.tx {
        for _ in 0..100 {
            match tx.try_send(PipelineMsg::Shutdown) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }
    }

    if let Ok(mut guard) = WRITER_JOIN.lock() {
        if let Some(join) = guard.take() {
            let _ = join.join();
        }
    }
    if let Ok(mut guard) = COMPACTION_TASK.lock() {
        if let Some(task) = guard.take() {
            task.abort();
        }
    }

    // Final durability barrier on the shared keyspace (writer already persists).
    #[cfg(feature = "fjall-backend")]
    if let Some(fh) = &h.fjall {
        use fjall::PersistMode;
        if let Err(e) = fh.keyspace.persist(PersistMode::SyncAll) {
            tracing::warn!(error = %e, "shutdown persist failed");
        }
    }

    tracing::info!("tonggeret shut down");
    Ok(())
}

/// Start the compaction background task (call when [`init`] ran before the
/// Tokio runtime existed, e.g. in `main()` ahead of `#[tokio::main]`).
///
/// Returns `false` when compaction is disabled, no runtime is running, or the
/// engine has no Fjall handle.
#[cfg(feature = "fjall-backend")]
pub fn spawn_compaction_task(config: &crate::config::FjallConfig) -> bool {
    if !config.enable_compaction {
        return false;
    }
    let Some(h) = GLOBAL.get() else {
        return false;
    };
    let Some(fh) = h.fjall.clone() else {
        return false;
    };
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(
            "no Tokio runtime; compaction not started (call spawn_compaction_task later)"
        );
        return false;
    };
    let cfg = config.clone();
    let handles = (*fh).clone();
    let task = rt.spawn(async move {
        crate::storage::parquet_exporter::compaction_loop(cfg, handles).await;
    });
    if let Ok(mut guard) = COMPACTION_TASK.lock() {
        guard.replace(task);
        return true;
    }
    false
}

#[cfg(feature = "fjall-backend")]
fn try_spawn_compaction(config: &Config) {
    if let Some(db) = &config.fjall {
        spawn_compaction_task(db);
    }
}

// ---------------------------------------------------------------------------
// Hot-path free functions (used by macros + middleware)
// ---------------------------------------------------------------------------

/// Dispatch without panicking when uninitialized (internal).
pub(crate) fn global_record(entry: MetricEntry) {
    if let Some(h) = GLOBAL.get() {
        h.record(entry);
    }
}

/// Direct counter API for non-macro call sites.
pub fn record_counter(name: &str, value: f64, labels: Vec<(String, String)>) {
    global_record(MetricEntry::new(name, value, MetricType::Counter, labels));
}

/// Direct gauge API for non-macro call sites.
pub fn record_gauge(name: &str, value: f64, labels: Vec<(String, String)>) {
    global_record(MetricEntry::new(name, value, MetricType::Gauge, labels));
}

/// Direct histogram API for non-macro call sites.
pub fn record_histogram(name: &str, value: f64, labels: Vec<(String, String)>) {
    global_record(MetricEntry::new(name, value, MetricType::Histogram, labels));
}

/// Render Prometheus text from the global registry.
#[cfg(feature = "prometheus-exporter")]
pub fn prometheus_text() -> Result<String> {
    GLOBAL.get().ok_or(Error::NotInitialized)?.prometheus_text()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_record_is_noop() {
        // Must never panic, even without init.
        record_counter("noop_total", 1.0, vec![]);
        record_gauge("noop_gauge", 1.0, vec![]);
        record_histogram("noop_hist", 1.0, vec![]);
    }
}
