//! Visitor counting with unique visitors by region.
//!
//! Two series, both dual-dispatched through [`crate::engine`] (Prometheus +
//! Fjall) like every other observation:
//!
//! * `visitors_total{region}` (counter) — every observed visit.
//! * `unique_visitors_estimate{region}` (gauge) — HyperLogLog estimate,
//!   refreshed by [`VisitorsTracker::snapshot`].
//!
//! # Why HyperLogLog, not a label per visitor?
//!
//! A `visitor_id` label would create one Prometheus series **per visitor** —
//! unbounded cardinality that breaks the registry and contradicts the
//! [`crate::types`] limits. Instead each region keeps a 4 KiB HyperLogLog
//! sketch (~1.6 % standard error) fed by a hash of the visitor key. Raw
//! identifiers are never stored.
//!
//! # Region source
//!
//! Regions come from CDN country headers ([`parse_region`]), never from a
//! GeoIP database: only enable this behind a proxy/CDN that sets and
//! sanitizes one of the trusted headers. Spoofable headers can skew
//! *totals*; uniques stay sane because distinct regions are capped.
//!
//! ```rust
//! use tonggeret::visitors::{VisitorsTracker, parse_region, visitor_key};
//!
//! let tracker = VisitorsTracker::new(512);
//! let region = parse_region(Some("DE"), None, None);
//! let key = visitor_key(Some("203.0.113.7, 70.41.3.18"), "203.0.113.7", "curl/8");
//! assert!(tracker.observe(&key, &region));
//! assert_eq!(tracker.estimate(&region), 1);
//! assert_eq!(tracker.snapshot(), 1);
//! ```

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

/// Counter series for total visits, labelled by `region`.
pub const VISITORS_TOTAL: &str = "visitors_total";
/// Gauge series for the per-region unique-visitor estimate.
pub const UNIQUE_VISITORS_ESTIMATE: &str = "unique_visitors_estimate";
/// Region label used when no trusted country header is present or valid.
pub const REGION_UNKNOWN: &str = "unknown";
/// Region bucket for excess distinct regions beyond the tracker's cap.
pub const REGION_OTHER: &str = "other";
/// Default cap on distinct regions held by the global tracker.
pub const DEFAULT_MAX_REGIONS: usize = 512;
/// Default snapshot interval suggested for [`spawn_snapshot_task`].
pub const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Region
// ---------------------------------------------------------------------------

/// Pick a region from CDN country headers (first valid wins).
///
/// Checks `CF-IPCountry`, then `X-Vercel-IP-Country`, then
/// `CloudFront-Viewer-Country`. Values are trimmed, uppercased, and must be
/// exactly two ASCII letters; anything else falls through, ending at
/// [`REGION_UNKNOWN`].
///
/// ```rust
/// use tonggeret::visitors::{REGION_UNKNOWN, parse_region};
///
/// assert_eq!(parse_region(Some("de"), None, None), "DE");
/// assert_eq!(parse_region(None, Some("过期"), None), REGION_UNKNOWN);
/// assert_eq!(parse_region(None, None, None), REGION_UNKNOWN);
/// ```
#[must_use]
pub fn parse_region(
    cf_ip_country: Option<&str>,
    vercel_ip_country: Option<&str>,
    cloudfront_viewer_country: Option<&str>,
) -> String {
    for raw in [cf_ip_country, vercel_ip_country, cloudfront_viewer_country]
        .into_iter()
        .flatten()
    {
        let code = raw.trim().to_ascii_uppercase();
        if code.len() == 2 && code.bytes().all(|b| b.is_ascii_alphabetic()) {
            return code;
        }
    }
    REGION_UNKNOWN.to_string()
}

/// Build the visitor identity key hashed into the HyperLogLog sketch.
///
/// Uses the first `X-Forwarded-For` entry (the original client as reported
/// by the proxy chain) or `peer` when absent, combined with the user agent
/// so visitors behind one NAT address still separate. The key itself is
/// only hashed — callers must not log or store it.
///
/// ```rust
/// use tonggeret::visitors::visitor_key;
///
/// assert_eq!(visitor_key(Some("203.0.113.7, 70.41.3.18"), "10.0.0.1", "curl"), "203.0.113.7\0curl");
/// assert_eq!(visitor_key(None, "10.0.0.1", "curl"), "10.0.0.1\0curl");
/// ```
#[must_use]
pub fn visitor_key(forwarded_for: Option<&str>, peer: &str, user_agent: &str) -> String {
    let ip = forwarded_for
        .and_then(|h| h.split(',').next())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(peer);
    format!("{ip}\0{user_agent}")
}

// ---------------------------------------------------------------------------
// HyperLogLog (p = 12, std-only)
// ---------------------------------------------------------------------------

/// Precision: 2¹² registers ≈ 4 KiB per sketch, ~1.6 % standard error.
const HLL_P: u32 = 12;
/// Register count.
const HLL_M: usize = 1 << HLL_P; // 4096
/// Register count as `f64` (exact: 4096 « 2⁵³, so no precision is lost).
const HLL_M_F64: f64 = 4096.0;

/// Minimal HyperLogLog sketch over hashed visitor keys.
///
/// Not a general-purpose implementation: fixed precision, `u8` registers,
/// and [`std`] hashing (per-process SipHash keys are fine — sketches are
/// ephemeral and merged only within this process).
#[derive(Debug, Clone)]
pub struct Hll {
    registers: Box<[u8; HLL_M]>,
}

impl Hll {
    /// Empty sketch (estimates zero).
    #[must_use]
    pub fn new() -> Self {
        Self {
            registers: Box::new([0; HLL_M]),
        }
    }

    /// Fold one pre-hashed visitor into the sketch.
    pub fn add_hash(&mut self, hash: u64) {
        let idx = (hash >> (64 - HLL_P)) as usize;
        let rho = (hash << HLL_P).leading_zeros() + 1;
        let rho = rho.min(u32::from(u8::MAX));
        // `idx` is the top 12 bits of a u64, always < 4096; `rho` is
        // clamped to `u8::MAX` above, so neither conversion can fail.
        if let Some(slot) = self.registers.get_mut(idx) {
            *slot = (*slot).max(u8::try_from(rho).unwrap_or(u8::MAX));
        }
    }

    /// Fold one visitor key into the sketch.
    pub fn add(&mut self, visitor_key: &str) {
        self.add_hash(hash_key(visitor_key));
    }

    /// Estimated distinct count (linear counting for small ranges).
    #[must_use]
    pub fn estimate(&self) -> u64 {
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2f64.powi(-i32::from(r)))
            .sum();
        let alpha = 0.7213 / (1.0 + 1.079 / HLL_M_F64);
        let raw = alpha * HLL_M_F64 * HLL_M_F64 / sum;
        if raw <= 2.5 * HLL_M_F64 {
            // Count in `u32` (≤ 4096 registers) so the `f64` step is exact.
            let zeros = self
                .registers
                .iter()
                .fold(0u32, |acc, &r| acc + u32::from(r == 0));
            if zeros > 0 {
                let linear = HLL_M_F64 * (HLL_M_F64 / f64::from(zeros)).ln();
                return saturating_u64(linear);
            }
        }
        saturating_u64(raw)
    }
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

fn hash_key(key: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

/// Saturating `f64` → `u64` for non-negative estimates. The `as` cast is
/// total here (NaN → 0, overflow → `u64::MAX`) and estimates are ≥ 0 by
/// construction, so the pedantic cast lints are explicitly waived.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn saturating_u64(value: f64) -> u64 {
    value.round() as u64
}

// ---------------------------------------------------------------------------
// Tracker
// ---------------------------------------------------------------------------

/// Per-region HyperLogLog visitor tracker.
///
/// [`observe`](Self::observe) counts the visit in
/// [`VISITORS_TOTAL`] and folds the visitor key into the region's sketch;
/// [`snapshot`](Self::snapshot) exports each region's estimate to
/// [`UNIQUE_VISITORS_ESTIMATE`]. Both dispatches are non-blocking and
/// silent no-ops until [`crate::init`].
#[derive(Debug)]
pub struct VisitorsTracker {
    inner: Mutex<HashMap<String, Hll>>,
    max_regions: usize,
}

impl VisitorsTracker {
    /// New tracker holding at most `max_regions` distinct regions
    /// (clamped to 1..=4096; excess regions fold into [`REGION_OTHER`]).
    #[must_use]
    pub fn new(max_regions: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_regions: max_regions.clamp(1, 4096),
        }
    }

    /// Record one visit: total counter + unique sketch.
    ///
    /// Returns `true` when the unique sample was folded in, `false` when the
    /// sketch lock was contended (the total is still counted — same
    /// drop-philosophy as the engine channel).
    #[must_use]
    pub fn observe(&self, visitor_key: &str, region: &str) -> bool {
        let region = normalize_region(region);
        crate::engine::record_counter(
            VISITORS_TOTAL,
            1.0,
            vec![("region".to_string(), region.clone())],
        );
        let Ok(mut map) = self.inner.try_lock() else {
            return false;
        };
        let bucket = if !map.contains_key(&region) && map.len() >= self.max_regions {
            REGION_OTHER.to_string()
        } else {
            region
        };
        map.entry(bucket).or_default().add(visitor_key);
        true
    }

    /// Current estimate for one region (0 when unseen or contended).
    #[must_use]
    pub fn estimate(&self, region: &str) -> u64 {
        let Ok(map) = self.inner.try_lock() else {
            return 0;
        };
        map.get(&normalize_region(region)).map_or(0, Hll::estimate)
    }

    /// Export every region's estimate to [`UNIQUE_VISITORS_ESTIMATE`].
    ///
    /// Returns the number of regions exported (0 when contended).
    #[must_use]
    pub fn snapshot(&self) -> usize {
        let Ok(map) = self.inner.try_lock() else {
            return 0;
        };
        for (region, sketch) in map.iter() {
            // Precision loss past 2⁵³ is irrelevant: the estimate itself
            // carries ~1.6 % error and gauges are `f64` by API.
            #[allow(clippy::cast_precision_loss)]
            let value = sketch.estimate() as f64;
            crate::engine::record_gauge(
                UNIQUE_VISITORS_ESTIMATE,
                value,
                vec![("region".to_string(), region.clone())],
            );
        }
        map.len()
    }

    /// Distinct regions currently held (0 when contended).
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.inner.try_lock().map_or(0, |map| map.len())
    }
}

fn normalize_region(region: &str) -> String {
    let code = region.trim().to_ascii_uppercase();
    if code.len() == 2 && code.bytes().all(|b| b.is_ascii_alphabetic()) {
        return code;
    }
    // Preserve the internal buckets (callers may pass them back in).
    if code == REGION_OTHER.to_ascii_uppercase() {
        return REGION_OTHER.to_string();
    }
    REGION_UNKNOWN.to_string()
}

// ---------------------------------------------------------------------------
// Global tracker + snapshot task
// ---------------------------------------------------------------------------

static GLOBAL_VISITORS: OnceLock<Arc<VisitorsTracker>> = OnceLock::new();

/// Process-global tracker (created on first use with [`DEFAULT_MAX_REGIONS`]).
///
/// Mirrors the engine's global-handle pattern: cheap `Arc` clone, safe to
/// share across handlers. For isolated state (tests), construct
/// [`VisitorsTracker`] directly instead.
#[must_use]
pub fn global_tracker() -> Arc<VisitorsTracker> {
    GLOBAL_VISITORS
        .get_or_init(|| Arc::new(VisitorsTracker::new(DEFAULT_MAX_REGIONS)))
        .clone()
}

/// Record one visit on the global tracker (see [`VisitorsTracker::observe`]).
#[must_use]
pub fn observe_visitor(visitor_key: &str, region: &str) -> bool {
    global_tracker().observe(visitor_key, region)
}

/// Export global estimates (see [`VisitorsTracker::snapshot`]).
#[must_use]
pub fn snapshot_visitors() -> usize {
    global_tracker().snapshot()
}

/// Periodically [`snapshot`](VisitorsTracker::snapshot) on the global tracker.
///
/// Spawns onto the *current* Tokio runtime (no new OS threads) and returns
/// the task handle for shutdown; returns `None` when called outside a
/// runtime — then call [`snapshot_visitors`] manually (e.g. from your own
/// interval task), like [`crate::engine::spawn_compaction_task`].
pub fn spawn_snapshot_task(interval: Duration) -> Option<tokio::task::JoinHandle<()>> {
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!(
            "no Tokio runtime; visitor snapshot not started (call snapshot_visitors manually)"
        );
        return None;
    }
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let _ = snapshot_visitors();
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_estimates_known_cardinality() {
        let mut hll = Hll::new();
        assert_eq!(hll.estimate(), 0);
        for i in 0..10_000 {
            hll.add(&format!("visitor-{i}"));
        }
        let est = hll.estimate();
        let err = (f64::from(u32::try_from(est).unwrap_or(u32::MAX)) - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.05, "estimate {est} too far from 10000");
    }

    #[test]
    fn hll_ignores_duplicates() {
        let mut hll = Hll::new();
        for _ in 0..1_000 {
            hll.add("same-visitor");
        }
        assert_eq!(hll.estimate(), 1);
    }

    #[test]
    fn tracker_counts_totals_and_uniques_per_region() {
        let tracker = VisitorsTracker::new(512);
        for i in 0..100 {
            assert!(tracker.observe(&format!("de-{i}"), "DE"));
            assert!(tracker.observe(&format!("us-{i}"), "US"));
        }
        assert!(tracker.observe("de-0", "DE")); // duplicate
        assert_eq!(tracker.region_count(), 2);
        let de = tracker.estimate("DE");
        assert!((95..=105).contains(&de), "DE estimate {de}");
        assert_eq!(tracker.snapshot(), 2);
    }

    #[test]
    fn tracker_caps_regions_into_other() {
        let tracker = VisitorsTracker::new(2);
        assert!(tracker.observe("a", "AA"));
        assert!(tracker.observe("b", "BB"));
        assert!(tracker.observe("c", "CC")); // over cap → other
        assert_eq!(tracker.region_count(), 3); // AA, BB, other
        assert_eq!(tracker.estimate("CC"), 0);
        assert!(tracker.estimate(REGION_OTHER) >= 1);
    }

    #[test]
    fn invalid_regions_normalize_to_unknown() {
        assert_eq!(normalize_region(""), REGION_UNKNOWN);
        assert_eq!(normalize_region("USA"), REGION_UNKNOWN);
        assert_eq!(normalize_region("1A"), REGION_UNKNOWN);
        assert_eq!(normalize_region(" de "), "DE");
        let tracker = VisitorsTracker::new(512);
        assert!(tracker.observe("x", "!!!"));
        assert_eq!(tracker.estimate(REGION_UNKNOWN), 1);
    }
}
