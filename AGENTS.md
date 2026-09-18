# AGENTS.md — tonggeret contributor guide

> Crate `tonggeret` in this dir; GitHub remote is
> `maulanasly/tonggeret` (public).

## What this is

Ultra-low-memory Rust telemetry: embedded Fjall LSM-tree (<10 MiB RAM)
for local ingestion + Prometheus exposition + Parquet cold storage,
behind a non-blocking lock-free hot path.

* Dual-mode: every `counter!` / `gauge!` / `histogram!` updates Prometheus
  atomics **and** streams to Fjall via bounded channel → writer thread → LSM.
* Pure Rust, `forbid(unsafe_code)`: `fjall` + `parquet` + `arrow` only.
* Axum (`from_fn`) + Actix-web (`Transform`) middleware:
  `http_requests_total` + `http_request_duration_ms`.

## Commands

```bash
cargo build
cargo test                                        # default features (axum + prometheus)
cargo test --features fjall-backend               # full dual-mode
cargo test --all-features                         # includes actix
cargo run --example axum_server                   # Prometheus-only
cargo run --example axum_server --features fjall-backend
cargo clippy --all-targets --features fjall-backend -- -D warnings
cargo fmt && cargo fmt --check
cargo doc --no-deps --open
```

MSRV 1.85+, edition 2024. `cargo clippy` pedantic is enforced
(see `Cargo.toml` `[lints.clippy]`).

## Architecture map

* `src/lib.rs` — macros `counter!/gauge!/histogram!`, re-exports, crate docs.
* `src/engine.rs` — `OnceLock<Arc<EngineHandle>>` global, `try_send`-only
  `record()`, `init()` / `shutdown()` / `spawn_compaction_task()`.
* `src/config.rs` — `Config{channel_capacity:16384, fjall:Option, prometheus}`,
  `FjallConfig{dir, cache 8MiB, memtable 2MiB, retention 24h, compaction 1h, batch 1000}`.
* `src/types.rs` — `MetricEntry`, `MetricType`, sanitization
  (`[a-zA-Z_:][a-zA-Z0-9_:]*`, values ≤256 chars, ≤16 pairs).
* `src/prometheus.rs` — lazy `CounterVec/GaugeVec/HistogramVec`, `tonggeret_dropped_total`.
* `src/storage/fjall_engine.rs` — single `open_handles()`, key
  `{micros:016x}:{seq:08x}:{name}`, JSON value, `writer_loop`.
* `src/storage/parquet_exporter.rs` — hourly `metrics_cold_*.parquet` ZSTD,
  rename-then-purge. `SCHEMA_VERSION` in `src/storage/mod.rs`.
* `src/middleware/axum.rs`, `src/middleware/actix.rs` — route-template labels,
  `/metrics`, `GET /telemetry/parquet`.
* `tests/{engine_noop,fjall_roundtrip,prometheus_export}.rs`,
  `examples/{axum_server.rs,embedded_dashboard.html}`.

Feature flags: `prometheus-exporter` (default on), `axum` (default on),
`fjall-backend` (default off), `actix` (off).

## Invariants (do not break)

* Hot path never `.await` / never blocks: `try_send` only. Full channel ⇒
  drop for Fjall, still count in Prometheus + `tonggeret_dropped_total`.
* Uninitialized ⇒ silent no-op. `init()` once, `shutdown()` once at exit.
* Exactly one Fjall `open_handles()` per process; share via `Arc`. Never open same dir twice.
* `SCHEMA_VERSION` bump + `CHANGELOG.md` entry on any key/value/Parquet breaking change.
* `forbid(unsafe_code)`, clippy pedantic clean, `///` docs + doctests on public API.
* Label cardinality: route templates (`MatchedPath`), never raw IDs / tokens / user IDs.

## Graphify context memory (local-only, gitignored)

`graphify-out/` is **local agent memory, never committed** (see `.gitignore`).
Rebuild locally with `/graphify .`. It produces `graph.html`, `GRAPH_REPORT.md`,
`graph.json` — queryable structure + audit trail.

```bash
/graphify .                  # full build on this repo
/graphify . --update         # incremental after edits
graphify query "<question>"  # BFS broad context from existing graph
graphify query "<q>" --dfs   # trace a specific path
graphify path "EngineHandle" "FjallHandles"
graphify explain "writer_loop"
graphify . --watch           # auto-rebuild on change (optional)
graphify export html         # interactive viz (optional)
```

If `graphify-out/graph.json` exists: **query it first, don't rebuild**.
Cite `source_location` when using graph facts. Never invent edges.

## MANDATORY WORKFLOW — always run these steps in order

### 1. Always create a new branch for every new feature / bug fix

Never commit directly to `main`.

```bash
git checkout main && git pull --ff-only
git checkout -b feat/<short-slug>   # feature
git checkout -b fix/<short-slug>    # bug fix
git checkout -b docs/<short-slug>   # docs-only
git checkout -b chore/<short-slug>  # tooling
```

One branch = one concern. Rebase on `main` if stale, resolve conflicts locally.

### 2. Read context memory before touching code

1. If `graphify-out/graph.json` exists: run
   `graphify query "<task in plain words>"` first. Follow up with
   `graphify path` / `graphify explain` for hot-path → engine → storage links.
2. Else read: `GRAPH_REPORT.md` (if present), `src/lib.rs`, `src/engine.rs`,
   plus the directly touched module (`config|types|storage/*|middleware/*`).
3. Check `CHANGELOG.md` for the current version contract and whether your
   change is breaking (`SCHEMA_VERSION`, `Config` renames, Parquet schema).

Do not write code without completing this step.

### 3. Always check for a better approach — think like a principal engineer

Before implementing, write down (in PR description or comments for non-trivial changes):

* 2–3 candidate approaches + tradeoffs: RAM budget (<10 MiB steady-state?),
  hot-path blocking risk, API break surface, `SCHEMA_VERSION` impact,
  operability (retention/compaction/disk growth).
* Why the chosen approach is simplest that preserves invariants in
  “Invariants” above.
* What you explicitly decided **not** to do and why.

Prefer: boring + lock-free + tested over clever. No new background threads,
no new channel types, no SQL engine, no `unsafe`, no unbounded labels —
unless justified in writing.

### 4. Avoid sloppy code

* `cargo fmt` clean; `cargo clippy --all-targets --features fjall-backend -- -D warnings` clean.
* No `unwrap()`/`expect()` in non-test code; use `thiserror` `Error`.
* No dead code, no commented-out blocks, no `println!` (use `tracing`).
* Public API: `///` docs + doctest example; keep doctests runnable offline.
* Respect existing naming: `Config::default_light/default_full`,
  `with_fjall`, `has_fjall`, `dropped_count`, key codec helpers.
* Keep truncations/sanitization behavior; add regression test if you touch them.

### 5. Add tests and run them before commit

* New behavior ⇒ new test. Bug fix ⇒ failing-first regression test.
* Put integration coverage in `tests/` (`engine_noop`, `fjall_roundtrip`,
  `prometheus_export` are the models); unit tests next to code in `#[cfg(test)]`.
* Run both suites before every commit:

```bash
cargo test
cargo test --features fjall-backend
```

Doctests count: `cargo test --doc` is included above. All green or don't commit.

### 6. Run pre-commit

Preferred (framework):

```bash
pre-commit run --all-files
```

Fallback when the framework isn't installed (documents the “1 and 2” policy):

```bash
cargo fmt --check
cargo clippy --all-targets --features fjall-backend -- -D warnings
cargo test --features fjall-backend
```

Config lives in `.pre-commit-config.yaml` (fmt + clippy + test as local hooks).
Fix hook failures, re-stage, re-run until green.

### 7. Submit PR

```bash
git push -u origin feat/<slug>
gh pr create --fill   # or gh pr create --title "..." --body "..."
```

PR body must include: what/why, approach tradeoff note from step 3,
test evidence (`cargo test` outputs), breaking-change + `SCHEMA_VERSION`
impact, screenshots/`curl` output for middleware/route changes.
Wait for checks. Address review; never force-push to `main`, never merge your
own PR without green CI (when CI exists).

### 8. Ask for release if necessary

After PR merge, ask the user: “Is a release necessary?”

Release only on explicit yes:

1. Bump `version` in `Cargo.toml` (semver) + update `CHANGELOG.md`
   (Keep-a-Changelog: `Added/Changed/Fixed`).
2. Commit on a `chore/release-vX.Y.Z` branch → PR → merge.
3. Tag + GitHub release:

```bash
git checkout main && git pull --ff-only
git tag -a vX.Y.Z -m "tonggeret vX.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --generate-notes
```

Patch = fix, minor = additive, major = breaking (incl. `SCHEMA_VERSION` bump).

## Troubleshooting for agents

* `fjall config supplied but fjall-backend feature is not enabled` → enable feature.
* `/metrics` 503 → `init()` not called (macros no-op by design).
* `/telemetry/parquet` 404 → no export yet (compaction hourly, only keys older than `retention`).
* Compaction never runs → `init` ran before Tokio runtime; call `spawn_compaction_task()` inside runtime.
* `arrow-arith` + `chrono` build failure → keep pinned `chrono = "=0.4.38"`.
