# ARCHITECTURE.md — Design Decisions & System Structure

Governed by rules in `AGENTS.md`. This file is a **living document** — append, never silently rewrite. Not auto-loaded by Codex — read explicitly when a task touches architecture.

---

## DESIGN DECISION LOG (ADRs)

Every major architectural decision gets a numbered entry. Template:

```
### ADR-XXX: <title>
Status: Proposed | Accepted | Superseded by ADR-YYY
Date:
Risk level: LOW | MEDIUM | HIGH | CRITICAL

Problem:
Alternatives considered:
Chosen solution:
Reasoning:
Tradeoffs accepted:
Consequences for future modules:
Modules affected:
Research references:
```

**Never silently revise a past decision.** If a prior ADR turns out to be wrong or outdated, write a new ADR that explicitly supersedes it — the old one stays in the log with a "superseded by ADR-XXX" marker.

### ADR-001: Select Milvus as the primary range/threshold-query research backend

Status: Accepted
Date: 2026-08-01
Risk level: HIGH
Evidence status: SUPPORTED for documented capabilities; INFERRED for comparative operational and reproducibility conclusions; no performance claim is VERIFIED until an EXP entry records local measurements.

Problem:

The Core scope requires one primary vector database backend for continuous range/threshold-query tuning under workload drift. The backend must expose correct threshold semantics, query-time controls that can be changed without rebuilding the index, a usable Python client, a reproducible local deployment, and enough experimental surface to measure recall/latency tradeoffs. Selecting the wrong backend would constrain the research question or introduce infrastructure noise into benchmark results.

Decision drivers:

1. Correct range/threshold-query semantics for L2, inner product, and cosine metrics.
2. Runtime tunability of HNSW search breadth (`ef`/`hnsw_ef`) and, where available, IVF probe count (`nprobe`) without index rebuilds.
3. Mature, scriptable Python client support.
4. Version-pinnable Docker deployment with controllable resource limits.
5. Research reproducibility: explicit independent variables, exact ground-truth path, low hidden state, and repeatable environment capture.
6. Per `AGENTS.md`: complexity, scalability, memory, latency, and research support.

Alternatives considered:

| Criterion | Option A — Qdrant | Option B — Milvus | Assessment |
|---|---|---|---|
| Range query support | `score_threshold` filters dense-search results by metric-aware score, but the query remains bounded by `limit`. | Explicit range-search parameters `radius` and `range_filter`, with metric-specific boundary rules; results are also bounded by top-K. | Milvus has the clearer first-class range/annulus model. Both require an explicit result cap, which the benchmark contract must record. |
| Runtime parameter tunability | `hnsw_ef` is supplied in per-query search parameters; changing it does not rebuild the index. Qdrant does not provide an IVF `nprobe` axis for its primary HNSW path. | HNSW `ef` is a search-only parameter; IVF `nprobe` is a search parameter in `[1, nlist]`. Both can vary per request without rebuilding. | Milvus provides two independent runtime tuning axes and supports comparison across HNSW and IVF families. |
| Python client maturity | Official `qdrant-client` supports synchronous and asynchronous clients with matching methods and provides a convenient local mode. | Official PyMilvus provides the synchronous `MilvusClient`; an async client exists, but its documented maturity and API coverage must be verified against the pinned server/client release. | Qdrant has the ergonomics edge. Both are adequate for a synchronous benchmark harness. Qdrant local mode must not substitute for server-mode benchmark evidence. |
| Docker setup complexity | One server container plus one persistent volume is sufficient for local server-mode experiments. | Standalone Docker Compose conventionally runs Milvus with etcd and MinIO, creating more services, volumes, health states, and resource variables. | Qdrant is materially simpler to operate and reproduce. |
| Research reproducibility | Fewer services reduce environmental variance; exact search can provide a ground-truth path. The main runtime ANN control is HNSW breadth. | More infrastructure must be pinned, but explicit range semantics, FLAT exact search, HNSW `ef`, IVF `nprobe`, and range-specific controls provide a richer controlled experiment surface. | Qdrant wins environment simplicity; Milvus wins construct validity and independent-variable coverage. Milvus is preferred because correctness and research validity outrank convenience. |
| Complexity | Low deployment and client complexity. | Medium deployment complexity and a broader parameter/index matrix that requires stricter experiment governance. | Qdrant advantage. |
| Scalability | Supports distributed deployment, but the Core study does not require multi-node operation. | Standalone supports the Core study and the architecture has a cluster path if later evidence requires scale-out. | No decision-driving advantage for Core; all Core experiments remain single-node. |
| Memory | HNSW graph and optional quantization create workload-dependent memory costs. | HNSW has graph overhead; IVF and FLAT provide alternative memory/performance profiles, while Milvus dependencies add baseline memory overhead. | No winner without measurement. Memory is a required benchmark response variable. |
| Latency | `hnsw_ef` exposes the expected recall/latency tradeoff. | HNSW `ef` and IVF `nprobe` expose recall/latency tradeoffs across two index families. | No performance winner is claimed before benchmarking; Milvus offers the broader tunable design space. |
| Research support | Official documentation supports threshold filtering, per-query HNSW breadth, and exact search. | Official documentation defines range search, HNSW search breadth, IVF probing, FLAT exact search, and a range-specific termination control. | Milvus better matches the Core experimental construct. |

Chosen solution:

Use **Milvus Standalone as the primary Core backend**, initially through the synchronous PyMilvus client. Start with two controlled index tracks:

1. `FLAT` for exact ground truth and semantic validation.
2. One approximate track at a time: HNSW with runtime `ef`, followed by IVF_FLAT with runtime `nprobe` only after the HNSW experiment contract is verified.

The exact Milvus, PyMilvus, Docker image, etcd, and MinIO versions must be pinned in the experiment environment before EXP-001. Automatic adaptation is initially restricted to per-query parameters (`ef` or `nprobe`). Index-build parameters such as `M`, `efConstruction`, and `nlist` require rebuilds and are not eligible for live automatic actuation under this ADR.

Reasoning:

1. **Correctness and research validity:** Milvus models the target operation directly with `radius` and `range_filter`, reducing semantic translation between the research question and the backend API.
2. **Experimental leverage:** runtime `ef` and `nprobe` offer distinct, falsifiable tuning dimensions without rebuild latency contaminating the online-adaptation loop.
3. **Ground truth:** FLAT provides an exact baseline within the same backend, reducing cross-system differences when calculating recall.
4. **Reproducibility:** Milvus has more operational variables than Qdrant, but they are controllable through pinned images, committed Compose configuration, fixed resources, health checks, seeded datasets, and recorded checksums.
5. **Tradeoff:** Qdrant is the better choice for minimum operational complexity, but convenience ranks below correctness and research validity in `AGENTS.md`.

Tradeoffs accepted:

- Higher local resource use and more failure modes from Milvus, etcd, and object storage.
- More version-compatibility work between server and PyMilvus.
- A larger index/parameter design space that must be constrained to prevent invalid cross-experiment comparisons.
- A result `limit` remains part of range-search semantics; experiments must treat threshold and result cap as separate workload variables.
- Qdrant remains an unimplemented fallback, not a second Core backend; multi-backend policy transfer remains Future Work.

Consequences for future modules:

- Define a backend port so workload generation, drift detection, policy logic, and metrics do not import PyMilvus types directly.
- Register `radius`, `range_filter`, `limit`, HNSW `ef`, and IVF `nprobe` in the Configuration Registry before policy actuation.
- Treat metric-specific threshold direction and boundary inclusion as contract-tested behavior.
- The safe actuation layer must retain the last-known-good per-query parameter set; rollback for `ef`/`nprobe` is a request-configuration restore, not an index rebuild.
- Keep index-build changes recommendation-only until a later ADR defines asynchronous rebuild, validation, cutover, and rollback.
- Do not mark the Milvus compatibility-matrix row verified from documentation alone; populate it only after direct integration tests.

Benchmark and validation plan:

1. Pin all container images and Python dependencies; record host CPU, memory limit, storage, Docker version, and configuration checksums.
2. Build a deterministic seeded dataset with an exact distance implementation independent of Milvus.
3. Verify L2 and cosine threshold boundaries against both the independent oracle and Milvus FLAT, including empty, all-match, threshold-equality, and result-cap cases.
4. Hold dataset, query stream, index build, resources, and result cap constant while sweeping HNSW `ef`; then repeat separately for IVF `nprobe`.
5. Confirm by index metadata and elapsed rebuild monitoring that runtime sweeps do not rebuild or replace the index.
6. Run repeated trials after cold and warm starts; record latency distribution, recall, QPS, memory, result cardinality, and run-to-run variance in `EXPERIMENT_LOG.md`.
7. Do not freeze this decision until the smoke benchmark and failure tests pass with raw output reviewed.

Rollback plan:

- Before data or backend-specific code becomes production-critical, preserve Qdrant as the fallback candidate behind the same backend port.
- If Milvus fails semantic contract tests, cannot vary `ef`/`nprobe` without rebuild, or produces unacceptable run-to-run variance after resource pinning, write a superseding ADR selecting Qdrant.
- No dataset migration is required during Phase 1: regenerate the versioned benchmark dataset from its source manifest and seed.

Modules affected:

Backend adapter; benchmark harness; workload monitor; tuning policy; safe actuation layer; configuration registry; experiment environment.

Research references:

- [Milvus range search](https://milvus.io/docs/range-search.md)
- [Milvus HNSW parameters](https://milvus.io/docs/hnsw.md)
- [Milvus in-memory index and IVF parameters](https://milvus.io/docs/index.md)
- [Milvus Docker configuration](https://milvus.io/docs/configure-docker.md)
- [PyMilvus installation and version compatibility](https://milvus.io/docs/install-pymilvus.md)
- [Qdrant search, score threshold, and query parameters](https://qdrant.tech/documentation/search/search/)
- [Qdrant local Docker quickstart](https://qdrant.tech/documentation/quick-start/)
- [Qdrant Python async API](https://qdrant.tech/documentation/database-tutorials/async-api/)

---

## BACKEND COMPATIBILITY MATRIX

| Backend | Index types | Distance metrics | Filter support | Update/delete | Persistence | GPU | Limitations | Implementation status | Benchmark status |
|---|---|---|---|---|---|---|---|---|---|
| Qdrant | — | — | — | — | — | — | — | not started | not run |
| Milvus | — | — | — | — | — | — | — | not started | not run |

**Never assume feature parity across backends.** Check this matrix before generalizing a tuning policy across backends. Fill in rows as each backend is actually integrated and verified — checked directly, not from documentation alone.

---

## CONFIGURATION REGISTRY

Per Configuration Governance in `AGENTS.md` — every tunable parameter gets an entry here before the policy is allowed to set it.

```
### <parameter name>
Type:
Default:
Valid range:
Validation rule:
Dependencies:
Risk level:
Rollback behavior:
Research reference:
```

The entries below authorize EXP-001 configuration only. They do **not** authorize live automatic actuation. General backend ranges remain documentation-supported until direct contract tests verify them.

### EXP-001 pinned environment (ENV-001)

These are non-tunable execution pins. Versions, image references, and digests are copied verbatim from `artifacts/exp-001/environment/ENV-001_PROVISIONING.md`, `infra/milvus/env-001/compose.vendor.yml`, and `infra/milvus/env-001/compose.override.yml`; no registry lookup or tag re-resolution is used here.

| Component | EXP-001 pin | Recorded evidence / validation |
|---|---|---|
| Milvus connection | `http://localhost:19530`; health endpoint `http://localhost:9091/healthz` | Endpoints exercised by the ENV-001 persistence and health probe. |
| Milvus server | `3.0.0`; build commit `f46a032855` | Live `milvus_build_info` output in ENV-001 evidence. |
| PyMilvus | `3.0.1` | Isolated compatibility probe connected to the recorded endpoint, returned server `3.0.0`, listed collections, and ended with `compatibility_probe=PASS`; this confirms control-plane compatibility for EXP-001 setup, not the future benchmark lockfile. |
| Milvus image | Vendor tag `milvusdb/milvus:v3.0.0`; pinned index digest `sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862`; `linux/arm64` manifest `sha256:bfab7739a0479cd81ffdf5e473f88c5b143678c2520a06a19f86f35ecd586cad` | Tag from vendor Compose; immutable references and local architecture from ENV-001 evidence/override. |
| etcd image | Vendor tag `quay.io/coreos/etcd:v3.5.25`; pinned index digest `sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366`; `linux/arm64` manifest `sha256:8da34a9df5dc1bd879bea716a301113c4e49b6bbdbe5778214707c6043ccf65d` | Tag and effective service configuration from vendor Compose plus the digest/resource/volume override. |
| MinIO image | Vendor tag `minio/minio:RELEASE.2024-05-28T17-19-04Z`; pinned index digest `sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08`; `linux/arm64` manifest `sha256:fa7be14ee3f914469274c5dfc05949e0092500a71de4681f1f1b6b39275a13b1` | Tag and effective service configuration from vendor Compose plus the digest/resource/volume override. |
| Docker runtime | Docker Desktop `4.84.0` build `234817`; Engine `29.6.2`; Compose `v5.3.1` | Live version output in ENV-001 evidence. |
| Compose artifacts | Vendor SHA-256 `4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1`; override SHA-256 `bd97b91052ac642593c0af33aa7e90519e472a168d4ada48ba71f0846a4ee8c6`; effective-config SHA-256 `76310aee683a1dab714679f0f9202bc193ad87019e2e8bbf3c25fb46454ea217` | Recorded before stack startup in ENV-001 evidence. |
| Resource controls | Docker VM: 6 vCPU, 6 GiB RAM, 2 GiB swap; Milvus: 4 CPU/4 GiB; etcd: 1 CPU/512 MiB; MinIO: 1 CPU/1 GiB; no `cpuset` key is configured in the source/override Compose | Live daemon allocation and container `NanoCpus`/memory values in ENV-001 evidence; Compose files record the configured limits. |

The detailed EXP-001 tunable registry follows. Its fixed HNSW build identity is `M=16` and `efConstruction=200`; its only HNSW query-time sweep is `ef in {100, 200, 400, 800, 1600}`. Any other value or any rebuild between `ef` values is out of contract.

### metric_type
Type: Enum
Default: No hidden default; EXP-001 runs separate `L2` and `COSINE` collections.
Valid range: EXP-001 allowlist `{L2, COSINE}`.
Validation rule: Collection metric, FLAT metric, HNSW metric, query metric, independent-oracle function, and threshold-direction rule must match exactly. Reject mixed metrics before collection creation.
Dependencies: Determines valid `radius`/`range_filter` ordering and oracle calculation.
Risk level: HIGH — a mismatch silently invalidates recall and threshold semantics.
Rollback behavior: Drop the invalid experiment-scoped collection and recreate it with the recorded metric; never mutate metric identity in place.
Research reference: ADR-001; EXP-001 Query contract; [Milvus metric types](https://milvus.io/docs/metric.md).

### index_type
Type: Enum
Default: No hidden default; EXP-001 tracks are `FLAT` and `HNSW`.
Valid range: EXP-001 allowlist `{FLAT, HNSW}`.
Validation rule: FLAT is the exact reference track; HNSW is the approximate track. IVF and all other index families are rejected for EXP-001.
Dependencies: HNSW requires `M`, `efConstruction`, and query-time `ef`; FLAT accepts none of those parameters.
Risk level: HIGH — mixing index tracks invalidates attribution.
Rollback behavior: Drop and recreate the experiment-scoped collection/index from DATASET-001 artifacts.
Research reference: ADR-001 Chosen solution; EXP-001 Index tracks; [Milvus index overview](https://milvus.io/docs/index.md).

### radius
Type: Float
Default: Required; no implicit default. Values are calibrated and frozen from DATASET-001's 50 calibration queries before measured queries.
Valid range: L2 `(0, +inf)`; COSINE `[-1.0, 1.0)` for EXP-001.
Validation rule: L2 uses `0.0 <= distance < radius`; COSINE uses `radius < score <= 1.0`. Reject NaN, infinities, out-of-range cosine values, and values not present in the immutable run manifest.
Dependencies: `metric_type`, `range_filter`, threshold-calibration artifact, numeric comparison tolerance.
Risk level: HIGH — threshold errors directly invalidate the primary research construct.
Rollback behavior: Restore the last manifest-approved query configuration; no index rebuild is required.
Research reference: ADR-001; EXP-001 Query contract; [Milvus range search](https://milvus.io/docs/range-search.md).

### range_filter
Type: Float
Default: L2 `0.0`; COSINE `1.0`.
Valid range: EXP-001 fixed mapping `{L2: 0.0, COSINE: 1.0}`.
Validation rule: For L2 require `range_filter < radius`; for COSINE require `radius < range_filter <= 1.0`. Reject any value differing from the fixed EXP-001 mapping.
Dependencies: `metric_type`, `radius`.
Risk level: HIGH — reversed bounds can create empty or semantically inverted results.
Rollback behavior: Restore the metric-specific fixed value in the next request; no index rebuild is required.
Research reference: ADR-001; EXP-001 Query contract; [Milvus range search](https://milvus.io/docs/range-search.md).

### limit
Type: Integer
Default: `100`.
Valid range: EXP-001 fixed allowlist `{100}`; broader Milvus limits are out of contract.
Validation rule: Require exactly `100`; require HNSW `ef >= limit`; report full oracle cardinality and whether the returned result was capped.
Dependencies: `ef`, recall@threshold definition, result-cardinality metrics.
Risk level: HIGH — changing the cap changes the recall denominator and workload semantics.
Rollback behavior: Restore `100` in the next request; invalidate mixed-limit measurements rather than combining them.
Research reference: ADR-001 Tradeoffs accepted; EXP-001 Query contract and Metrics measured.

### consistency_level
Type: Enum
Default: `Strong` for EXP-001.
Valid range: EXP-001 allowlist `{Strong}`.
Validation rule: Set explicitly for every collection/query path where supported; do not rely on a server default. Begin measurements only after entity counts, index state, and load state are verified.
Dependencies: Completed ingestion, loaded collection, identical setting across FLAT and HNSW tracks.
Risk level: HIGH — stale/inconsistent reads can masquerade as ANN recall loss.
Rollback behavior: Abort the run and restart from clean experiment-scoped collections using `Strong` consistency.
Research reference: EXP-001 Execution protocol; [Milvus consistency](https://milvus.io/docs/consistency.md).

### HNSW.M
Type: Integer
Default: `16` for EXP-001.
Valid range: EXP-001 fixed allowlist `{16}`; documented backend range `[2, 2048]` is not an EXP-001 sweep.
Validation rule: Require exactly `16` in index metadata before measurements. Any change requires a new experiment ID and index rebuild.
Dependencies: `index_type=HNSW`; index construction.
Risk level: HIGH — changing graph degree changes memory, build cost, latency, and recall.
Rollback behavior: No in-place rollback. Recreate a clean HNSW collection with `M=16`; live automatic actuation is prohibited.
Research reference: ADR-001; EXP-001 Index tracks; [Milvus HNSW](https://milvus.io/docs/hnsw.md).

### HNSW.efConstruction
Type: Integer
Default: `200` for EXP-001.
Valid range: EXP-001 fixed allowlist `{200}`; other positive backend-supported values are outside this contract.
Validation rule: Require exactly `200` in index metadata before measurements. Any change requires a new experiment ID and index rebuild.
Dependencies: `index_type=HNSW`; `HNSW.M`; index construction.
Risk level: HIGH — changing construction breadth changes graph quality and invalidates cross-run comparison.
Rollback behavior: No in-place rollback. Recreate a clean HNSW collection with `efConstruction=200`; live automatic actuation is prohibited.
Research reference: ADR-001; EXP-001 Index tracks; [Milvus HNSW](https://milvus.io/docs/hnsw.md).

### HNSW.ef
Type: Integer
Default: No hidden default; every HNSW query must provide an explicit value.
Valid range: EXP-001 sweep allowlist `{100, 200, 400, 800, 1600}`.
Validation rule: Require membership in the allowlist and `ef >= limit`. Reject booleans, non-integers, and out-of-contract values before sending a request. Verify index identity is unchanged before and after the sweep.
Dependencies: `index_type=HNSW`; `limit=100`; loaded HNSW index.
Risk level: HIGH for EXP-001; CRITICAL if later permitted for live automatic actuation.
Rollback behavior: Restore the last-known-good explicit `ef` in the next request; no index rebuild is permitted or expected. Automatic rollback remains unauthorized until the safe-actuation layer is verified.
Research reference: ADR-001; EXP-001 Index tracks; [Milvus HNSW](https://milvus.io/docs/hnsw.md).

EXP-001 controls that are experiment metadata rather than database tunables remain governed by `EXPERIMENT_LOG.md`: DATASET-001, seed `20260801`, 50 calibration queries, 200 measured queries, five repetitions, deterministic ordering, one synchronous client, concurrency 1, warm-up protocol, timing boundaries, metrics, artifact paths, and failure checks.
