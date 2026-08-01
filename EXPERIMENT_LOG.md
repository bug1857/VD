# EXPERIMENT_LOG.md — Empirical Runs & Benchmark Results

Governed by rules in `AGENTS.md`. **Append-only** — never overwrite a past result. Not auto-loaded by Codex — read explicitly when a task touches benchmarking.

---

## BENCHMARK GOVERNANCE (rules — apply to every entry below)

Every benchmark result must be reported alongside: dataset used (Dataset ID, see `RESEARCH_PLAN.md`) · hardware specs · software versions (DB engine, driver, OS) · random seed · full configuration used · metrics measured · number of runs · confidence interval/variance · statistical significance where comparing · location of raw output · git commit hash · Docker image/environment identifier, OS, CPU, RAM.

**Never claim an improvement without a pasted, real measurement.** Never compare results from different benchmark environments without disclosing the difference.

---

## EXPERIMENT LOG

Template per entry — ADRs (in `ARCHITECTURE.md`) record *decisions*; EXPs record *empirical runs*, keep them separate:

```
### EXP-XXX: <short title>
Date:
Objective:
Hypothesis:
Configuration:
Dataset ID:
Hardware:
Git commit:
Random seed:
Metrics measured:
Raw output location:
Result:
Conclusion:
Follow-up actions:
```

**Never overwrite a past experiment's result.** If repeated (new seed, fixed bug, different config), it gets a new EXP ID even if "basically the same test."

### EXP-001: Milvus range/threshold-query smoke benchmark contract

Status: CONTRACT DEFINED — NOT RUN
Date: 2026-08-01
Risk level: HIGH (research-validity gate for ADR-001; no live actuation)

Objective:

Validate the minimum empirical contract required by ADR-001 before implementing the adaptive system:

1. Milvus FLAT range/threshold results match an independent exact-distance oracle.
2. Milvus HNSW returns only threshold-valid results and exposes a measurable recall/latency tradeoff as query-time `ef` changes.
3. Changing `ef` between requests does not rebuild or replace the HNSW index.
4. The harness can reproducibly emit recall, latency, throughput, and cardinality measurements from a version-pinned environment.

This smoke benchmark validates semantics and harness viability. It does not select an optimal `ef`, establish publication-quality performance, test workload drift, or authorize automatic actuation.

Hypothesis:

- **H1 — SUPPORTED:** FLAT results, under the same metric, threshold bounds, ordering, and result `limit`, will match the independent oracle exactly.
- **H2 — HYPOTHESIS:** Increasing HNSW `ef` will improve aggregate recall@threshold or leave it unchanged, at the cost of higher aggregate query latency; strict monotonicity is not required for every query or repetition.
- **H3 — SUPPORTED:** HNSW `ef` is a search-only parameter and can change per request without an index rebuild.
- **H4 — HYPOTHESIS:** With pinned software and resources, repeated measurements will be stable enough for p95-latency coefficient of variation to remain at or below 20% for each configuration.

Configuration:

#### Dataset specification

- **Dataset ID:** `DATASET-001` (reserved; add the formal entry to `RESEARCH_PLAN.md` before execution).
- **Source:** deterministic synthetic dense vectors generated locally; no external dataset download.
- **License:** project-generated data; repository licensing status must be recorded in the DATASET-001 entry before execution.
- **Dimensions:** 128.
- **Data type:** little-endian IEEE-754 `float32`.
- **Base vectors:** 10,000 samples from an independent standard normal distribution.
- **Queries:** 250 independent samples from the same distribution: 50 calibration queries and 200 disjoint measured queries.
- **Generator:** NumPy `Generator(PCG64(seed))`; exact NumPy version must be pinned.
- **Random seed:** `20260801` for dataset generation. Derive separate deterministic seeds from it for query ordering and configuration ordering; record all derived values in the run manifest.
- **Metrics:** run separate L2 and COSINE collections over the same generated vectors. The independent oracle uses float64 accumulation over the stored float32 vectors; cosine scores are computed from dot products and norms, with only final comparison values clamped to the metric's valid numeric range.
- **Threshold calibration:** using only the 50 calibration queries, select and freeze three thresholds per metric targeting median full-oracle cardinalities of approximately 5, 25, and 75. Persist the selected thresholds before measured queries run. Calibration queries must never appear in measured latency or recall samples.
- **Boundary fixtures:** add a separate deterministic micro-dataset with exact threshold-equality, empty-result, all-match, duplicate-distance, and result-cap cases. Boundary fixtures validate semantics only and are excluded from performance metrics.
- **Artifacts:** persist base vectors, calibration queries, measured queries, thresholds, and manifest; record SHA-256 checksums for every artifact before ingestion.

#### Query contract

- Fixed result `limit`: `100` for both index tracks and all `ef` values.
- L2 threshold interval: `0.0 <= distance < radius`; set `range_filter=0.0` and use the calibrated threshold as `radius`.
- COSINE threshold interval: `radius < score <= 1.0`; use the calibrated threshold as `radius` and `range_filter=1.0`.
- FLAT and HNSW must receive identical query vectors, metric, threshold parameters, output fields, consistency settings, and result `limit`.
- Do not request payloads or stored vectors in timed searches.
- Capture index identity/description before and after every `ef` sweep. Any observed rebuild, replacement, or index-identity change invalidates the run.

#### Index tracks

1. **FLAT — exact reference track**
   - Index type: `FLAT`.
   - Purpose: validate Milvus range semantics against the independent oracle and supply the capped exact reference set used for HNSW recall.
   - No query parameter sweep.
2. **HNSW — approximate track**
   - Index type: `HNSW`.
   - Fixed build parameters: `M=16`, `efConstruction=200`.
   - Query-time sweep: `ef in [100, 200, 400, 800, 1600]`.
   - `ef` must be at least the fixed result `limit`; invalid values must be rejected before a request is sent.
   - Build the HNSW index once per metric. Do not rebuild between `ef` values.

#### Execution protocol

1. Start from empty, uniquely named collections and verify Milvus health.
2. Generate and checksum the dataset artifacts, then ingest the same IDs and vectors into FLAT and HNSW collections.
3. Wait for ingestion/index completion and collection load; record entity counts and index metadata.
4. Run the independent oracle and FLAT semantic checks before any HNSW timing. Abort on disagreement.
5. For each metric/threshold/index/`ef` configuration, run one unmeasured 50-query warm-up pass followed by five measured repetitions of all 200 measured queries.
6. Randomize configuration order and measured-query order deterministically per repetition; persist the order in the manifest.
7. Use one synchronous client and one outstanding request for the primary smoke measurement. Do not mix concurrent-load results into EXP-001.
8. Start timing immediately before the client search call and stop after the complete response is materialized. Exclude connection setup, ingestion, index build, collection load, oracle computation, warm-up, and artifact writes.
9. Record every failed/timeout query. A configuration with any failed measured query is a failed smoke configuration and has no valid QPS comparison.
10. Deliberate failure checks: verify an `ef < limit` configuration is rejected locally, and verify a stopped/unreachable Milvus instance fails fast without writing a successful result record.

Dataset ID:

`DATASET-001` — deterministic synthetic 10k-base/250-query/128-dimensional dataset; reserved by this contract and not yet registered or generated.

Hardware:

TBD at execution. Record host model, CPU model/architecture, physical/logical core counts, RAM, storage type, OS/kernel, Docker resource allocation, and any CPU-frequency or power-mode controls. The run is invalid if CPU or RAM limits are omitted.

Environment pinning checklist:

- [ ] Milvus server version recorded exactly.
- [ ] PyMilvus version recorded exactly and confirmed compatible with the server version.
- [ ] Milvus Docker image tag and immutable digest recorded.
- [ ] etcd image tag, immutable digest, and effective configuration recorded.
- [ ] MinIO image tag, immutable digest, and effective configuration recorded.
- [ ] Docker Engine/Desktop and Docker Compose versions recorded.
- [ ] Compose file, Milvus configuration, and environment-file SHA-256 checksums recorded.
- [ ] Container CPU quota/cpuset and RAM limit recorded for Milvus, etcd, and MinIO.
- [ ] Host CPU model/architecture, core counts, RAM, storage, OS, and kernel recorded.
- [ ] Python and NumPy versions plus complete lockfile/environment export recorded.
- [ ] Dataset seed `20260801`, derived ordering seeds, generator algorithm, and artifact checksums recorded.
- [ ] Milvus collection schema, consistency level, metric, index parameters, query parameters, and result `limit` recorded.
- [ ] Background workloads disabled or disclosed; container health and resource snapshots captured before and after the run.
- [ ] Git commit hash and clean/dirty working-tree state recorded.

Git commit:

TBD at execution. Must identify the benchmark implementation and configuration commit, not this contract-only commit unless they are identical.

Random seed:

Primary seed `20260801`; derived seeds and derivation method must be recorded in the run manifest.

Metrics measured:

1. **Recall@threshold (`limit=100`):** for query `q`, threshold `t`, and HNSW result IDs `A`, compare against the ordered FLAT/oracle reference IDs `G` produced using the same threshold and cap: `|A ∩ G| / |G|`. If `G` is empty, recall is `1.0` only when `A` is also empty; otherwise `0.0`. Report mean, median, minimum, and 95% confidence interval across measured queries and repetitions. This is explicitly capped threshold recall, not recall over an unbounded range result.
2. **p50 and p95 latency:** client-observed per-query milliseconds, computed per repetition and summarized across five repetitions with mean, sample standard deviation, and 95% confidence interval.
3. **QPS:** `200 successful measured queries / measured wall-clock seconds` per repetition under concurrency 1; summarize across repetitions with mean, sample standard deviation, and 95% confidence interval.
4. **Result cardinality:** report returned count for FLAT and HNSW, full threshold-eligible count from the independent oracle, fraction of queries capped by `limit`, empty-result rate, and absolute HNSW-versus-reference count difference.
5. **Validity diagnostics:** failed query count, threshold violations, FLAT/oracle ID-set disagreements, index identity changes, warm/cold state, Milvus health, and container CPU/RAM snapshots.

Raw output location:

Planned: `artifacts/exp-001/<UTC-run-id>/`. The directory must contain an immutable manifest, raw per-query JSONL/Parquet results, summary tables, stdout/stderr, container logs, health/resource snapshots, environment exports, checksums, and the exact invocation. This path does not exist yet.

Acceptance criteria:

- Environment checklist complete; `DATASET-001` registered before execution.
- Independent oracle and FLAT agree on threshold validity, ordering, and capped ID sets for every semantic fixture and measured query.
- Every HNSW result satisfies the metric-specific threshold within a recorded numeric tolerance.
- All five `ef` values complete with zero failed measured queries and no HNSW rebuild/index-identity change.
- All required metrics and uncertainty summaries are emitted with raw records traceable to the manifest.
- p95-latency coefficient of variation is at most 20% per configuration; otherwise EXP-001 is inconclusive and the environment must be stabilized before performance interpretation.
- H2 is evaluated but is not a smoke-pass condition; non-monotonic aggregate recall/latency must be reported, not hidden.
- No claim of superiority, optimality, drift adaptation, or production readiness may be made from EXP-001.

Result:

NOT RUN — contract only. No measurements exist and no hypothesis is VERIFIED by this entry.

Conclusion:

Pending execution. ADR-001 remains accepted but not frozen until this contract is implemented and the smoke benchmark passes with raw output reviewed.

Follow-up actions:

1. Register `DATASET-001` in `RESEARCH_PLAN.md`, including licensing disposition and artifact checksum procedure.
2. Pin the Milvus/PyMilvus/Compose environment and record all tunable parameters in the `ARCHITECTURE.md` Configuration Registry.
3. Design the benchmark harness and semantic oracle against this contract; do not implement until separately authorized.
4. Execute as EXP-002 (or append a clearly immutable execution result under a new EXP ID) so this contract entry remains unchanged.
