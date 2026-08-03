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

### ADR-002: Define drift-detector and HNSW tuning-policy contracts

Status: Accepted
Date: 2026-08-01
Risk level: CRITICAL
Evidence status: INFERRED design contract. EXP-001 verifies the Milvus HNSW `ef` measurement values and their smoke-scale recall/latency tradeoff; ADR-002 narrows that measured set before actuation. EXP-001 does not verify this detector, policy, safety thresholds, rollback behavior, or production readiness.
Acceptance note: Design reviewed; implementation committed through `9442ea4`; `131/131` tests pass. Integration into the live benchmark harness is a separate task and remains pending.

Problem:

The Core system must detect workload drift in range/threshold queries and decide whether to change Milvus HNSW query-time `ef`. “Drift” cannot mean any unexplained metric movement: query-distribution change, threshold/selectivity change, ANN quality loss, backend failure, and index/data replacement have different causes and require different responses. A vague trigger would make both research claims and live actuation unfalsifiable.

This ADR defines the contracts before implementation. It does not authorize automatic tuning. Drift-trigger logic and live actuation are CRITICAL under `AGENTS.md`; implementation requires a separate reviewed experiment contract, rollback test, failure test, and explicit human approval.

Scope:

- Core: L2 and COSINE range/threshold queries on the ADR-001 Milvus HNSW backend, using query-time `ef` only.
- Excluded: changing thresholds, `limit`, metric, index type, `M`, `efConstruction`, collection/data identity, or consistency level; IVF, k-NN/ANN, hybrid, multi-tenant, and multi-backend policy transfer.
- The numerical gates below are proposed research-profile defaults. They are precise enough to test but are not VERIFIED findings and must not be silently retuned after evaluation begins.

Decision drivers:

1. Separate observable workload change from ANN/backend quality degradation.
2. Make every positive decision reproducible from immutable observations and explicit statistics.
3. Control family-wise false positives across multiple signals.
4. Fail closed when exact audit evidence or environment identity is missing.
5. Limit policy actions to the EXP-001-verified `ef` sweep and make rollback a request-configuration restore, never an index rebuild.
6. Satisfy the `AGENTS.md` safety gates: health checks, failure detection, validation, hard step limits, dry-run mode, audit logging, and tested rollback.

Alternatives considered:

| Criterion | Option A — operational signals only | Option B — composite statistical detector plus exact shadow audit | Option C — learned multivariate detector/policy | Assessment |
|---|---|---|---|---|
| Signals | Threshold and returned-cardinality shifts. | Query-vector, threshold, exact-cardinality, and fixed-`ef` recall signals, with input and quality drift separated. | A learned representation combines query and runtime telemetry. | B observes both workload geometry and realized quality without conflating their labels. |
| Advantages | Simple, low-cost, explainable. | Falsifiable, attribution-aware, auditable, and directly tied to range-query behavior. | May detect nonlinear interactions and gradual drift. | B best matches the current research question. |
| Disadvantages | Misses geometry changes and cardinality saturation at `limit`; cannot measure real recall. | Shadow FLAT/oracle work adds compute and storage cost; fixed tests may miss novel drift. | Requires representative labels/training data and introduces calibration and explanation risk. | A is underpowered; C adds unjustified scope before a baseline exists. |
| Complexity | Low. | Medium. | High. | B is acceptable for a CRITICAL module because its state is inspectable. |
| Scalability | High, but weak signal quality. | Audit sampling bounds exact-computation cost; metric strata can be processed independently. | Inference can scale, but retraining and feature-version governance are substantial. | B has an explicit cost control. |
| Memory | Window summaries plus 200 observations. | Two 200-query windows per metric plus 50 audited outcomes per window. | Model state, training sets, and feature history. | B is bounded and modest at Core scale. |
| Latency impact | Negligible. | No timed foreground-path oracle work; shadow auditing consumes asynchronous capacity. | Low inference latency but potentially high feature/training cost. | B must remain shadowed and rate-limited. |
| Research support | Standard control-chart style monitoring, but incomplete for vector geometry. | Kernel two-sample testing, two-sample distribution testing, multiplicity control, and exact recall auditing are established, independently testable tools. | Plausible Future Work after labeled drift scenarios exist. | Choose B as the interpretable baseline. |

Chosen solution:

Choose **Option B**: a metric-stratified composite detector with distinct `INPUT_DRIFT` and `QUALITY_DRIFT` classifications, followed by a constrained, safety-gated `ef` policy. The detector emits one of three evidence states—`NO_DRIFT`, `DRIFT`, or `INSUFFICIENT_EVIDENCE`—rather than treating missing evidence as no drift.

#### 1. Workload-drift definition

Unit of evaluation:

- Evaluate L2 and COSINE independently. Never pool their vectors, thresholds, cardinalities, recall, p-values, or decisions.
- A reference window `R_m` and each current window `C_m,t` contain exactly 200 eligible queries for metric `m`. Current windows are ordered, non-overlapping, and compared to the same immutable reference until an approved rebaseline.
- An eligible query must have the same collection/data version, vector dimension, metric, index identity/build parameters, `limit=100`, and consistency contract as the reference. A change to any identity field is a configuration/data event, not workload drift, and yields `INSUFFICIENT_EVIDENCE` until separately validated and rebaselined.
- The reference window is accepted only after health, semantic, and audit checks pass. It cannot update automatically after a drift alarm; that would allow adaptation of the baseline to hide persistent drift.

Observable signals for each metric stratum:

| Signal | Observable and statistic | Minimum effect-size gate | Classification |
|---|---|---:|---|
| Query-vector distribution | Unbiased squared maximum mean discrepancy (`MMD²`) between 200 reference and 200 current vectors using the Gaussian RBF kernel `k(x,y)=exp(-||x-y||²/(2*sigma²))`. COSINE vectors are L2-normalized; L2 vectors are standardized with reference-window per-dimension mean and standard deviation. Zero-variance dimensions remain zero. `sigma` is the median non-zero pairwise Euclidean distance in the reference window. The p-value comes from 9,999 deterministic label permutations whose seed is stored in the detector manifest. | `MMD² >= 0.01` | `INPUT_DRIFT` |
| Threshold distribution | Two-sided two-sample Kolmogorov–Smirnov statistic on the 200 explicit `radius` values. Metric stratification makes cross-metric normalization unnecessary. The p-value comes from the same deterministic permutation procedure. | `D >= 0.20` | `INPUT_DRIFT` |
| Exact result-cardinality distribution | Two-sided two-sample Kolmogorov–Smirnov statistic on full, uncapped threshold-eligible cardinalities from the deterministic audit sample. Returned HNSW cardinality is telemetry only and cannot substitute because `limit=100` censors it. The p-value comes from the same deterministic permutation procedure. | `D >= 0.20` | `INPUT_DRIFT` |
| Recall at fixed search effort | Absolute decrease `delta_recall = mean_recall(R_m, ef=100) - mean_recall(C_m,t, ef=100)` in capped recall@threshold, measured against the independent exact oracle and checked against FLAT semantics. The one-sided p-value comes from 9,999 deterministic permutations. The serving `ef` does not replace this sentinel measurement. | `delta_recall >= 0.02` | `QUALITY_DRIFT` |

Audit sampling:

- Exactly 50 of each 200-query window (25%) are selected by ranking a stable keyed hash of `(detector_seed, metric, window_id, query_id)` and taking the lowest 50 hashes. The seed and selected IDs are persisted.
- Each selected query is shadowed through FLAT, HNSW at sentinel `ef=100`, and the independent float64 oracle. Shadow work is excluded from foreground latency and may not delay the serving response.
- FLAT and oracle must agree on metric-specific threshold validity and capped ordered IDs before HNSW recall is accepted. The oracle also records uncapped exact cardinality. Any disagreement makes the window `INSUFFICIENT_EVIDENCE` and raises a correctness alarm outside the drift classifier.

##### Normative implementation conventions

These conventions are part of the ADR-002 contract; conforming implementations may not substitute language-default hashing, process-randomized hashes, another RNG, another floating-point width, or an implicit numerical fallback.

Canonical tuple serialization:

- Every tuple used for seed derivation or keyed hashing has a fixed field order defined by this ADR. Normalize textual identifiers to Unicode NFC before UTF-8 encoding. Encode integers as minimal base-10 ASCII (`0` for zero; no leading zeroes or leading `+`). Use the exact uppercase contract spellings `L2`, `COSINE`, `QUERY_VECTOR`, `THRESHOLD`, `CARDINALITY`, and `RECALL` for metric and signal fields.
- Serialize a tuple as a 4-byte unsigned big-endian field count followed, for each field, by an 8-byte unsigned big-endian byte length and the field's normalized UTF-8 bytes. Field positions have fixed schema types, so an integer field and textual field are never interchangeable. Reject values that cannot be represented canonically; do not fall back to `repr`, locale-dependent formatting, or platform-native byte order.

Seed and randomness contract:

- For each permutation family, serialize `(detector_seed, metric, window_id, signal)` exactly in that order, compute SHA-256 over the serialized bytes, take digest bytes `[0:8]`, and decode them as one unsigned big-endian 64-bit integer.
- Construct `NumPy Generator(PCG64(seed_u64))` from that integer. All random permutations in ADR-002 use this generator. The generator name, derived integer seed, and full SHA-256 digest must be persisted with the signal evidence.
- Exactly 9,999 label permutations are generated for each signal/window test. They may be evaluated in deterministic batches without changing their generator order. No global NumPy RNG state, operating-system entropy, or process hash seed may affect the result.

Audit-ranking hash contract:

- Derive the 32-byte BLAKE2b key as `SHA256(serialize((detector_seed,)))`. For each query, compute keyed BLAKE2b with `digest_size=32` over `serialize((metric, window_id, query_id))`.
- Sort ascending by the 32 digest bytes interpreted lexicographically as unsigned bytes, then by the canonical serialized `query_id` bytes as the deterministic tie-break. Query IDs must be unique within the 200-query window; duplicates, fewer or more than 200 eligible IDs, or an encoding failure make the sample `INSUFFICIENT_EVIDENCE`.
- Select exactly the first 50 ranked query IDs and persist each ID and digest. This construction is the exact implementation of the earlier shorthand keyed hash over `(detector_seed, metric, window_id, query_id)`; `detector_seed` is represented by the derived BLAKE2b key rather than repeated in the message.

Floating-point and preprocessing contract:

- Convert all statistical inputs to IEEE-754 float64 before validation or arithmetic, and retain float64 throughout statistics, kernel construction, effect sizes, and permutation evaluation.
- For L2, compute reference-window per-dimension mean and population standard deviation with `ddof=0`. Transform both the true reference and true current windows with those reference statistics. Where reference standard deviation is exactly zero, set that coordinate to exactly `0.0` in both transformed windows; do not divide by a replacement epsilon.
- For COSINE, L2-normalize every true reference and true current vector independently in float64. Any non-finite component or zero-norm vector makes the affected window `INSUFFICIENT_EVIDENCE`.
- Compute standardization, normalization, the reference-only median-heuristic `sigma`, transformed arrays, and the combined Gaussian-kernel matrix once from the true-labeled reference/current split. Hold all of them fixed across the 9,999 label permutations; a permutation changes only membership in the reference-sized and current-sized groups. Never recompute preprocessing statistics or `sigma` from permuted labels.
- Compute `sigma` as the median of finite, strictly positive pairwise Euclidean distances from the transformed true reference window, excluding the diagonal. If that set is empty, its median is non-finite, or the resulting `sigma <= 0`, the query-vector signal is `INSUFFICIENT_EVIDENCE`. No epsilon, unit-sigma, or other computed fallback is permitted.

Recall-signal permutation input and completeness contract:

- The true reference input and true current input are separate float64 arrays of shape exactly `(50,)`, one capped recall@threshold value per persisted audit-selected query for the same metric. Values must be finite and in `[0.0, 1.0]`. They are two samples from different 200-query windows, not paired observations across windows.
- Each array is complete only when all 50 expected unique audit IDs are present exactly once; every value was produced by sentinel HNSW `ef=100`; corresponding FLAT and independent-oracle capped IDs agree; metric, `limit=100`, collection/data identity, and index-build identity match the reference contract; and no audited query failed, timed out, or violated its threshold. Any failed condition makes the window `INSUFFICIENT_EVIDENCE`; do not calculate or impute a p-value.
- The observed one-sided statistic is `mean(reference_recall) - mean(current_recall)`. Concatenate the two arrays into 100 fixed values. For each permutation, use the signal's PCG64 generator to permute indices `0..99`, assign the first 50 indices to the permuted reference group and the remaining 50 to the permuted current group, and recompute only the difference of means. Calculate the p-value with the ADR-002 `(1 + exceedance_count) / 10,000` rule, where an exceedance is a permuted statistic greater than or equal to the observed statistic.

Governance status:

- ADR-002 is **Accepted**. These conventions are normative, do not change the evidence status, and do not by themselves authorize live policy actuation.

Statistical decision rule:

1. Compute the four raw p-values above for each complete metric/window evaluation. Each permutation p-value is `(1 + count(permuted_statistic >= observed_statistic)) / 10,000` over 9,999 label permutations; use the absolute KS statistic, MMD², and the one-sided positive recall decrease as their respective statistics. Derive and persist a distinct permutation seed for every `(detector_seed, metric, window_id, signal)` tuple.
2. Apply Holm’s step-down correction to that four-test family with family-wise `alpha=0.01`.
3. A signal breaches only when its Holm-adjusted p-value is `<= 0.01` **and** its minimum effect-size gate is met. Statistical significance without the effect floor, or effect size without corrected significance, is not a breach.
4. `INPUT_DRIFT` requires the same input signal to breach in two consecutive complete windows. `QUALITY_DRIFT` requires the recall signal to breach in two consecutive complete windows. A single breached window is `INSUFFICIENT_EVIDENCE` with reason `PENDING_CONFIRMATION`, not `DRIFT`.
5. If both classifications qualify, emit `INPUT_AND_QUALITY_DRIFT`. Quality drift alone does not prove workload causation: backend health, data/index identity, and semantic checks must remain explicit possible causes.

This definition is falsifiable: for any retained pair of windows, another evaluator can reproduce the eligibility decision, audit sample, test statistics, corrected p-values, effect gates, consecutive-window history, and final state.

False-positive target:

- Target: at most **1% false `DRIFT` decisions per complete metric-stratum detector decision** under stationary replay. A false positive is a `DRIFT` emitted when reference and current windows are sampled from the same frozen stationary workload and environment contract.
- Holm correction controls the within-window four-signal family at `0.01`; the two-window rule adds persistence filtering. No independence assumption is used to claim the operational target.
- Before acceptance, a dedicated stationary-replay experiment must show a false-positive point estimate `<= 1%` and a one-sided 95% exact binomial upper confidence bound `<= 1%`. With zero false positives this requires at least 299 complete decisions. Until that evidence exists, detector output is research evidence only and policy mode is `DRY_RUN`/`RECOMMEND`.

#### 2. Drift Detector input/output contract

Inputs:

- Versioned detector configuration and deterministic seeds.
- Immutable reference-window ID and the two most recent complete current-window IDs for one metric.
- Per query: stable query ID, event time/order, exact query vector, explicit `radius`, `range_filter`, `limit`, metric, served `ef`, returned IDs/count, client latency, and failure/timeout/threshold-violation status.
- For each audited query: FLAT IDs/distances, sentinel-HNSW (`ef=100`) IDs/distances, oracle capped IDs/distances, oracle uncapped cardinality, and semantic-agreement status.
- Environment identity: collection/data version, index identity and build parameters, consistency level, server/client versions, and health state.

Outputs:

- `state`: exactly one of `NO_DRIFT | DRIFT | INSUFFICIENT_EVIDENCE`.
- `classification`: `NONE | INPUT_DRIFT | QUALITY_DRIFT | INPUT_AND_QUALITY_DRIFT`; it is `NONE` unless `state=DRIFT`.
- `signal_evidence`: for every signal, sample count, statistic, raw and Holm-adjusted p-values, effect value, effect floor, gate ratio (`effect/floor`), current-window breach, previous-window breach, and consecutive qualification.
- `decision_confidence`: for a `DRIFT`, the minimum of `1 - adjusted_p` across the two qualifying windows for the triggering signal; therefore a valid trigger is at least `0.99`. This is an evidence score, not a posterior probability. It is null for `NO_DRIFT` and `INSUFFICIENT_EVIDENCE`, because failure to reject is not proof of stationarity.
- `drift_magnitude`: the triggering signal’s minimum gate ratio across its two qualifying windows; report all per-signal raw magnitudes as well. A valid trigger has magnitude at least `1.0`.
- Audit coverage, baseline/window/configuration identifiers, deterministic seeds, reason codes, and an immutable decision/audit-log identifier.

State semantics:

- `NO_DRIFT`: all 200 observations and 50 audits are complete and valid, no signal is pending or consecutively qualified, and all identity/health/semantic prerequisites pass.
- `DRIFT`: at least one signal satisfies corrected significance, effect size, and the two-consecutive-window rule.
- `INSUFFICIENT_EVIDENCE`: fewer than 200 eligible observations, fewer than 50 valid audits, a first unconfirmed breach, missing/invalid metadata, health failure, FLAT/oracle disagreement, identity change, or statistical computation failure. It must never be coerced to `NO_DRIFT`.

#### 3. Tuning Policy contract

Policy inputs:

- A complete detector output and its immutable evidence record.
- Current explicit HNSW `ef`, last-known-good `ef`, metric stratum, and current/reference window IDs.
- The candidate response estimates for capped recall and client p95 latency, including uncertainty and evidence provenance.
- Pre-action health/configuration/index-identity checks, rollback readiness, current policy mode, and the experiment ID authorizing the action class.
- The research-profile SLOs and action limits defined below.

Policy output:

- One decision: `NO_CHANGE`, `RECOMMEND_EF`, `START_CANARY`, or `ROLLBACK`.
- Current, candidate, and last-known-good `ef`; expected recall and p95 latency; predicted improvement; reason; detector confidence/magnitude; safety-gate results; mode; and immutable audit ID.
- The policy proposes an action. A separate safe-actuation boundary validates and applies it. No detector or policy component may call PyMilvus directly.

##### Tuning Policy implementation conventions

These conventions are normative for the offline policy implementation and resolve the input, uncertainty, mode, identity, and candidate-selection ambiguities in this Proposed ADR. They do not authorize live actuation.

- **Response-estimate input:** The policy receives `response_estimates: Mapping[int, ResponseEstimate]`, keyed by `ef`. Each immutable `ResponseEstimate` contains metric, canonical threshold-stratum identifier, `ef`, mean capped recall, recall lower one-sided 95% confidence bound, client p95 latency, latency upper one-sided 95% confidence bound, validated-response-model status, and evidence provenance. The mapping key must equal the contained `ef`; the selected current, candidate, and last-known-good estimates must match the policy metric and threshold stratum exactly. Missing, duplicate, mismatched, non-finite, or internally inconsistent estimates fail closed.
- **Conservative uncertainty gates:** Safety and SLO gates use one-sided 95% confidence bounds computed from the applicable canary/audit observations: the recall lower confidence bound and latency upper confidence bound. Point estimates are retained as evidence but never substitute for these bounds when deciding whether a safety gate passes. The offline policy consumes and validates these precomputed bounds; it does not estimate, resample, or otherwise compute confidence intervals. The confidence-bound estimator is an explicit dependency to be defined and validated by a future statistics/experiment contract. A missing, non-finite, or inapplicable required bound fails closed and cannot produce `START_CANARY` or qualify a last-known-good value.
- **Distinct safety inputs:** `PreActionSafety` contains pre-canary health, configuration, collection-load, index/data/configuration-identity, current failure/threshold-violation, response-model provenance, action-class authorization, and transition-specific rollback-readiness checks. `CanaryObservation` contains actual post-canary candidate and paired last-known-good recall/latency observations and confidence bounds, completed-query count, query failures/timeouts, threshold or semantic violations, service/load/configuration/index-identity state, audit-record presence, and actuation exceptions. `QualificationWindow` contains one 200-query window's completeness, `ef`, recall/latency observations and confidence bounds, SLO results, health/correctness results, configuration/index/data identity, and rollback-clean status. Predicted response estimates are not `CanaryObservation` and cannot trigger or clear a post-canary rollback condition.
- **Policy modes:** Policy mode is exactly `DRY_RUN` or `CANARY_ENABLED`. Only `CANARY_ENABLED` may emit `START_CANARY`; `DRY_RUN` may emit recommendations but never an actuation-stage decision. Either mode may emit `ROLLBACK` when an actual `CanaryObservation` meets a mandatory rollback condition, because rollback is a safety response rather than a new tuning action.
- **Deterministic `INPUT_DRIFT` direction:** For `INPUT_DRIFT` without quality drift, if current recall is below `0.95`, evaluate only the next-higher adjacent `ef` for recall recovery. If current recall is at least `0.95`, evaluate only the next-lower adjacent `ef` for the latency objective. Do not rank candidates across recall and latency objectives, consider both directions simultaneously, or substitute a non-adjacent candidate. The separately validated response-model and evidence requirements still apply.
- **Canonical threshold stratum:** Every response estimate, `PreActionSafety`, `CanaryObservation`, and `QualificationWindow` carries an explicit canonical threshold-stratum identifier, such as `target-075`, in addition to the metric. Exception matching requires exact metric and threshold-stratum equality; an absent, malformed, or non-matching identifier fails closed to the standard `1.25 *` relative-latency ceiling.
- **Last-known-good qualification:** A last-known-good value is qualified from exactly two consecutive `QualificationWindow` records, not from a bare `ef`. Both records must be complete and passing, use the same eligible `ef`, and have identical configuration, index, and data identities. Each must pass health, correctness, conservative-bound recall and latency SLOs, and rollback-clean checks. `ef=100` remains ineligible regardless of its window evidence.
- **Externally supplied audit identity:** The pure policy function accepts an immutable audit ID supplied by the audit-log boundary and returns it unchanged. It never generates, derives, normalizes, or replaces audit identity internally. With no active canary (`CanaryObservation` absent), a missing or empty audit ID produces `NO_CHANGE` with reason `AUDIT_ID_MISSING`; it cannot produce a recommendation, start a canary, or qualify a last-known-good value. With an active canary (`CanaryObservation` present), a missing or empty supplied audit ID or a missing canary audit record is an immediate hard failure and produces `ROLLBACK` with reason `AUDIT_ID_MISSING`; the supplied empty audit-ID value is returned unchanged.

ADR-002 is **Accepted**. These conventions are normative for the committed offline policy and do not by themselves authorize live use.

Action space and transition rules:

- EXP-001's verified measurement sweep remains `{100, 200, 400, 800, 1600}`, but the actuation and last-known-good ladder is the strict subset `{200, 400, 800, 1600}`. `ef=100` is excluded from serving-policy actuation and retained only as the fixed shadow sentinel for `delta_recall` so that quality drift is always measured at constant search effort.
- Resolution rationale: EXP-001 measured aggregate mean recall `0.895965` at `ef=100`, and all six metric/threshold configurations were below the `0.95` recall floor (`0.852422` to `0.947203`). Lowering the floor would weaken the safety objective to admit a setting already shown to miss it. At `ef=200`, aggregate mean recall was `0.970187`, and all six configurations exceeded the floor (`0.959667` to `0.983986`), making `ef=200` the lowest empirically eligible actuation value. These smoke measurements justify exclusion, not production readiness; every live candidate must still pass the ADR-002 shadow/canary gates.
- A normal decision may move by at most one adjacent actuation value: `200 <-> 400 <-> 800 <-> 1600`. Direct jumps are invalid. Emergency rollback may restore the persisted last-known-good actuation value directly. If serving configuration is `ef=100`, automatic actuation cannot bootstrap itself: remain in `DRY_RUN`, emit a configuration/SLO alert, and require an explicitly approved initialization at an eligible value followed by last-known-good qualification.
- `INSUFFICIENT_EVIDENCE` or `NO_DRIFT` produces `NO_CHANGE` for this drift-triggered policy.
- `QUALITY_DRIFT` may recommend only the next higher adjacent `ef`, because its immediate objective is recall recovery. At `ef=1600`, it emits `NO_CHANGE` plus an unsatisfied-SLO alert.
- **Documented quality-recovery exception:** L2 `target-075`, `ef=400 -> 800`, may use a relative p95 canary ceiling of `1.50 *` last-known-good instead of `1.25 *`, but only for `QUALITY_DRIFT` or `INPUT_AND_QUALITY_DRIFT`. EXP-001 measured this transition at `3.465897 ms -> 4.860332 ms` (`1.402330 *`), so the standard ceiling would reject its only adjacent upward recovery step before judging recall recovery. The `1.50 *` cap admits the measured transition with about 7% multiplicative headroom while preserving the absolute `10.0 ms` ceiling. To use the exception, canary mean recall must remain `>= 0.95` and improve by at least `0.005` absolute over paired last-known-good recall; EXP-001 observed `0.989706 -> 0.997350` (`+0.007643`) for this transition. It is not available to input-drift/latency-optimization actions, does not permit a non-adjacent jump, and requires a dedicated EXP entry to authorize this exact transition under drift; EXP-001 supplies the conflict evidence only.
- `INPUT_DRIFT` without quality drift may recommend one adjacent value in either direction only when a separately validated response model predicts the change will satisfy both SLOs and the minimum-improvement gate. Without that model/evidence it remains `RECOMMEND_EF` in dry-run mode.
- When both classifications occur, the quality rule dominates: only an upward adjacent candidate is eligible.
- The policy may not alter `radius`, `range_filter`, `limit`, metric, index/build parameters, data, collection, or consistency level.

Minimum predicted improvement:

- If current mean capped recall is below the recall floor, the candidate must predict at least `+0.01` absolute mean recall and remain within the latency ceilings.
- If current recall already satisfies the floor, a lower-`ef` candidate must predict at least a `5%` p95-latency reduction while preserving the recall floor and all safety ceilings.
- These predictions do not authorize actuation unless a prior dedicated EXP entry supports this action class. EXP-001 alone is insufficient because it did not test drift-triggered decisions or rollback.

#### 4. Safety, bad-decision detection, and rollback contract

Research-profile SLOs:

- **Recall floor:** audited mean capped recall@threshold must be `>= 0.95`. A candidate also may not reduce paired mean recall by more than `0.01` absolute versus the last-known-good `ef` on the same audited queries.
- **Latency ceiling:** foreground client p95 latency must be `<= 10.0 ms` and, by default, `<= 1.25 *` the last-known-good p95 measured on the same canary interval. The sole proposed exception is the documented L2 `target-075`, `ef=400 -> 800` quality-recovery transition, whose relative ceiling is `1.50 *`; both ceilings remain mandatory, so the exception never overrides `10.0 ms`.
- These values apply only to the pinned single-client/concurrency-1 Core research profile. A deployment with a different latency objective must supply and validate a stricter or explicitly superseding SLO before actuation; missing SLOs force dry-run mode.

Last-known-good contract:

- Persist the current explicit `ef`, configuration/index/data identity, SLO evidence, and audit ID before any canary.
- Only `ef in {200, 400, 800, 1600}` can become last-known-good. `ef=100` can never qualify, regardless of a particular window's observed recall, because its sole ADR-002 role is the fixed sentinel.
- An eligible `ef` becomes last-known-good only after two complete 200-query windows pass health, correctness, recall, and latency SLOs with no rollback condition.
- Last-known-good state must survive process restart and be readable before actuation. If absent, stale, or identity-mismatched, actuation is prohibited.

Pre-action and staged-exposure gates:

1. Default mode is `DRY_RUN`; recommendation and actuation logs are mandatory.
2. Validate candidate membership, adjacent-step limit, metric/threshold/limit/index identity, applicable relative-latency ceiling, detector confidence `>= 0.99`, drift magnitude `>= 1.0`, minimum predicted improvement, prior EXP authorization, and tested rollback for that exact transition. Fail closed to the standard `1.25 *` ceiling if exception identity or authorization is missing.
3. Require Milvus, etcd, and MinIO health; zero current query failures/threshold violations; loaded collection; unchanged HNSW identity; and an available last-known-good record.
4. Shadow the candidate and last-known-good `ef` on the same 50 audited queries before serving the candidate. Both are compared to FLAT/oracle; failure blocks the canary.
5. If shadow checks pass, expose the candidate to at most 10% of eligible foreground traffic until 50 candidate queries have completed, while retaining last-known-good service for the remainder and collecting paired shadow evidence.

A policy decision is bad, and rollback is mandatory, when any of the following occurs:

- Immediate hard failure: any failed/timeout candidate query, threshold violation, FLAT/oracle disagreement, unhealthy required service, unloaded collection, configuration-validation failure, index-identity change, missing audit record, or actuation exception.
- Recall failure after the 50-query canary: mean capped recall `< 0.95` or paired mean recall more than `0.01` below last-known-good on the same audited queries. For the L2 `target-075`, `ef=400 -> 800` quality-recovery exception, paired mean recall improvement `< 0.005` is also a rollback trigger.
- Latency failure after the 50-query canary: candidate p95 `> 10.0 ms` or above the applicable relative ceiling on the same canary interval—`1.25 *` by default, or `1.50 *` only for the authorized L2 `target-075`, `ef=400 -> 800` quality-recovery exception.

Rollback behavior:

1. Stop assigning new queries to the candidate and restore the persisted last-known-good explicit `ef` on the next request. Rollback must not rebuild or replace the HNSW index.
2. Record trigger, affected query IDs, old/candidate/restored values, timestamps, health, index identity, SLO evidence, and outcome in an append-only audit record.
3. Re-run health/configuration checks and a 50-query FLAT/oracle audit at the restored value. Failure escalates to operator intervention; the policy remains disabled.
4. Enter `DRY_RUN`, prohibit further automatic actions for at least two complete 200-query windows, and require explicit human approval to leave the cooldown.
5. If restoration cannot be confirmed, fail closed: keep automatic actuation disabled and alert. Never advance to another candidate in response to a failed rollback.

Automatic actuation remains unauthorized until every `AGENTS.md` decision-gate condition is backed by reviewed evidence: confidence calibration, minimum-improvement validation, pre-action health checks, hard bounds, dry-run behavior, audit completeness, deliberate failures, and transition-specific rollback across process restart.

Consequences and tradeoffs accepted:

- Exact auditing increases background compute but prevents capped result counts from masquerading as true cardinality and supplies real recall.
- Two 200-query windows delay detection by at least 400 eligible queries; this is accepted to reduce transient false alarms. Extreme correctness or health failures bypass drift classification and trigger safety handling immediately.
- Fixed effect floors and SLOs improve falsifiability but may be suboptimal. Changing them after experiments begin requires a new experiment contract and, once this ADR is accepted, a superseding ADR—not an undocumented configuration edit.
- MMD and KS identify distribution change but do not identify its business cause. Detector evidence supports a tuning decision, not a causal claim.
- `QUALITY_DRIFT` may arise without workload drift. The separate label prevents the research report from claiming workload causation when only fixed-effort recall changed.
- The policy is intentionally conservative: it can miss short-lived drift, decline to act on incomplete evidence, and take multiple windows to traverse the `ef` ladder.

Benchmark and verification plan required before implementation acceptance:

1. Pre-register stationary, abrupt, gradual, vector-only, threshold-only, cardinality-only, quality-only, mixed, and recovery scenarios under new experiment IDs.
2. Verify each statistic, Holm adjustment, effect gate, window/hysteresis transition, deterministic audit sample, and three-state output against boundary fixtures.
3. Demonstrate the stationary false-positive target, including the exact binomial upper bound, without tuning thresholds on the evaluation replay.
4. Measure detection delay, false-negative rate, classification accuracy, shadow overhead, and sensitivity to audit rate.
5. Test missing audits, metric mixing, identity changes, DB unavailability, semantic disagreement, extreme drift, invalid candidates, stale last-known-good state, actuation failure, rollback failure, and restart persistence.
6. Run every adjacent actuation transition in `{200, 400, 800, 1600}` in dry-run and canary mode, deliberately violate each recall/latency/hard-failure guardrail, and show raw rollback evidence. Validate both sides of the L2 `target-075`, `ef=400 -> 800` exception (`1.50 *` passes only with recall recovery; a higher ratio or failed recovery rolls back). Validate `ef=100` separately as a non-actuating sentinel and as a rejected last-known-good candidate.
7. Require manual architecture review and explicit human approval before changing status from Proposed or enabling implementation/live actuation.

Modules affected:

Workload observation port; exact-audit sampler; drift detector; detector evidence store; tuning policy; response model; safe-actuation boundary; health/failure monitor; last-known-good store; audit log; experiment harness.

Research references:

- Gretton et al., [“A Kernel Two-Sample Test”](https://www.jmlr.org/papers/v13/gretton12a.html), JMLR 13 (2012), for MMD-based distribution comparison.
- Holm, [“A Simple Sequentially Rejective Multiple Test Procedure”](https://doi.org/10.2307/4615733), Scandinavian Journal of Statistics 6 (1979), for family-wise multiplicity control.
- ADR-001 and EXP-001/EXP-004 for verified Milvus range semantics, fixed HNSW build identity, and the `ef in {100, 200, 400, 800, 1600}` smoke measurement surface from which ADR-002 derives its narrower actuation ladder.
- `AGENTS.md` Risk Classification, Safety Rules, Configuration Governance, Testing Policy, and Failure Policy.

---

### ADR-003: Correct MMD permutation exchangeability, zero-variance exclusion, and confidence redundancy

Status: Proposed — implementation review required before code changes
Date: 2026-08-02
Risk level: CRITICAL
Evidence status: INFERRED design contract. Supersedes the MMD preprocessing convention, zero-variance handling, and decision_confidence gate defined in ADR-002. Does not change the KS signal, recall signal, Holm correction, effect-size gates, three-state output, consecutive-window rule, or actuation ladder.

Problem:

ADR-002's MMD query-vector signal contains three flaws:

1. Label-exchangeability violation: standardization mean/std and median-heuristic sigma are computed from the reference window only, then held fixed during permutations. The original reference group is therefore mathematically special (exactly zero mean, unit variance). Permuted pseudo-reference groups are not, breaking the exchangeability assumption required for a valid permutation p-value.
2. Zero-variance false negative: a dimension with zero variance in the reference window is zeroed in both windows. If the current window develops variance in that dimension, the drift is invisible to MMD.
3. Redundant and misleading gate: decision_confidence = 1 - adjusted_p is used as an independent >= 0.99 policy gate. This is redundant — adjusted_p <= 0.01 is already required by the Holm-corrected breach rule. The name "confidence" falsely implies a posterior probability.

Alternatives considered:

| Option | Correctness | Cost | Assessment |
|---|---|---|---|
| A — Full per-permutation recomputation of mean/std/sigma | Exact | O(N²) per permutation; prohibitively slow at 9,999 permutations | Rejected |
| B — Pooled preprocessing (chosen) | Exact exchangeability | Same asymptotic cost as current; one extra pass over combined data | Accepted |
| C — Conditional permutation test formulation | Correct under different assumptions | Requires separate theoretical justification and different null hypothesis | Future work if pooled approach is insufficient |

Chosen solution:

1. Pooled preprocessing: compute mean, std, and sigma from the pooled combination of true reference and true current windows (shape 400×D for standard windows). Build the kernel matrix once from pooled-standardized data. All permutations swap group membership over this fixed label-independent kernel. Preprocessing is computed once before permutations begin and never recomputed per permutation.
2. Zero-variance exclusion: if a dimension has zero variance in the pooled data it is excluded entirely from MMD (not zeroed). The count and indices of excluded dimensions must be recorded in SignalEvidence. A dimension that varies in either window will have non-zero pooled variance and will be included.
3. Rename and remove: rename decision_confidence to significance_evidence_score in DriftDecision and all references. Remove DETECTOR_CONFIDENCE_FLOOR from policy.py and its two reference sites (lines 46 and 1014 at time of this ADR). The policy must rely on detector state (DRIFT) and drift_magnitude >= 1.0 — the adjusted_p <= 0.01 requirement is already enforced inside the detector.

Consequences:

- Validation completion: corrected implementation commit `8278711` reproduced the stored stationary figures (L2 `0/299`, COSINE `0/299`) and drift-injection figures (`0 FN`, `10/10`); the triggering-magnitude range measured `2.333960876921x–6.901880012192x` versus the stored `2.3x–7.1x` rounded range. This is documented in `PROJECT_BIBLE.md` and `SESSION_HANDOFF.md` at commit `c0594ec`; status: `PROVISIONAL → VALIDATED`.
- drift.py: _prepare_mmd must be replaced with a pooled variant; zero-variance exclusion must use pooled std, not reference std; sigma must use pooled pairwise distances.
- policy.py: DETECTOR_CONFIDENCE_FLOOR constant and its two usage sites must be removed; decision_confidence gate must be removed from all policy evaluation paths.
- DriftDecision dataclass: decision_confidence field renamed to significance_evidence_score; all callers updated.
- All tests referencing decision_confidence or DETECTOR_CONFIDENCE_FLOOR must be updated.
- EXP-005 contract (commits ed4e877, ef7f7d3) remains valid in structure; its detector calls will use the corrected implementation after this ADR is implemented.

Modules affected: drift.py, policy.py, and their test files.

Research references:
- Gretton et al., "A Kernel Two-Sample Test", JMLR 13 (2012) — original MMD formulation.
- ADR-002 normative implementation conventions — superseded for MMD preprocessing only.

---

### ADR-004: Carry immutable evidence provenance from shadow traces through policy and actuation

Status: Accepted — implementation in progress
Date: 2026-08-03
Risk level: CRITICAL
Evidence status: INFERRED design contract. This ADR closes an EXP-005 integration gap; it does not authorize live actuation.

Problem:

EXP-005 requires live-shadow evidence to remain independently reviewable from persisted traces through detector, policy, and the safe-actuation audit. The existing `WindowEvidence` and `DriftDecision` carry statistical results but not the immutable window manifests, identities, or deterministic audit selections from which those results were derived. Passing configuration/data/index fields separately to policy would permit a structurally valid but unbound decision path.

Alternatives considered:

| Option | Advantages | Disadvantages | Decision |
|---|---|---|---|
| A — External side-channel map keyed by window or audit ID | No existing value-object changes. | Mutable, restart-fragile, and can be mismatched with a detector result. | Rejected. |
| B — Immutable evidence-provenance value propagated with evidence and decisions | Explicit, restart-auditable, validates every boundary, and preserves backward compatibility with optional fields. | Adds value-object and persistence schema work. | Chosen. |
| C — Re-query Milvus at policy time to rediscover identity | Uses live state. | Cannot prove the detector used the same evidence; adds query side effects and race windows. | Rejected. |

Chosen solution:

1. Introduce an immutable, versioned `EvidenceProvenance` value. It records the metric, threshold stratum, reference/current window IDs and manifest SHA-256 values, configuration/data identities, FLAT/HNSW binding identities, deterministic audit selections and ranking digests, and a canonical provenance SHA-256.
2. `shadow_extraction` constructs provenance only from two validated `AssembledShadowWindow` values and the actual `AuditSelection` results. It must not accept caller-supplied identities, manifests, selected IDs, or provenance hashes.
3. Add an optional provenance field to `WindowEvidence`, propagate the current comparison provenance into `DriftDecision`, then into `PolicyDecision` and the immutable `ActuationAuditRecord`. Existing synthetic unit fixtures remain valid with `None`; they can never serve as real EXP-005/action evidence.
4. When both consecutive `WindowEvidence` values have provenance, `evaluate_drift_decision` must fail closed if their metric, stratum, reference manifest, configuration/data identities, or FLAT/HNSW bindings differ. The current-window manifest and audit selection may differ and must remain separately retained.
5. A `DRIFT` decision may yield `RECOMMEND_EF` or `START_CANARY` only when its provenance is present, structurally valid, and matches `PreActionSafety` on metric, stratum, configuration identity, data identity, and FLAT/HNSW binding identities. Missing or mismatched provenance yields `NO_CHANGE` with an explicit reason. `NO_DRIFT` remains a safe no-op, but EXP-005 may not claim end-to-end provenance without it.
6. The safe-actuation boundary must validate a `START_CANARY` decision's provenance against `ActuationContext` and persist it in the append-only audit record. It must never reconstruct provenance through a new database query. A rollback triggered by an actual failing canary is deliberately not blocked by missing detector provenance: failing closed must preserve the ability to restore the last-known-good setting.
7. Canonical provenance serialization uses the repository's NFC/UTF-8 canonical JSON rules, rejects unsupported/non-finite values, and computes lowercase SHA-256. Every persisted audit reader validates the schema and recomputes the digest before trusting it.

Consequences:

- EXP-005 Stage 3 must use three independently assembled windows (twelve traces): `reference→current-1` produces the previous `WindowEvidence`; `reference→current-2` produces the current `WindowEvidence`; only then may `evaluate_drift_decision(previous, current)` run.
- `previous=None` remains `INSUFFICIENT_EVIDENCE`, never a stationary `NO_DRIFT` result.
- DRY_RUN policy evaluation and a safe-boundary `NO_OP` remain non-actuating; fake-client rollback/canary failures stay separate deliberate-failure tests and cannot be represented as an EXP-005 stationary no-op.
- This supersedes no statistical rule in ADR-002/ADR-003. It adds provenance binding only.

Verification plan:

1. Unit-test canonical provenance construction, digest recomputation, malformed/tampered provenance, and every detector/policy/boundary identity mismatch.
2. Extend persistence tests to reject malformed or digest-mismatched provenance records fail closed across restart.
3. Add the EXP-005 offline Stage 3 test using twelve independently assembled synthetic traces and actual extraction, detector, policy, and safe-boundary functions; prove `NO_DRIFT → NO_CHANGE → NO_OP` with zero client calls.
4. Before EXP-005 live acquisition, capture and persist provenance from real trace manifests; demonstrate a deliberate identity mismatch reaches `INSUFFICIENT_EVIDENCE`/non-action before any live action.

Modules affected: detector-owned provenance value type; shadow extraction; drift detector values; tuning policy; safe-actuation boundary; append-only audit persistence; focused unit/integration tests.

---

### ADR-005: Define the online workload monitor and DRY_RUN orchestration boundary

Status: Accepted — implementation and offline safety/recovery evidence verified; live event source remains separately unimplemented
Date: 2026-08-03  
Risk level: CRITICAL  
Evidence status: VERIFIED for the monitor's offline DRY_RUN scope through EXP-006 (commit `6650c06`). No live event-source integration or automatic actuation is authorized.

Problem:

The EXP-001 benchmark harness in `runner.py` and the ADR-002 detector in `drift.py` are disconnected. `runner.py` performs bounded benchmark runs; it does not consume live query evidence or invoke the detector. `drift.py` accepts finalized `WindowEvidence`; it has no facility to acquire, persist, group, or continuously assemble live shadow traces.

EXP-005 established a reviewed read-only path for one stationary capture, but it is not an online monitor: it captures a preplanned twelve-trace experiment and exits. The system therefore lacks a restart-safe component that can continuously assemble eligible live shadow observations into immutable windows, evaluate the detector and policy in `DRY_RUN`, and record every outcome without modifying Milvus.

Scope:

In scope:

- Continuous assembly of 200-query raw windows from persisted 50-query `ShadowAuditTrace` envelopes.
- An immutable reference window plus two ordered, non-overlapping current windows per compatible metric/threshold/identity stream.
- Detector evaluation using the existing extraction and drift interfaces.
- Policy evaluation exclusively in `DRY_RUN`.
- Restart-safe monitor state and append-only audit records for successful, incomplete, and rejected evaluations.

Out of scope:

- Automatic full-traffic apply, canary execution, rollback execution, or any Milvus mutation.
- Changing `ef`, index rebuilds, collection changes, or serving-parameter changes.
- Multi-backend, multi-tenant, k-NN tuning, hybrid search, and policy transfer; these remain Future Work.
- Replacing EXP-005’s controlled acquisition runner or redefining `ShadowAuditTrace`, `AssembledShadowWindow`, detector statistics, or policy gates.

Alternatives considered:

| Option | Coupling | Testability | Scope-creep risk | Existing-interface fit |
|---|---|---|---|---|
| A — Polling loop inside `runner.py` | High: joins benchmark lifecycle to monitoring | Weak: requires benchmark/Milvus setup | High: EXP-001 code becomes an online service | Poor: `runner.py` emits benchmark records, not persisted trace envelopes |
| B — Standalone `workload_monitor.py` consuming persisted trace-event queue/buffer | Low: depends on explicit protocols and immutable artifacts | Strong: source, state store, policy-input provider, and audit sink are injectable | Low to medium: narrow orchestration boundary | Strong: composes `assemble_shadow_window`, `extract_window_evidence`, `evaluate_drift_decision`, and `evaluate_tuning_policy` unchanged |
| C — Extend `exp005_acquisition.py` into a continuous runner | Medium: reuses capture machinery but mixes experiment and service lifecycles | Moderate | High: controlled EXP-005 artifacts risk becoming an undeclared production protocol | Partial: useful producer code, but its fixed twelve-trace lifecycle does not model continuous state |

Chosen solution:

Choose **Option B**: a standalone, dependency-injected `workload_monitor.py`.

It preserves the benchmark harness as a controlled measurement tool, reuses EXP-005’s validated persistence/assembly path, and makes the online loop independently replayable from immutable trace artifacts. `exp005_acquisition.py` remains a controlled trace producer and reference implementation, not a daemon.

Architecture and interface contract:

A trace producer must persist each envelope with `persist_shadow_trace_envelope(...)` before publishing an event. The monitor consumes only a reference to an immutable persisted envelope; it never trusts an unpersisted in-memory trace as evidence.

```python
@dataclass(frozen=True, slots=True)
class MonitorStreamKey:
    metric: Metric
    threshold_stratum: str
    configuration_identity: str
    data_identity: str
    flat_binding_id: str
    hnsw_binding_id: str


@dataclass(frozen=True, slots=True)
class ShadowTraceEvent:
    event_id: str
    stream_key: MonitorStreamKey
    window_id: int | str
    envelope_path: Path
    expected_trace_sha256: str


class ShadowTraceEventSource(Protocol):
    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]: ...


class MonitorStateStore(Protocol):
    def load(self, stream_key: MonitorStreamKey) -> MonitorStreamState: ...
    def save(self, state: MonitorStreamState) -> None: ...


class DryRunPolicyInputProvider(Protocol):
    def resolve(
        self,
        *,
        decision: DriftDecision,
        provenance: EvidenceProvenance,
    ) -> DryRunPolicyInputs: ...


class MonitorAuditSink(Protocol):
    def append(self, record: MonitorDecisionRecord) -> None: ...


class WorkloadMonitor:
    def run_once(self, *, max_events: int) -> tuple[MonitorCycleResult, ...]: ...
```

For every event, the monitor must:

1. Load it with `load_persisted_shadow_trace_envelope`.
2. Verify its trace checksum matches the event declaration.
3. Group only four envelopes sharing one immutable `MonitorStreamKey` and externally assigned `window_id`.
4. Call:

```python
assemble_shadow_window(window_id=..., envelopes=...)
```

5. Retain an accepted immutable reference window; it cannot be replaced automatically after a drift outcome.
6. For two subsequent complete current windows, call:

```python
extract_window_evidence(
    reference_window=reference,
    current_window=current,
    metric=stream_key.metric,
    detector_seed=frozen_detector_seed,
)
```

7. Call:

```python
evaluate_drift_decision(
    previous=reference_to_current_1,
    current=reference_to_current_2,
)
```

8. Resolve the complete policy context externally, then call:

```python
evaluate_tuning_policy(
    detector=decision,
    current_ef=...,
    response_estimates=...,
    pre_action=...,
    canary_observation=None,
    qualification_windows=None,
    last_known_good=...,
    mode=PolicyMode.DRY_RUN,
    threshold_stratum=stream_key.threshold_stratum,
    audit_id=externally_reserved_audit_id,
)
```

The monitor does not create response estimates, invent a last-known-good value, derive the policy audit ID, contact PyMilvus, or call a canary/rollback executor.

Safety invariants:

1. **DRY_RUN only.** The monitor hardcodes `PolicyMode.DRY_RUN`; it exposes no configuration path to `CANARY_ENABLED`.
2. **No automatic actuation.** It must not import or call a Milvus actuation client or safe-actuation executor. Any future transition from a recorded recommendation to an execution path requires a separate approved ADR and experiment authorization.
3. **Fail closed.** A malformed event, checksum failure, duplicate event, incompatible identity, incomplete trace/window, chronology failure, extraction failure, missing policy input, or provenance mismatch produces an audited invalid result and stops before detector, policy, or actuation processing as appropriate.
4. **Immutable reference.** A drift result never triggers automatic rebaselining. Rebaseline requires an explicit human-approved workflow.
5. **No evidence fabrication.** The monitor must not impute traces, substitute query IDs, reuse a prior audit sample, or coerce incomplete evidence to `NO_DRIFT`.
6. **Audit every outcome.** Every consumed event group records source envelope IDs/hashes, stream key, window IDs, assembled manifest hashes, reason codes, detector result when reached, policy result when reached, and an externally reserved immutable audit ID.
7. **Restart safety.** Deduplication state, the accepted reference window, pending envelope groups, prior/current `WindowEvidence`, and audit cursor must survive restart atomically. A missing or corrupt state store fails closed until manually repaired or rebaselined.
8. **Foreground isolation.** Trace acquisition and monitor evaluation remain off the serving request’s timing path; monitor backpressure must drop/hold monitoring work, never delay live queries.

Consequences:

- `runner.py` remains a benchmark harness and stays decoupled from online monitoring.
- EXP-005’s persistence, window assembly, extraction, provenance, detector, and policy contracts become the only allowed evidence path.
- A future implementation requires a new CRITICAL experiment covering restart recovery, queue/event duplication, malformed envelopes, identity change, monitor backpressure, decision auditing, and proof that DRY_RUN never calls an actuation client.
- This ADR does not authorize any live configuration change.

Modules affected:

New workload monitor/orchestration module; persisted trace-event source; monitor state store; audit sink; EXP-005 acquisition integration; `shadow_window.py`, `shadow_extraction.py`, `drift.py`, and `policy.py` as composed dependencies only.

Research references:

- ADR-001 for backend/benchmark separation.
- ADR-002 for detector, policy, audit, dry-run, and rollback constraints.
- ADR-003 for corrected MMD implementation.
- ADR-004 for immutable evidence provenance.
- EXP-005 for persisted live-shadow evidence and no-mutation verification.

---

### ADR-006: Use a host-side durable trace outbox as the live `ShadowTraceEventSource`

Status: Accepted — offline source/outbox implementation and EXP-007 evidence verified; host hook/worker and serving integration remain separately unimplemented
Date: 2026-08-03
Risk level: CRITICAL
Evidence status: VERIFIED for the offline single-host outbox at commit `ad635c7`, through EXP-007 run `artifacts/exp-007/run-20260803T152516Z/`. This ADR does not authorize serving-path mutation, live parameter changes, or automatic actuation.

Problem:

ADR-005 verifies a DRY_RUN-only monitor that consumes immutable persisted trace events, but the repository has no source that produces those events from an actual query-serving integration. The source must close that gap without placing database work, filesystem I/O, queue waits, or monitor evaluation on the foreground query path. It must also preserve the evidence invariants established by EXP-005: a monitor event may name only a checksum-valid, immutable 50-query `ShadowAuditTrace` envelope.

Raw query vectors and threshold parameters are necessary detector evidence. They may be sensitive in a non-synthetic deployment, so the event transport must never duplicate them into queue records, logs, or monitor state.

Alternatives considered:

| Option | Advantages | Disadvantages | Coupling / safety assessment |
|---|---|---|---|
| A — Host-side instrumentation plus durable local trace outbox | Separates request path from auditing; reuses `ShadowAuditTrace`, `persist_shadow_trace_envelope`, `ShadowTraceEvent`, and `WorkloadMonitor`; deterministic at-least-once delivery; testable without a database | Requires the actual serving application to call an instrumentation hook; single-host outbox is not a multi-host broker | Strong fit. Bounded background work and persist-before-publish preserve the existing proof chain. |
| B — Transparent network proxy / gRPC interceptor | Requires no application-library call at the apparent integration point | Cannot reliably reconstruct application-level metric/stratum/identity, oracle context, or post-response outcome; adds latency and a new failure domain | Rejected. It risks silently incomplete evidence and couples monitoring to protocol implementation details. |
| C — Tail Milvus logs or database internals | No application changes | Milvus does not expose the complete application query, threshold, FLAT/oracle, and identity evidence required by `ShadowAuditTrace`; storage/server logs are not an evidence API | Rejected. It cannot satisfy the detector's complete-input contract. |
| D — Extend `runner.py` or `exp005_acquisition.py` into a daemon | Reuses familiar test code | Turns controlled benchmark/acquisition code into serving infrastructure and cannot observe real host traffic | Rejected. It violates ADR-001/005 separation of experiment and online-service lifecycles. |

Chosen solution:

Choose **Option A**: the serving application invokes a narrow post-response instrumentation hook that enqueues an observation without blocking. A dedicated background trace worker owns all shadow auditing. Once it has a complete 50-query `ShadowAuditTrace`, it writes a checksum-valid envelope into a single-host durable outbox and only then publishes a `ShadowTraceEvent` to the existing monitor protocol.

The initial implementation is deliberately **single-host and single-logical-consumer**. It exposes an at-least-once interface; the monitor's existing durable deduplication is the exactly-once-effect boundary. Multi-host brokers, remote transport, and distributed ordering are Future Work and must not be implied by v1.

Interface contract:

The producer boundary is split into two isolated paths:

```python
@dataclass(frozen=True, slots=True)
class TracePublicationContext:
    stream_key: MonitorStreamKey
    window_id: int | str
    window_sequence: int
    trace_sequence_index: int  # exactly 0, 1, 2, or 3
    trace_id: str
    captured_at_utc: str


class ShadowTracePublisher(Protocol):
    def publish(
        self,
        *,
        trace: ShadowAuditTrace,
        context: TracePublicationContext,
    ) -> TracePublicationReceipt: ...


class ShadowTraceEventSource(Protocol):
    def poll(self, *, limit: int) -> tuple[ShadowTraceEvent, ...]: ...
    def acknowledge(self, event_ids: tuple[str, ...]) -> None: ...
```

1. The foreground serving hook records no disk state and never contacts Milvus beyond its already-completed serving query. It offers an immutable observation to a bounded in-memory queue. It returns immediately whether the observation was accepted or monitoring work was dropped.
2. The worker batches compatible observations, performs only the existing read-only shadow/FLAT/oracle capture in a background context, and emits a complete or explicitly incomplete `ShadowAuditTrace`. This worker must not call `start_canary`, `stop_candidate`, `restore_last_known_good`, `verify_restoration`, `evaluate_tuning_policy`, or an actuation boundary.
3. `ShadowTracePublisher.publish` validates the context, derives a deterministic event ID from canonical `live-shadow-event-v1` fields `(stream_key, window_id, window_sequence, trace_sequence_index, trace_id, expected_trace_sha256)`, and atomically persists the envelope before atomically creating its pending event record.
4. Pending event records contain only event ID, `MonitorStreamKey`, window membership, envelope path, and expected checksum. They never contain query vectors, FLAT hits, oracle results, thresholds, or other trace payload data.
5. `poll` returns a deterministic bounded pending prefix. `acknowledge` moves an exact pending event to an acknowledged ledger using an atomic same-filesystem rename and directory fsync. Replays before acknowledgement are expected and harmless; an event is never deleted merely because it was delivered.
6. A duplicate publication with byte-identical context and checksum is idempotent. A reused event/trace identity with different context, payload checksum, or envelope metadata fails closed and emits an operator-visible conflict reason. A publisher never silently deduplicates conflicting evidence.
7. The producer does not assign a four-trace window implicitly. The host-side scheduler supplies the `TracePublicationContext`; it must ensure one trace for each sequence index `0..3` per externally ordered window. The downstream assembler remains the authority that validates this invariant.

Safety, privacy, and rollback invariants:

1. **No serving-path delay:** Queue-full behavior returns `DROPPED_BACKPRESSURE` immediately and records only non-sensitive counters/reason codes. It may lose monitoring coverage but never hold, retry, or modify a serving request.
2. **Persist before publish:** A pending event cannot become visible until its exact envelope exists, has passed checksum validation, and has been directory-fsynced. A crash after envelope persistence but before publication yields an unreachable orphan, not fabricated evidence; the publisher reports it for explicit recovery/retention handling.
3. **Fail closed:** Invalid context, noncanonical timestamp/identity, incomplete trace, storage permission failure, malformed queue record, checksum mismatch, queue conflict, or capacity exhaustion yields no monitor event. A transient outbox failure does not retry on the foreground path.
4. **Data minimization:** Only the encrypted-or-owner-only trace store may contain raw vectors. Queue/audit records carry identifiers and hashes only. The initial local store must reject group/world-readable directories and symbolic-link traversal; deployments requiring encryption at rest must provide it at the host-volume/key-management layer before live data is admitted.
5. **Bounded resource use:** Both the in-memory observation queue and durable pending-event queue have configured count and byte limits. The producer exports drops, pending depth, oldest-pending age, orphan count, persistence failures, and acknowledged count. No unbounded retention or automatic destructive cleanup is permitted.
6. **No actuation:** Producer and event-source modules may type-reference the immutable `ShadowAuditTrace` value, but must not construct or invoke `MilvusActuationClient`, import PyMilvus, `policy.py`, `actuation.py`, or `WorkloadMonitor`. They publish evidence only. The monitor remains hardcoded to `DRY_RUN` under ADR-005.
7. **Recovery:** Reopening the outbox verifies every queued event and its envelope checksum before it is deliverable. Missing/corrupt state blocks only that item with an explicit reason; it must not reorder or substitute another trace. Manual recovery/rebaseline is separate from automatic source recovery.
8. **Source rollback:** Disabling the host hook and stopping its worker halts new monitoring publication immediately and has no Milvus-side effect. Already-persisted evidence is retained for explicit operator disposition; no automatic deletion, rebaseline, canary, rollback, or configuration restore is performed.

Consequences:

- A new producer/outbox module can be tested entirely offline with synthetic immutable `ShadowAuditTrace` values, then used by a real host integration without changing detector, policy, monitor, or actuation code.
- The v1 producer accepts a completed trace; it does not implement the host application's query sampler or shadow-audit scheduler. That host-owned component must remain background-only and earn separate live-integration evidence before continuous-traffic coverage is claimed.
- The application integration is an explicit dependency, not an invisible proxy. Until a host calls the instrumentation hook, the system has no claim to continuous live-workload coverage.
- The Core system gains a safe at-least-once evidence path, but not multi-host scalability or production authorization for automatic tuning.
- EXP-007 is mandatory before a live host integration or capture. It must demonstrate publication ordering, crash/restart recovery, bounded backpressure, conflicting duplicate rejection, data-minimizing event records, and end-to-end monitor consumption in `DRY_RUN`.

Modules affected:

New `shadow_event_source.py` and tests; existing `shadow_artifacts.py`, `milvus_actuation.py`, and `workload_monitor.py` only through defined protocols. No detector, policy, monitor, or actuation logic may be duplicated or changed.

Research references:

- ADR-002 for DRY_RUN and no-actuation safety gates.
- ADR-004 for immutable evidence provenance.
- ADR-005 and EXP-006 for monitor/event semantics and fail-closed state handling.
- EXP-005 for trace shape, persistence, and stationary live-shadow evidence.

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
