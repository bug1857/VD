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

Status: Accepted — implementation and offline safety/recovery evidence verified; ADR-006 source/outbox is separately verified, while the host sampler/shadow worker remains unimplemented
Date: 2026-08-03  
Risk level: CRITICAL  
Evidence status: VERIFIED for the monitor's offline DRY_RUN scope through EXP-006 (commit `6650c06`) and the separate source/outbox through EXP-007 (commit `ad635c7`). No host integration or automatic actuation is authorized.

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

### ADR-007: Use a framework-neutral host observation recorder and background shadow worker

Status: Accepted — reference implementation and EXP-008 live DRY_RUN evidence verified
Date: 2026-08-03
Risk level: CRITICAL
Evidence status: VERIFIED for the framework-neutral reference in-process gateway, worker, durable source/outbox, monitor, and read-only DRY_RUN composition through EXP-008 (`2403799` stationary evidence; `76600f8` H1/H4 evidence). No external serving-application deployment, automatic configuration change, canary, or rollback is authorized by this ADR.
Acceptance note: The implementation and strict H1/H4 evidence verifier are committed through `76600f8`; the full repository suite passed (`354` tests). A production host invokes the same post-response boundary, but must earn separate deployment-specific evidence.

Problem:

ADR-006 provides a verified durable boundary once a complete 50-query `ShadowAuditTrace` exists. The repository deliberately contains no production HTTP/gRPC serving application, and `runner.py` is a bounded benchmark rather than a service lifecycle. There is therefore no safe component that can accept a completed foreground range-query observation, keep its request path independent from monitoring work, group compatible observations, perform the required read-only shadow/FLAT/oracle capture, and deliver a complete trace to the verified source/outbox.

Treating `runner.py` as a live host would hide this distinction and invalidate any claim of continuous-workload coverage. A framework-specific web server would add an unrelated product surface without an existing host to integrate.

Alternatives considered:

| Option | Advantages | Disadvantages | Decision |
|---|---|---|---|
| A — Add monitoring callbacks directly to `runner.py` | Reuses existing Milvus setup and dataset loading | Mixes finite benchmark and online-service lifecycles; cannot observe post-response host traffic; risks benchmark contamination | Rejected |
| B — Build a new FastAPI/gRPC serving product | Demonstrates one concrete deployment | No serving application currently exists; introduces a framework, network API, auth surface, and deployment scope unrelated to the Core range-tuning question | Rejected |
| C — Framework-neutral in-process recorder plus injected background shadow executor | Explicit host seam; testable with fake queues/executors; no foreground disk, network, or policy work; usable from a future HTTP/gRPC/application host | Requires a separately implemented read-only executor and a host to call the recorder | Chosen |
| D — Inspect Milvus/gRPC traffic after the fact | No host integration call | Cannot recover response identity, threshold semantics, request outcome, or oracle context; violates the complete-evidence contract | Rejected |

Chosen solution:

Choose **Option C**. Implement a small library boundary with a constant-time, post-response `offer()` operation and an independently scheduled `run_once()` background worker. The foreground host retains ownership of its actual query. It records an immutable observation only after that query finishes, then immediately returns the original response path. The worker alone groups 50 compatible observations and invokes an injected, read-only `ShadowAuditExecutor` to produce the existing immutable `ShadowAuditTrace`; the existing `FileShadowTraceEventSource` then performs persistence and publication.

The first integration is a reference in-process gateway for EXP-008, not an HTTP/gRPC server and not a claim that a real application has been instrumented. A production host calls the same recorder contract from its post-response hook.

Interface contract:

```python
@dataclass(frozen=True, slots=True)
class CompletedRangeQueryObservation:
    request_id: int | str
    captured_at_utc: str
    stream_key: MonitorStreamKey
    query_vector: tuple[float, ...]
    threshold_radius: float
    range_filter: float
    limit: int
    served_ef: int
    served_outcome: ServedQueryOutcome


class HostObservationRecorder(Protocol):
    def offer(
        self, observation: CompletedRangeQueryObservation
    ) -> ObservationReceipt: ...


class ShadowAuditExecutor(Protocol):
    def capture(
        self, observations: tuple[CompletedRangeQueryObservation, ...]
    ) -> ShadowAuditTrace: ...


class HostWorkerStateStore(Protocol):
    def recover(self) -> HostWorkerState: ...
    def save(self, state: HostWorkerState) -> None: ...


class BackgroundShadowWorker:
    def run_once(self, *, max_observations: int) -> WorkerCycleResult: ...
```

1. `offer()` uses only a bounded in-memory queue and a non-blocking insertion (`put_nowait` or equivalent). It may validate fixed-size scalar/identity fields, but must not contact Milvus, write any file, wait for the worker, retry, evaluate drift/policy, call the publisher, or invoke an actuation client. A full queue returns `DROPPED_BACKPRESSURE` with a non-sensitive reason; it never delays or modifies the served query.
2. `CompletedRangeQueryObservation` is immutable and carries the canonical request ID, metric/stratum/index/data lineage, query vector, threshold/range filter, result limit, served `ef`, and a minimal immutable served outcome. Raw query payload exists only in volatile worker memory and the owner-only completed trace envelope; no event, monitor state, drop metric, or error log may duplicate it.
3. The worker preserves FIFO order within every exact `MonitorStreamKey` and groups exactly 50 compatible observations. A strict, non-sensitive `HostWorkerStateStore` atomically persists each stream's next trace ordinal, blocked status, and partial-observation count—not raw observations. It derives `(window_sequence, trace_sequence_index) = divmod(next_trace_ordinal, 4)` and `window_id = f"{stream_id}:window:{window_sequence}"` only after a complete group is available. It never joins metric, stratum, data identity, configuration identity, FLAT binding, or HNSW binding across groups.
4. The injected `ShadowAuditExecutor` is the sole component allowed to perform background read-only shadow, FLAT, and oracle work. It must return a complete trace whose 50 canonical query IDs match the supplied observations in order, metric/stratum/identity match the group, and candidate/LKG/sentinel settings are already registered. A mismatch, timeout, failed stage, or incomplete trace yields an explicit worker rejection and no source publication.
5. Only after those checks pass does the worker call the existing `ShadowTracePublisher.publish(trace=..., context=...)`. Publication retains ADR-006 persist-before-publish, at-least-once, and fail-closed behavior. The worker advances the persisted trace ordinal only after `PUBLISHED` or `IDEMPOTENT`; an unknown/error publication outcome blocks that stream for explicit operator recovery rather than reusing an ambiguous slot. The worker does not implement a second trace serializer or queue ledger.
6. The worker owns volatile partial batches. On startup, the state store's persisted partial counts become an exact, non-sensitive restart-loss record and are cleared before new evidence is accepted; their raw observations cannot be recovered. The worker must never fabricate a partial trace, replay unknown raw observations, or rebaseline the monitor. Published envelopes retain ADR-006 recovery semantics.
7. The recorder, worker, and reference gateway must not import `policy.py`, `actuation.py`, an automatic-action controller, or `WorkloadMonitor`. The background-only executor may consume the immutable `ActuationContext`, `ShadowResult`, and `QualificationResult` value contracts currently co-located in the actuation/policy modules, and may call an injected `MilvusActuationClient.shadow_candidate` only; it must not import or construct `SafeActuationBoundary`, invoke policy evaluation, or reference `start_canary`, `stop_candidate`, `restore_last_known_good`, or `verify_restoration`. The executor may use a lazily imported Milvus client only in the background worker path. `DRY_RUN` policy evaluation remains downstream of the existing monitor only.

Executor-adapter refinement (implementation contract):

The first `MilvusHostShadowExecutor` composes the existing
`MilvusActuationClient.shadow_candidate` trace path rather than reimplementing
range search, FLAT/oracle comparison, HNSW search, or identity capture. It owns
an immutable `HostShadowPlan` for every exact `MonitorStreamKey`, containing
only the registered candidate/LKG `ef` pair and the required served `ef`. Before
capture it requires exactly 50 homogeneous observations whose canonical IDs,
float32 vectors, radius/range/limit, served `ef`, metric/stratum, configuration
identity, data identity, and FLAT/HNSW bindings exactly match the plan and its
injected adapter workload. It verifies etcd/MinIO health, both collections'
`Loaded` state, and both identity bindings before *and* after the shadow call.

The adapter is injected—not constructed from a URI—and owns an exclusive,
lock-protected temporary in-memory trace sink. A pre-existing adapter trace sink
is a fail-closed ownership conflict; the original `None` value is restored in a
`finally` block. The executor invokes only `shadow_candidate`; it never calls
`start_canary`, `stop_candidate`, `restore_last_known_good`,
`verify_restoration`, collection/schema/index mutation, or policy code. A failed
shadow result raises a non-sensitive executor error; a structurally incomplete
captured trace is returned to the worker, which records the existing explicit
trace rejection and never publishes it.

Reference-serving refinement (implementation contract):

`MilvusRangeServingExecutor` is a separate injected adapter for the reference
gateway's *foreground* HNSW range query. Its immutable per-stream plan binds
the HNSW and FLAT collection names/identities, threshold radius, vector
dimension, and allowed served `ef` values to the exact `MonitorStreamKey`.
`preflight()` is an explicit read-only admission check, run before a capture:
it verifies stack health plus both tracks' `Loaded` state and identity bindings.
It is never called from `execute()`.

For every accepted request, `execute()` validates only in-memory stream, range,
dimension, and `ef` values, then issues exactly one HNSW `MilvusHarness.search`
call using the request's explicit range configuration. It neither contacts
etcd/MinIO, describes an index, reads a load state, writes a file, retries,
invokes the recorder, nor performs FLAT/oracle/shadow/policy/action work.
It returns a minimal `ServedQueryOutcome`; a search exception becomes a
non-sensitive failed/timeout outcome so the gateway preserves the host result
and the worker later rejects that observation rather than manufacturing trace
evidence. Collection/index/schema/configuration mutation is prohibited.

Safety, resource, and privacy invariants:

1. **Foreground isolation:** `offer()` is bounded and non-blocking. Full/closed/invalid monitoring state is observable but never a request failure.
2. **Read-only background data plane:** shadow/FLAT/oracle calls use validated range parameters and never create/drop/load/index/mutate a collection or change `ef` server-side.
3. **Bounded memory and scheduling:** queue capacity, worker drain limit, partial-batch count, and maximum observation age are registered configuration values with explicit drop behavior. The persisted state store contains only schedule/counter/blocked-state metadata; no hidden unbounded lists, raw-payload persistence, or retry loops are allowed.
4. **Privacy minimization:** request vectors and hit payloads never enter events, monitor state, policy input, drop counters, or exception text. Before live data is admitted, the trace/outbox host volume must meet ADR-006 owner-only/encryption requirements.
5. **No automatic actuation:** this layer cannot evaluate or execute policy. `NO_CHANGE`, recommendations, canaries, rollbacks, and configuration writes remain outside its imports and call graph.
6. **Operator rollback:** disable the host hook or stop the worker to halt new observations. Preserve already published evidence; do not delete, rebaseline, or modify Milvus automatically.

Consequences:

- The project gains an honest, framework-neutral host seam rather than an invented application server or a benchmark callback disguised as continuous monitoring.
- A read-only Milvus-backed executor and a reference gateway can be tested independently with fakes, then validated against ENV-001 without changing the detector, policy, source/outbox, or monitor contracts.
- The v1 worker loses only unpersisted observations on restart; that loss is explicitly observable and cannot become detector evidence. Durable replay begins only at the ADR-006 outbox.
- Multi-host coordination, distributed queues, web API/authentication, multi-tenant isolation, and automatic actuation remain out of scope.

Verification plan:

1. Unit-test constant-time/non-blocking recorder behavior with traps for filesystem, Milvus, publisher, policy, and actuation access; test queue-full, invalid observation, FIFO grouping, identity isolation, partial-batch restart loss accounting, durable trace-slot advancement/unknown-publication blocking, and executor/trace mismatch rejection.
2. Unit-test a fake executor and reference gateway to prove the background worker is the only path that can invoke capture or publish.
3. Pre-register EXP-008 before implementation. It must validate real ENV-001 read-only range queries through the reference gateway for separate L2 and COSINE stationary traffic, then prove `trace → source → monitor → NO_DRIFT → NO_CHANGE` with no actuation.
4. Add deliberate live DRY_RUN failure probes: source unavailable, queue full, executor timeout/failure, identity change, and worker restart. Each must preserve served-query success and record an explicit non-sensitive reason.

Modules affected:

New host-observation recorder, background worker, reference gateway, and focused tests; a new lazy/read-only executor adapter; EXP-008 validator and evidence artifacts. Existing `shadow_event_source.py`, `workload_monitor.py`, `shadow_extraction.py`, `drift.py`, `policy.py`, and all actuation modules are composed through their existing contracts only.

Research references:

- ADR-001 for benchmark/service separation.
- ADR-002 for range-query safety and DRY_RUN actuation governance.
- ADR-005 for monitor orchestration and foreground isolation.
- ADR-006 and EXP-007 for durable source/outbox semantics and data minimization.

---

### ADR-008: Require a validated 60-of-600, human-gated canary contract before any candidate routing

Status: Proposed — statistics contract, workload contract, implementation, and EXP-009 evidence required
Date: 2026-08-03
Risk level: CRITICAL
Evidence status: INFERRED mathematical correction and safety design. This ADR authorizes neither candidate routing nor rollback against live traffic.

Problem:

ADR-002 requires a one-sided 95% upper confidence bound for p95 latency while its canary contract fixes exactly 50 candidate queries from a 500-query batch. That combination is not distribution-free valid for an IID superpopulation: even the most conservative order-statistic upper bound—the maximum of 50 independent observations—covers a population p95 with probability only `1 - 0.95^50 = 0.923055024723`. At least 59 IID observations are required to reach 95% coverage (`1 - 0.95^59 = 0.951505474751`).

The controlled reference canary has a more honest available target: the frozen 600-occurrence manifest itself, rather than an unobserved IID production-latency population. Define that manifest's p95 by nearest rank `ceil(0.95 * 600) = 570`. If 60 candidate occurrences are selected uniformly without replacement from the manifest *after the eligible occurrence list is frozen and before any candidate result is read*, the sample maximum is at least that p95 threshold with probability at least `1 - C(600 - 30, 60) / C(600, 60) = 0.961003033592`. This finite-population randomization statement does not require sequential laptop/Docker latency observations to be IID. It does require a CSPRNG-backed simple-random selection, a fixed potential-latency population for the declared run, no post-freeze selection, and no unmeasured route-assignment interference; if any condition is unsupported, no latency bound is available.

This corrects the latency tolerance-bound minimum only; it does not make a distribution-free lower confidence bound for *mean capped recall* feasible at 60 observations. For bounded `[0,1]` recall values, the one-sided Hoeffding margin at `n=60`, `alpha=0.05` is `sqrt(log(20)/(2*60)) = 0.158001378516`, so even an observed mean of `1.0` has a lower bound below `0.95`. At `n=600` the same margin is `0.049964422956`, which reaches the floor only for an essentially perfect observed mean. The recall estimator therefore requires its own pre-registered assumptions and calibration; no document or implementation may imply that 60 solves both confidence problems.

There is an independent workload gap: DATASET-001 contains 200 measured query IDs, while the existing `MilvusActuationClient` contract requires 500 unique canary IDs. Repeating a vector under invented IDs, or calling a deterministic replay sample independent when it is not, would weaken the scientific claim and must not be silently introduced.

The current policy deliberately consumes confidence bounds without estimating them, and the current actuation adapter deliberately models query-time `ef` routing without a host routing authority. These are appropriate offline seams, but insufficient to support a real candidate route. A human approval boundary, an explicit sampling model, an immutable workload definition, and an independently verifiable rollback path are required first.

Decision drivers:

- Preserve the ADR-002 one-sided 95% confidence promise instead of weakening it after evidence collection.
- Keep candidate exposure at or below 10%; no server-side index/configuration mutation is involved because `ef` is query-time only.
- Bind a human approval to one exact detector/policy decision, metric/stratum, identities, transition, workload, and expiry.
- Make process restart fail to last-known-good routing, never to a partially remembered candidate state.
- Distinguish a finite-manifest randomized reference canary from an external production-serving deployment and from a claim of IID production traffic.

Alternatives considered:

| Option | Statistical validity | Safety | Cost/complexity | Decision |
|---|---|---|---|---|
| A — Retain 50 candidate queries and call the resulting maximum an IID-superpopulation 95% p95 upper bound | Invalid: maximum coverage is 92.3055% | Appears conservative but violates the stated confidence contract | Lowest | Rejected |
| B — Retain 50 and use a parametric/bootstrap bound | Potentially useful only under declared, calibrated assumptions; not distribution-free | Unsafe to use before calibration and sensitivity analysis | Medium | Deferred; may be evaluated only as an EXP-009 comparator |
| C — Require a CSPRNG-selected 60-of-600 finite-manifest sample with a human-gated route | Supports at least 96.1003% coverage of the frozen manifest's upper 5% latency tail under the declared finite-population potential-outcome model; otherwise fails closed | Explicit approval, bounded route, restart-to-LKG, durable audit | Higher but bounded | Chosen, subject to EXP-009 validation |
| D — Change a persistent Milvus/index setting for a canary | Does not solve the confidence issue and contradicts query-time `ef` semantics | Broad blast radius and hard rollback | High | Rejected |

Chosen solution:

No candidate route may be implemented or enabled until EXP-009 validates the following proposed contract.

1. **Workload and sampling contract.** A canary consumes exactly 600 distinct routing occurrence IDs and routes exactly 60 to the candidate (`0.10`), with all occurrence IDs, vector bindings, threshold/range/limit, metric, stratum, and ordering frozen in an immutable eligible-workload manifest before any candidate result is read. A CSPRNG selects a simple random 60-without-replacement candidate set only after that eligible manifest is immutable; a canonical selection record then binds the exact candidate IDs, its CSPRNG provenance, and the eligible-workload digest. The selected Stage-1 candidate is planned DATASET-002: 1,800 independent project-generated query vectors—600 unique routing vectors plus 1,200 disjoint background recall-audit vectors—against DATASET-001's unchanged base vectors and frozen thresholds. It is not generated or usable until its registry entry, generator, checksums, and verifier exist. The manifest must state whether occurrence IDs map one-to-one to unique vectors or intentionally reuse a vector, and must not call reused observations independent. If the workload cannot supply a scientifically justified population, candidate routing is prohibited; DATASET-001 alone does not satisfy this requirement today.
2. **Confidence-bound contract.** The experiment must define one estimator for mean capped-recall lower bounds and one for p95-latency upper bounds, their exact target population, their assumptions, and their coverage-calibration procedure. The proposed conservative pair is: (a) a finite-manifest p95 latency upper-bound statement equal to the maximum of the 60 candidate-route latencies. With the CSPRNG simple-random selection above, it is at least the frozen manifest's nearest-rank p95 threshold with probability at least `1 - C(570, 60) / C(600, 60) = 0.961003033592`, conditional on fixed potential outcomes for the declared run and no route-assignment interference; it makes no IID-superpopulation or production-latency claim; and (b) a one-sided Hoeffding lower bound on mean capped recall from the 1,200 disjoint background candidate audits, whose `[0,1]` margin is `sqrt(log(20)/(2*1200)) = 0.035330182290`. The latter can pass the `0.95` floor only when the observed background-audit mean is at least `0.985330182290`; it does not make a claim about a different population. The 60-candidate route is a finite-latency-bound sample, not a recall-bound sample-size guarantee. Stage 1 may verify the calculation and selection contract, but it cannot prove live no-interference; the controlled live stage must collect its schedule-stability evidence. If the CSPRNG selection, frozen potential-outcome model, no-interference condition, query-generator independence, replay design, or either estimator's assumptions are unsupported, the policy receives no applicable bound and fails closed.
3. **Approval contract.** A `CanaryApprovalGrant` is an immutable, signed, one-time record from an externally managed operator key. It binds the exact `PolicyDecision` digest and audit ID; EXP authorization ID; metric/stratum; current/candidate/last-known-good `ef`; configuration/index/data/FLAT identities; eligible-workload manifest digest; canonical candidate-selection-record digest; maximum traffic fraction `0.10`; issue and expiry timestamps; and a rollback pre-authorization for that exact transition. The verifier uses an injected public-key trust store; private keys, credentials, raw CSPRNG entropy, and raw query payloads never enter the repository or audit artifacts. Missing, expired, invalid, replayed, revoked, or mismatched grants fail closed before a candidate query is sent.
4. **Routing contract.** An approved route is an immutable in-memory plan consumed by the existing host serving seam. Foreground routing performs only an atomic plan read and deterministic occurrence-ID membership lookup; it does not read an approval file, verify a signature, contact Milvus health endpoints, write an audit record, wait for a worker, or invoke policy. The route selects exactly the candidate-selection record’s 60 candidate occurrence IDs; every other eligible occurrence uses the persisted last-known-good `ef`. There is exactly one routing authority—no competing route inside `MilvusActuationClient`, the host executor, or the policy.
5. **Lifecycle and rollback contract.** A coordinator validates grant, policy gates, pre-action health/identity, last-known-good persistence, and a paired 50-query shadow audit before atomically installing the route. It writes an append-only approval/action record before activation. Any hard failure, SLO failure, process restart, grant expiry, identity change, or unable-to-verify state clears the candidate route immediately and restores last-known-good routing. The grant’s explicit rollback pre-authorization permits this safety restoration only; it does not authorize another candidate or a full-traffic change. Successful rollback requires the existing health/identity checks and a 50-query FLAT/oracle restoration audit; failure disables further actions and requires operator intervention.
6. **Scope.** The first implementation remains a controlled reference canary. It performs HNSW query-time `ef` routing only; it does not mutate a Milvus collection, index, schema, or server configuration. It does not create an HTTP/gRPC service, integrate an external application, or authorize autonomous/full-traffic tuning.

Safety invariants:

- `DRY_RUN` remains the default and the only available mode until a verified approval grant is presented to the coordinator.
- A policy decision alone is insufficient to route a candidate; `START_CANARY` without a valid exact grant is a logged, non-actioning refusal.
- The candidate route is capped at exactly 60 of 600 eligible occurrence IDs and may not be expanded in-place.
- A restart or incomplete recovery defaults to the persisted last-known-good `ef`; no candidate route is reconstructed from volatile memory.
- Every approval, refusal, activation, candidate result, rollback trigger, restoration check, and operator re-enable is append-only auditable with non-sensitive identities and digests.
- No confidence interval, recall claim, latency claim, or production-traffic claim may be reported outside its declared sampling population.

Consequences:

- ADR-002’s 50-query canary sizing is superseded for any future candidate-routing implementation; its existing offline tests and DRY_RUN behavior remain valid, but they are not live-actuation evidence.
- `CANARY_QUERY_COUNT`, the 500-ID adapter workload constraint, deterministic selector, policy tests, and all related contracts must not be changed until EXP-009's Stage 1 validates the workload/statistics gates. The existing keyed-hash selector is an offline-test seam, not an eligible finite-population randomizer for candidate routing until it is superseded by the required CSPRNG selection-record contract.
- The project gains an explicit path to test a real reversible query-time transition without inventing a production host, but incurs a larger controlled workload and a new signed-approval/trust-store boundary.
- If EXP-009 cannot validate the sampling/estimator contract, the system remains DRY_RUN-only. This is a correct safety outcome, not a reason to relax SLO confidence requirements.

Verification plan:

1. Pre-register EXP-009 before code. It must separately validate the workload population, bound-estimator calibration, approval cryptography/expiry/replay handling, foreground non-blocking routing, candidate route cardinality, and rollback/restart behavior.
2. Unit-test invalid/missing/expired/replayed grant, wrong decision/identity/workload/selection record, invalid CSPRNG provenance, 59-vs-60 boundary, 600/60 routing cardinality, approval-audit write failure, restart/expiry failback, and every rollback trigger with no alternate candidate.
3. Verify the finite-population coverage calculation independently; test CSPRNG selection-record cardinality, uniqueness, pre-result binding, and absence of post-selection mutation against pre-registered synthetic finite populations. Run pre-registered stationary replays for the recall estimator. Define the live schedule-stability/no-interference checks that Stage 4 must capture; an offline test must never claim to prove them. Report every invalid bound and assumption failure without retuning an estimator on its evaluation replay.
4. Before any live candidate route, require a clean commit, verified DATASET/workload manifest, healthy ENV-001 stack, qualified last-known-good, exact action-class authorization, and raw preflight evidence.
5. In the controlled live stage, demonstrate one approved adjacent transition and its deliberate rollback path with raw candidate/last-known-good observations, CSPRNG selection evidence, schedule-stability/no-interference checks, identity checks, no server-side mutation, restoration audit, restart behavior, and immutable artifact verification. This stage remains human-gated; no automatic full-traffic action is authorized.

Modules affected:

Future approval-grant verifier, canary coordinator/routing-plan boundary, host-serving adapter injection point, confidence-bound estimator, actuation workload/selector, audit persistence, focused tests, and EXP-009 artifacts. `drift.py` remains a detector; `policy.py` remains pure; no module may duplicate their decisions. Existing `SafeActuationBoundary` and `MilvusActuationClient` may be composed only through their public contracts after the workload/statistical interfaces are revised.

#### EXP-009 Stage 2 implementation contract (proposed)

Stage 2 is a Core, offline-only security and routing boundary. It does not
construct a Milvus client, issue a search, mutate an index, or activate a
candidate route. Its purpose is to make every future candidate route depend on
one exact, independently verifiable human approval and an immutable route
partition. ADR-008 remains **Proposed** until the Stage 2 implementation and
the complete EXP-009 evidence have been reviewed.

Security options considered:

| Option | Correctness and security | Operational complexity | Scope and decision |
|---|---|---|---|
| A — shared-secret HMAC grant | A process able to verify can also mint grants; it cannot represent an externally held operator approval key | Low | Rejected: violates the approval separation required by this ADR |
| B — Ed25519 detached signature over a versioned canonical payload | Public-key verification is local, deterministic, and separates the external signing key from the verifier | Moderate, one audited dependency | Chosen |
| C — generic JWT/JWS framework | Can represent the same property, but adds claims/algorithm negotiation and token-processing scope not required by this single-process reference canary | Higher | Rejected for Stage 2; may be revisited only for a future external host deployment |

The implementation will add the pinned `cryptography==49.0.0` dependency and
use only its Ed25519 public-key verification API. It will not implement
cryptography itself. The pinned package supports this project's Python 3.14
runtime; its version, wheel/source hashes, and lockfile will be captured before
any Stage-4 use. Private keys, signing commands, raw signing entropy, and key
material are prohibited from the repository, test fixtures, and EXP artifacts.

**Canonical approval binding.** `CanaryApprovalGrant` is a strict
`canary-approval-grant-v1` record with exactly these unsigned fields:

- `grant_id`, `key_id`, `issued_at_utc`, `expires_at_utc`, and
  `experiment_id` (`EXP-009` only);
- `policy_decision_sha256` and the unchanged policy `audit_id`;
- `metric`, `threshold_stratum`, `current_ef`, `candidate_ef`, and
  `last_known_good_ef`;
- `configuration_identity`, `data_identity`, `flat_binding_id`, and
  `hnsw_binding_id`;
- `eligible_workload_sha256`, `candidate_selection_sha256`,
  `routing_population_count` (`600`), `candidate_count` (`60`), and
  `maximum_fraction` (`0.10`); and
- `rollback_pre_authorized` (`true`).

The signed message is exactly
`b"vdbench.canary-approval/v1\\0" + canonical_utf8_json(unsigned_fields)`,
where canonical JSON uses NFC-normalized strings, UTF-8, sorted keys, compact
separators, a terminal newline, lower-case SHA-256 values, no duplicate keys,
and rejection of unsupported or non-finite values. Finite floating-point values
are represented in the signed projection only as their exact IEEE-754 binary64
`float.hex()` strings, so a signing tool receives one unambiguous byte sequence
rather than relying on a language-specific JSON decimal formatter. The
persisted envelope adds
only `signature_algorithm: "Ed25519"` and an unpadded base64url detached
signature. `policy_decision_sha256` is computed from an exact, schema-versioned
projection of **every** `PolicyDecision` field, including all safety-gate
results and full evidence provenance; no caller may provide that digest. A new
field in `PolicyDecision` therefore requires a deliberate projection/schema
update and a failing compatibility test rather than silently being unsigned.

An injected `CanaryApprovalTrustStore` maps canonical `key_id` values to
Ed25519 public keys and independently answers whether a key or grant is
revoked. Verification rejects absent, malformed, non-canonical, unsupported
algorithm, invalid-signature, not-yet-valid, expired, revoked, wrong-EXP,
wrong-decision, wrong-audit-ID, wrong transition, wrong identity, wrong
workload, wrong selection record, wrong `60/600/0.10` contract, or missing
rollback pre-authorization with a stable non-sensitive refusal code. A valid
signature alone never installs a route.

**One-time and audit lifecycle.** Before route installation, a strict durable
`CanaryGrantUseStore` reserves the exact `grant_id` and signed-payload digest.
It is append-only for terminal states and refuses any duplicate or conflicting
use. The coordinator then persists an append-only approval/action audit record
and an activation marker before publishing a candidate plan. If either durable
write fails, the coordinator records `REFUSED_AUDIT_WRITE_FAILED` in the grant
ledger where possible, never installs a plan, and permanently consumes that
grant rather than allowing a potentially ambiguous retry. The policy audit ID
is a binding input; lifecycle record IDs are distinct, deterministic derived
identifiers so an audit sink's duplicate-ID protection remains meaningful.

**Grant-use ledger implementation convention (proposed).** This local reference
canary needs a crash-safe, single-host reservation primitive before a later
coordinator can install any route. The alternatives are:

| Option | Correctness / operations | Decision |
|---|---|---|
| A — atomic JSON record plus advisory file lock | Can serialize a single file, but makes unique grant-ID and payload-digest constraints, append-only terminal history, and corruption detection easy to implement incorrectly | Rejected for this security-critical lifecycle |
| B — standard-library SQLite ledger with explicit transactions | Enforces uniqueness and atomic reserve/terminal transitions across process restart without a new service or third-party dependency | Chosen for the single-host reference boundary |
| C — external transaction service | Can support a future multi-host deployment, but introduces credentials, availability dependencies, and deployment scope that the controlled reference canary explicitly excludes | Deferred to a separate ADR for multi-host serving |

`CanaryGrantUseStore` therefore uses a strict, schema-versioned local SQLite
database in a pre-existing private directory. It enables foreign keys, uses an
explicit `BEGIN IMMEDIATE` transaction for every state transition, SQLite
rollback-journal mode, and `synchronous=FULL`. A reservation stores only the
canonical `grant_id`, signed-payload SHA-256, and externally supplied RFC3339
UTC timestamp. Both `grant_id` and signed-payload digest are unique: reuse of
either is a fail-closed terminal refusal, including reuse under a different
grant ID. No raw grant JSON, signature, vector, threshold, secret, or private
key enters this ledger.

The ledger has two immutable facts: a successful `RESERVED` reservation and at
most one terminal event for it. The terminal event uses a deterministic
lifecycle-record ID derived from the grant ID and terminal reason, is written
in the same transaction as the reservation-state transition, and cannot be
deleted, reset, or overwritten by the process. The first implementation may
record only a supplied terminal reason; it neither installs nor removes a
route. A later coordinator must reserve first, write the independent approval
audit and activation marker, then publish the plan. On any audit/marker error,
it must record `REFUSED_AUDIT_WRITE_FAILED` when possible, leave LKG-only
routing, and never retry that grant. If the ledger is missing an expected
schema, malformed/corrupt, unreadable, lock-contended beyond its bounded
operation, or cannot durably commit, the caller receives an explicit store
failure and must not install a candidate route. There is no runtime migration,
automatic repair, or reset path.

Focused tests must prove same-ID replay, same-payload/different-ID conflict,
concurrent reservation linearization, terminal-event immutability, restart
readback, corruption/schema failure, and audit-failure consumption. This is
still an offline prerequisite; it does not authorize a candidate search,
route installation, or live Milvus action.

**Activation-coordinator implementation convention (proposed).** The sole
offline coordinator composes the verifier, immutable plan, grant ledger,
lifecycle audit, route-state marker, and in-memory authority in this exact
order: (1) verify the externally signed grant against the exact policy and
artifact bindings; (2) cross-check the verified grant, plan, and LKG binding;
(3) reserve the one-time grant/payload pair; (4) fsync an
`ACTIVATION_AUTHORIZED` lifecycle audit record; (5) atomically write the
`ACTIVATING` marker; then and only then (6) publish the immutable plan to the
authority. The authority starts empty and is never published on a refusal.

Every failure before publication produces an inactive/LKG-only result. An
audit-write failure consumes the grant with `REFUSED_AUDIT_WRITE_FAILED` and
must not write a marker. A marker or authority failure clears the authority,
attempts to restore the LKG-only marker, and consumes the grant with a specific
terminal reason. The coordinator neither performs a candidate search nor
accepts a later retry of a reserved grant. It returns immutable attempt evidence
only; Stage 3 owns runtime rollback/restoration auditing and Stage 4 remains
human-gated. Tests must trap every downstream dependency at each refusal and
prove the authority made zero candidate route assignments until all six steps
succeed.

**Immutable route authority.** The future `CanaryRoutePlan` is constructed
only from a verified eligible-workload manifest and candidate-selection record.
It binds all 600 canonical occurrence IDs to their immutable DATASET-002 query
IDs and route parameters, stores exactly 60 candidate occurrence IDs in a
frozen membership set, and derives a plan digest from its canonical document.
`resolve(occurrence_id)` is the sole foreground operation: it reads the current
immutable plan once, performs one occurrence-ID lookup and a bounded in-memory
one-shot claim, and returns the bound DATASET-002 query ID plus either candidate
`ef` or LKG `ef`. Claims are cleared only with the immutable plan and reject a
repeated occurrence before dispatch, making the exact 600-call contract
enforceable rather than merely auditable. The operation performs no filesystem
I/O, signature verification, health check, policy call, audit write, network
call, retry, allocation proportional to the workload, or Milvus call. An
unknown, duplicate, non-canonical, or out-of-manifest occurrence is refused
before any search dispatch. No module in `policy.py`,
`MilvusActuationClient`, or the background shadow worker may choose an alternate
`ef`.

The first reference integration is deliberately serial (`concurrency=1`, as
already frozen for EXP-009 Stage 4). Install and removal replace the complete
immutable snapshot, never mutate its membership collection in place. A future
multi-threaded or multi-host serving deployment requires a separate ADR and
linearizable routing implementation; it is not implied by Stage 2 evidence.

**Approval-expiry lease enforcement (proposed).** A one-time approval must not
remain candidate-capable merely because its in-memory plan was published before
its signed expiry. The options are:

| Option | Safety / foreground cost | Decision |
|---|---|---|
| A — coordinator timer only | A delayed or failed background task can leave the authority candidate-capable after expiry | Rejected |
| B — injected UTC clock checked atomically by the in-memory authority | Every foreground lookup can fail closed before it emits a candidate route; bounded constant-time comparison with no I/O | Chosen |
| C — persist/reload the route plan with expiry | Couples recovery to candidate-plan reconstruction and violates restart-to-LKG | Rejected |

The coordinator passes the already signature-verified `expires_at_utc` only to
the in-memory authority when publishing a plan. The authority parses it as a
strict RFC3339 UTC instant, rejects a plan already expired at publication, and
uses an injected UTC clock under the same lock before every snapshot and
occurrence claim. At or after expiry—or if the clock is malformed or
unavailable—it atomically removes the complete plan and every claim, then
returns `ROUTE_APPROVAL_EXPIRED` (or `ROUTE_CLOCK_UNAVAILABLE`) without an ef
or query ID. The foreground path performs no timer scheduling, filesystem I/O,
audit write, signature operation, retry, network call, or Milvus action. The
marker deliberately still contains no expiry or candidate route. An off-path
Stage-2 expiry reconciler observes only the authority's inactive expiry reason,
then atomically writes the LKG-only marker, appends the non-sensitive lifecycle
record, and terminates the grant exactly once. It is retry-safe: a durable
terminal grant record suppresses repeat audit writes. Stage 3 separately owns
the restoration audit for hard/recall/latency rollback triggers.

**Recovery and failback.** The route plan itself is memory-only and is never
reconstructed after a process restart. A strict atomic route-state marker holds
only non-sensitive LKG identity, LKG `ef`, grant ID, and plan digest. On every
startup, a marker indicating an interrupted candidate activation is atomically
transitioned to `LKG_ONLY`, audited as `RECOVERY_FAILBACK`, and yields no
candidate plan. Expiry, identity mismatch, malformed/corrupt state, failed
pre-action validation, or an explicit removal take the same failback path.
The Stage 3 rollback coordinator will add restoration-audit execution; Stage 2
only establishes the prerequisite that it can clear the only routing authority.

**Route-state marker implementation convention (proposed).** The marker is a
separate single-record recovery boundary, not a duplicate grant ledger or a
serialized candidate plan. The alternatives are:

| Option | Recovery correctness / operational complexity | Decision |
|---|---|---|
| A — persist and reconstruct the candidate route plan | Could resume exposure after a crash, but contradicts the mandatory restart-to-LKG safety rule | Rejected |
| B — strict atomic JSON marker containing only LKG binding and opaque activation identifiers | Makes every restart fail closed without retaining candidate membership, vectors, or parameters | Chosen |
| C — reuse the grant-use SQLite ledger as the mutable route marker | Preserves transactions, but conflates one-time authorization evidence with a replaceable recovery snapshot | Rejected |

`FileCanaryRouteStateStore` uses a `canary-route-state-v1` JSON document in a
pre-existing private directory. Its only states are `LKG_ONLY` and
`ACTIVATING`. Every document contains the canonical metric/stratum, explicit
LKG `ef`, configuration/data/FLAT/HNSW identities, externally supplied UTC
timestamp, and a stable reason code. `ACTIVATING` additionally requires the
canonical grant ID and route-plan SHA-256; `LKG_ONLY` requires both fields to
be null. It stores neither candidate `ef`, occurrence membership, vectors,
thresholds, signatures, nor any data capable of reconstructing a route.
Writes are same-directory temporary-file write+fsync, `os.replace`, and parent
directory fsync; the resulting file is owner-only. The store refuses symlinks,
non-private paths, unknown/missing fields, duplicate JSON keys, noncanonical
values, unsupported schema, and identity mismatches.

On every startup, the route authority begins empty before it reads any marker.
`ACTIVATING`, expired, malformed, corrupted, missing-required-binding, or
identity-mismatched marker state produces an in-memory `LKG_ONLY` recovery
result with no candidate plan. If possible, the store atomically replaces the
marker with an `LKG_ONLY` record carrying `RECOVERY_FAILBACK` (or a specific
invalid-marker reason). A later coordinator must append the corresponding
audit event before any new activation; if this write fails, the system remains
LKG-only and refuses new activation. Explicit removal follows the same
LKG-only transition. This component itself never installs a plan, dispatches a
query, verifies an approval, or emits an audit; it returns immutable recovery
evidence for the coordinator to audit.

Focused tests must prove atomic activation-marker write, restart failback,
identity mismatch, malformed/schema-corrupt state, write-failure containment,
strict no-candidate reconstruction, and no route/policy/Milvus import. This
marker is an offline Stage-2 prerequisite only; it does not authorize live
candidate routing.

**Stage-2 test and evidence gate.** TDD tests must first prove each refusal
leaves the authority in LKG-only state and makes zero candidate dispatch calls:
missing, invalid-signature, expired, revoked, decision/audit/transition/identity
mismatch, workload/selection mismatch, invalid selection provenance, replay,
and audit-write failure. They must independently test 59/60/61 candidate and
599/600/601 population boundaries, exact disjoint 60/540 partition, duplicate
occurrence IDs, unknown occurrence rejection, plan install/remove atomicity,
and restart/expiry/corruption failback. The focused and full suites, strict
canonical serialization tests, `git diff --check`, package-lock/hash evidence,
and an offline verifier bundle are required before marking Stage 2 verified.
No result from those tests authorizes Stage 3, Stage 4, or a live candidate
query without their separately required evidence.

**Stage-3 rollback-containment implementation convention (proposed).** Stage 3
is a separate, offline-only orchestration boundary.  It closes the gap between
an observed bad canary outcome and the existing one-time route authority; it
does not create a Milvus client, issue a search, or authorize a candidate
route.  The alternatives are:

| Option | Safety and correctness | Operational cost | Decision |
|---|---|---|---|
| A — call `SafeActuationBoundary`'s generic `ROLLBACK` path directly | Reuses a tested adapter seam, but it neither clears the sole 60-of-600 route authority nor binds the grant ledger/route marker | Low | Rejected: cannot prove Stage-3 route containment |
| B — `CanaryRollbackCoordinator` composes the authority, LKG marker, grant ledger, lifecycle audit, automatic-action controller, and an injected restoration-audit port | Removes the only candidate route before any slow/durable operation; gives one typed, idempotent evidence path for every trigger | Moderate, bounded | Chosen |
| C — let the serving adapter perform rollback when it notices a failure | Couples foreground latency and partial serving state to recovery, duplicates routing authority, and cannot make durable ordering reviewable | Low initially, high risk | Rejected |

`CanaryRollbackCoordinator` accepts an immutable, identity-bound rollback
context (`grant_id`, signed-grant digest, policy audit ID, route-plan digest,
`RouteStateBinding`, timestamp) and either an actual policy `ROLLBACK`
decision for hard/recall/latency evidence or a strict external trigger for
route-state corruption, approval expiry, or identity change.  All inputs must
match the active authority snapshot and the persisted activation marker; an
unavailable or inconsistent snapshot is itself a fail-closed trigger, never a
reason to leave candidate routing active.  The coordinator serializes attempts
per authority and gives repeated observations of one terminal grant a stable
no-op result rather than a second audit/write sequence.

For every trigger, the ordering is mandatory: (1) clear the in-memory
authority to LKG-only before any audit, network, or disk-dependent restoration
work; (2) persist the LKG-only route marker and disable automatic actions;
(3) append a non-sensitive `ROLLBACK_TRIGGERED` lifecycle record and terminally
consume the active grant, so that grant cannot authorize an alternate route;
then (4) invoke an injected `RestorationAuditPort` and append either
`ROLLBACK_RESTORATION_VERIFIED` or `ROLLBACK_RESTORATION_UNVERIFIED`.
The port’s immutable result must disclose health, configuration/index/data
identity, exactly-50-query FLAT/oracle restoration-audit completeness, and a
reason code.  It is an offline fake in Stage 3; a later separately approved
adapter may perform the actual health/read-only audit.  Stage 3 never invokes
`SafeActuationBoundary`, `MilvusActuationClient`, or PyMilvus.

If any durable write, controller disable, identity check, or restoration audit
fails, the authority remains cleared, the marker is retried/best-effort only
as LKG-only, automatic actions remain disabled where persistence is available,
and the outcome is explicitly unverified.  The coordinator never promotes a
different candidate, reconstructs a plan after restart, or re-enables itself.
Expiry uses the existing Stage-2 expiry reconciler for its exactly-once
marker/audit/ledger transition, followed by the same restoration-audit and
controller-disable path; it must not emit a duplicate expiry lifecycle record.
An explicit human re-enable remains outside this coordinator and requires the
existing confirmation token plus a new exact human approval grant.

The Stage-3 test/evidence gate requires TDD coverage for every ADR-002
hard/recall/latency trigger, route-state corruption, identity change, expiry,
restoration-audit failure, audit/marker/ledger/controller write failures,
duplicate/replayed rollback, and process restart.  Each test must prove
authority clear happens before the injected restoration port, LKG is the only
postcondition, the grant cannot support another candidate, lifecycle evidence
is append-only/non-sensitive, and no client/query call occurs.  A real local
composition test must restart the file/SQLite/JSONL stores and verify the same
postconditions.  A new immutable offline EXP-009 Stage-3 verifier bundle is
required before this convention, ADR-008, or any live candidate route can be
considered accepted.

**Stage-4 admission-preflight implementation convention (proposed).** Stage 4
adds a narrow, non-actuating admission boundary before the existing activation
coordinator can be composed with any serving path.  Its purpose is to make the
clean-revision, immutable-workload, exact-transition, qualified-LKG,
health/identity, schedule-contract, and policy-action prerequisites explicit
and independently testable; it does not weaken the separate signed-grant gate.

| Option | Correctness and safety | Operational cost | Decision |
|---|---|---|---|
| A — add live checks to `CanaryActivationCoordinator` | Couples the deliberately offline Stage-2 security primitive to Milvus/runtime state and makes its refusal tests depend on a serving environment | Low initially, high long-term coupling | Rejected |
| B — let `MilvusRangeServingExecutor` decide activation readiness | It can verify one serving plan but cannot bind the immutable workload, policy, LKG qualification, repository evidence, or later approval lifecycle | Moderate, incomplete | Rejected |
| C — add a pure `canary_admission.py` boundary that consumes independently collected evidence and emits an immutable receipt only | Keeps the activation primitive and foreground executor narrow while making every required Stage-4 precondition fail closed before route publication | Moderate, explicit composition | Chosen |

`evaluate_stage4_admission(...)` will receive a rebuilt immutable
600-occurrence workload/60-ID selection/route plan, a `START_CANARY` policy
decision in `CANARY_ENABLED` mode, a qualified LKG result, a clean commit
attestation, and an exact runtime readiness record containing the active
metric/stratum/identities plus a successful serving preflight.  It must return
only an immutable admission receipt: it cannot read a private key, verify or
reserve a grant, install or claim a route, create a Milvus client, issue a
search, mutate configuration, write an audit record, or invoke rollback.  A
receipt is valid only for the first frozen transition (L2 / `target-075`, LKG
`ef=400`, candidate `ef=800`) and only when the supplied plan, policy,
qualification, and runtime bindings are identical.  Any malformed, stale,
unclean, incomplete, mismatched, or unavailable input yields explicit
non-sensitive reason codes and no side effect.

The later live composition root must independently verify the one-time
Ed25519 grant through `CanaryActivationCoordinator`, then require a passing
admission receipt immediately before publication.  It must re-run the runtime
health/load/index-identity preflight after approval and before the first
foreground occurrence; any changed evidence invalidates the receipt and leaves
the authority LKG-only.  The receipt is therefore neither an approval nor an
authorization token, and it cannot be cached across a process restart,
identity change, or repository revision.  No private key, grant payload,
query vector, or CSPRNG entropy appears in it.

Focused tests must use only immutable fakes and prove: a complete exact
transition admits without any route/Milvus/activation call; dirty revision,
malformed or rebuilt-mismatched workload/selection, non-`START_CANARY` or
`DRY_RUN` policy, unqualified or identity-mismatched LKG, failed policy safety
gate, incomplete runtime preflight, and every metric/stratum/`ef`/identity
mismatch all refuse with a stable code.  The source must be AST-checked for no
PyMilvus, Milvus execution, route-authority claim, activation, rollback, or
audit persistence imports.  The eventual live runner must additionally collect
the frozen schedule-stability and raw approval/route/response evidence defined
by EXP-009; this preflight convention alone does not execute or validate those
live controls.

Research references:

- ADR-002 for conservative bounds, action ladder, SLOs, and rollback obligations.
- ADR-004 for immutable provenance binding.
- ADR-005 through ADR-007 and EXP-008 for the verified reference observation/evidence path.
- [NIST: tolerance intervals based on largest/smallest observations](https://itl.nist.gov/div898/handbook/prc/section2/prc264.htm) for nonparametric order-statistic tolerance concepts.
- Finite-population randomization identity for an upper 5% tail of 30 outcomes: `P(sample maximum exceeds the tail threshold) = 1 - C(600 - 30, 60) / C(600, 60)`.
- [Hoeffding (1963)](https://doi.org/10.1080/01621459.1963.10500830) for bounded-sum concentration and sampling-without-replacement context.

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
