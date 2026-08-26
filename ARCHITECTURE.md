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

**LKG qualification amendment (approved 2026-08-06, design-approved; implementation pending):** Supersedes the two-`QualificationWindow` model above for last-known-good qualification only. The `ResponseEstimate` predictive per-`ef` confidence-bound contract used elsewhere in this ADR (candidate/current/last-known-good response prediction for `START_CANARY` authorization) is a separate, unresolved contract and is not addressed or settled by this amendment.

- LKG qualification uses two **globally adjacent** qualification epochs at the same eligible `ef`. If epoch 1 uses source-window sequence numbers `N..N+5`, epoch 2 must use `N+6..N+11` -- no eligible same-lineage window may be skipped between them.
- Each epoch is composed of exactly six consecutive, disjoint 200-query monitor windows (FR-021, unchanged), yielding exactly 1,200 raw observations per epoch. Each constituent window must independently pass non-statistical health, correctness, identity, and rollback-readiness checks before its observations may contribute to the epoch; a window that fails any check invalidates the whole epoch and is never dropped, replaced, or averaged away by the other five.
- Qualification uses the **exact observed mean capped recall** and the **exact nearest-rank observed p95 latency** (rank `ceil(0.95 * 1200) = 1140`) over each epoch's 1,200 realized observations, compared directly against the existing recall floor and latency ceiling. This makes **no population-level confidence claim** -- it is a realized-controlled-workload qualification, not a sampling-based estimate. No field in this contract may be named `recall_lower_bound_95` or `latency_upper_bound_95_ms`.
- Independence/stationarity diagnostics (split-half stability, lag-1 autocorrelation, warm-up exclusion) are descriptive only under this contract; they do not gate PASSING/FAILING and are recorded separately from status-determining reason codes.
- The query workload for this contract is DATASET-003 (`lkg_qualification` role, 2,400 deterministic queries, role-disjoint from DATASET-001's base vectors and both DATASET-002 roles) -- never DATASET-002's `routing`/`recall_audit` roles, and never a substitute for base `data_identity`. Evidence for this contract must carry `qualification_dataset_id`, `qualification_dataset_version`, `qualification_manifest_sha256`, and `qualification_query_role` as first-class identity fields, distinct from Milvus collection/base-data identity.
- **DATASET-002 verification-scope separation:** DATASET-002's query-identity contract (manifest schema, every artifact hash, `SHA256SUMS`, exact file inventory, inherited DATASET-001 identity, deterministic routing/recall-audit array regeneration, role disjointness) and its oracle-correctness contract (`oracle_records.jsonl` semantic agreement with a fresh exact-oracle recomputation) are separable verification scopes, exposed as two distinct public functions in `dataset002.py`: `verify_dataset002_query_identity` (query-identity only, returns a result whose `verification_scope` is always `QUERY_IDENTITY_ONLY`) and `verify_dataset002_artifacts` (the complete strict verifier, unchanged in meaning, still performing the byte-exact oracle-record recomputation with no tolerance). DATASET-003 depends only on the former. DATASET-002's complete EXP-009 Stage 1 acceptance contract is unaffected and still requires the latter.
- **Known evidence-portability item (unresolved):** the accepted DATASET-002 artifact (`artifacts/exp-009/dataset`) was found to exhibit environment-sensitive COSINE floating-point score reproduction, most plausibly caused by BLAS/Accelerate differences; the exact historical environmental trigger remains unresolved. In the current environment, every manifest hash, `SHA256SUMS` entry, inherited DATASET-001 identity, and deterministically-regenerated routing/recall-audit array matches exactly; only 4,984 of 10,800 `oracle_records.jsonl` entries -- exclusively COSINE, zero L2 -- disagree with a fresh recomputation, and exclusively at few-ULP score precision (max delta `1.665e-16`), with hit membership, hit order, `full_count`, and `capped` unaffected in every case. This is tracked as a separate, unresolved evidence-portability item for DATASET-002's own EXP-009 Stage 1 acceptance contract; it is not fixed, tolerated, or worked around here, and DATASET-002-v1's bytes were not modified.
- Implemented: DATASET-003's query workload (code, tests, and the real 2,400-query `DATASET-003-v1` artifact at `artifacts/exp-009/dataset003/`). Not yet implemented: raw per-query evidence capture, epoch assembly, the constituent-consumption ledger, and `policy.py`/`last_known_good.py`/`canary_admission.py` integration.

**LKG qualification amendment, Phase 2 addendum (evidence sealing and constituent-window evaluation) (Accepted 2026-08-07):** Extends the LKG qualification amendment above with the concrete evidence-sealing, failure-lineage, readiness, status, and evaluation contracts required to turn Phase 1's raw per-query evidence (implemented) into the two epoch evaluations FR-071 requires: one 200-query monitor window per constituent window, six consecutive, disjoint windows (1,200 positions) per epoch, and two globally adjacent epochs (2,400 positions total) per qualification run. Nothing in this addendum changes FR-071's fixed model or the exact-observed-statistic contract already Accepted above; it specifies how that model is evaluated from durable evidence.

- **Sealing is atomic and is a mandatory prerequisite for Phase-2 consumption.** A Phase-1 qualification run's evidence SHALL be sealed via one additional, append-only, single-row table inside the same SQLite file. The seal operation SHALL acquire an immediate write lock before reading any run, workload-position, attempt, chain, or schema-version state, and SHALL perform every read, classification, comparison, and the seal insertion itself within that single transaction — the lock SHALL remain held through comparison, and the transaction SHALL commit only once verification has succeeded; any mismatch or error SHALL roll back rather than commit-then-fail. Sealing SHALL read the ledger's actual schema-version pragma directly and require it to match both the implementation's expectation and the value separately recorded on the run row. Sealing SHALL classify every one of the run's fixed query positions from zero or more durable attempts each, and SHALL persist both position-level and attempt-row-level counts as distinct, separately-checked figures. Sealing SHALL permanently prohibit further attempt appends from the moment of sealing onward, enforced at the schema level.
- **Sealing is idempotent and requires an explicit, caller-asserted expected outcome.** A caller sealing a run for the first time SHALL supply the completion state it expects and a stable reason for sealing now; sealing SHALL be refused, before any irreversible write, if the independently derived completion state disagrees with that expectation. A second sealing call against an already-sealed, unchanged run SHALL return the original seal exactly — same timestamp, same digest, never regenerated. Every subsequent Phase-2 open or consumption operation SHALL re-derive and re-verify the seal within its own single coherent, lock-held transaction, comparing before committing, never trusting a cached prior verification.
- **Per-position classification is mutually exclusive and exhaustive over every one of a run's fixed query positions.** For each position, counting its durable attempts by outcome: more than one durable `SUCCESS` attempt classifies the position `MALFORMED` (irrespective of whether durable failures are also present, which SHALL still be recorded in that position's reason codes); otherwise any durable non-`SUCCESS` attempt classifies it `FAILED`; otherwise exactly one durable `SUCCESS` attempt classifies it `CLEAN_SUCCESS`; otherwise (zero durable attempts) it is `MISSING`. This rule SHALL be applied so that the four resulting position counts always sum to exactly the run's expected query count, with no position counted twice and none omitted.
- **A durable failed or malformed-lineage position permanently invalidates that position's window, the window's epoch, and the whole qualification run.** A later successful retry at the same position does not erase, replace, or neutralize an earlier durable failure or malformed lineage. Recovery is always a fresh run identity and a complete new traversal of the full qualification workload — no mechanism exists, or may be added, for repairing an invalidated window, epoch, or run by re-dispatching or re-consuming any part of it inside the same run.
- **Constituent-window operational readiness is captured at the end of its own 200-query window, strictly before the complete qualification run can be sealed — its evidence therefore cannot and does not bind to a seal that does not yet exist at that moment.** The original readiness evidence for a window SHALL bind only facts available at that moment: the run identity and run-binding digest (both fixed from run creation), the window's own index, epoch, and exact sequence range, the check's stable idempotency key and provider-run identity, its original timestamp and monotonic duration, and separate, independently identified provenance for its health and rollback-readiness dimensions. Exactly one logical readiness check SHALL exist per constituent window for the life of a qualification run; the idempotency key identifies retries of that one check, never a mechanism for creating a distinct, later check, and a window's readiness evidence, once durably recorded, is permanent for the run — a later check cannot supersede it, and a durably recorded failing result invalidates the window irrecoverably within that run, exactly as a durable attempt failure does. A provider queried for an already-checked window, at any later time including after sealing, SHALL return the historically recorded evidence unchanged, byte-for-byte, or fail explicitly; it SHALL NOT perform a new check at that later time and represent the result as belonging to the earlier window. Only after the complete run is sealed may Phase 2 bind that unchanged original evidence to the resulting seal digest and to Phase 2's own source binding, as a distinct, later record that references but does not alter the original. Every timestamp in this contract (checked, sealed, ingested) is a canonical RFC3339 UTC value that SHALL be parsed and canonical-round-trip validated on read, and any comparison between two such timestamps (including verifying a readiness check was not recorded after its run's sealing) SHALL compare them as parsed instants, never as raw strings, since differing sub-second precision makes lexicographic string comparison of RFC3339 timestamps unsound. Correctness and identity remain derived mechanically from sealed Phase-1 evidence and the run binding alone, never caller-supplied.
- **Every window, epoch, and qualification-pair evaluation is one of exactly three states — `INCOMPLETE`, `PASSING`, or `FAILING`** — with fixed precedence: `FAILING` outranks `INCOMPLETE`, which outranks `PASSING`. Missing, unsealed, unverifiable, or not-yet-attempted evidence SHALL always resolve to `INCOMPLETE`, never silently to `PASSING` or `FAILING`.
- **Every qualification evaluation is bound to an immutable, versioned evaluation contract** containing only statistical thresholds, sizing, and statistic-definition versions — never a code version or source-control revision, which are recorded as separate build-provenance fields alongside, not inside, that contract's identity digest, and which are asserted by the caller rather than independently attested.
- **Phase 2's sole output is one immutable `LkgQualificationEvaluation` artifact per run**, carrying both epoch evaluations, adjacency and identity-consistency proofs, final status, a `qualified` flag true if and only if status is `PASSING`, and complete evidence lineage back to the sealed chain head. Phase 3 SHALL consume this artifact as an opaque, already-verified fact and SHALL NOT recompute windows, epochs, recall, or latency from raw Phase-1 rows.

Implemented: none of the above (design only). Not yet implemented: the Phase-1 sealing extension, the Phase-2 evidence/evaluation types, the consumption ledger, the operational-readiness fake provider, and the final-artifact assembly.

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

Status: Accepted — corrected implementation and validation rerun verified
Date: 2026-08-02
Risk level: CRITICAL
Evidence status: VERIFIED for the corrected implementation and its registered stationary/injection reruns at `8278711`; it does not by itself establish live-production drift detection. Supersedes the MMD preprocessing convention, zero-variance handling, and decision_confidence gate defined in ADR-002. Does not change the KS signal, recall signal, Holm correction, effect-size gates, three-state output, consecutive-window rule, or actuation ladder.

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

Status: Accepted — implementation and EXP-005 provenance evidence verified
Date: 2026-08-03
Risk level: CRITICAL
Evidence status: VERIFIED for the immutable provenance implementation and EXP-005 trace-to-policy evidence path; it does not authorize live actuation.

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

The live composition root will obtain vectors only through a separate
`Dataset002CanaryQuerySource`.  At construction it must re-run the existing
DATASET-002 checksum/schema/oracle verifier, check every routing occurrence and
every schedule-control vector against the already admitted manifest's float32
SHA-256 binding, and retain immutable in-memory mappings only.  Its routing
lookup receives both canonical occurrence ID and expected DATASET-002 query ID
and fails closed on any mismatch; its control lookup accepts only the frozen
IDs `600…649`; and its separate recall-audit lookup accepts only the verified
disjoint `600…1799` DATASET-002 recall-audit IDs.  It does not choose an `ef`,
claim an occurrence, issue a search, expose a bulk/raw-array export, or persist
query payloads.  This keeps artifact verification out of the foreground
request loop while ensuring a later serial runner cannot silently substitute a
vector after Stage-1 verification.

**Stage-4 serial-schedule implementation convention (proposed).** Before a
live composition root exists, Stage 4 requires one pure, immutable execution
schedule derived from the already-admitted manifest and route plan.  It must
contain exactly 1,200 ordered slots: three complete 50-query LKG-only control
sweeps, the 600 routing occurrences in manifest order, one complete control
sweep after each contiguous 100 routing slots, and three complete post-routing
control sweeps.  Thus it represents 600 controls and 600 routes, with the
route partition copied only from the frozen 60/540 `CanaryRoutePlan`; it never
selects an occurrence or reimplements route membership.

| Option | Correctness and safety | Decision |
|---|---|---|
| A — embed cadence in a future Milvus runner | Couples immutable workload proof to client I/O, making schedule mistakes harder to unit-test and audit | Rejected |
| B — let the route authority emit the control/routing sequence | Expands the atomic foreground authority beyond one-shot membership claims and risks conflating scheduling with authorization | Rejected |
| C — add pure `canary_schedule.py`, consumed later by a serial composition root | Makes the exact 1,200-slot cadence, control order, route partition, and canonical digest independently reproducible before any query is possible | Chosen |

`build_stage4_execution_schedule(manifest, plan)` must validate the manifest
and plan independently, require their metric/stratum/transition/identity and
every routed occurrence binding to match exactly, and return a schema-versioned
immutable object with a canonical SHA-256 digest.  A control slot contains only
the frozen control ID/digest and the LKG `ef`; a routing slot contains only the
already-bound route occurrence and its plan-selected `ef`/kind.  Raw vectors,
approval material, runtime state, timestamps, outcomes, and mutable clients
are forbidden.  A later runner must rebuild this schedule from its admitted
inputs and fail closed on any digest or structure mismatch before dispatch.

Focused tests must prove exact 1,200-slot ordering, all twelve control sweeps
in frozen ID order, routes `0…599` in manifest order, exactly 60 candidate and
540 LKG route slots, malformed manifest/plan mismatch refusal, schedule digest
tamper refusal, and absence of Milvus, activation, route-authority, query-source,
or filesystem imports.  This convention creates no route authority and does
not authorize a candidate query; the later runner still requires the Stage-4
admission receipt, fresh pre-dispatch preflight, and an externally signed grant.

**Stage-4 execution-ledger implementation convention (proposed).** The later
serial runner requires a durable, independently inspectable record of the
attempted frozen schedule, without letting a log implementation dispatch or
interpret queries.  The choices are:

| Option | Correctness and operational behavior | Decision |
|---|---|---|
| A — append JSONL records only | Simple, but an incomplete final line, duplicate index, restart resume, and cross-record ordering require bespoke locking and recovery rules | Rejected |
| B — local SQLite append-only record table, strict schema, and SHA-256 chain | Uses the established single-host durable-store pattern; transactions enforce one next slot while the chain makes accidental/reordered record corruption detectable | Chosen |
| C — rely on Milvus/client logs | Those logs do not bind the frozen schedule, route partition, or per-slot monotonic interval and cannot prove no unscheduled runner search | Rejected |

`Stage4ExecutionLedger(path, run_id, schedule)` will accept only a verified
immutable schedule.  Its private single-host database has immutable run
metadata (`run_id`, schedule digest, deterministic genesis hash) and an
append-only row for each attempted step.  Every row binds its exact next
execution index, expected `ef`, observed non-sensitive outcome, strict
monotonic start/end interval, previous-record hash, and canonical record hash.
SQLite triggers prohibit row or metadata updates/deletes through the ledger
database.  The implementation revalidates schema, schedule digest, hash chain,
and sequence before every append and after restart; corruption, wrong schedule,
out-of-order/repeated slot, non-monotonic interval, or private-path failure is a
stable fail-closed error.  One failed/timed-out/threshold-invalid/
health-invalid/identity-invalid outcome is recorded then terminally blocks all
later slots.  Only exactly 1,200 valid records constitute a complete ledger.

This is integrity evidence against accidental loss, reordering, and ordinary
store misuse; it is not a tamper-proof audit system against an attacker who can
replace both the local database and all external evidence.  A later evidence
bundle must bind the final chain hash into an independently verified manifest.
The ledger contains no raw vector, hit, score, oracle result, approval grant,
credential, client object, filesystem path, or query dispatch code.  It cannot
resume or authorize candidate routing after process restart; Stage-3 recovery
still clears to LKG-only and a new human-approved activation is required.

**Stage-4 schedule-evaluation implementation convention (proposed).** The
schedule-stability result is a pure post-run calculation, not a property of the
query executor or durable ledger.  The evaluator receives only a rebuilt
schedule and records already verified by `Stage4ExecutionLedger`; it rechecks
slot/`ef` correspondence, derives each 50-control sweep's median and
nearest-rank p95, derives `m0` as the median of the three pre-sweep medians and
`p0` as the nearest-rank p95 of the three pre-sweep p95s, and applies the
pre-registered `10 ms`, `1.50 × p0`, and `1.25 × m0` ceilings to all twelve
sweeps.  It separately returns the maximum of the exact 60 candidate-route
latencies and the already-frozen finite-manifest coverage statement.  It must
label this result *NOT APPLICABLE* on any incomplete/failed ledger, binding
mismatch, non-finite value, wrong route partition, invalid baseline, or ceiling
breach.  It does not estimate recall: the 1,200-query recall audit remains a
separate required Stage-4 evidence stream.

**Stage-4 offline serial-composition implementation convention (proposed).**
The remaining offline gap is a narrow composition test of the already separate
admission receipt, immutable 1,200-slot schedule, verified DATASET-002 vector
source, slot-executor seam, durable execution ledger, and pure evaluator. It
must prove that these contracts agree on exact bindings and fail closed before
any later human-gated composition root can be considered. It is expressly not
a live runner, grant verifier, route authority, activation coordinator, or
Milvus client.

| Option | Correctness and safety | Decision |
|---|---|---|
| A — compose the first runner directly with `MilvusRangeServingExecutor` and activation/grant code | Would make the remaining offline proof candidate-capable and conflate fake-only composition with the separately human-authorized Stage-4 boundary | Rejected |
| B — make the ledger or evaluator dispatch the schedule | Violates their narrow persistence/statistics responsibilities and makes query capability difficult to audit | Rejected |
| C — add `canary_serial_runner.py` as an offline-only, dependency-injected composition boundary | Proves source-to-slot binding, strict serial ledger persistence/restart, terminal containment, and evaluator hand-off using fakes while importing no Milvus, activation, approval, or route-authority module | Chosen |

`Stage4SerialRunner` receives only an already computed `Stage4AdmissionResult`,
a validated `Stage4ExecutionSchedule`, a verified-DATASET-002 source adapter, a
slot-executor protocol, a `Stage4ExecutionLedger`, and injected clocks. A
passing admission receipt is necessary but is not a grant; this runner does not
accept, parse, verify, reserve, cache, or derive any grant. It binds the
receipt's `plan_sha256` to the schedule before obtaining a vector, and resumes
only from the ledger's exact next safe slot. Its source adapter reconstructs a
`ScheduleControl` solely from a control step's frozen ID/digest and calls the
existing DATASET-002 source; routing lookups pass the schedule's canonical
occurrence ID, DATASET-002 query ID, and vector digest unchanged. A source
failure, executor exception, invalid outcome, ledger refusal, or unsafe result
is represented as one durable failed slot where possible and stops the run; it
never retries, skips, reorders, selects a route, or chooses another `ef`.

The executor receives one immutable step and one verified in-memory vector and
returns only non-sensitive success/timeout/threshold/health/identity/result
facts. The runner, not the executor, owns strict injected monotonic start/end
timestamps and derives the ledger latency from that interval. Vectors, raw
hits, scores, grants, and credentials are forbidden from ledger records and
run-result objects. A `max_slots` test bound may stop an otherwise safe run in
`IN_PROGRESS`; it is not a retry mechanism. Only ledger completion hands
evidence to the existing pure evaluator, whose finite-manifest latency result
remains conditional and whose recall bound remains unevaluated.

Focused tests must prove: exact synthetic 1,200-slot completion (600 controls,
540 LKG routes, 60 candidate-labelled slots); receipt/schedule mismatch causes
zero source or executor calls; source and executor failure each persist one
terminal record and prevent a later call; unsafe returned health/identity or
threshold facts do the same; a restart continues at precisely the next ledger
index without re-dispatch; a completed stable fake run reaches the existing
conditional evaluation; and AST inspection finds no Milvus, serving,
activation, approval, grant, or route-authority import. This implementation
does not dispatch a live search and cannot provide Stage-4 live evidence or
authorization.

**Stage-4 live serial-composition implementation convention (proposed).** The
offline runner deliberately cannot be upgraded in place: it has no
approval/authority/rollback dependency, and allowing a generic executor to
become live would let a valid admission receipt substitute for a human grant.
The eventual candidate-capable root is therefore a separate
`canary_live_runner.py` composition boundary. It is the only future component
permitted to combine a human-approved activation with schedule dispatch; it
must be fake-tested first and must not be used in a live run until all EXP-009
Stage-4 preconditions are freshly verified.

| Option | Correctness, measurement, and containment | Decision |
|---|---|---|
| A — attach a live executor to `Stage4SerialRunner` | The offline runner cannot verify/reserve a grant, claim one-shot occurrences, perform post-activation preflight, or invoke rollback. Treating its admission receipt as authority would violate the Stage-2/3 boundary. | Rejected |
| B — make `MilvusRangeServingExecutor` own approval, schedule, or rollback | Conflates a one-search read-only adapter with human approval, durable evidence, and route containment; it also makes future host serving paths candidate-capable by construction. | Rejected |
| C — add a narrow `Stage4LiveRunner` with injected activation, claim, probe, search, ledger, and rollback ports | Keeps the existing contracts independently auditable while binding all candidate-capable dispatch to an exact activated context, strict serial schedule, and mandatory LKG restoration. | Chosen |

The root must receive an immutable `Stage4LiveRunRequest` containing the
rebuilt Stage-1 manifest/selection/plan, policy/LKG/repository evidence,
external signed grant and trust context, `RouteStateBinding`, and a fresh
run identifier. It must obtain a first read-only runtime readiness result,
evaluate `evaluate_stage4_admission`, then invoke
`CanaryActivationCoordinator.activate`. Immediately after activation it must
run the complete health/load/index-identity preflight again, rebuild and
re-evaluate admission from that fresh runtime evidence, and dispatch **zero**
slots if either preflight or either admission result is incomplete. Any such
post-activation refusal first clears the authority and invokes the existing
rollback path; it never leaves an active candidate plan for a later process.

The activation output needs one additive, non-secret
`ActiveCanaryContext`: grant ID, verified signed-payload SHA-256, policy audit
ID, plan SHA-256, exact route-state binding, and activation timestamp. The
coordinator alone creates it after approval verification, reservation, durable
audit, state marker, and authority publication. This closes the current
interface gap: a live root otherwise cannot construct the exact
`RollbackContext` required to contain a later slot/probe failure without
duplicating grant verification or trusting unverified caller input.

For each immutable schedule step, `Stage4LiveRunner` must resolve the verified
vector only through `Dataset002ScheduleVectorSource`. A routing step must then
call the injected authority's `resolve_and_claim` exactly once and require an
exact occurrence ID, DATASET-002 query ID, planned `ef`, and route kind match
before the one HNSW search port is invoked. A control step is LKG-only and may
not claim a routing occurrence. The search port returns a compact
`ServedQueryOutcome`; it may be implemented by the existing injected
`MilvusRangeServingExecutor`, but the root itself imports neither PyMilvus nor
a configuration-mutation API. The root takes the monotonic timing boundary
around the one search only. Health/load/index-identity probes occur immediately
before and after each slot **outside** that latency interval and populate the
ledger's existing before/after facts. This stricter probe cadence is explicit
control-plane load within the experiment; the frozen schedule-stability
controls remain the falsification evidence for any resulting interference and
the reported latency bound remains conditional on the stated no-interference
assumption.

Any claim refusal, source/search exception, timeout, threshold-semantic error,
non-finite or malformed outcome, pre/post health failure, identity mismatch,
ledger refusal, clock failure, or post-activation preflight failure must stop
further dispatch. The root must append one terminal ledger observation where
the existing ledger permits it, clear candidate authority before any durable
or audit work, and invoke `CanaryRollbackCoordinator` with an explicit new
`SLOT_SAFETY_FAILURE` or `RUNTIME_PREFLIGHT_FAILURE` trigger as appropriate.
The mandatory successful completion uses the distinct `COMPLETED_CANARY`
trigger. The rollback trigger vocabulary must be extended rather than
relabeling these conditions as policy, corruption, or identity events. A
complete 1,200-slot run is not a success exit while candidate routing remains
installed. It must perform the separately grant-pre-authorized deliberate
rollback, including the
existing 50-query restoration audit, and return success only if containment
and restoration are both verified. No alternate candidate, retry, batching,
concurrency above one, configuration mutation, or automatic re-enable is
permitted.

Before an implementation is eligible for live evidence, focused fake-port tests
must cover: first-preflight refusal with zero activation/search; post-activation
preflight refusal with zero search and one containment attempt; exact
claim-to-step binding; duplicate/unknown/wrong-`ef` claim refusal; source,
search, timeout, threshold, health, identity, clock, and ledger failures each
causing one terminal containment with no later slot; exact serial 1,200-slot
completion with 60/540 route assignments; mandatory successful final rollback;
and restoration-audit failure disabling automatic actions. Tests must prove
that no query happens outside the injected search port and that the root cannot
construct an approval grant, bypass authority claim, or call a Milvus mutation
API. A new sealed offline EXP-009 evidence bundle is required after those tests
before a human grant is requested. The later live evidence remains subject to
the original EXP-009 Stage-4 contract and is not authorized by this convention.

**Implementation/evidence update — 2026-08-04.** `Stage4LiveRunner` was added
at `94fe22c` as the chosen injected composition root, without a Milvus,
configuration-mutation, or approval-verification import. Its strict fake-port
tests cover initial and post-activation refusal, exact 1,200-slot serial
completion (60 candidate / 1,140 LKG searches), claim/source/search/timeout/
threshold/health/identity/clock/ledger failures, malformed port values,
non-fresh-ledger refusal, active-context mismatch, and mandatory final rollback.
The separate profile-aware verifier at `920aab9` preserves verification of the
older offline-composition bundle and seals a distinct live-root fake-only
bundle. That bundle is published by `1614521` at
`artifacts/exp-009/run-20260804T141850Z/`: 14 commands passed from clean commit
`920aab9371b49501bdfbea644a0c5575a15a96e6`, including 525 repository tests in
165.256 seconds and dependency integrity. Its verifier records zero Milvus
clients/searches/configuration mutations, zero real grant verification, and
zero real candidate-route enablement/claims. This is VERIFIED fake-only
composition evidence; ADR-008 remains **Proposed**, and it neither authorizes
nor substitutes for the required human-granted controlled live canary.

**Implementation/evidence update — 2026-08-11 (durability remediation).** A
required-before-live durability gap was found by direct call-chain review:
`Stage4LiveRunner` dispatched a candidate search before any per-slot durable
record existed, so a crash between search dispatch and the terminal ledger
append could leave zero durable evidence that the slot was attempted, and a
restart could see an empty ledger and appear fresh. `canary_execution_ledger.py`'s
`Stage4ExecutionLedger` was versioned from schema v1 (terminal-only records)
to v2: a new, independently hash-chained `execution_starts` table records one
durable STARTED marker, committed via a new `start_slot` method, strictly
before any search dispatch; the existing terminal-record method was renamed
`complete_slot` and now requires and binds the exact `started_record_sha256`
it closes. A new `Stage4LedgerStatus.AMBIGUOUS` status is reported whenever a
durable STARTED marker has no matching terminal record; `start_slot` and
`complete_slot` both refuse while it holds, and only the exact in-process
`Stage4ExecutionLedger` instance that itself committed a STARTED marker may
close it -- a freshly reopened instance, including one reopened after a crash
and holding the identical digest, is refused (`STARTED_SESSION_MISMATCH`).
`Stage4LiveRunner._execute_slot` now calls `start_slot` before any
preflight/claim/dispatch step, so the search port is unreachable without a
prior durable commit. `Stage4SerialRunner` (the offline preflight composition
root; no Milvus import, injected-executor only) was updated identically for
consistency, since it shares the same ledger primitive, though it remains
structurally incapable of live dispatch. v1 database files are not migrated
or reinterpreted: schema verification refuses to open them under v2 code
(`LEDGER_SCHEMA_MISMATCH`); this repository has not produced any real
governed live Stage-4 evidence under v1, so no migration path was required or
provided. Existing 600/60 schedule cardinality, admission/route/grant
lineage, `Stage4EvidenceBinding` binding, and rollback trigger semantics are
unchanged. This does not change ADR-008's own **Proposed** status above, and
it still authorizes no live candidate route.

**Stage-4 runtime-probe adapter implementation convention (proposed).** The
live root intentionally accepts a small `preflight(binding) ->
Stage4RuntimeReadiness` / `slot_safety(binding) -> Stage4SlotSafety` port, while
the existing read-only `MilvusRangeServingExecutor.preflight()` produces a
framework-neutral `ServingPreflightResult`. The missing adapter must bridge
those contracts without giving either the root or the adapter approval, routing,
search, or configuration-mutation capability.

| Option | Correctness and coupling | Decision |
|---|---|---|
| A — add Stage-4 semantics directly to `MilvusRangeServingExecutor` | Couples a reusable reference-serving adapter to approval-stage types and makes ordinary host serving depend on canary lifecycle concerns. | Rejected |
| B — have `Stage4LiveRunner` call a Milvus client/preflight directly | Breaks the injected-port boundary, expands the candidate-capable root's authority, and makes fake-only verification weaker. | Rejected |
| C — add a narrow `Stage4ServingRuntimeProbe` that consumes only a structural read-only preflight port | Keeps the root dependency-injected, keeps serving reusable, and gives one auditable fail-closed mapping from exact stream identity/readiness to Stage-4 values. | Chosen |

The adapter is bound at construction to exactly one `MonitorStreamKey` and one
`RouteStateBinding`; metric, threshold stratum, configuration/data identity,
and FLAT/HNSW binding IDs must agree exactly. Every call rejects a requested
binding mismatch. It invokes the injected preflight once, never calls a serving
`execute` method, and requires `complete=true` with exactly one checked stream
before reporting a passing `Stage4RuntimeReadiness`. For an incomplete result,
the one bound stream must report zero checked streams alongside a known health,
load, identity, or binding failure. Any other count is scope-ambiguous and
fails closed. Its UTC evidence clock must be valid; an unavailable clock is
incomplete and cannot admit activation.

For `slot_safety`, the same read-only preflight is called immediately before or
after a slot by the root, outside its search timing boundary. Stack-health and
load-state failures map to `health_ok=false`; identity/binding failures map to
`identity_ok=false`; unknown, malformed, unavailable, scope-ambiguous, or
otherwise incomplete results set both false. Every returned reason is converted
only from the documented stable preflight vocabulary to a canonical
non-sensitive Stage-4 code; an unknown reason is never propagated or treated as
safe. The adapter imports neither approval, route authority, policy, rollback,
serving execution, PyMilvus, nor configuration-mutation code. Focused fake-port
tests must prove exact binding/scope success; health, load, identity, binding,
unknown-reason, malformed-result, exception, clock, and requested-binding
failures; one preflight call per adapter operation; and an AST import boundary.
No live preflight is authorized until this offline implementation has passed its
own tests and sealed evidence.

**Implementation/evidence update — 2026-08-04.** The adapter and neutral
runtime values were implemented at `f0f0511`. They preserve the existing
admission/root public re-exports while removing the adapter's transitive
dependency on the candidate-capable root. The separate fake-only verifier
profile was added at `a8373ad` and its immutable bundle was published at
`1c995f7`: `artifacts/exp-009/run-20260804T145434Z/`. Generated from clean
commit `a8373adf14a2efff117479f580b817c2e0c381f6`, independent verification
returns `COMPLETE` with manifest SHA-256
`ed6bf268a9c61d97e2f04b7cd02217f6a068741417fc15b4a35eea12d85fd890`, raw-result
SHA-256 `59bdfc984f75d52bfbdbf8b1a8a46835952bb80da31dc59785c66b08340289a2`, and
receipt SHA-256 `f8ae51cce6770a6d70100efbeb89f8c720c51c65ea4c2a0fe4e63b58bbcbb12c`.
Its eight commands passed, including five focused suites and 537 repository
tests in 153.481 seconds; `pip check` found no broken requirements. The sealed
profile records false for real-client construction, real serving preflight,
live search, real-grant verification, candidate-route enablement, and Milvus
configuration mutation. This verifies only offline adapter conformance. It
permits a separately captured read-only ENV-001 preflight; ADR-008 remains
**Proposed** and no candidate routing, grant use, or configuration change is
authorized.

**Read-only ENV-001 preflight evidence convention (proposed).** The permitted
next operation is exactly one frozen L2 / `target-075` stream built from the
reviewed EXP-005 baseline
`artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json`. Its exact
`MonitorStreamKey` and `RouteStateBinding` must be derived from that baseline,
with `last_known_good_ef=400`; no first-seen identity is trusted. The preflight
composition may lazily construct a PyMilvus client solely behind a narrow
facade exposing `get_load_state` and `describe_index`. The facade must record
every call and reject any other client method, so a runtime proof can show zero
search, insert, collection-create/delete, index-create/drop, grant, route, and
configuration-mutation calls.

The runner must capture validated baseline hash and DATASET-001 manifest hash,
health/load/identity evidence, pre/post exact FLAT and HNSW identities, the
adapter's timestamped `Stage4RuntimeReadiness`, its `Stage4SlotSafety`, and the
facade call transcript in a no-replacement immutable local evidence directory.
It must run adapter admission and slot-safety operations separately, so each
invokes one structural serving preflight; the expected successful transcript is
four `get_load_state` and eight `describe_index` calls, with zero calls of every
disallowed kind. Any incomplete/unsafe adapter result, changed pre/post
identity, baseline mismatch, invalid data artifact, invalid timestamp, wrong
call transcript, or failed local evidence write is a fail-closed preflight
failure. A passing preflight proves only the captured point-in-time health,
load, and identity facts. It does not qualify a policy/LKG state, establish
no-interference, verify or use a grant, claim a route, or authorize candidate
traffic.

**Live evidence update — 2026-08-04.** The permitted read-only preflight was
captured from clean commit `d03bbc352520a780ab6e76382d41f2fa09eb5692` and
published in evidence commit `3353992` at
`artifacts/exp-009/run-20260804T153006Z/`. Independent verification returned
`COMPLETE` for the frozen L2 / `target-075` binding: baseline SHA-256
`6e26b0793ca44732ec464fe08e09287d28c87356f0f0e8dd71691e3e8658dc52` and
DATASET-001 manifest SHA-256
`b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9` were
bound, pre/post FLAT and HNSW identities matched exactly (including HNSW
`M=16`, `efConstruction=200`), and the recorded facade transcript contains
exactly four `get_load_state` and eight `describe_index` calls. The raw-result,
manifest, and receipt self-hashes are respectively
`977d6b3d9fe978d4e3a757b9156351b3d019169ebeca67580c7d177a5e38065c`,
`6487b1798be6b904a84078dc7e9fec2bd8949be207346babbec60d03b37b7449`, and
`613a30e2320025cd166d2214edf31ee1de773668d09bbb7e494f4cdb8a250c61`.
The evidence assertion records zero search, insert, collection/index mutation,
grant/route use, or configuration mutation. The pinned virtual environment
then completed 550 repository tests in 193.153 seconds. This verifies only
the defined point-in-time preflight; ADR-008 remains **Proposed**, and no
candidate routing, approval use, rollback, or automatic tuning is authorized.

**Stage-4 recall/latency evidence-binding repair convention (implemented).** A
review of unintegrated recall-audit draft code found that a recall result and a
latency result could otherwise be combined without proving they came from the
same run and exact configuration. That is unacceptable for EXP-009: a passing
report assembled from different metric, stratum, `ef`, identity, workload, or
run contexts must be impossible, not merely discouraged.

| Option | Integrity and scope | Decision |
|---|---|---|
| A — retain independently supplied recall and latency JSON | Cannot prove that two passing inputs describe the same experiment; a hand-authored latency document can be misleading. | Rejected |
| B — add loose matching checks in the human report | Improves one caller but leaves other callers and stored evidence without a canonical provenance boundary. | Rejected |
| C — require one canonical, hash-bound `Stage4EvidenceBinding` across durable recall and latency evidence before combination | Makes the experiment/run/configuration/workload identity explicit, independently verifiable, and reusable by every consumer. | Chosen |

`Stage4EvidenceBinding` must be a strict, schema-versioned, canonical document
whose digest is immutable before either evidence stream is evaluated. It binds:
the EXP-009 run ID and clean source revision; metric and threshold stratum;
current/candidate/LKG `ef`; `WorkloadIdentityBinding` configuration/data/FLAT/
HNSW identities; DATASET-002 manifest and frozen recall-audit-ID-set digests;
eligible-workload, candidate-selection, and execution-schedule digests; and
the exact recall and latency evidence schema versions. A full Stage-4 decision
accepts only a recall evaluation and a latency-evidence wrapper carrying the
same binding digest. Absent, malformed, unequal, or unverified bindings yield
`INCOMPLETE`, never `PASSING` or a generic SLO failure.

The recall ledger is raw durable evidence, not an authorization token. It must
persist its binding digest at run creation; make each accepted observation
append-only through a verified hash chain and schema guards; reject conflicting
replays; and expose a final chain digest. Its evidence publication must include
an external immutable manifest hash, because local SQLite permissions and
triggers are tamper-evident safeguards, not a signature against a hostile
writer. The latency side must be rebuilt from the verified schedule/ledger,
then wrapped with the same binding digest and the verified latency-ledger
digest; a free-form `latency-evaluation.json` is prohibited.

The report CLI must verify the canonical binding, both evidence artifacts and
their expected SHA-256 values before it can emit a full qualification document.
It may still emit a clearly labeled recall-only report. Missing/corrupt latency
evidence is `INCOMPLETE`; complete evidence that breaches a pre-registered
ceiling is `FAILING`. This convention neither changes the two-window
`QualificationResult` contract required for LKG nor claims that a 1,200-query
candidate recall audit establishes current LKG qualification.

Required adversarial tests: mismatched metric/stratum/`ef`/identity/workload/
run binding; hand-authored or hash-mismatched latency input; tampered recall
row, chain, schema, or external manifest; missing latency evidence; and a
complete, correctly bound evidence pair that fails its SLO. All must prove no
candidate routing, approval, policy actuation, or Milvus mutation.

**Implementation evidence — 2026-08-05.** Implemented and committed at
`088d325cfce099754f0efa63e0f46f1dc4e2f68d` ("fix: bind Stage-4 recall and
latency evidence to verified ledgers"), pushed to `origin/main`. Touches
`canary_recall_audit_ledger.py` (schema v2; binding-bound genesis; append-only
hash chain with a dedicated `insertion_seq` rowid alias so chain order depends
on arrival order, not the caller-supplied `query_id`; external immutable
manifest publication), `canary_recall_audit_evaluation.py` (a binding-mismatch
gate checked before any observation is examined), `canary_stage4_latency_evidence.py`
(new — wraps the verified execution ledger and schedule with the same binding
digest), `canary_stage4_decision.py` (rewritten — a full decision requires an
equal `evidence_binding_sha256` on both the recall and latency sides, else it
is forced `INCOMPLETE` even when both sides individually report `PASSING`),
and `canary_stage4_qualification_report.py` (rewritten — the free-form
`--latency-evaluation-json` CLI path is removed entirely; latency evidence can
only be derived from a verified ledger, schedule, and binding, each checked
against an independently supplied SHA-256 before being trusted).

All required adversarial test categories above are covered, including a direct
proof (`test_hand_fabricated_latency_evidence_cannot_combine_with_real_recall_evidence`
in `test_canary_recall_audit_pipeline.py`) that individually valid recall and
latency evidence bound to different run contexts cannot combine into a
`PASSING` or `FAILING` decision. Focused suites: recall-audit ledger 38/38,
recall evaluator 26/26, decision combiner 14/14, latency evidence 7/7,
end-to-end pipeline 3/3, qualification-report CLI 21/21 — 109/109 total, 0
failures. Full repository suite: 662/662 passing, 0 failures, 0 errors,
872.428 seconds. This closes the evidence-binding repair itself; it does not
change ADR-008's own **Proposed** status above, and it still authorizes no
candidate routing, approval use, rollback, or automatic tuning.

#### Phase-3 D3 amendment — Checkpoint-C authority at Stage-4 admission (proposed)

Status: Accepted — implementation and adversarial verification pending
Date: 2026-08-08
Risk level: CRITICAL
Scope: This amendment narrows the Stage-4 admission trust boundary. It does
not authorize candidate routing, change a signed-grant schema, or recompute
qualification evidence.

**Context.** The existing Stage-4 admission request accepts the legacy
`QualificationResult` value. Phase-3 D1 and D2 now provide a stronger trust
chain: D1 can expose an immutable `LkgPhase3Authority` only after fresh replay
verification of one terminal, `PASSING` Checkpoint-C evaluation against its
Phase-1 and Phase-2 ledgers, while D2 persists append-only identity/lineage
references without creating a qualification verdict. Stage-4 admission must
consume that trust chain directly so Checkpoint C is the sole new LKG
qualification authority. It must not infer authority from a detached D2 row,
legacy windows, or repeated statistics.

**Decision.** A candidate-capable Stage-4 admission evaluation requires both:

1. a freshly resolved D1 `LkgPhase3Authority` whose exact terminal
   Checkpoint-C artifact has passed the D1 upstream replay and lineage gates;
   and
2. an opaque, privately constructed D2 **verified-latest-chain-head** value
   issued by `LkgPhase3AuthorityReferenceStore` after the store has verified
   its complete schema, canonical row/document agreement, record digest,
   append-only chain, timestamp order, and current chain head.

A plain or detached historical `PersistedLkgPhase3AuthorityReference` is an
identity-only record and never authorizes admission, even when its fields look
valid. D2 evidence alone never authorizes. D1 authority without matching,
store-verified latest D2 persistence also never authorizes. The D2-issued
latest-head value remains non-authorizing by itself and must not expose a
public construction path.

The fresh D1 authority and verified D2 latest head must match exactly on every
identity/lineage field D2 persists: canonical Checkpoint-C evaluation digest;
source run ID and run-binding digest; source-run seal and sealed Phase-1 chain
head; Phase-2 source-binding digest; evaluated `ef`; search-configuration
digest; metric and threshold stratum; collection, HNSW index, and base-data
identities; qualification dataset ID/version/manifest, query role, ordered-ID,
ID-array, query-array, and expected-count commitments; environment identity;
and LKG source revision. A missing, malformed, unequal, non-latest, or
unable-to-verify field fails closed.

The fresh-authority provider returns exactly one immutable pair containing the
D1 authority and its verified-latest D2 head for each refresh operation. An
authority from one refresh and a D2 value from another refresh must never be
mixed, even if selected fields happen to compare equal. Pair construction and
validation are one fail-closed operation; an incomplete pair is unusable.

**Explicit configuration bridge.** `Stage4EvidenceBinding` is the sole D3
bridge between the LKG search configuration and the Stage-4 route-plan
configuration identity. Admission must:

1. require the binding's metric, threshold stratum, current/LKG/candidate
   `ef`, workload and selection digests, and complete `WorkloadIdentityBinding`
   to match the rebuilt `CanaryRoutePlan` exactly;
2. derive the expected candidate `SearchConfiguration` from the fresh D1
   authority's complete LKG `SearchConfiguration` by changing **only** `ef` to
   `plan.candidate_ef`; and
3. require exact object equality between that derived value and
   `Stage4EvidenceBinding.candidate_search_configuration`, then bind the
   evidence-binding digest into the admission receipt.

**Execution-schedule binding.** `Stage4AdmissionRequest` consumes the actual
immutable `Stage4ExecutionSchedule`, not only a caller-supplied schedule
digest. Admission validates the schedule through its public contract and must
require both:

- `schedule.plan_sha256 == rebuilt_plan.plan_sha256`; and
- `Stage4EvidenceBinding.execution_schedule_sha256 ==
  schedule.schedule_sha256`.

The successful admission receipt binds `execution_schedule_sha256`. Both the
candidate-capable live runner and the non-actuating offline serial runner must
require exact equality between the receipt's execution-schedule digest and the
actual immutable schedule they consume. A schedule substitution, a receipt
for another schedule, or an unable-to-validate schedule fails before dispatch.

Admission must never equate `plan.configuration_identity` with
`LkgPhase3Authority.search_configuration_digest`: these are distinct,
domain-specific identities. The former remains the opaque Stage-4 workload
configuration identity carried by `Stage4EvidenceBinding`; the latter is the
domain-separated digest of the complete typed LKG `SearchConfiguration`.
Likewise, the LKG run/source revision and Stage-4 run/source revision are
separate identities. They are each validated and bound, but are not required
to be equal unless a separate existing contract explicitly requires that
equality.

The fresh authority must additionally match `CanaryRoutePlan` on
`last_known_good_ef`, metric, threshold stratum, data identity, and HNSW
identity. The plan's FLAT identity remains governed by the Stage-4 evidence,
policy-provenance, and runtime bindings; it has no fabricated D1 equivalent.
Admission may compare canonical values and digests only. It must not inspect
or recompute Phase-1 attempts, Phase-2 readiness, windows, epochs, recall,
latency, confidence bounds, or any other A/B/C statistic.

**Passing admission receipt and refusal boundary.** A successful evaluation returns
an immutable, privately constructed admission receipt. Private construction
prevents supported/manual construction through the public API; it is not a
cryptographic authenticity mechanism. Trust comes from fresh D1 replay, D2
store verification, complete admission validation, canonical receipt hashing,
and the caller's preservation of those boundaries. Field replacement or
reconstruction from an untrusted mapping must not be a supported path to an
authorizing receipt. A refusal/result remains a separate non-authorizing value
and must carry stable, non-sensitive reason codes; a boolean or plan digest in
a refusal is never an authority token.

The successful receipt uses a strict canonical schema and a domain-separated
digest over all fields. At minimum it binds: the Checkpoint-C canonical
evaluation digest; D2 canonical record digest, sequence number, and persisted
timestamp; source run ID, run-binding digest, source-run seal, sealed Phase-1
chain head, and Phase-2 source-binding digest; evaluated LKG `ef` and LKG
search-configuration digest; `Stage4EvidenceBinding` digest; route-plan digest;
execution-schedule digest; policy audit ID; repository commit; Stage-4
configuration, data, and HNSW identities; and runtime observation timestamp.
It may include additional canonical non-sensitive fields only through an
explicit receipt-schema revision. Receipt construction occurs only after every
admission gate passes.

**Freshness and two-admission invariant.** Candidate-capable live composition
must reacquire and freshly verify the latest D1/D2 pair before **both** Stage-4
admission evaluations: once before activation and again immediately after
activation alongside fresh runtime evidence. Reusing an admission-request
snapshot and replacing only its runtime evidence is insufficient. The
composition boundary therefore consumes an injected fresh-authority provider,
not a long-lived authority/reference pair embedded in the original request.
No A/B/C or D2 replay may occur while a D2 database lock is held.

The two successful receipts are not required to be byte-identical: their fresh
runtime-preflight evidence and runtime observation timestamps may legitimately
differ, so their canonical receipt digests may also differ. Instead, the live
composition root performs a stable-lineage comparison. Both receipts must
match exactly on Checkpoint-C digest; D2 record digest and sequence number;
`Stage4EvidenceBinding` digest; execution-schedule digest; route-plan digest;
policy audit ID; configuration, data, and HNSW identities; LKG `ef`; and LKG
search-configuration digest. Runtime-specific timestamps and evidence are
excluded from this stable-lineage equality check but remain validated and
bound in their respective receipts.

Any refresh failure, a newly different D2 head, different D1 authority,
stable-lineage mismatch, or inability to prove equality fails closed. A
post-activation failure first contains the candidate route and executes the
existing rollback path before return; no query is dispatched from an unmatched
lineage.

**Impact set.** The D3 implementation and regression scope is limited to:

- `canary_admission.py`: replace legacy `QualificationResult` admission with
  fresh D1 authority, verified-latest D2 evidence, explicit configuration
  bridge and immutable execution-schedule validation, private success-receipt
  construction, and canonical receipt hashing;
- `lkg_phase3_persistence.py`: expose a store-issued, private-construction
  verified-latest-head boundary while preserving the rule that loading or
  persistence alone never creates usable authority;
- `canary_live_runner.py`: reacquire the D1/D2 pair before both admissions and
  require stable-lineage equality, while permitting separately validated
  runtime evidence and timestamps, before dispatch;
- `canary_serial_runner.py`: consume only the non-actuating receipt identity
  needed by its existing boundary and require receipt/schedule equality; it
  must neither resolve authority nor become candidate-capable; and
- the corresponding admission, persistence, live-runner, and serial-runner
  tests, including deliberate historical-reference, non-latest-head,
  D1/D2-field substitution, configuration-bridge, receipt-forgery, refresh,
  schedule-substitution, mixed-refresh-pair, stable-lineage-mismatch, and
  differing-valid-runtime-timestamp regressions.

`policy.py`, Checkpoint A/B/C evaluators, Milvus adapters, routing algorithms,
and statistical finalization are outside this amendment. Legacy
`QualificationResult` compatibility may remain elsewhere during migration,
but it is not accepted by the D3 Stage-4 admission boundary.

**Deferred signed-activation propagation.** No signed-grant schema changes are
authorized by D3. Before the admission receipt becomes part of final live
authorization, its Checkpoint-C/D2 lineage and canonical receipt digest must be
propagated into the signed activation chain, active context, lifecycle audit,
and rollback correlation under a separately reviewed amendment. Until then,
D3 is a non-actuating admission/evidence migration and must not be cited as
complete end-to-end live authorization.

Research references:

- ADR-002 for conservative bounds, action ladder, SLOs, and rollback obligations.
- ADR-004 for immutable provenance binding.
- ADR-005 through ADR-007 and EXP-008 for the verified reference observation/evidence path.
- [NIST: tolerance intervals based on largest/smallest observations](https://itl.nist.gov/div898/handbook/prc/section2/prc264.htm) for nonparametric order-statistic tolerance concepts.
- Finite-population randomization identity for an upper 5% tail of 30 outcomes: `P(sample maximum exceeds the tail threshold) = 1 - C(600 - 30, 60) / C(600, 60)`.
- [Hoeffding (1963)](https://doi.org/10.1080/01621459.1963.10500830) for bounded-sum concentration and sampling-without-replacement context.

#### Phase-3 downstream authority closure — policy and generic actuation

Status: Accepted — implementation pending
Date: 2026-08-09
Risk level: CRITICAL
Scope: Close the remaining legacy LKG authority paths in policy and generic
actuation without changing the separately governed signed activation chain.

**Context.** Phase-3 D1 makes a freshly replay-verified, terminal `PASSING`
Checkpoint-C evaluation the sole new LKG qualification authority. D2 persists
that authority's identity and lineage without creating a verdict, and the D3
Stage-4 admission boundary now requires a fresh D1 authority paired with the
verified latest D2 chain head. Three older paths remain outside that closure:

1. `evaluate_tuning_policy(...)` can still derive a candidate-capable decision
   from legacy `QualificationWindow` or `QualificationResult` values; and
2. `SafeActuationBoundary` can still execute `START_CANARY` from a legacy
   `QualificationResult`, independently of Stage-4 admission and its fresh
   Checkpoint-C/D2 lineage; and
3. the legacy `MilvusActuationClient.start_canary(...)` API still contains a
   candidate-capable deterministic 50-of-500 routing path that can be called
   outside `Stage4LiveRunner`'s one-search serving boundary.

Read-only shadow acquisition also constructs deliberately unqualified legacy
results solely because identity, shadow, and rollback inputs share the old
`ActuationContext`. This is accidental coupling, not qualification evidence.
Historical LKG and actuation-audit records must remain reviewable, but must not
be upgraded or reinterpreted into Phase-3 authority.

**Options considered.**

| Option | Correctness and dependency direction | Compatibility and operational cost | Decision |
|---|---|---|---|
| A — duplicate D1/D2 pair validation in policy and admission | Avoids moving an existing type, but creates two security-critical validators that can drift and disagree | Low initial cost; high review and maintenance risk | Rejected |
| B — make policy import `Stage4LkgAuthorityPair` from `canary_admission.py` | Reuses validation, but reverses the dependency direction because admission already imports policy | Low code volume; creates a policy↔admission cycle and couples general policy to Stage 4 | Rejected |
| C — generalize the pair into a neutral lower-level Phase-3 authority-binding module consumed by policy and admission | One fail-closed validator, acyclic dependencies, and no statistical recomputation | Moderate migration cost with explicit compatibility seams | Chosen |

**Neutral Phase-3 authority pair.** A neutral lower-level module owns one
immutable, private-construction Phase-3 authority-pair value and its sole pure
binder. The value contains exactly one concrete D1 `LkgPhase3Authority` and one
concrete D2 `VerifiedLatestLkgPhase3AuthorityReference`. The neutral module may
import D1/D2 identity contracts, but must not import policy, admission,
actuation, Milvus, or any A/B/C evaluator or ledger.

Pair construction compares every D2-persisted identity field against D1:
Checkpoint-C evaluation digest; source run ID, run-binding digest, source-run
seal, sealed Phase-1 chain head, and Phase-2 source-binding digest; evaluated
`ef` and search-configuration digest; metric and threshold stratum; collection,
HNSW-index, and base-data identities; qualification dataset ID/version/manifest,
query role, ordered-ID digest, ID-array digest, query-array digest, and expected
query count; environment identity; and source revision. D2 record metadata
(schema version, sequence number, persistence timestamp, previous-record
digest, and canonical record digest) remains store-issued metadata and is not
compared to D1.

A plain `PersistedLkgPhase3AuthorityReference`, a verified-latest D2 wrapper by
itself, a D1 authority by itself, or an object-forged/nonconcrete value never
forms a usable pair. Private construction is API discipline, not cryptographic
authenticity. The pure binder proves exact D1/D2 identity equality only. It
cannot and must not claim to distinguish two refreshes that observed identical
D1 authority and verified D2 head values; those observations are equivalent at
this boundary.

The injected provider/composition root owns refresh atomicity: one refresh
acquires both components, invokes the binder, and exposes only the completed
pair. Consumers accept neither separate D1/D2 arguments nor a field-level
reassembly API, and never cache or split the pair. A changed latest head,
mismatched identity, partial refresh, or refresh failure fails closed. The pair
is a verified snapshot at one refresh instant and does not claim to remain the
latest head forever. No pair operation inspects or recomputes Phase-1 attempts,
Phase-2 readiness, windows, epochs, recall, latency, confidence bounds, or any
other A/B/C statistic.

**Policy authority contract and source precedence.** Policy remains pure: it
performs no D1 replay, D2 I/O, freshness query, or upstream-ledger access. The
following order is normative:

1. **Active-canary safety first.** When an active `CanaryObservation` is
   present, hard-failure, recall, latency, and completion evaluation runs before
   any qualification-source validation. A required `ROLLBACK` depends on
   neither the neutral pair nor legacy qualification and remains available in
   both policy modes.
2. **Candidate-enabled evaluation.** `CANARY_ENABLED` with no active canary
   requires exactly one fresh neutral Phase-3 pair supplied by one provider
   refresh immediately for that evaluation. Legacy `QualificationWindow` or
   `QualificationResult` values cannot substitute. A missing pair returns
   `NO_CHANGE` with `PHASE3_LKG_AUTHORITY_REQUIRED`; a malformed, nonconcrete,
   mismatched, changed-head, or provider-rejected pair returns `NO_CHANGE` with
   `PHASE3_LKG_AUTHORITY_INVALID`. Supplying legacy and Phase-3 sources together
   returns `NO_CHANGE` with `LKG_AUTHORITY_SOURCES_CONFLICT`.
3. **DRY_RUN compatibility.** Exactly one LKG source is permitted: either one
   neutral pair or one explicitly selected legacy compatibility source.
   Supplying neither returns `NO_CHANGE` with `LKG_AUTHORITY_SOURCE_REQUIRED`;
   mixed sources return `NO_CHANGE` with `LKG_AUTHORITY_SOURCES_CONFLICT`.
   Legacy input may support `NO_CHANGE` or `RECOMMEND_EF`, but never
   `START_CANARY` or any other candidate authorization.

The neutral Phase-3 authority maps into policy only as LKG `ef`, metric,
threshold stratum, HNSW/index identity, and data identity. It has no Stage-4
`configuration_identity` and no FLAT identity. Policy must never equate
`search_configuration_digest` with `configuration_identity`, and must never
fabricate a FLAT identity from D1. Existing independently validated detector
provenance and `PreActionSafety` bindings continue to govern configuration and
FLAT identities and must match each other under their existing contract.

Policy must not reconstruct legacy qualification windows from Checkpoint-C
epochs. `PolicyDecision` remains schema-compatible in this checkpoint;
Phase-3 authority is independently revalidated and bound by the Stage-4
admission receipt. Any future requirement to persist authority lineage inside
`PolicyDecision` itself belongs to the deferred signed-chain amendment.

**Generic and adapter actuation bypass closure.** `SafeActuationBoundary`
permanently refuses `START_CANARY`. After this migration, that action must
produce a stable, audited, non-executed refusal and must make zero client calls,
regardless of legacy qualification, policy gates, traffic fraction, or any
supplied Phase-3 value. The boundary must expose no configuration switch,
compatibility mode, or alternate method that restores generic candidate
activation.

The generic `ActuationClientLike` protocol no longer contains
`start_canary(...)`. The public production
`MilvusActuationClient.start_canary(...)` candidate path is removed. If a
temporary compatibility symbol is required solely to make migration failures
explicit, it must raise a stable retirement error before route selection,
keyed-hash ranking, search, adapter-state change, or any client call. Its legacy
500-query/50-candidate routing implementation and measurements are historical
evidence only and are not a callable candidate-serving path. Read-only
`shadow_candidate(...)` remains permitted.

`Stage4LiveRunner`, guarded by fresh dual admission, immutable schedule,
human-signed activation, route authority, rollback composition, and its injected
one-search serving port, is the sole candidate-serving composition root.
Generic `NO_CHANGE` and `RECOMMEND_EF` remain audited zero-call no-ops. Generic
`ROLLBACK` remains available and fail-closed even when fresh qualification
authority is unavailable; rollback validation uses the explicit expected LKG
`ef`, runtime identity, active-route evidence available to that boundary, and
restoration audit rather than a legacy qualification verdict.

**Qualification-free context separation.** Identity and execution context is
separated from qualification authority. A common immutable identity projection
contains canonical metric, threshold stratum, collection name, opaque
configuration identity, HNSW/index identity, FLAT identity, data identity, and
observation timestamp. Read-only shadow context composes that projection with
the exactly 50 audited query IDs required by `shadow_candidate(...)`. Rollback
context composes it with the expected LKG `ef` and exactly 50 restoration-audit
query IDs required to verify containment. Neither context contains
`QualificationResult`, qualification windows, a neutral Phase-3 pair, D1/D2
ledger handles, or a fabricated qualification value.

The Milvus adapter remains dependency-injected and query-time `ef` remains
stateless. Read-only EXP-005/EXP-008 and host-shadow paths migrate mechanically
to the qualification-free shadow context without changing trace payloads,
collection identities, query ordering, results, or historical experiment
claims. Existing artifacts and recorded evidence are never rewritten or
relabeled as Phase-3 evidence.

**Persistence and schema compatibility.** The file-based last-known-good schema
v1 is frozen as legacy historical evidence. It remains readable under its
existing strict decoder but is non-authorizing and must never be converted into
a D1 authority, neutral pair, D2 reference, Stage-4 receipt, or candidate token.
No migration rewrites existing v1 files.

Actuation-audit schema v2 remains readable, immutable historical evidence. A v2
record's embedded legacy `QualificationResult` describes the old invocation
only and never establishes Phase-3 authority. New appends use only an
actuation-audit v3 envelope with the exact top-level fields
`{"schema_version", "record"}` and integer `schema_version = 3`. The v3
`record` retains the existing exact audit fields—`audit_id`, `action`, `outcome`,
`attempted`, `success`, `reason`, `context`, `current_ef`, `candidate_ef`,
`last_known_good_ef`, `traffic_fraction`, `policy_reason`,
`safety_gate_results`, `shadow_result`, `canary_observation`,
`rollback_verification`, `automatic_actions_disabled`, and
`evidence_provenance`—so record-level meaning is not silently renamed. Because
generic candidate start is retired, v3 `shadow_result` and
`canary_observation` must always be `null`.

The v3 `context` is a strict tagged union. Both kinds contain exactly the common
fields `context_schema_version`, `context_kind`, `metric`,
`threshold_stratum`, `collection_name`, `configuration_identity`,
`index_identity`, `flat_index_identity`, `data_identity`, and
`occurred_at_utc`, with `context_schema_version = "actuation-context-v3"`.

- `context_kind = "POLICY"` is required for `NO_CHANGE`, `RECOMMEND_EF`, and
  refused `START_CANARY`; it has no additional fields.
- `context_kind = "ROLLBACK"` is required for `ROLLBACK`; it adds exactly
  `expected_last_known_good_ef` and `audited_query_ids`. The `ef` must be an
  eligible non-sentinel actuation value and equal the decision's
  `last_known_good_ef`; `audited_query_ids` must contain exactly 50 canonical,
  distinct restoration-audit IDs.

Read-only shadow context is not serialized into this generic actuation audit;
its existing trace/evidence codecs remain the durable shadow record. The v3
decoder validates the exact field set for the selected context kind and all
canonical value contracts. Reader dispatch is exclusively by the explicit
envelope version and never by field-shape inference. Mixed-version duplicate
audit IDs, malformed records, unsupported versions, context-kind/action
mismatches, extra/missing fields, and downgrade/substitution attempts fail
closed. V2 bytes are never normalized or rewritten, and no v2 field is used to
construct a Phase-3 authority, pair, receipt, or candidate token.

**Dependency direction.** The required direction is:

```text
Checkpoint A/B/C ledgers -> D1 authority -> D2 identity reference
                                      \-> neutral Phase-3 pair
neutral Phase-3 pair -> policy
neutral Phase-3 pair -> Stage-4 admission -> receipt
policy decision + Stage-4 receipt -> existing live composition
identity/rollback contexts -> generic rollback and read-only shadow adapters
```

Policy must not import admission. Admission and policy must not import D1/D2
ledgers. The neutral pair module must not perform I/O. Generic actuation must not
accept the neutral pair as a replacement route to `START_CANARY`.
`MilvusActuationClient` may depend on qualification-free shadow/rollback
contexts but exposes no generic candidate-routing method. Candidate serving
flows only from `Stage4LiveRunner` to its narrow one-search serving port.

**Fail-closed invariants.** Implementation and adversarial tests must prove:

- only the neutral binder can produce the supported pair value, and it compares
  every persisted D1/D2 identity without recomputation while making no
  unobservable same-refresh claim;
- providers expose one atomically acquired completed pair, consumers accept no
  separate components, and changed/mismatched heads or identities fail closed;
- D2 alone, a historical reference, legacy qualification, source conflicts, and
  object-forged values cannot authorize candidate policy output;
- active-canary rollback evaluation precedes and is independent of LKG source
  validation;
- `CANARY_ENABLED` without exactly one fresh valid pair emits `NO_CHANGE` with
  the pinned stable reason, and DRY_RUN accepts exactly one source;
- policy maps D1 only to LKG `ef`, metric, stratum, HNSW/index identity, and data
  identity; it neither equates search/configuration identities nor fabricates a
  FLAT identity;
- DRY_RUN legacy compatibility cannot escape into `START_CANARY`;
- `SafeActuationBoundary` makes zero client calls for every `START_CANARY`
  input, including otherwise-valid legacy inputs;
- `ActuationClientLike` has no `start_canary(...)`, and any retained
  `MilvusActuationClient` compatibility stub fails before routing, search, state
  change, or client access;
- legacy 50-of-500 routing is unreachable from production candidate-serving
  APIs, while `shadow_candidate(...)` remains read-only;
- generic rollback no longer requires legacy qualification and is never refused
  solely because qualification authority is missing;
- read-only shadow capture requires no qualification object and remains
  byte-compatible at the trace/evidence boundary;
- v3 audit records use only the pinned `POLICY`/`ROLLBACK` tagged context
  projections; v1 LKG and v2 audit records remain historical-only; and
- malformed, mixed-version, duplicate-ID, wrong-kind/action, downgrade, schema
  substitution, and replay attempts fail closed; and
- no new direct Milvus, network, A/B/C statistical, or ledger dependency enters
  policy, admission, neutral pairing, generic actuation, or audit decoding.

**Ordered implementation checkpoints.** Each checkpoint receives focused
adversarial tests and review before the next begins:

**A. Neutral authority pair.** Add the neutral pair/binder, migrate Stage-4
admission and its fresh-authority provider to consume it, preserve receipt
semantics, and remove the admission-owned duplicate validator.

**B. Policy migration.** Add the fresh neutral-pair input for
`CANARY_ENABLED`, confine legacy qualification to DRY_RUN, preserve
`PolicyDecision` schema, enforce the exact source precedence and stable reason
codes above, and prove active-canary rollback remains source-independent.

**C. Generic `START_CANARY` retirement and rollback decoupling.** Remove
`start_canary(...)` from `ActuationClientLike`, remove or fail-closed-stub the
legacy Milvus method before any side effect, make every generic start request an
audited zero-client-call refusal, and remove legacy qualification as a
prerequisite for generic `ROLLBACK`. Preserve no-op and rollback behavior.

**D. Qualification-free context split.** Introduce the shadow and rollback
identity contexts, structurally remove the obsolete qualification field, and
migrate the Milvus adapter, host shadow executor, generic rollback, and
read-only acquisition/composition callers without changing trace artifacts.

**E. Actuation audit v3 and compatibility hardening.** Write only v3 records,
using the pinned envelope and `POLICY`/`ROLLBACK` context projections above;
read strict v2/v3 evidence, freeze LKG v1, and add restart, corruption,
duplicate, wrong-kind/action, downgrade, schema-confusion, and
historical-non-authorization tests. Checkpoint E is not authorized to begin
unless its implementation contract reproduces those exact v3 field sets and
cross-field invariants.

**Impact set.** Expected implementation files are limited to:

- new neutral authority-binding module, expected as
  `src/vdbench/lkg_phase3_binding.py`;
- `lkg_phase3_authority.py` and `lkg_phase3_persistence.py` only if a narrow
  public identity projection is required by the neutral binder—no ledger or
  schema change is authorized;
- `canary_admission.py` and `canary_live_runner.py` for the neutral pair type and
  provider migration, with receipt and stable-lineage semantics unchanged;
- `policy.py` and `workload_monitor.py` for candidate-capable authority input and
  explicit DRY_RUN compatibility;
- `actuation.py` and `actuation_persistence.py` for permanent generic-start
  refusal, removal of `ActuationClientLike.start_canary`, source-independent
  rollback, qualification-free contexts, and the pinned audit v3 schema;
- `milvus_actuation.py`, `milvus_host_executor.py`, `exp005_acquisition.py`, and
  `exp008_acquisition.py` only for removal/retirement of the legacy candidate
  method and qualification-free read-only context wiring;
- `experiments/exp005_evaluate.py`, `experiments/exp006_validate.py`, and
  `experiments/exp007_validate.py` only where explicit DRY_RUN compatibility or
  qualification-free no-op context construction must follow the new API; their
  stored artifacts, seeds, outputs, and historical conclusions are unchanged;
- the corresponding focused tests:
  `test_lkg_phase3_binding.py`, `test_canary_admission.py`,
  `test_canary_live_runner.py`, `test_policy.py`, `test_workload_monitor.py`,
  `test_actuation.py`, `test_actuation_persistence.py`,
  `test_milvus_actuation.py`, `test_milvus_host_executor.py`,
  `test_exp005_acquisition.py`, `test_exp008_acquisition.py`,
  `test_last_known_good.py`, `test_drift_policy_integration.py`,
  `test_exp005_provenance_pipeline.py`, `test_shadow_event_source.py`,
  `test_exp008_offline_composition.py`, and existing Phase-3
  authority/persistence regression suites. These tests must include exact
  policy-source precedence and reason codes, active-canary rollback precedence,
  protocol/API removal, compatibility-stub zero-side-effect behavior,
  unreachable legacy 50-of-500 routing, qualification-free context shape, and
  exact v3 envelope/context-field regressions; and
- legacy persistence and historical experiment tests only to prove continued
  readability and non-authorization; original evidence artifacts remain
  untouched.

Checkpoint A must not alter D1/D2 storage or replay semantics. Checkpoint B must
not alter drift detection or statistical finalization. Checkpoints C–E must not
create a second candidate-capable composition root.

**Explicit deferrals.** This amendment does not authorize or design:

- signed approval-grant schema v2;
- admission-receipt lineage in activation or `ActiveCanaryContext`;
- route-state schema v2;
- lifecycle-audit or rollback signed-receipt correlation; or
- execution-ledger schema v2 receipt binding.

Those changes form one separate CRITICAL Phase-3 signed-activation-lineage
amendment and must be reviewed atomically before implementation. No field from
this amendment may be silently added to the signed chain in advance.

**Consequences.** Candidate activation has one composition root and one new LKG
authority source. Policy and admission share one validator without dependency
cycles; read-only shadow code no longer fabricates qualification; rollback
remains available during authority outages; and historical evidence stays
reviewable without gaining authority. The costs are an explicit compatibility
surface for old DRY_RUN callers, a versioned actuation-audit reader, and a
multi-checkpoint migration whose signed-chain completion remains separately
pending.

Related decisions: ADR-002 (policy and rollback safety), ADR-004 (provenance),
ADR-005 through ADR-007 (monitor and shadow path), ADR-008 (candidate-routing
research contract), and the accepted Phase-3 D3 Stage-4 admission amendment.

---

### ADR-009: Define a calibrated empirical response profile for predictive `ef` evidence

Status: Accepted
Date: 2026-08-09
Risk level: CRITICAL
Evidence status: CONTRACT DRAFT ONLY — NOT IMPLEMENTED — NOT RUN
Acceptance note (2026-08-09): The reviewed R0 contract is architecture-approved,
including its detached profile digest, result-independent population freeze,
deterministic binary64 recall arithmetic, and exact one-query blocking schedule.
Implementation note (corrected 2026-08-11): ADR-009 §Policy consumption rule 4
is enforced as a temporary hard interlock in `evaluate_tuning_policy`.
`CANARY_ENABLED` cannot yield `START_CANARY` from any current value, including a
valid bare R1 `CalibratedResponseProfile` or an object-forged instance; it returns
`RECOMMEND_EF` with reason `RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE`. Active-canary
rollback precedence and DRY_RUN compatibility are preserved. R2-C through R2-F,
independent root-pinned issuance, freshness governance, policy-chain profile-
digest binding, and EXP-010 remain NOT IMPLEMENTED — NOT RUN.

Problem:

ADR-002 defines `ResponseEstimate` as policy input for prospective HNSW `ef`
choices, including fields named `recall_lower_bound_95` and
`latency_upper_bound_95_ms`. The committed implementation validates numeric
shape and ordering, but no production component computes those values and no
accepted contract binds them to a sampling population, estimator, workload,
index, environment, source revision, or freshness interval. A caller-supplied
`validated_model=True` boolean and non-empty free-form provenance string cannot
establish statistical validity.

This gap is separate from Phase-3 last-known-good qualification. Phase-3 is
observed qualification evidence over its own governed workload. It must never
be reused, reinterpreted, or relabelled as predictive response evidence.
Likewise, Stage-4 recall/latency evidence, EXP-001 measurements, detector input,
admission receipts, grants, and execution ledgers have their own populations
and authorities; none may silently become a response profile.

Decision drivers:

- Make every nominal 95% bound falsifiable and reproducible.
- Keep predictive evidence atomic so entries from different runs cannot be
  mixed into one policy evaluation.
- Make workload, search, index, data, environment, and source compatibility
  mechanically checkable.
- Preserve deterministic adjacent-step policy selection; a response estimator
  evaluates a candidate but does not invent a new action space.
- Fail closed when evidence is missing, stale, incompatible, unsupported, or
  statistically incomplete.
- Keep prediction informational: it may inform policy but never becomes LKG
  qualification, admission, grant, route, or execution authority.

Alternatives considered:

| Option | Statistical integrity | Operational cost | Main risk | Decision |
|---|---|---|---|---|
| A — Keep `Mapping[int, ResponseEstimate]` plus caller-supplied `validated_model` and provenance | No evidence lineage or enforceable confidence construction | Low | Fabricated or mixed estimates can look valid | Rejected |
| B — Populate a deterministic table from EXP-001 | Reproducible only for the historical EXP-001 workload/environment | Low | Stale, non-transportable predictive claims | Rejected |
| C — Fit a learned/parametric model immediately | Potentially efficient after substantial calibration | Medium/high | Misspecification, extrapolation, and uncalibrated tail risk | Deferred comparator only |
| D — Build one exact-cell empirical profile from a disjoint post-trigger replay | Directly measurable, auditable, and requires no interpolation | Higher read-only query cost | Conditional stationarity and environment transportability remain explicit assumptions | Chosen for v1 |

Decision:

#### Atomic profile and supported domain

1. The v1 policy input is one immutable, canonical
   `CalibratedResponseProfile`, not `Mapping[int, ResponseEstimate]`. It contains
   one shared evidence/identity envelope and exactly one response point for each
   `ef` in the ordered supported family `(200, 400, 800, 1600)`.
2. The supported family is exact. v1 performs no interpolation, extrapolation,
   nearest-value substitution, monotonic repair, cross-metric transfer, or
   cross-stratum transfer. A missing or additional `ef` invalidates the profile.
3. `validated_model` is removed as an authority signal. No free-form provenance
   string can make a profile usable. Validity follows only from strict schema,
   evidence, estimator, identity, digest, and freshness verification.
4. Changing the supported `ef` family, number of confidence claims, confidence
   allocation, estimands, formulas, sample population, or rank convention
   requires a new estimator-contract version. Existing profiles retain their
   historical meaning and are never silently recomputed under new semantics.

#### Calibration population and replay protocol

1. One profile uses exactly 1,200 distinct post-trigger measured query
   observations. They are collected only after the detector evidence that
   triggered calibration and are disjoint by canonical observation/query ID and
   query-payload digest from all detector evidence and all Phase-3 qualification
   evidence. Evidence already consumed by Stage-4 or historical EXP-001 is not
   eligible.
2. Calibration membership, role assignment, canonical order, and payload
   bindings are frozen before any response-profile replay result at a supported
   `ef` is inspected or used for selection. Existing foreground served results
   may already exist, but they may not influence inclusion, exclusion,
   ordering, replacement, or role assignment. The frozen population binds the
   ordered IDs, vectors, threshold radii, range filters, limits, and
   metric/stratum. Duplicate IDs, duplicate payloads represented as independent
   observations, omissions, replacements, retries-as-new-observations, or
   post-result selection invalidate the profile.
3. Warm-up observations are disjoint from the 1,200 measured observations and
   are never included in a point estimate or confidence bound. Their count,
   source, ordering, and exclusion are part of the control profile.
4. Each measured query is one replay block against the same immutable
   data/index state. First deterministically permute the 1,200-query canonical
   workload order. Then, independently for each query, deterministically
   permute `(200, 400, 800, 1600)` and execute all four values exactly once
   before proceeding to the next query. The complete realized ordered sequence
   of `(query_id, ef)` pairs and its seed-derivation inputs are bound by the
   replay-schedule/control-profile digest. The schedule is frozen before any
   response-profile replay result is inspected or used for selection.
   Concurrency, consistency level, timeout, timing boundaries, retry
   prohibition, warm-up, and schedule algorithm/seed lineage are bound by the
   same control profile.
5. Recall is capped range-query recall computed against the independent exact
   oracle. Latency is client-observed elapsed time under the bound execution and
   environment profile. Neither value may be generalized to another workload,
   concurrency level, client/runtime, host, Milvus state, or environment without
   new compatible evidence.
6. A failed/timed-out query, threshold violation, FLAT/oracle disagreement,
   identity change, service/load failure, non-finite observation, incomplete
   schedule, or result-count mismatch invalidates the complete profile; v1 does
   not impute or drop observations.

#### Statistical contract

Let `E = (200, 400, 800, 1600)`, `n = 1200`, family alpha
`alpha_family = 0.05`, and
`alpha_cell = alpha_family / (4 * |E|) = 0.05 / 16 = 0.003125`.
The v1 profile makes exactly sixteen one-sided claims:

```
4 ef values *
  {recall LCB, recall UCB, p95-latency LCB, p95-latency UCB}
```

Bonferroni allocation therefore gives simultaneous family-wise confidence of
at least 95% without assuming independence among `ef` values or among the
sixteen claims. The confidence target is conditional on the declared sampling,
stationarity, identity, and control-profile assumptions.

The recall construction requires the 1,200 query observations to be
independent bounded draws from the declared workload regime. The exact latency
order-statistic construction requires the 1,200 latency observations at each
`ef` to be IID/exchangeable draws from one unchanged latency distribution under
the bound control/environment profile. Block randomization mitigates temporal
confounding but does not prove either assumption. If independence,
exchangeability, stationarity, or no-interference is unsupported, the affected
confidence claim and therefore the complete profile are unavailable.

For each `ef=e`, let capped recalls in the frozen canonical workload order be
the finite IEEE-754 binary64 values `r[e,1], ..., r[e,n]` in `[0,1]`. The recall
point estimate and one-sided Hoeffding bounds are computed, never accepted as
caller-supplied constants:

```
mean_recall[e] = math.fsum((r[e,1], ..., r[e,1200])) / 1200
epsilon = sqrt(log(1 / alpha_cell) / (2 * n))
        = sqrt(log(320) / 2400)
        = 0.04902516783837398
recall_lcb[e] = max(0.0, mean_recall[e] - epsilon)
recall_ucb[e] = min(1.0, mean_recall[e] + epsilon)
```

Inputs are evaluated as finite float64 values; booleans are not numeric input.
Clipping occurs only at the mathematical recall support `[0,1]`. Exactly 1,200
valid observations are required for every `ef`; another count is
`INSUFFICIENT_EVIDENCE`, not a profile with a modified margin. The computed
binary64 point and formula-derived bounds are bound using the repository's
canonical serialization contract; verification recomputes them from canonical
ordered observations and rejects any stored-value disagreement.

For latency, let non-negative finite client latencies sorted in nondecreasing
order be `x[e,(1)] <= ... <= x[e,(n)]`, using one-based order-statistic ranks.
Equal values remain separate observations, are not jittered, and may make two
bounds equal. The point estimate uses the nearest-rank quantile convention:

```
p95_latency[e] = x[e,(ceil(0.95 * n))] = x[e,(1140)]
```

For the exact distribution-free order-statistic bounds, let
`B ~ Binomial(n=1200, p=0.95)`. Define:

```
k_lower = max{k in 1..n : P(B >= k) >= 1 - alpha_cell} = 1118
k_upper = min{k in 1..n : P(B <= k - 1) >= 1 - alpha_cell} = 1161
p95_latency_lcb[e] = x[e,(1118)]
p95_latency_ucb[e] = x[e,(1161)]
```

These ranks use the left quantile `q(p) = inf{x : F(x) >= p}`. The lower bound
uses the largest qualifying rank and the upper bound uses the smallest
qualifying rank; implementations must not round an approximate normal
quantile. Exact binomial tail/CDF inversion determines the rank. If a future
contract's sample size/confidence allocation yields no rank in `1..n`, the
bound is unavailable and evaluation fails closed. Latency values are not
clipped; negative, non-finite, missing, or non-numeric values invalidate the
profile.

The formulas above provide confidence statements for the declared workload
distribution and execution profile under their assumptions. They do not prove
that a workload remains stationary, that replay latency transports to arbitrary
production traffic, or that any universal freshness interval exists.

#### Identity and lineage contract

The strict canonical `profile_payload` contains exactly:

- `schema_version` and `estimator_contract_version`;
- `metric`, `threshold_stratum`, and exact ordered `supported_efs`;
- `search_configurations`, containing one complete validated HNSW
  `SearchConfiguration` per `ef`, including radius, derived range filter,
  limit, consistency level, and index track;
- `hnsw_index_identity` and `data_identity`;
- `workload_manifest_sha256` and `ordered_query_payload_sha256`;
- `replay_schedule_sha256`, binding the realized `(query_id, ef)` order and
  seed derivation, and `control_profile_sha256`;
- `environment_manifest_sha256`, including server/client/runtime and resource
  controls material to the latency claim;
- `raw_evidence_sha256`, identifying the verified raw-evidence manifest or
  ledger chain head;
- `source_revision`;
- `calibration_started_at_utc`, `calibration_completed_at_utc`, and
  `generated_at_utc`; and
- `estimates`, an `ef`-ordered array whose entries contain exactly `ef`,
  `observation_count`, `mean_recall`, `recall_lcb`, `recall_ucb`,
  `p95_latency_ms`, `p95_latency_lcb_ms`, and `p95_latency_ucb_ms`.

`profile_sha256` is not a member of `profile_payload`. It is stored alongside
that payload and is exactly:

```
SHA256(
  b"VD::CALIBRATED_RESPONSE_PROFILE::V1\x00"
  + canonical_json_bytes(profile_payload)
).hexdigest()
```

`canonical_json_bytes` is exactly `vdbench.artifacts.canonical_json_bytes`, the
repository's shared canonical JSON serialization contract. Verification
reconstructs the exact payload, recomputes the domain-separated digest, and
compares it to the stored `profile_sha256`.
Caller-supplied, fixed-point, self-referential, or digest-included payload
semantics are invalid.

A mutable collection name may be retained as operational metadata but never
substitutes for HNSW/index and data identity. `search_configuration_digest` is
not equated with any opaque Stage-4 `configuration_identity`. No FLAT identity,
Phase-3 authority, admission receipt, approval grant, or execution receipt is
fabricated from this profile.

#### Freshness and invalidation

Identity, search-configuration, workload, control-profile, environment, source-
revision, or estimator-version incompatibility invalidates a profile
immediately. A later detected regime change also invalidates a profile for the
new regime.

EXP-010 may measure prospective stability and inform a future governed
freshness policy, but it cannot statistically prove a universal time-to-live.
Until an explicit expiry/invalidation rule is separately reviewed and accepted,
no profile is candidate-capable. A timestamp or caller-selected `expires_at`
value cannot create freshness by itself.

#### Policy consumption

1. Actual active-canary safety and mandatory rollback evaluation has precedence
   and does not depend on a response profile.
2. For inactive drift evaluation, predictive point estimates support only the
   existing deterministic direction and utility calculations. A profile does
   not rank arbitrary `ef` values or broaden the adjacent-step action space.
3. Safety uses conservative bounds: candidate recall floor uses candidate LCB;
   paired recall degradation and the L2 exception use candidate LCB against LKG
   UCB; absolute latency uses candidate p95 UCB; relative latency uses candidate
   p95 UCB against LKG p95 LCB.
4. Missing, incomplete, stale, identity-mismatched, unsupported, malformed, or
   non-canonical profile evidence never yields `START_CANARY`. When deterministic
   direction is known, policy may emit a non-actuating recommendation with a
   stable refusal reason; otherwise it emits `NO_CHANGE`.
5. Before candidate-capable policy consumption is enabled, the canonical
   profile digest must be mechanically bound into the canonical policy evidence
   chain. Free-form safety-gate detail or audit prose is insufficient. The exact
   downstream schema mechanism requires separate review; no Phase-3,
   admission, grant, route, or execution schema is changed by this ADR draft.
6. A response profile is predictive evidence only. It cannot qualify LKG,
   satisfy Stage-4 admission, authorize or sign a grant, install a route, or
   prove execution/rollback success.

Consequences:

- The current `ResponseEstimate` mapping and its `validated_model`/free-form
  provenance convention are superseded for future production use once ADR-009
  is accepted and implemented. Existing deterministic fixtures remain test-only
  and cannot become candidate authority.
- v1 is intentionally conservative and may refuse every transition. That is a
  valid safety outcome, not permission to tune intervals after observing data.
- Collection and replay cost increases to at least 4,800 measured HNSW searches
  plus disjoint warm-up work per profile, but produces exact per-`ef` evidence
  without interpolation.
- Empirical-Bernstein recall bounds, paired transition-specific uncertainty,
  monotonicity diagnostics, digest caches/reviewer tools, and learned response
  models may be evaluated as non-authorizing comparators. None may silently
  replace the v1 estimator.

Implementation and verification gate:

ADR-009 must be accepted and EXP-010 must be pre-registered before code. Pure
statistics/schema verification precedes durable evidence storage, policy
migration, any read-only live producer, and any candidate-capable integration.
Candidate-capable policy remains disabled until EXP-010 evidence is reviewed,
an explicit freshness rule is accepted, and profile-digest propagation is
mechanically designed and verified.

Related decisions: ADR-002 (detector/policy), ADR-003 (statistical naming and
exchangeability), ADR-004 (provenance), ADR-005 through ADR-007 (monitor and
shadow observation), ADR-008 (human-gated candidate routing), and the accepted
Phase-3 authority amendments. ADR-009 changes none of their authority
boundaries.

#### R2 raw-evidence provenance clarification (2026-08-09)

This append-only clarification governs the raw-evidence boundary that must be
implemented before an R1 `CalibratedResponseProfile` can be treated as carrying
root-pinned calibration provenance. It does not change R1 statistics, profile
schema, supported `ef` values, or any candidate-authority boundary.

**Pre-result population commitment and canonical digests.** Before any
supported-`ef` response-profile replay result is inspected or used for
selection, the detector-trigger boundary, role assignments, canonical order,
query payloads, complete 1,200-query calibration population, replay schedule,
control profile, and environment profile must exist as immutable, checksum-
verified commitments. Canonical query IDs remain exactly R1-compatible: a
normalized ID is an exact integer or an NFC-normalized non-empty string,
booleans are forbidden, and:

```
canonical_query_id_bytes =
    canonical_serialize_tuple((normalized_query_id,))

query_id_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_QUERY_ID::V1\x00"
    + canonical_query_id_bytes
).hexdigest()
```

Consequently integer `1` and string `"1"` are a canonical collision and may
not coexist, while other mixed integer/string IDs remain valid. R2 must not
produce evidence that unchanged R1 verification would reject.

A query vector is a non-empty finite contiguous little-endian float32 vector.
Its digest is exactly:

```
vector_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_QUERY_VECTOR::V1\x00"
    + canonical_serialize_tuple(("dtype", "<f4", "dimensions", dimension_count))
    + contiguous_little_endian_float32_bytes
).hexdigest()
```

The strict `response-profile-query-payload-v1` payload contains exactly the
schema version, vector digest, metric, threshold stratum, radius, range filter,
limit, and consistency level, and its digest is:

```
query_payload_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_QUERY_PAYLOAD::V1\x00"
    + canonical_json_bytes(query_payload)
).hexdigest()
```

It excludes query ID, role, source namespace/position, canonical or replay
order, `ef`, timestamps, result values, and index/data identity. Invented IDs,
role changes, or ordering changes therefore cannot make a repeated vector or
payload independent. `query_id_sha256` is local to its source namespace, not a
global identity. Within one source namespace, role-membership comparisons use
that canonical query-ID identity; across distinct source namespaces they use
`observation_identity_sha256`. Required vector-digest and query-payload-digest
disjointness applies unchanged across every source namespace and role.

Local query IDs are not global identities. A versioned source-namespace digest
binds one strict discriminated payload. `ARTIFACT` source identity contains
exactly source kind, dataset ID, dataset version, and immutable generation-
manifest SHA-256. `LIVE_STREAM` source identity contains exactly source kind,
stable stream ID, data identity, and immutable source-workload-manifest SHA-256.
The common payload also contains only its schema version. Role, `ef`, search/
index configuration, timestamps, and source revision are excluded so the same
source cannot gain a new namespace through reassignment, retuning, recapture,
or code revision. Source revision remains separately bound by the evidence
root. The digests are exactly:

```
source_namespace_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_SOURCE_NAMESPACE::V1\x00"
    + canonical_json_bytes(source_namespace_payload)
).hexdigest()

observation_identity_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_OBSERVATION_IDENTITY::V1\x00"
    + canonical_json_bytes({
        "schema_version": "response-profile-observation-identity-v1",
        "source_namespace_sha256": source_namespace_sha256,
        "query_id_sha256": query_id_sha256,
    })
).hexdigest()
```

Role is deliberately excluded from observation identity so reassignment cannot
create a new observation.

**Closed role and disjointness catalog.** v1 recognizes only detector evidence,
response-profile warm-up, response-profile calibration, the twenty indexed
prospective-validation segments, Phase-3 qualification, Stage-4 routing,
Stage-4 recall audit, Stage-4 schedule control, historical EXP-001 calibration,
historical EXP-001 measured evidence, and the prohibited DATASET-001/002/003
query/vector inventories. DATASET-001 may remain the searched base corpus, but
none of its rows or query populations may be reused as a predictive query.
Every calibration role manifest must include detector evidence explicitly and
prove no overlap with all already materialized prohibited roles. Prospective
segments are later-bound: their memberships, roles, order, and payloads must be
frozen before their own supported-`ef` results are inspected, and completed
EXP-010 evidence must prove pairwise disjointness across all twenty segments
and every other catalog role. Omission of a required role manifest, duplicate
vector, duplicate payload, or role overlap is `INCOMPLETE`. These uniqueness
and disjointness rules apply to frozen role membership, not to repeated
execution evidence for an already frozen member.

**Deterministic schedule.** The v1 schedule uses master seed `20260810`. Query-
order seed material is exactly:

```
(20260810, cell_id, role_or_segment_id,
 workload_manifest_sha256, source_revision, "QUERY_ORDER")
```

Per-query `ef`-order seed material is exactly:

```
(20260810, cell_id, role_or_segment_id,
 workload_manifest_sha256, source_revision, "EF_ORDER", query_id_sha256)
```

Each tuple is encoded with `canonical_serialize_tuple`, prefixed by
`b"VD::RESPONSE_PROFILE_SCHEDULE_SEED::V1\x00"`, hashed with SHA-256, and
converted to `seed_u64` from the first eight digest bytes as an unsigned big-
endian integer. Query order uses one fresh
`numpy.random.Generator(numpy.random.PCG64(seed_u64)).permutation(1200)` call;
each query's `ef` order uses a separate fresh generator and one
`permutation(4)` call over `(200, 400, 800, 1600)`. The schedule contract binds
its schema/algorithm version, exact NumPy version, complete seed tuples, full
seed digests, derived uint64 values, and all 4,800 realized `(query_id, ef)`
positions. Any derivation, RNG/permutation implementation, version, or family
change requires a new schedule-contract version.

**Durable measurement lifecycle, block closure, and restart.** One query is one
four-`ef` block. Before any measured search call, its exact schedule position
must be durably and append-only recorded as `STARTED`; one matching
`COMPLETED` record may be appended only for that start. A `STARTED` position
without a valid `COMPLETED` record is terminal, can never be retried as a
measured observation, and invalidates the run even without an explicit
invalidation record. A usable profile requires exactly 4,800 matching,
successful measured completions over exactly 1,200 queries, with no missing,
extra, duplicate, substituted, or retried-as-new position.

Each block binds immutable pre/post runtime snapshot receipts and is closed only
after its four scheduled completions and post-block verification succeed.
Full health/index/data documents may be stored once in immutable runtime,
epoch, or block receipts and referenced from measurement records by digest;
equivalent verification strength is required, and a reference may not hide a
missing or changed identity. Latency is recomputed from finite, ordered client
monotonic start/end timestamps; recall is recomputed from candidate IDs and
independently reverified exact-oracle evidence. Stored aggregate recall,
latency, result-count, threshold-violation, health, or identity assertions are
never trusted without reconstruction.

Every initial or resumed runtime epoch must durably establish successful replay
of the entire frozen warm-up role before its first measured `STARTED` record.
Every resumed epoch replays the exact same frozen, non-measured warm-up
membership. Each replay creates execution evidence only: it does not create a
new observation, population, role, or membership and never enters the 1,200
calibration observations. Missing, incomplete, failed, or identity-incompatible
warm-up evidence invalidates the run. Restart may resume only at a fully closed
block boundary under a fresh epoch after complete warm-up replay. An orphan
`STARTED`, one-to-three completed positions in an unclosed block, or any other
mid-block restart is terminal; continuation requires a new run ID.

`RUN_SEALED` and `RUN_INVALIDATED` records are audit/publication evidence only.
Validity and invalidity are always mechanically derived from full manifest and
record-chain verification. A seal cannot repair incomplete evidence, and the
absence of an invalidation record cannot neutralize an orphan start, partial
block, failure, tamper, identity change, or incomplete schedule.

**Detached root and two verification levels.** The strict canonical raw-
evidence-root payload excludes its own digest and binds every population/role,
vector, oracle/FLAT, schedule, control, environment, runtime/epoch/block,
record-chain, source-revision, count, and timestamp commitment. Its detached
digest is exactly:

```
raw_evidence_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_RAW_EVIDENCE_ROOT::V1\x00"
    + canonical_json_bytes(raw_evidence_root_payload)
).hexdigest()
```

Internal integrity verification takes a bundle plus expected identity and
returns only a non-authorizing integrity report. Root-pinned issuance takes the
bundle, expected identity, and an independently supplied governed root pin;
it must rerun/reconstruct complete bundle verification itself and must not
trust a caller-supplied integrity report as sufficient evidence. The expected
root must never be derived from the same bundle inside the issuing call.
Successful comparison may issue a private-construction root-pinned calibration-
evidence capability, but hashes and private constructors are API/integrity
discipline, not signatures or hostile-host attestation.

R2 remains predictive provenance only. A compromised producer can still lie
about unobserved searches, timing boundaries, omitted external roles, or a root
pin it controls; cryptographic signer identity, transparency, or remote
attestation requires a separate decision. R1 remains unchanged, and R2 creates
no freshness, policy, Milvus producer, Phase-3, admission, grant, route,
execution, rollback, or actuation authority.

##### R2-G.1 schedule and population identifier clarification (2026-08-09)

This append-only clarification removes ambiguity from the identifier fields in
R2's accepted schedule seed. It changes no R1 field or behavior and changes no
statistic, sample count, role, schedule operation, freshness rule, or authority
boundary.

`cell_id` is the detached domain-separated SHA-256 of exactly:

```json
{
  "schema_version": "response-profile-cell-v1",
  "metric": "<exact Metric value>",
  "threshold_stratum": "<canonical stratum>"
}
```

Its digest domain is exactly
`b"VD::RESPONSE_PROFILE_CELL::V1\x00"`.

`role_or_segment_id` is the detached domain-separated SHA-256 of exactly the
governed role descriptor:

```json
{
  "schema_version": "response-profile-role-v1",
  "kind": "<closed role kind>",
  "prospective_segment_index": null
}
```

`prospective_segment_index` is `null` for every non-prospective role and is an
exact integer in `0..19` for a prospective segment. Its digest domain is
exactly `b"VD::RESPONSE_PROFILE_ROLE::V1\x00"`.

For R2 response-profile calibration, `workload_manifest_sha256` is exactly the
detached digest of the canonical
`response-profile-calibration-population-v1` payload under domain
`b"VD::RESPONSE_PROFILE_CALIBRATION_POPULATION::V1\x00"`. The same digest is
used both as the schedule seed's `workload_manifest_sha256` and as R1
`ResponseProfileIdentity.workload_manifest_sha256`; no alternate workload
digest or alias may substitute for it.

`ordered_query_payload_sha256` is the detached domain-separated SHA-256 of
exactly:

```json
{
  "schema_version": "response-profile-ordered-query-payloads-v1",
  "query_payload_sha256": ["<exactly 1200 digests in frozen canonical order>"]
}
```

Its digest domain is exactly
`b"VD::RESPONSE_PROFILE_ORDERED_QUERY_PAYLOADS::V1\x00"`.

These four canonical payloads contain no response result, timing observation,
runtime epoch, retry, authorization, routing, or execution evidence. Generic
closed-role manifests have role-specific cardinality: warm-up has exactly 200
members, calibration has exactly 1,200 members, and each prospective segment
has exactly 1,200 members. No generic manifest rule invents 1,200-member
semantics for any unrelated governed role.

#### R2-G.2 lifecycle-ledger clarification (proposed 2026-08-09)

This append-only clarification governs only the R2-B durable structural
lifecycle that consumes the immutable R2-A calibration population and replay
schedule. It does not change R1, R2-A, the response-profile statistics, or any
qualification, policy, authorization, routing, execution, or freshness
contract.

**Canonical run binding.** The schema version is exactly
`response-profile-lifecycle-run-binding-v1`. Its exact canonical payload is:

```json
{
  "schema_version": "response-profile-lifecycle-run-binding-v1",
  "run_id": "<canonical non-empty NFC identifier>",
  "created_at_utc": "<strict RFC3339 UTC timestamp>",
  "cell_id": "<R2-A cell_id>",
  "workload_manifest_sha256": "<R2-A calibration-population digest>",
  "replay_schedule_sha256": "<R2-A replay-schedule digest>",
  "warmup_role_manifest_sha256": "<R2-A 200-member warm-up role-manifest digest>",
  "source_revision": "<canonical non-empty NFC source revision>"
}
```

`run_binding_sha256` is the lowercase hexadecimal SHA-256 of
`b"VD::RESPONSE_PROFILE_LIFECYCLE_RUN_BINDING::V1\x00"` followed by the
repository-canonical JSON bytes of that payload. The R2-A population, schedule,
and warm-up manifest must pass full reconstruction before their digests enter
this payload. The run binding contains no response result.

**Opaque evidence blobs.** The schema version is exactly
`response-profile-opaque-evidence-blob-v1`. Evidence bytes are stored as SQLite
`BLOB` data, not as a path, URI, or caller assertion. Their exact detached
descriptor payload is:

```json
{
  "schema_version": "response-profile-opaque-evidence-blob-v1",
  "run_binding_sha256": "<canonical run-binding digest>",
  "event_seq": "<exact non-negative integer of the referencing event>",
  "evidence_role": "<closed v1 evidence role>",
  "byte_length": "<exact positive integer>",
  "evidence_bytes_sha256": "<lowercase SHA-256 of the exact stored bytes>"
}
```

`opaque_evidence_sha256` is the lowercase hexadecimal SHA-256 of
`b"VD::RESPONSE_PROFILE_OPAQUE_EVIDENCE_BLOB::V1\x00"` followed by the
repository-canonical JSON bytes of that descriptor. The closed v1
`evidence_role` catalog is exactly `WARMUP_EXECUTION`, `MEASURED_RESULT`,
`PRE_BLOCK_RUNTIME_SNAPSHOT`, and `POST_BLOCK_RUNTIME_SNAPSHOT`. Respectively,
those roles may be referenced only by `WARMUP_COMPLETED`,
`MEASUREMENT_COMPLETED`, `BLOCK_STARTED`, and `BLOCK_CLOSED` events. The blob
and its sole referencing event are inserted in one SQLite transaction; the
descriptor's run binding, event sequence, and role must match that event.
R2-B verifies exact bytes, role, byte length, byte digest, descriptor digest,
and referential binding only. R2-C owns semantic interpretation and validation
of those bytes.

**Canonical lifecycle event.** The schema version is exactly
`response-profile-lifecycle-event-v1`. Its exact common canonical payload is:

```json
{
  "schema_version": "response-profile-lifecycle-event-v1",
  "run_binding_sha256": "<canonical run-binding digest>",
  "event_seq": "<exact contiguous non-negative integer>",
  "event_kind": "<closed v1 lifecycle event kind>",
  "epoch_index": "<exact non-negative integer or null>",
  "block_index": "<exact integer in 0..1199 or null>",
  "position_index": "<exact integer in 0..4799 or null>",
  "recorded_at_utc": "<strict RFC3339 UTC metadata timestamp>",
  "event_data": "<exact event-kind-specific object>",
  "previous_event_sha256": "<run-binding digest for event 0; otherwise the immediately preceding event digest>"
}
```

`lifecycle_event_sha256` is the lowercase hexadecimal SHA-256 of
`b"VD::RESPONSE_PROFILE_LIFECYCLE_EVENT::V1\x00"` followed by the
repository-canonical JSON bytes of that payload. The closed event catalog and
exact `event_data` variants are:

- `EPOCH_STARTED`: `{}`.
- `WARMUP_COMPLETED`:
  `{"warmup_role_manifest_sha256": "<run-bound digest>",
  "warmup_execution_blob_sha256": "<blob digest>"}`.
- `BLOCK_STARTED`:
  `{"pre_block_runtime_snapshot_blob_sha256": "<blob digest>"}`.
- `MEASUREMENT_STARTED`:
  `{"within_block_index": <0..3>, "canonical_query_index": <0..1199>,
  "query_id": <R1-compatible canonical query ID>,
  "query_id_sha256": "<source-local digest>",
  "observation_identity_sha256": "<cross-source digest>",
  "ef": <the exact R2-A scheduled ef>,
  "started_monotonic_ns": <exact non-negative integer>}`.
- `MEASUREMENT_COMPLETED`:
  `{"measurement_started_event_sha256": "<matching STARTED event digest>",
  "measured_result_blob_sha256": "<blob digest>",
  "completed_monotonic_ns": <exact integer greater than the matching start>}`.
- `BLOCK_CLOSED`:
  `{"block_started_event_sha256": "<matching BLOCK_STARTED event digest>",
  "measurement_completed_event_sha256": ["<exactly four matching COMPLETED event digests in scheduled within-block order>"],
  "post_block_runtime_snapshot_blob_sha256": "<blob digest>"}`.
- `RUN_SEALED`: `{}`.
- `RUN_INVALIDATED`: `{"reason_code": "<canonical non-empty stable reason>"}`.

Exact fields and types are required and unknown fields fail closed.
Epoch-level events have a non-null `epoch_index`; block events additionally
have a non-null `block_index`; measurement events additionally have a non-null
`position_index`; run-level audit events use null epoch, block, and position
indexes. Every position identity and order must equal the corresponding R2-A
schedule position.

**Lifecycle, order, and restart semantics.** “One logical record per schedule
position” means exactly one immutable `MEASUREMENT_STARTED` ->
`MEASUREMENT_COMPLETED` pair for that position, never one mutable row and never
a replacement pair. `event_seq` plus the verified previous-event hash chain is
the sole lifecycle ordering authority. UTC timestamps are metadata; they may
not establish, repair, or reorder lifecycle state. Latency chronology is
derived only from the matching same-epoch monotonic start/completion readings.

Each epoch must begin with `EPOCH_STARTED` and durably bind one opaque
`WARMUP_EXECUTION` blob through `WARMUP_COMPLETED` before `BLOCK_STARTED` or
`MEASUREMENT_STARTED`. At R2-B this is a structural completion claim only; R2-C
must verify that the bytes prove successful execution of all 200 frozen
warm-up members. Each measured block then contains one `BLOCK_STARTED`, the
four exact scheduled STARTED/completed pairs, and one `BLOCK_CLOSED`. A
structurally complete run has exactly 1,200 such closed blocks and exactly
4,800 completed measured pairs.

An orphan `MEASUREMENT_STARTED` is terminal and its position is never retried.
Any measured block that was started but not closed is terminal. Restart is
permitted only after a closed block and requires a fresh epoch plus replay of
the entire frozen 200-query warm-up role before the next measured start. An
epoch interrupted during warm-up, with no `BLOCK_STARTED` and no
`MEASUREMENT_STARTED`, is abandoned execution evidence: it does not invalidate
previously closed measured evidence. Reopen starts a fresh epoch and replays
the exact same frozen warm-up membership from the beginning. Such replay adds
no population member and no calibration observation. This is the precise
later clarification of the earlier requirement that missing or incomplete
warm-up evidence invalidates measured execution: incomplete warm-up is fatal
if measured/block execution begins in that epoch, but a warm-up-only abandoned
epoch cannot repair, replace, or invalidate prior closed blocks.

`RUN_SEALED` and `RUN_INVALIDATED` are append-only audit/publication events
only. Completeness and invalidity are always derived mechanically from the
verified run binding, full event chain, blob references, R2-A schedule, and
state transitions. A seal cannot make incomplete evidence valid; absence of an
invalidation event cannot make an orphan or partial block valid; neither event
can repair or replace lifecycle evidence.

R2-B structural completeness is necessary but insufficient for raw-evidence
validity or statistical validity. It never grants qualification, profile,
policy, admission, authorization, routing, or execution authority. R2-C must
semantically verify the opaque evidence before any later root-pinned
calibration-evidence capability can exist.

#### R2-G.3 semantic evidence, root pin, and R1 projection clarification (2026-08-11)

Status: Accepted — implementation and adversarial verification pending

Risk level: CRITICAL

This append-only clarification freezes the missing R2-C through R2-E boundary.
It changes no R1 statistic, R2-A population or schedule, R2-B lifecycle, Phase-3
authority, Stage-4 evidence, policy, grant, route, or actuation contract. The
response-profile path remains predictive and non-authorizing.

**Options considered.** Reusing Stage-4 recall/latency evidence was rejected:
its purpose, schedule, identity, and confidence contracts differ and would make
Stage-4 evidence accidental response-profile authority. Trusting producer-
supplied aggregate recall/latency was rejected because it cannot prove query-
level schedule, oracle, threshold, or timing semantics. The selected design uses
response-profile-specific canonical raw documents, complete reconstruction, an
independently supplied root pin, and the unchanged R1 estimator as separate
boundaries.

**Common canonical rules.** Every semantic document is strict canonical JSON
using `vdbench.artifacts.canonical_json_bytes`: exact field inventory and types,
no duplicate JSON keys at a byte-parsing boundary, NFC text, lowercase 64-hex
digests, finite binary64 numbers, booleans forbidden where integers or numbers
are required, and byte-identical reconstructive verification. Each document
stores its detached digest beside a payload that excludes that digest. Private
constructors are API discipline, not signatures or hostile-host attestation.

**Independent exact-oracle catalog.** Before replay, one immutable
`response-profile-oracle-manifest-v1` contains exactly 1,200 records in the
calibration population's frozen canonical order. Each strict
`response-profile-oracle-record-v1` binds observation identity, query-ID digest,
query-payload digest, exact positive result limit, exact non-negative full
threshold cardinality, and capped exact-oracle result IDs and distances in
metric order with integer-ID tie breaking. Result IDs are exact distinct
integers; distances are finite binary64 values satisfying governed range
semantics. Capped length is `min(full_count, limit)`. Record and manifest digest
domains are `b"VD::RESPONSE_PROFILE_ORACLE_RECORD::V1\x00"` and
`b"VD::RESPONSE_PROFILE_ORACLE_MANIFEST::V1\x00"`. Verification receives an
independently verified expected oracle-manifest digest; a digest derived from
the measured bundle inside the verifier cannot satisfy that expectation.

**Warm-up execution.** `WARMUP_EXECUTION` bytes are one strict
`response-profile-warmup-execution-v1` document under digest domain
`b"VD::RESPONSE_PROFILE_WARMUP_EXECUTION::V1\x00"`. It binds lifecycle run,
epoch, 200-member warm-up role-manifest digest, control/environment digests,
HNSW/index and data identities, source revision, and exactly 800 execution
records. In frozen role-member order, each member executes exactly once at each
`ef` in `(200, 400, 800, 1600)` ascending order. Each record binds observation
identity, query-ID digest, query-payload digest, exact validated HNSW search-
configuration digest, `ef`, and outcome `SUCCESS`. Warm-up records contain no
latency, recall, result IDs, distances, or calibration observation. Missing,
failed, duplicate, extra, reordered, identity-mismatched, or configuration-
mismatched execution invalidates the semantic run. A new epoch repeats the same
membership as execution evidence only.

**Runtime snapshots.** Both runtime roles use schema
`response-profile-runtime-snapshot-v1` and digest domain
`b"VD::RESPONSE_PROFILE_RUNTIME_SNAPSHOT::V1\x00"`. The payload binds lifecycle
run, epoch, block, phase `PRE_BLOCK` or `POST_BLOCK`, RFC3339 UTC observation
metadata, metric, threshold stratum, control/environment digests, HNSW/index
and data identities, source revision, all four exact HNSW search-configuration
digests in supported-`ef` order, collection-loaded state, and Milvus/etcd/MinIO
health. Health and load values must be exact `true`; all identities and
configurations must equal expected profile identity. PRE binds only
`BLOCK_STARTED`; POST binds only its matching `BLOCK_CLOSED`. One block's PRE
and POST agree on every non-time field. Snapshots contain no response result.

**Measured results.** `MEASURED_RESULT` bytes use schema
`response-profile-measured-result-v1` and digest domain
`b"VD::RESPONSE_PROFILE_MEASURED_RESULT::V1\x00"`. The payload binds lifecycle
run, epoch, block, schedule position, matching STARTED-event digest,
observation identity, query-ID digest, query-payload digest, scheduled `ef`,
exact validated HNSW search-configuration digest, independently expected oracle
record digest, outcome, candidate IDs, candidate distances, and failure code.
Outcome is exactly `SUCCESS`, `FAILED`, or `TIMED_OUT`. A usable calibration run
requires `SUCCESS`; failed/timed-out documents preserve evidence but invalidate
semantic completion and contain empty result arrays plus a canonical non-empty
failure code. Successful documents have a null failure code, exact distinct
integer IDs, same-length finite binary64 distances, at most `limit` results,
metric-monotonic order, and no threshold violation. Candidate tie order is not
authority; recall is set-based against capped oracle IDs.

The verifier recomputes capped recall with the repository's governed empty-
reference rule and recomputes latency milliseconds exactly as
`(completed_monotonic_ns - started_monotonic_ns) / 1_000_000.0` from matching
R2-B events. It trusts no result-supplied recall, latency, cardinality,
threshold, health, or identity assertion. All derived values must be finite.
Cross-position chronology and event/result association remain governed by R2-B.

**R2-C bundle and semantic report.** One immutable in-memory bundle contains
fully reconstructed R2-A calibration population, warm-up manifest, replay
schedule, R2-B run binding, complete ordered event chain, every referenced
opaque blob, independent oracle manifest, and expected R1 profile identity.
R2-C reruns R2-A and R2-B reconstruction, then verifies every semantic document
and exact one-to-one reference. Unreferenced, multiply referenced, missing,
substituted, or wrong-role bytes fail closed. The strict
`response-profile-semantic-verification-v1` report binds all expected identity
fields; run/event/blob/oracle/population/schedule digests; exact counts; ordered
query/`ef` derived recall and latency observations; calibration timestamps;
stable reason codes; and `complete`. Its detached digest domain is
`b"VD::RESPONSE_PROFILE_SEMANTIC_VERIFICATION::V1\x00"`. It is a non-authorizing
integrity report and cannot itself be consumed by policy.

**Raw-evidence root.** A complete report constructs the already-governed
`response-profile-raw-evidence-root-v1` payload from exact profile identity,
population/role/schedule/run/oracle/report digests, ordered lifecycle-event
digests, ordered opaque-descriptor and byte digests, counts, and calibration
timestamps. The payload excludes its digest. The existing
`b"VD::RESPONSE_PROFILE_RAW_EVIDENCE_ROOT::V1\x00"` formula remains unchanged.
Internal verification returns the report and computed root only; it issues no
candidate-capable evidence.

**R2-D independently root-pinned capability.** Issuance takes the entire bundle,
exact expectation, and one independently supplied lowercase raw-evidence root.
It reruns R2-C and compares the computed root with `hmac.compare_digest`; a
caller-supplied prior report is insufficient. Only a complete match privately
constructs immutable `root-pinned-response-profile-evidence-v1`, binding the
semantic-report digest, raw root, all R1 identity fields, and exact query-major
derived observations. Its detached digest domain is
`b"VD::ROOT_PINNED_RESPONSE_PROFILE_EVIDENCE::V1\x00"`. This is root-pinned
integrity, not qualification, freshness, admission, grant, routing, execution,
or signer authority.

**R2-E deterministic R1 projection.** Projection accepts only the exact concrete
root-pinned capability plus independently supplied expected root and R1
identity. It reconstructs the capability, compares root and identity, builds
`ResponseProfileCalibrationEvidence` in frozen query order with responses in
`(200, 400, 800, 1600)` order, and invokes existing R1
`build_calibrated_response_profile`. R1 formulas, ranks, profile schema, and
digest are not reimplemented. The resulting profile's
`raw_evidence_sha256` equals the independent root pin. Bare R1 profiles and R2-C
reports remain non-authorizing.

**Failure and dependency boundaries.** Malformed/non-canonical documents,
unsupported schemas, source/configuration/identity mismatch, missing roles,
failed searches, oracle disagreement, threshold violation, health failure,
schedule substitution, lifecycle/count/digest/root mismatch, and unreferenced
or reused evidence fail closed with stable reasons and no partial capability or
profile. R2-C through R2-E import no policy, Phase-3, Stage-4, grant, route,
actuation, Milvus client, or live-service module. Freshness, policy profile-
digest propagation, producer execution, and adversarial publication remain
separate reviewed checkpoints.

Consequences: v1 stores substantial query-level evidence and performs complete
reconstruction, prioritizing research validity over throughput. The 800-search
per-epoch warm-up cost is explicit. Compression or authenticated signing needs
a new contract with equivalence evidence; v1 historical bytes are never
reinterpreted.

#### R2-G.4 offline producer, durable export, and adversarial publication clarification (2026-08-11)

Status: Accepted — implementation and adversarial verification pending

Risk level: CRITICAL

This append-only clarification governs R2-F's offline composition. It does not
authorize a Milvus adapter, candidate traffic, policy consumption, freshness,
grant, routing, or actuation.

**Verified durable export.** The R2-B2 ledger may expose one explicit immutable
`response-profile-lifecycle-export-v1` value containing only the exact verified
run binding, complete ordered event chain, and complete event-ordered opaque
evidence collection. Export occurs under the ledger's existing ownership and
thread lock, in one coherent SQLite read transaction, after file, schema,
pragma, run-binding, canonical-document, hash-chain, head, foreign-key, and full
R2-B reconstruction checks. It is refused for a poisoned/closed instance,
terminal recovery, an active measurement permit, a recovery interlock, or a
structurally incomplete lifecycle. Exported bytes are copied immutable values;
the metadata-only `current_view` remains unchanged. The export is evidence
transport only, never authority, and R2-C must independently reconstruct it.

**Producer ports and trusted inputs.** One offline `ResponseProfileProducer`
composition consumes exact R2-A calibration/warm-up manifests and replay
schedule through the immutable R2-B run binding; one expected R1 identity; one
independently constructed exact-oracle manifest; canonical query material for
every frozen calibration and warm-up member; the durable ledger; and injected
ports for query execution, runtime readiness snapshots, a monotonic clock, and
UTC metadata time. Query material is accepted only when rebuilding the existing
R2-A vector/query-payload/observation identities yields the exact frozen member.
The producer neither constructs nor chooses the independent oracle/root pin.

The query-execution port accepts one immutable query vector plus the exact
validated HNSW `SearchConfiguration` selected by the governed schedule and
returns only exact candidate IDs/distances. The producer records monotonic time
immediately before durable STARTED and immediately after the external call; it
does not trust a client-reported latency. Runtime readiness is collected outside
the SQLite lock and encoded into the governed PRE/POST snapshots. No external
query, health, oracle, filesystem publication, or reviewer operation occurs
while a ledger transaction is held.

**Execution and crash ordering.** Every initial or resumed epoch executes all
800 governed non-measured warm-up searches successfully before one atomic
`WARMUP_COMPLETED` append. For each next block the producer collects and appends
PRE, durably appends each `MEASUREMENT_STARTED`, invokes exactly one search only
after receiving that current-instance permit, atomically appends the matching
result and completion, collects/appends POST, and closes the block. A client
failure or timeout is persisted as the governed failed result and ends the run
fail closed; it is not replaced or retried. A crash after STARTED remains the
R2-B terminal orphan. Resume is derived only from verified ledger state: closed-
block reopen begins a fresh epoch and complete warm-up; warm-up-only interruption
also begins a fresh epoch; a terminal orphan/partial block cannot resume.

**Publication boundary.** After exactly 1,200 closed blocks and 4,800 completed
positions, the producer may append audit-only `RUN_SEALED`, request the verified
durable export, assemble the R2-C bundle from that export and the frozen
independent inputs, and run internal R2-C verification. Its result is only the
non-authorizing semantic report and computed raw root. R2-D issuance remains a
separate reviewer/composition call that receives an independently supplied
expected root and reruns R2-C. The producer must not feed its own computed root
back as the independent pin.

**Failure and bounded execution.** Construction rejects missing, duplicate,
extra, noncanonical, or identity-mismatched query material before dispatch. A
bounded `run(max_blocks)` operation may stop only at a closed-block boundary and
reports progress without claiming completion. Unexpected port, ledger, encoding,
or verification failures fail closed with stable reason codes and no retry of a
durably started position. No mutable aggregate, caller-supplied cursor, seal, or
result count is progress authority; ledger reconstruction is authoritative.

R2-F adversarial verification must prove zero search before durable STARTED,
exact 800-search warm-up per epoch, exact 4,800 measured calls, deterministic
resume, failure/timeout persistence, no measured retry, query-material
substitution refusal, unhealthy snapshot refusal by R2-C, explicit export
reconstruction, self-derived-root non-issuance, and static absence of policy,
Stage-4, grant, route, actuation, and direct Milvus dependencies.

##### R2-G.4a runtime/final identity composition clarification (2026-08-11)

The producer cannot possess final calibration timestamps before performing the
measurements that define them. It therefore receives one immutable static
calibration context containing exactly the R1 identity fields other than
`calibration_started_at_utc`, `calibration_completed_at_utc`, and
`generated_at_utc`. Runtime semantic documents bind only that static projection.
After verified durable export, the first measured STARTED metadata timestamp and
last measured COMPLETED metadata timestamp become the calibration interval; an
injected UTC clock supplies `generated_at_utc` no earlier than completion. The
producer then constructs the ordinary unchanged R1 `ResponseProfileIdentity`.
No planned, caller-predicted, or rewritten measurement timestamp is accepted.
R2-C still receives and verifies the final complete R1 identity.

A failed or interrupted non-measured warm-up never emits `WARMUP_COMPLETED` and
cannot proceed to measurement. It may be abandoned under the already-governed
warm-up-only recovery rule; the next producer attempt starts a fresh epoch and
replays all 800 warm-up calls. This is not retry of a measured observation and
does not change population membership. Once any measured STARTED is durable,
the existing orphan/partial-block terminal rules apply without exception.

---

### ADR-010: Bind response profiles to an atomic latest detector head without a universal TTL

Status: Proposed — offline structural implementation under review; candidate use remains disabled

Risk level: CRITICAL

#### Context and decision drivers

ADR-009 intentionally forbids inventing a universal response-profile TTL. The
completed R2-C through R2-F boundaries prove raw calibration integrity but do
not yet prove which detector trigger caused calibration or that no later
detector evaluation superseded it. `control_profile_sha256` is currently only
an opaque identity pin, and `FileMonitorStateStore` cannot atomically issue a
verified latest-head capability. Treating either as freshness would be a
candidate-authority bypass.

The design must preserve rollback-first policy behavior, keep pure policy free
of I/O, bind the exact detector trigger before calibration results, detect every
later detector evaluation, and remain fail closed when monitoring stalls or its
state cannot be verified.

#### Options considered

1. **Fixed elapsed-time TTL.** Rejected: EXP-010 has not established a
   transportable duration, and a timestamp cannot prove workload stationarity.
2. **Read `FileMonitorStateStore` immediately before policy.** Rejected: it has
   no atomic latest-decision record, process ownership, hash chain, or coherent
   issue-and-compare transaction.
3. **Human assertion that the profile is fresh.** Rejected as machine
   authority; a human may still refuse or approve a later signed grant.
4. **Pre-result trigger binding plus append-only atomic detector-head ledger.**
   Chosen: freshness becomes exact lineage compatibility, not elapsed-time
   inference.

#### Pre-result control binding

Before any calibration result is inspected, one strict canonical
`response-profile-control-v1` document binds exactly:

- schema version;
- monitor stream identity (`stream_id`, metric, threshold stratum,
  configuration identity, data identity, FLAT identity, HNSW identity);
- exact detector `EvidenceProvenance.sha256`, reference/current window IDs and
  manifests, and the exact evaluated current `window_sequence`;
- the exact canonical detector-head digest plus the verified durable head-record
  sequence, record digest, and persistence timestamp from which the control was
  frozen;
- calibration-population, warm-up-role, ordered-query-payload, and replay-
  schedule digests;
- environment-manifest digest and source revision; and
- `frozen_at_utc` metadata captured before response-profile results.

Its detached digest is
`SHA256(b"VD::RESPONSE_PROFILE_CONTROL::V1\x00" + canonical_json_bytes(payload))`.
R2-C receives the concrete control document independently, reconstructs its
detector provenance, verifies exact R2-A/R2-B/R1 identity agreement, and
requires its digest to equal `ResponseProfileIdentity.control_profile_sha256`.
The control document becomes part of the semantic bundle and raw-root
projection through that already-bound identity digest. A caller-chosen opaque
digest can no longer establish trigger lineage.

#### Atomic latest detector head

A new hardened SQLite store owns one append-only chain per monitor stream. Each
terminal detector evaluation appends one strict
`response-profile-detector-head-v1` record in the same transaction that stores
the monitor state/outbox transition exposing that evaluation. The record binds
stream identity, exact window sequence, detector state/classification, and the
complete detector provenance. Detector state and classification are retained
because provenance alone does not bind the terminal detector outcome and could
otherwise substitute a different classification over the same evidence
lineage. Policy audit ID is deliberately excluded because policy follows the
detector trigger and must not become part of detector authority. A separate
detector-evaluation timestamp is excluded because it cannot prove durable
ordering. The durable head record instead binds a unique store-instance
identity, persistence timestamp, previous-record digest, sequence, and canonical
record digest. The store-instance identity prevents a semantically identical
head from another ledger from substituting for the committed trigger; the
record digest and timestamp make transaction identity and ordering mechanically
checkable. STRICT tables, exact schema verification,
UPDATE/DELETE triggers, `BEGIN IMMEDIATE`, process ownership, path hardening,
and full chain reconstruction follow the existing hardened-ledger contracts.

The legacy file store remains valid for DRY_RUN compatibility but can never
issue freshness evidence. Candidate-capable composition requires the SQLite
store's private `VerifiedLatestDetectorHead`, issued from one coherent read
transaction after complete chain verification. It is a snapshot at that
instant, not a promise that it remains latest forever.

#### Non-authorizing verified-refresh evidence

One pure binder consumes the exact concrete root-pinned R2-D capability, its
deterministically projected R1 profile, the verified control document, and one
verified-latest detector head. It reconstructs all inputs and requires:

- profile/root/capability/control digests agree exactly;
- control trigger provenance, terminal detector outcome, window sequence, head
  digest, durable head-record sequence, record digest, and persistence timestamp
  equal the verified latest head;
- metric, stratum, configuration, data, FLAT, and HNSW identities agree with
  the detector provenance and policy pre-action evidence;
- durable head-record persistence precedes control freeze, control freeze
  strictly precedes profile calibration start, and calibration uses the
  exact governed population/schedule/environment/source identities; and
- no newer detector head exists at the issuing refresh.

`FreshResponseProfileEvidence` is historical, non-authorizing evidence of one
verified refresh instant only: at that instant, the root-pinned profile/control
lineage matched the store-issued latest detector head. It does not promise
continuing freshness and cannot establish qualification, policy, admission,
grant, activation, routing, or execution authority. Private construction is API
discipline, not cryptographic authenticity. The binder performs no statistics
and no I/O.

Policy remains rollback-first and the existing B-001 candidate interlock remains
closed. Action 7A does not consume this evidence in candidate-capable policy.
Future candidate-capable evaluation, if separately accepted after prospective
evidence, must acquire one newly verified latest-head snapshot in the same
governed policy-evaluation composition and bind that exact durable head-record
digest into the resulting policy and signed lineage. A previously issued latest
head or refresh-evidence value is historical only. Future activation must
revalidate the governed latest-head lineage under the separately reviewed
signed-lineage contract. DRY_RUN may inspect verified predictive evidence for
diagnostics, but no bare profile, legacy estimate, or Action-7A wrapper may
authorize `START_CANARY`.

#### Evidence gate and consequences

This proposal defines structural machinery for detecting exact detector-lineage
compatibility; it does not define the empirical rule for when a later head
should invalidate a profile and does not claim a time TTL. Candidate-capable
issuance remains disabled until EXP-010/EXP-011 prospective evidence is produced
and independently reviewed, an invalidation policy is accepted, and the atomic-
head implementation passes restart, race, tamper, stale-head, and TOCTOU tests.
The mechanism can detect a later or incompatible head; whether a stationary
later head preserves predictive validity is an empirical question, not an
Action-7A architecture claim.

Signed grant, activation, route-state, lifecycle, and execution-ledger v2
propagation are deliberately deferred to ADR-011. No existing v1 grant or
historical audit record is reinterpreted as carrying response-profile lineage.

---

### ADR-011: Signed-lineage v2 for response-profile candidate authority (design only)

Status: Proposed — design sketch only; no implementation authorized by this document

Risk level: CRITICAL

Evidence status: none. This ADR authorizes no code. It exists to satisfy ADR-010's
explicit deferral ("Signed grant, activation, route-state, lifecycle, and
execution-ledger v2 propagation are deliberately deferred to ADR-011") with a
concrete design proposal, so a future, separately reviewed implementation
session has a governed starting point rather than an unstated intent.

#### Context and decision drivers

ADR-010 defines exact detector-lineage compatibility evidence
(`FreshResponseProfileEvidence`) but is explicit that this evidence is
historical and non-authorizing, and that it "cannot establish qualification,
policy, admission, grant, activation, routing, or execution authority." The
B-001 interlock in `policy.py` remains an unconditional refusal
(`RESPONSE_PROFILE_CANDIDATE_CAPABILITY_AVAILABLE = False`, no import of any
`response_profile*` module) with no dynamic evidence-consumption path to
migrate. Before that interlock can be lifted for any real candidate use, the
project needs a governed answer to a second, independent question ADR-010
deliberately does not answer: once freshness evidence is real and accepted,
how does the exact admitted decision remain the exact signed, activated, and
executed decision, with no substitution possible at any step?

A mature answer to that question already exists for the unrelated ADR-008
canary track: `canary_approval.py` (real Ed25519 signer/verifier against an
injected trust store), `canary_admission.py` (`Stage4AdmissionRequest`/
`Result`), `canary_grant_store.py` (`CanaryGrantUseStore`, one-time grant
reservation), `canary_activation.py` (`CanaryActivationCoordinator`,
`ActiveCanaryContext`), `canary_route_state.py` (`RouteState`/
`RouteStateBinding`, restart-to-LKG-only default), `canary_live_runner.py`
(`Stage4LiveRunner`), and `canary_execution_ledger.py`
(`Stage4ExecutionLedger`, atomic idempotent-resume append). These modules have
zero import relationship with any `response_profile*` module today. The
question this ADR must answer is not "do we build something new," but
precisely which of these primitives generalize by mechanism and which are
bound to canary/`ef`-routing semantics specific enough that reusing them
as-is would silently blur two separate authority domains.

Decision drivers:

- Preserve the required invariant `ADMITTED == SIGNED == ACTIVATED == EXECUTED`
  as a mechanically checked property, not a documentation claim: the exact
  `PolicyDecision` digest an admission step approved must equal the digest a
  human-signed grant covers, must equal the digest the activation coordinator
  reads when installing a route, must equal the digest the execution ledger
  records for that live run. Any substitution at any step must fail closed.
- Reuse proven ADR-008 machinery wherever its actual transaction/cryptographic
  semantics match, per the "better-option" discipline already applied
  elsewhere in this document — but do not declare a primitive sufficient by
  resemblance alone; a per-primitive fit check is required (below).
- Keep the two authority domains (ADR-008 canary/`ef`-routing grants and any
  future response-profile candidate grants) in separate schemas and separate
  storage, so a response-profile grant can never be read as, or substituted
  for, a canary grant or vice versa.
- Do not create a real signer, a real grant, or any code path that could issue
  candidate authority in this pass or in this document.

#### Options considered

1. **Build new response-profile-specific signer, grant, and ledger primitives
   from scratch.** Rejected: duplicates already-proven ADR-008 cryptographic
   and transaction machinery for no safety benefit, and multiplies the audit
   surface that must be independently reviewed.
2. **Reuse `CanaryApprovalGrant`, `Stage4ExecutionLedger`, and
   `canary_route_state.py` unmodified, overloading their existing fields to
   also carry response-profile identities.** Rejected: `CanaryApprovalGrant`
   binds fields specific to the 600/60 finite-manifest canary contract
   (candidate/last-known-good `ef`, eligible-workload manifest digest,
   canonical candidate-selection-record digest, maximum traffic fraction);
   none of these have a response-profile analogue, and overloading them would
   let a reviewer mistake a response-profile grant for a canary grant (or the
   reverse) — exactly the kind of authority-domain collapse this project's
   governance discipline exists to prevent.
3. **Define new response-profile-specific types that reuse ADR-008's proven
   mechanisms (signer, ledger transaction pattern, route-state state machine)
   without reusing its canary-specific schemas.** Chosen.

#### Per-primitive fit (informs the future implementation, not this document)

| ADR-008 primitive | Mechanism | Fit for response-profile reuse |
|---|---|---|
| `canary_approval.py`'s Ed25519 signer/verifier + injected trust store | Domain-agnostic cryptographic verification of an immutable signed document | Reuse the mechanism directly; do not reuse `CanaryApprovalGrant`'s schema |
| `CanaryApprovalGrant` dataclass | Canary/`ef`-routing-specific field set | Do not reuse as-is; define a new `ResponseProfileGrant` type binding the identities listed below, verified with the same discipline (immutable, signed, one-time, exact-digest bound, private keys never in the repository) |
| `canary_grant_store.py`'s `CanaryGrantUseStore` | One-time SQLite reservation ledger, domain-agnostic shape | Reuse the pattern; separate table/store instance, never shared rows with canary grants |
| `canary_execution_ledger.py`'s `Stage4ExecutionLedger` | Atomic append, idempotent resume via durable-row inventory | Reuse the pattern if a response-profile execution row's schema fits this shape; otherwise a schema-compatible sibling table under the same transaction discipline, not a new persistence framework |
| `canary_route_state.py`'s `RouteState`/`RouteStateBinding` | Bounded state machine, restart-to-LKG-only default | Reuse the state-machine shape; a response-profile route state is a separate instance, never sharing storage with canary route state |
| `canary_activation.py`'s `CanaryActivationCoordinator` | Validates grant/gates/health/identity before atomically installing a route | Reuse the coordination pattern; the concrete gate checks differ (response-profile lineage instead of canary workload/selection-record checks) |

#### Bound identities (candidate list; confirm before implementation)

A `ResponseProfileGrant` and its downstream signed lineage must bind, where
applicable: the policy decision digest; response-profile candidate authority
digest; R2-C raw evidence root; R2-D root-pinned capability digest; R2-E
projected profile digest; `response-profile-control-v1` digest; ADR-010
freshness/invalidation evidence digest; the exact detector-head digest; the
durable detector-head record sequence and digest; the issuing store's
binding/identity; configuration, data, index (FLAT/HNSW), and environment
identity; source revision; workload identity; execution-schedule identity;
the Stage-4 evidence binding (if a response-profile candidate route composes
with the existing Stage-4 live path rather than replacing it — an open
question for the implementation session, not resolved here); admission
receipt; route plan; route-state binding; runtime readiness; grant identity;
active-canary context (only if composition with the existing canary path is
chosen); live request identity; and rollback/failback lineage. This list is a
starting point derived from ADR-010's own bound-identity set plus
`CanaryApprovalGrant`'s field set; the implementation session must confirm
each field against the concrete `ResponseProfileGrant` schema it defines, not
treat this list as already authoritative.

#### Required invariant

`ADMITTED == SIGNED == ACTIVATED == EXECUTED`. Mechanically: the admission
step's approved `PolicyDecision` digest must equal the digest embedded in the
`ResponseProfileGrant` a human operator key signs, must equal the digest the
activation coordinator reads when it installs a route, and must equal the
digest the execution ledger records for the resulting live run. A stale
detector-head record, a historical `FreshResponseProfileEvidence` wrapper, a
changed data/index/environment/source-revision/schedule identity, a changed
`Stage4EvidenceBinding`, a changed admission receipt, a changed route
binding, or a replayed prior signed lineage must each independently cause
verification to fail closed at whichever step first detects the mismatch —
never at a later step, and never silently.

#### Explicit prerequisites before any implementation session

1. Accepted freshness/invalidation governance: ADR-010 must move from
   Proposed to Accepted (or be superseded) on the strength of real, reviewed
   evidence — this ADR does not itself accept ADR-010.
2. Real EXP-011 prospective evidence: the structural/offline scenario
   coverage produced under this same effort (labeled
   `STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE`) is explicitly insufficient;
   a real, read-only-Milvus, elapsed-prospective-window run is required and
   must be independently reviewed.
3. An accepted invalidation policy answering the empirical question ADR-010
   leaves open: whether, and under what conditions, a later detector head
   preserves or invalidates a profile's predictive validity.
4. Until 1–3 are satisfied, `policy.py`'s B-001 interlock remains closed
   exactly as implemented today. This ADR does not authorize touching
   `policy.py`, and no code in this campaign does.
5. This ADR itself requires a dedicated review and acceptance cycle, separate
   from and after the above, before any `ResponseProfileGrant`/ledger/route-
   state code is written. Drafting this document is not equivalent to
   authorizing that code.

#### Consequences

No code is authorized by this document. The existing ADR-008 canary/`ef`-
routing authority track (signer, grant store, route authority, activation,
execution ledger, rollback) is completely unaffected and unmodified by this
proposal. No existing v1 grant, route-state record, or execution-ledger row
is reinterpreted as carrying response-profile lineage, now or by a future
implementation of this ADR. If prerequisites 1–3 are never satisfied, this ADR
remains Proposed indefinitely and `START_CANARY` remains unavailable for
response-profile-derived candidates; that is a correct safety outcome, not a
defect to be worked around.

#### Verification plan (required once accepted, not run by this document)

Adversarial coverage must include at minimum: profile A paired with lineage
signature B; freshness evidence A paired with decision B; admission A paired
with signature B; route A paired with grant B; a stale or superseded
detector-head record; a historical `FreshResponseProfileEvidence` wrapper
presented as current; changed data/index/environment/source-revision/
schedule identity; a changed `Stage4EvidenceBinding`; a changed admission
receipt or route binding; a changed active-canary context; replay of a prior
valid signed lineage; an incomplete signed-lineage document; and an
object-forged instance of any bound-identity value. None of this is
implemented by this document.

---

### ADR-012: Shared durable host-window lineage v2 for future detector and genuine-workload evidence

Status: Accepted — human approved 2026-08-12; offline implementation complete in the current worktree, external review pending

Risk level: CRITICAL

#### Context

The current host path records a genuine completed served request at
`ReferenceRangeGateway.execute()` before shadow filtering, but the current
detector `window_sequence` is assigned later by `BackgroundShadowWorker` from
successful, post-filtered shadow traces: four successful 50-query traces form
one 200-query detector window. Volatile buffering, stale/failed/timed-out
filtering, trace capture, and restart recovery can therefore omit or reorder
the relationship between the original served population and a v1 detector
window. A v1 detector window cannot mechanically prove that it is the same
200-request population required by EXP-010. Reusing trace or detector-outbox
order as the EXP-010 source would silently post-select the workload and is
prohibited.

Historical v1 evidence, manifests, hashes, window numbers, detector heads,
EXP-008/009/010 artifacts, and their interpretation remain immutable
historical evidence. This proposal does not validate them retroactively or
change their semantics.

#### Decision

For future, explicitly v2 streams only, introduce the versioned contract
`response-profile-host-window-lineage-v2`. It owns one append-only durable
canonical source sequence at the genuine host post-response boundary, before
any shadow buffering or filtering. The sequence is per exact governed stream
identity; it is not global across arbitrary host deployments.

Each governed eligible completed served observation receives one immutable
event identity and a contiguous non-negative `source_sequence`. The canonical
window mapping is mechanically derived only within that exact stream:

```
window_sequence = source_sequence // 200
within_window_index = source_sequence % 200
```

The v2 record binds at minimum its event identity; source/window/index
positions; canonical query identity; canonical vector binding and vector
digest; stream and workload identity; source revision; environment digest;
completion timestamp; exact search configuration; and the serving outcome
needed by downstream host logic. The v2 ledger must be append-only,
integrity-protected, restart-reconstructible, and reject gaps, reorderings,
duplicates, or schema/path/hash tampering. Its append must commit before an
observation is offered to any shadow/filtering path. It neither generates a
query nor mutates serving configuration.

Source population membership is determined only by these durable host records.
Shadow and detector evaluation eligibility is a separate, explicitly recorded
property. A filtered, failed, stale, or otherwise unsuitable downstream
evaluation preserves its v2 source position and cannot renumber later
observations, collapse a window, or create a post-selected detector window.

For a future v2 detector path, each shadow-evidence item must bind its exact
v2 source event/position and source-window digest. A detector v2 head must
bind that same host-window identity/digest. Exact value equality is not a
substitute for identity binding. Subject to the unresolved decision below,
the intended equality is:

```
host source window N
  == shadow evidence bound to source window N
  == detector-v2 head bound to source window N
  == EXP-010 capture source window N
```

The EXP-010 v2 adapter is an independent, at-least-once, acknowledged reader
of the host-window ledger. It must not consume detector or shadow outbox
delivery state. Its governed sequence is trigger detector source window `N`,
warm-up source window `N + 1`, and calibration source windows `N + 2` through
`N + 7`; this consumes the same canonical served population without generating
queries or using post-selected shadow traces.

#### Canonical-window and detector progression semantics

`WINDOW_INCOMPLETE` is transient only. It means fewer than 200 contiguous,
durably committed source positions exist for a canonical v2 window. It may
not produce detector evidence, a detector decision, a detector head, a
reference-state update, or an EXP-010 trigger. The v2 ledger derives this
state from the source sequence; no caller-supplied counter or timestamp may
declare it complete.

Once all 200 positions exist, a window remains pending evaluation until every
position has either supplied the required, source-bound shadow evidence or a
durable terminal evaluation-eligibility result. A fully bounded window with
one or more terminally unsuitable positions is `WINDOW_UNEVALUABLE`. This is
terminal and fail closed for that canonical window: it binds all original
positions and their exact ineligibility reasons; produces no DRIFT or
NO_DRIFT decision, detector head, reference/current evidence update, or
EXP-010 trigger; and cannot be repaired by omitting, replacing, or renumbering
a position.

Source progression is independent of evaluability. Every later genuine source
observation retains its natural `N + 1`, `N + 2`, and later window identities
even after an earlier window is unevaluable. A v2 stream records the resulting
evaluation gap durably. It must not silently compare a later window against a
reference across that gap. Instead, the first later fully evaluable v2 window
is a fail-closed rebaseline/reference-establishment window: it may establish a
new reference only, emits neither DRIFT nor NO_DRIFT, creates no detector head,
and cannot trigger EXP-010. Only the next fully evaluable canonical window may
be compared against that newly established reference. A future detector
algorithm that can prove a different nonconsecutive comparison rule requires a
new versioned detector contract; it may not reinterpret this rule.

Only a successful comparison of a fully evaluated v2 current window against a
valid v2 reference may emit a v2 DRIFT or NO_DRIFT decision and persist a v2
detector head. A v2 head binds the exact canonical host-window digest, source
window sequence, reference window digest/sequence, current window
digest/sequence, and the complete source-bound detector provenance. Only a
persisted v2 **DRIFT** head from such a successful comparison may trigger
EXP-010. `WINDOW_INCOMPLETE`, pending evaluation, `WINDOW_UNEVALUABLE`, and
rebaseline windows never do.

Restart reconstructs source positions, window completeness, terminal
eligibility outcomes, evaluation gaps, and reference eligibility exclusively
from the v2 append-only ledger and source-bound shadow evidence. It must never
retry a source position as a new member, renumber a source window, promote an
incomplete/unevaluable/rebaseline window, or let a seal/audit assertion repair
derived state.

#### Migration and non-authority constraints

v1 and v2 schemas must be disjoint and exact-schema verified. A v2 record or
head may never masquerade as v1, and a v1 artifact may never be upgraded by
interpretation. Any future v2 components are evidence/provenance mechanisms;
they create no qualification, policy, grant, routing, actuation, or candidate
authority. They require no live service action for offline structural tests.

#### Detector-v2 evaluator trust boundary

`SQLiteHostWindowDetectorV2Store.process_window` accepts a **caller-supplied**
`evaluator` callable and durably persists whatever `DriftDecision` it returns.
The store validates only that the decision's `EvidenceProvenance` binds the
exact reference and current source windows being processed; it never invokes
`vdbench.drift.evaluate_drift` and performs no statistical computation.

A `V2DetectorHead` therefore proves exactly three things:

1. its reference/current source windows are the durably committed ADR-013
   windows it names;
2. its provenance and shadow-window digests bind those exact windows;
3. the reference/gap/rebaseline progression recorded around it is internally
   consistent and reconstructs identically after restart.

A head **does not** prove that a real governed statistical detector executed.
A head minted from a structural or deterministic-fake evaluator is
indistinguishable by type or field from one minted by a real detector.
Structural/fake-evaluator heads are consequently authorized for **offline
structural use only**, which is the entire scope this ADR grants.

A real EXP-010 trigger will additionally require a **separately governed
real-detector attestation** binding the head to an actual ADR-002/ADR-003
evaluation. That attestation does not exist in this ADR or its implementation,
and until it is separately accepted no v2 head may be treated as real detector
evidence. Consistent with the constraint above, no qualification, policy,
grant, routing, admission, activation, actuation, or candidate authority is
created by a v2 detector head under any circumstances.

#### Expected implementation impact after acceptance

The expected future implementation surface is limited to: a durable genuine
host tee at `host_observation.py`/`ReferenceRangeGateway`; a versioned host
source ledger/store and its tests; source-position bindings in the background
shadow worker and shadow-trace artifact/event contracts; v2 monitor state and
detector-head schemas in `workload_monitor.py`,
`response_profile_detector_head.py`, and
`response_profile_monitor_store.py`; an independent
`GenuineWorkloadObservationSource` adapter in
`response_profile_workload_capture.py`; and an injected offline
`ReadOnlyCaptureMetadataProvider` composition boundary. Related host, shadow,
monitor, detector-head/store, capture, and restart/adversarial tests must be
updated. Existing v1 schemas and loaders remain historical compatibility paths
only; no migration rewrites their records.

#### Required acceptance and verification before implementation

Human acceptance was recorded on 2026-08-12. Implementation must prove:
pre-filter sequence assignment; no
renumbering after filtering; restart-stable 200-position windows; exact
source-to-shadow-to-head bindings; incomplete, pending, unevaluable, and
rebaseline behavior; v1 compatibility; v2/v1 non-substitution; independent
detector/shadow outbox delivery; at-least-once EXP-010 reading; tamper/path/
concurrency failure closure; and the exact N, N+1, N+2..N+7 EXP-010 relation.
No real workload, Milvus service, vector search, grant, routing activation,
EXP-011 run, or live canary is authorized by this ADR.

---

### ADR-013: Commit v2 host-window membership atomically with host response completion

Status: Accepted — human approved 2026-08-12 for the future v2 reference host; offline implementation complete in the current worktree, external review pending

Risk level: CRITICAL

#### Context

ADR-007 deliberately defines `HostObservationRecorder.offer()` as a
constant-time, nonblocking, no-I/O best-effort monitoring notification.
ADR-012 requires canonical v2 source membership to be durable before shadow
post-selection can affect that membership. The current path has no durable
host request/response transaction: `MilvusRangeServingExecutor.execute()`
performs a search and returns an in-memory `ServedQueryOutcome`;
`ReferenceRangeGateway.execute()` then constructs an observation and calls the
volatile recorder. Existing durable Stage-4 and response-profile ledgers record
experiment/canary lifecycle steps, not host-served requests. The ADR-006 trace
outbox persists only after background shadow capture and is therefore too late
to establish the genuine source population.

Writing a SQLite/file record in `offer()` would violate ADR-007. A volatile
queue before later durability leaves membership ambiguous when a process
crashes after a served response but before the worker persists it. Neither may
be treated as an ADR-012 v2 source boundary.

#### Decision

Introduce a future host-owned **response-commit boundary**, separate from
`HostObservationRecorder`, for explicitly configured v2 streams. The serving
application, not the generic recorder, owns this boundary. It atomically
commits a completed-response record and a v2 host-window outbox record before
the application makes that response externally complete/visible. The commit is
part of a host operation already responsible for response durability; it must
not be hidden as monitoring I/O under `offer()` or added to the generic
ADR-007 reference gateway.

The v2 source record contains the completed request's exact query/configuration
identity, outcome, stream binding, and a per-stream monotonic
`source_sequence` allocated by the same durable transaction. It derives:

```
window_sequence = source_sequence // 200
within_window_index = source_sequence % 200
```

The transaction writes an append-only, integrity-protected source record and a
separate pending v2 handoff record. A background v2 dispatcher verifies and
delivers only committed source records to the shadow worker. It is the sole
path by which a v2 observation can reach shadow processing. `offer()` may
continue as an optional volatile ADR-007/v1 notification, but may not create,
advance, acknowledge, or gate a v2 canonical source position. V1 remains
unchanged.

This is not an authorization to delay arbitrary responses for monitoring. A
host that lacks a mandatory durable response-commit operation cannot claim v2
membership and must use v1 best-effort monitoring only. Any future deployment
must separately demonstrate that its response-visible commit and v2 source
outbox share one atomic durability domain; a generic in-process gateway,
ordinary async queue, external best-effort logger, or timestamp reconciliation
does not satisfy this contract.

#### Crash semantics

| Boundary failure | v2 membership result |
|---|---|
| Before search dispatch | Definitely excluded; no completed response exists. |
| After dispatch, before a result | Definitely excluded; no completed response commit exists. |
| After result, before response-commit transaction | Definitely excluded; the response must not become externally complete. |
| During response-commit transaction | Definitely excluded unless the host can prove transaction commit; on uncertainty the host must fail the request/response rather than expose ambiguous v2 membership. |
| After committed response/source outbox, before `offer()` | Definitely included; dispatcher can recover it; `offer()` is irrelevant to v2. |
| After `offer()`, before shadow processing | Definitely included; v2 dispatcher reconstructs the committed pending record. |
| During shadow processing or after acknowledgement | Membership remains definitely included; only evaluation eligibility/delivery is pending or terminal under ADR-012. |

Thus no served response may be both externally complete and membership-ambiguous
for a v2 stream. If a host cannot enforce that rule, it is not a v2 host.

#### Relationship to ADR-007 and ADR-012

ADR-007 is preserved: `HostObservationRecorder.offer()` remains nonblocking,
no-I/O, and best effort; its queue-full/drop outcome does not affect serving or
v2 lineage. ADR-012 is preserved scientifically: source population membership
is durable before any shadow consumer may filter/evaluate it, and the durable
source ledger—not a trace/outbox sequence—defines canonical windows. The
durability point is the host response-commit transaction, not `offer()`.

#### Migration and verification after acceptance

Implementation would add a host-owned response-commit/outbox port and a
hardened v2 source ledger/dispatcher; bind source event/position/window digest
into v2 shadow artifacts/events, a v2 detector state/head/store path, and the
independent EXP-010 source adapter. `ReferenceRangeGateway`, ADR-007's
recorder, v1 worker, v1 traces, v1 detector heads, and historical evidence
remain unchanged. Required adversarial tests include commit-before-visible
ordering, commit ambiguity refusal, crash/restart at each table boundary,
outbox/source independence, no source consumption from `offer()`, and exact
v2 source-to-shadow-to-head-to-EXP-010 binding.

Human acceptance names `SQLiteHostResponseCommitStore` as the reference v2
host response-durability mechanism. This authorizes its offline structural
implementation and verification only. It does not claim an external production
host deployment and authorizes no live service, workload, search, grant,
routing, EXP-011, or canary action.

---

### ADR-014: Governed real-detector attestation, durable previous-window evidence, and production v2 host composition

Status: Accepted — human approved 2026-08-12; offline implementation in progress, external review pending
Date: 2026-08-12
Risk level: CRITICAL
Evidence status: none. This ADR authorizes no live service, workload, search, EXP-010 run, oracle generation, EXP-011 run, grant, routing, activation, actuation, or canary.

Problem:

ADR-012 records that `SQLiteHostWindowDetectorV2Store.process_window` accepts a
caller-supplied evaluator, so a `V2DetectorHead` proves source-window binding,
provenance binding, and durable restart-consistent progression — never that the
accepted ADR-002/ADR-003 statistical detector executed. Two further gaps block a
genuine EXP-010 trigger. First, ADR-002 does not classify drift from one window:
it requires two comparisons against one reference, so the *previous*
`WindowEvidence` needs durable, restart-safe, non-caller-injectable authority.
Second, a 200-query drift window is assembled from exactly four 50-query
`ShadowAuditTrace` envelopes, so four envelope digests exist per window, not two
hundred, and no per-query canonical digest is exported.

Decision:

1. `RealDetectorAttestation` is a private-construction value emitted only by the
   concrete `GovernedV2DetectorEvaluator`, and only as a by-product of calling
   the unchanged `shadow_extraction.extract_window_evidence` and
   `drift.evaluate_drift_decision`. No generic caller-supplied evaluator can
   issue one. MMD, KS, recall, Holm, audit sampling, and the two-window rule are
   reused unchanged and are never reimplemented.

2. The detector contract identity is derived mechanically by domain-separated
   SHA-256 over the governed constants `drift.py` actually uses:
   `PERMUTATION_COUNT`, `PERMUTATION_DENOMINATOR`, `FAMILY_WISE_ALPHA`,
   `ELIGIBLE_QUERY_COUNT`, `AUDIT_QUERY_COUNT`, `RESULT_LIMIT`, `SENTINEL_EF`,
   the per-signal effect floors, and the canonical signal order. A
   caller-supplied contract digest is never accepted.

3. Previous `WindowEvidence` is never a parameter. The governed evaluator
   fetches it from the attestation store and accepts it only when the stream,
   reference window sequence, reference source-window digest, and detector
   contract identity all match; the attested window is the immediate
   predecessor; the attested head is still the head the detector store's own
   reconstruction records for that window; and the persisted evidence decodes
   under the existing `monitor_evidence.decode_persisted_window_evidence`.
   `WindowEvidence` is persisted with the existing canonical, digest-bound
   `encode_persisted_window_evidence` codec; no second codec is introduced.

4. Because both reference identity and sequence adjacency are required,
   `WINDOW_UNEVALUABLE` and mandatory `REBASELINE` structurally invalidate all
   evidence from the previous reference epoch. Pre-gap evidence is excluded
   twice over — wrong reference source-window digest and non-adjacent sequence.

5. Per-position shadow binding uses only fields that already exist. For source
   position `i` in a 200-source window the position evidence binds
   `source_sequence`, `window_sequence`, `within_window_index`, `source_sha256`,
   the containing envelope's `expected_trace_sha256`,
   `trace_sequence_index = i // 50`, `within_trace_index = i % 50`, and
   `assembled.query_records[i].query_id`, with
   `assembled.query_records[i].query_id == committed_sources[i].query_id`
   required. The v2 shadow worker sets `AssembledShadowWindow.window_id` equal
   to the v2 `window_sequence`, collapsing the two window-identifier namespaces.
   No per-query envelope digest is invented.

6. Attestations persist in a separate append-only hardened store keyed by
   `detector_head_sha256`. The ADR-012 detector-store schema, database version,
   and detector-event schema are not modified. Heads persist first and
   attestations second; a head without a matching attestation is simply not
   real-eligible, so a crash between the two writes fails closed.

7. Admissible detector states for a persisted `V2DetectorHead`. A head may
   record exactly `DRIFT`, `NO_DRIFT`, or `INSUFFICIENT_EVIDENCE`, with
   mandatory classification consistency: `DRIFT` iff the classification is not
   `NONE`; `NO_DRIFT` and `INSUFFICIENT_EVIDENCE` require `NONE`. This requires
   no change to the ADR-012 detector-event schema, the store schema, or the
   durable-progression state machine, because `detector_state` is a canonical
   JSON string inside the event payload and not a typed column.

   Such a head is evidence-bearing but never trigger-bearing. It exists so the
   first evaluated comparison under a new reference — which ADR-002 necessarily
   classifies `INSUFFICIENT_EVIDENCE / MISSING_PREVIOUS_WINDOW` because no
   previous `WindowEvidence` exists under that reference — is durably recorded,
   restart-reconstructable, and usable as the attested previous evidence for the
   next adjacent comparison. The first evaluated window after every `REBASELINE`
   must have state `INSUFFICIENT_EVIDENCE`.

8. `VerifiedRealDetectorHead` is store-issued only when the detector store's own
   `load_verified_latest` yields a head and a matching attestation binds it
   exactly, inheriting the ADR-012 gap/rebaseline `latest = None` semantics
   unchanged. Genuine EXP-010 capture requires such a head with
   `detector_state == DRIFT`; a plain `V2DetectorHead` never qualifies, and no
   `real=True` caller assertion exists. Structural/fake-evaluator heads remain
   offline-only under ADR-012.

9. The production v2 host composition is a sibling of ADR-007's
   `ReferenceRangeGateway`, not a modification of it.
   `HostObservationRecorder.offer()` remains nonblocking, I/O-free, and best
   effort; no synchronous v2 durability is inserted into that path.

10. The composition root derives `data_identity` mechanically as
    `<dataset version>:sha256:<verified generation manifest digest>` from a
    `verify_dataset_artifacts`-verified DATASET-001 corpus, and refuses a
    caller-supplied literal. This keeps a captured population consumable by
    `response_profile_oracle_producer`.

Explicitly not authorized:

No qualification, policy, grant, routing, admission, activation, actuation, or
candidate authority is created. B-001 remains in force and `policy.py` is
unchanged. No live workload, search, EXP-010 run, oracle generation, EXP-011
run, or canary is authorized by this ADR.

Consequences:

A private construction token, not cryptography, separates real from structural
heads. This matches every other private-construction authority type in this
repository (`V2DetectorHead`, `LkgPhase3Authority`, `Stage4AdmissionReceipt`)
and must never be described as a signature: an actor with arbitrary in-process
code execution can import the token module. The v2 host places one synchronous
SQLite commit on the serving response path, which is an accepted ADR-013 cost
rather than a change to ADR-007. A DRIFT trigger requires at least two evaluable
windows under one reference, so an unevaluable window measurably delays EXP-010
eligibility; that delay is intended and fail-closed.

Relationship to ADR-012 and ADR-013:

Additive. ADR-012 already recorded that a real EXP-010 trigger would require a
separately governed real-detector attestation; this ADR defines it. The ADR-012
window-status state machine, gap/rebaseline semantics, schema, and database
version are unchanged, and ADR-013's response-commit boundary is untouched.
Neither is superseded or amended.

Verification plan:

Offline (performed with this acceptance): the complete forgery matrix including
forged, stale, non-adjacent, and wrong-reference previous evidence; all
position-substitution attacks; both progression state machines including
restart between comparisons; crash between head and attestation persistence;
attestation-store tamper, restart, and concurrent-writer refusal; DATASET-001
identity pinning refusal; and oracle-producer compatibility. Live, requiring
separate explicit operator authorization and not performed here: the first real
v2-host serving and the first genuine EXP-010 capture.

#### Implementation-discovered clarification — provenance digest domains (human approved 2026-08-12)

Implementing this ADR falsified one of its own premises, and the correction is
recorded here rather than by editing any earlier decision.

The committed `V2DetectorHead` construction contract required
`EvidenceProvenance.reference_manifest_sha256 == reference.source_window_sha256`
and the equivalent current-window equality. That is an **invalid cross-domain
equality**. The unchanged real detector defines
`EvidenceProvenance.*_manifest_sha256` as the
`AssembledShadowWindow.manifest_sha256`, whereas `source_window_sha256` is the
canonical committed-source membership digest. They are digests over different
canonical objects and can never be equal, so the original requirement was
satisfiable only by an evaluator that fabricated provenance echoing the source
digest — that is, only by a structural fake. The real governed detector could
not produce an acceptable head at all.

1. `EvidenceProvenance.*_manifest_sha256` and V2 `source_window_sha256` are
   distinct digest domains, alongside a third: `shadow_window_sha256`.
2. `V2DetectorHead` must not assert equality between those domains. Exactly the
   two invalid comparisons are removed from head construction and from the head
   document check; nothing else in either check is relaxed.
3. ADR-012's durable source/shadow progression is unchanged. The head still
   binds `reference_source_window_sha256`, `current_source_window_sha256`, and
   `current_shadow_window_sha256` in its own fields and its canonical digest,
   and the provenance value itself remains inside the head document and digest,
   so it stays tamper-evident.
4. Real statistical provenance authority belongs to ADR-014's
   `RealDetectorAttestation`, not to the ADR-012 head.
5. The attestation binds all three domains rather than conflating any of them,
   and `GovernedV2DetectorEvaluator` explicitly asserts
   `provenance.reference_manifest_sha256 == reference AssembledShadowWindow.manifest_sha256`
   and the current-window equivalent, in the correct domain. The 4x50 -> 200
   position conservation is what bridges committed source membership to that
   assembled shadow evidence, so no duplicate identity field is introduced.
6. This clarification creates no policy, candidate, grant, routing, admission,
   activation, actuation, or canary authority. B-001 is unchanged.
7. No `V2ShadowWindow` schema or canonical-digest change, no detector database
   migration, and no rewrite of historical v1 evidence is required. A plain
   `V2DetectorHead` remains structurally insufficient for real EXP-010: only a
   `VerifiedRealDetectorHead` with `detector_state == DRIFT` is trigger
   eligible.

#### Clarification — governed v2 serving-configuration identity and durable query-id uniqueness (operator-directed 2026-08-13)

Preparing the real application ingress exposed two gaps. Both are recorded here
rather than by editing any earlier decision.

Approval status, stated precisely: on 2026-08-13 the operator was presented with
the blocker and a set of options, and directed that the identity be **derived**
(rather than declared as an opaque operator literal) and that request-id
uniqueness be made **durable** (rather than held in memory). That direction is
what is approved here. The specific field set, schema strings, and digests below
are the implementation of that direction and have not separately been through a
line-by-line human sign-off.

**A. `configuration_identity` in v2.** The only derivation in the repository was
`exp005_acquisition._derived_identities`, whose schema
`exp005-shadow-configuration-v1` binds `candidate_ef` and
`last_known_good_ef` — candidate/canary concepts absent from a v2 serving path,
which serves exactly one `served_ef`. Reusing it would fabricate candidate state
or silently redefine an accepted schema, and leaving the value an opaque
operator literal would bind no configuration facts at all.

1. `configuration_identity` for a v2 host is now derived by
   `exp010_serving_configuration.derive_serving_configuration_identity`, as
   `exp010-serving-config-v1:sha256:<64 hex>` over a domain-separated SHA-256 of
   the canonical payload, matching the repository's existing
   `<name>-v1:sha256:<hex>` convention.
2. It binds **serving/query semantics only**, under schema
   `exp010-serving-configuration-v1`: `metric`, `threshold_stratum`,
   `threshold_radius`, `range_filter`, `limit`, `served_ef`, `dimensions`, and
   `consistency_level`. The field set is closed; missing, unknown, bool-as-int,
   non-finite, off-ladder, and non-governed values all fail closed, and key
   order cannot affect the digest.
3. It deliberately **excludes** `data_identity` and the dataset manifest digest,
   `flat_binding_id`/`hnsw_binding_id`, `environment_manifest_sha256`,
   `deployment_identity`, `source_revision`, `stream_id`, `detector_seed`, and
   `observed_at_utc`, because each belongs to a separate authority domain.
   Collapsing them would let one digest stand in for evidence it does not cover.
4. It also excludes `sentinel_ef`, which is already governed twice elsewhere: by
   `real_detector_attestation.detector_contract_identity` and by every
   `ShadowAuditTrace` (and hence the assembled manifest and provenance). It is
   not duplicated into the serving domain.
5. EXP-005's configuration identity and all historical evidence are unchanged
   and are never retrospectively rewritten.

**B. Durable query-id uniqueness.** The v2 source ledger derived `event_id` from
`{stream, source_sequence, query_id, committed_at_utc}`, so a repeated
application request id produced a *different* `event_id` and was accepted, while
`build_calibration_population_manifest` rejects duplicate `query_id_sha256`.
A duplicate admitted at serve time therefore became a latent EXP-010 capture
failure up to 1,400 observations later.

6. `source_records` now carries `query_id_sha256 TEXT NOT NULL UNIQUE`, making
   canonical query-id uniqueness a durable, transactional invariant of source
   membership itself rather than a mutable dedup side table. The check runs
   inside the same `BEGIN IMMEDIATE` transaction as the source commit, so it is
   concurrency-safe and restart-safe, and a duplicate rolls the transaction back
   without consuming a `source_sequence` — contiguity is preserved.
7. A duplicate fails closed with the stable code
   `HOST_SOURCE_QUERY_ID_DUPLICATE` and is never silently remapped to a fresh
   id. Append-only triggers and every other v2 store property are unchanged.
8. The relational column is a uniqueness *mechanism* and never an authority.
   The canonical source record remains the sole authority, so every
   verification — including every reopen — requires
   `source_records.query_id_sha256` to equal the value reconstructed from
   `source_json` (which `source_sha256` covers) and carried on
   `CommittedHostObservation`. A record whose column was altered independently
   fails closed with `HOST_SOURCE_CHAIN_INVALID`, so the column cannot become an
   unverified side-channel. Source digest verification is unchanged and
   unweakened.
9. This requires source-store schema version **2**. Compatibility rule for older
   stores: a version-1 database is **rejected** with
   `HOST_SOURCE_SCHEMA_INVALID` on open. It is never migrated, rewritten,
   truncated, or altered — the rejection happens before any DDL or DML, and the
   file is left byte-identical, so it remains available to a future dedicated
   reader. Rejection is a refusal to *append* under v2 semantics, not a
   retrospective rewrite of evidence. As of this entry no version-1 (or any
   other) `SQLiteHostResponseCommitStore` database exists in the repository or
   in runtime evidence — no such store has ever been persisted outside
   temporary test directories — so no historical evidence is affected. Because
   nothing would be served by it today, no in-process v1 read path is
   implemented: adding one would duplicate the exact-set schema check and the
   source-loading path, which are the two most safety-critical parts of the
   store. The ADR-012 detector store, the attestation store, and the ADR-007 v1
   host path are untouched.

Neither clarification creates policy, admission, grant, routing, activation,
actuation, or candidate authority, and B-001 is unchanged.

---

### ADR-015: Exact-once durable shadow attempts and binary32 precision-tie ordering

Status: Accepted — operator-authorized 2026-08-14; implementation pending external review
Date: 2026-08-14
Risk level: CRITICAL
Evidence status: no new live evidence. V2 and V3 remain immutable failed/partial
historical campaigns under their original source revisions. This ADR authorizes
offline implementation and tests only; it does not authorize V4, a Milvus
search, workload replay, detector execution, EXP-010, a grant, routing,
activation, actuation, or canary traffic.

#### Context

The pre-ADR-015 `V2ShadowWorker` held four physical 50-query shadow results only
in process memory. A crash after search execution but before window assembly
could not distinguish an unexecuted attempt from an executed attempt whose
result was lost. A returned incomplete trace was also not durably preserved
before failure, and later execution could begin before each preceding result
had an independently durable terminal record. Separately, the capture path
required exact ordered FLAT/oracle IDs while reconstructive validation accepted
global set equality. The latter was too permissive in general and too strict in
the observed binary64-to-binary32 precision-tie case.

#### Options considered

1. Keep per-window in-memory capture and rerun after interruption. Rejected:
   physical execution is ambiguous and a replay can duplicate searches.
2. Persist only returned traces. Rejected: a crash after dispatch and before
   persistence still looks unexecuted.
3. Append an immutable STARTED event before each physical trace, append one
   terminal event immediately afterward, and centralize ordered comparison.
   Chosen: it is the smallest deterministic design that makes physical outcome
   ambiguity explicit, preserves evidence, and supports recovery without replay.

#### Decision — governed physical-attempt identity and lifecycle

Each 50-source attempt has one domain-separated canonical identity binding the
complete `MonitorStreamKey` (including configuration, data, and index
identities), source revision, environment-manifest SHA-256, window sequence,
trace sequence index, and exact ordered source sequences, source-record
SHA-256s, and canonical query-ID SHA-256s. A slot is the pair
`(window_sequence, trace_sequence_index)`; one slot can bind only one attempt
identity.

The prospective append-only SQLite journal permits exactly:

```
UNSEEN -> STARTED -> COMPLETED
UNSEEN -> STARTED -> FAILED
```

`STARTED` commits with `synchronous=FULL` before the capture executor is called.
A returned trace is encoded by the existing canonical persisted-shadow-envelope
codec and its detached trace SHA-256. Before another trace can execute, the
journal atomically appends either:

- `COMPLETED` for a canonical, source-matched trace whose `complete` field is
  true and whose one-trace validation has no failure; or
- `FAILED` with the returned canonical trace when available, its trace digest,
  its exact ordered `trace.reason_codes`, and a separate stable failure/error
  classification. An exception that returned no trace records only terminal
  failure metadata; it never fabricates a `ShadowAuditTrace`.

No update, delete, overwrite, duplicate STARTED, second terminal, or terminal
conversion is supported. `FAILED` remains historical failure evidence and is
never replayable, completable, detector-admissible, or silently discarded.
Any first per-trace failure stops the 200-source window immediately; no later
physical trace executes.

#### Orphan and restart semantics

A durable `STARTED` with no terminal event on reconstruction is classified as
`ORPHANED / EXECUTION_OUTCOME_UNKNOWN`. It is treated as physically ambiguous,
not unexecuted: automatic retry, detector admission, and continuation of that
window are forbidden. A fresh governed campaign/window is required.

A `COMPLETED` trace may be loaded after restart, but only after full schema,
store binding, event-chain, attempt identity, canonical envelope, and detached
digest verification. The worker reuses that persisted evidence and issues no
physical searches for the completed attempt. Assembly starts only after all
four exact slots load as verified `COMPLETED`; it consumes the reloaded values,
not transient executor returns. This is evidence recovery, never replay.

#### Decision — canonical FLAT/oracle ordered agreement

One comparator is shared by physical capture and reconstructive shadow-window
validation. It returns one of: exact ordered agreement, precision-tie-equivalent
agreement, membership mismatch, non-tie ordering mismatch, or invalid
score/evidence.

The independent oracle's binary64 score is converted to the exact IEEE-754
binary32 value by round-to-nearest conversion equivalent to little-endian
`struct.pack("<f", score)` followed by `struct.unpack("<f", ...)`; non-finite
or overflowed values fail. Positive and negative zero form one canonical group.
Each oracle ID inherits that canonical binary32 score-group identity. FLAT must
return exactly the oracle-selected capped ID membership, with distinct IDs,
finite scores, and metric-specific threshold validity. Its ordered sequence of
oracle score groups must equal the oracle's ordered sequence. Therefore exact
order passes, and permutations pass only within positions whose independent
oracle scores collapse to the same canonical binary32 group. Any permutation
across distinguishable groups fails. No epsilon or caller-selected tolerance
defines a tie, and global set equality alone is never sufficient. The existing
governed `NUMERIC_TOLERANCE = 1e-6` remains applicable only to metric-threshold
validity in both capture and reconstruction; it never creates a score-tie
group.

Binary32 oracle grouping governs only permitted ID-order equivalence. The raw
finite FLAT score sequence must independently remain metric ordered: monotonically
nondecreasing for L2 and monotonically nonincreasing for COSINE. Binary32 grouping
must never conceal a raw FLAT score inversion.

At the result-limit boundary, **no tied-member substitution is allowed**. The
exact oracle-selected capped membership remains mandatory even if an unreturned
candidate would share a binary32 tie with the final returned member. The
evidence available to this contract cannot prove interchangeable membership
beyond the capped oracle payload; allowing it would weaken the contract into
unverifiable set substitution.

#### 2026-08-21 accepted numerical-model amendment — L2 execution ties

The original oracle-final-cast rule above remains historical and continues to
classify `PRECISION_TIE_EQUIVALENT`. It is incomplete for Milvus L2 execution:
the independent oracle subtracts and accumulates in binary64 before its final
binary32 cast, whereas the governed Milvus/Knowhere/Faiss ARM path performs
binary32 component subtraction and binary32 product/FMA accumulation followed
by a binary32 reduction. The locally inspected `milvusdb/milvus:v3.0.0`
`libknowhere.so` exposes the Faiss `fvec_L2sqr_neon`, `fvec_L2sqr_sve`, and
reference L2 kernels; disassembly of the active-platform NEON kernel shows
`fsub`, `fmul`/`fmla`, block accumulation, and final pairwise `faddp`. A direct
NEON-order emulation from the frozen source-475 operands reproduces both live
returned score bit patterns exactly. Legal binary32 reduction order can
therefore collapse or reverse two distances whose binary64-oracle final casts
remain distinct.

For the governed 128-dimensional L2 contract only, the comparator adds the
distinct `EXECUTION_TIE_EQUIVALENT` outcome. Let `n = 128`, binary32 unit
roundoff `u32 = 2^-24`, binary64 unit roundoff `u64 = 2^-53`, and binary32
minimum normal `q32 = 2^-126`. The deliberately larger underflow constant also
covers a flush-to-zero implementation without relying on an unverified FPCR
mode. For a finite non-negative binary64 oracle score `O`, define:

```
g64 = ((n + 2) * u64) / (1 - ((n + 2) * u64))
S_lower = O / (1 + g64)
S_upper = O / (1 - g64)
m = n + 2
A = q32 * sum((1 + u32)^k for k in 0..m-1)
L = nextDown(max(0, S_lower * (1 - u32)^m - A))
U = nextUp(S_upper * (1 + u32)^m + A)
```

`g64` conservatively encloses the binary64 oracle subtraction, square, and
reduction around the exact real squared-L2 sum. The two binary32 subtraction
factors and at most `n` product/FMA/reduction roundings on any term's path give
`m`; an FMA has no greater error than this deliberately conservative separate
product/reduction model. `A` encloses both gradual-underflow and flush-to-zero
loss.
The outward binary64 `nextDown`/`nextUp` operations prevent host evaluation
from narrowing the mathematical interval; the implementation evaluates the
formula with exact rational arithmetic before outward binary64 conversion.
This is an analytical IEEE-754 bound, not an epsilon, ULP allowance, or value
calibrated from source 475.

A cross-oracle-group permutation passes this new rule only when all of the
following hold simultaneously:

1. the existing exact capped membership, threshold validity, distinct-ID, and
   raw metric-order requirements already pass;
2. every changed position belongs to one **contiguous** returned FLAT block
   whose scores are exact binary32 values with the same canonical IEEE-754 bit
   pattern (binary64 values that merely round into that pattern do not qualify);
3. that block's IDs occupy exactly the same contiguous oracle-rank interval;
4. the common returned binary32 value lies in `[L, U]` independently for every
   oracle member of the block; and
5. the metric is L2 and the supplied, reconstructively verified query
   dimensionality is exactly 128.

No sorting, global-set-only agreement, capped-member substitution, arbitrary
tolerance, or noncontiguous movement is permitted. Exact ordered and original
same-oracle-binary32-group outcomes retain their original classifications.
Different returned score bit patterns, a score outside any member's analytical
interval, malformed dimensions/evidence, and distinguishable inversions remain
fail-closed. COSINE and every unsupported dimensionality retain the original
ADR-015 semantics; this amendment makes no COSINE numerical claim.

The failed `exp012-scale2400-v1` campaign remains immutable `FAILED_CLOSED`
under source revision `810c569cb712296169a0bfe6c4dfd3d40aece0cf`. Offline
reclassification under this amendment is forensic analysis only and cannot
repair, retry, complete, or reinterpret that historical run. Any scientific
scale claim requires a fresh campaign under the amended source authority.

#### Store and failure hardening

The store uses private owner-controlled paths, mode `0600`, regular/single-link
checks, a sidecar exclusive process-lifetime lock, STRICT tables, exact schema
verification, `journal_mode=DELETE`, `synchronous=FULL`,
`trusted_schema=OFF`, `BEGIN IMMEDIATE`, append-only triggers, canonical JSON,
and a domain-separated event hash chain. Binding, schema, trace, chain, slot,
or transition corruption fails closed. Store hashes and private constructors
are integrity/API mechanisms, not signatures and not defenses against a
hostile same-user raw filesystem writer.

The sidecar lock is owned by the PID that opened the store. A forked child may
close its inherited local descriptor, but must never issue `LOCK_UN` against
the shared open-file description or remove the parent's in-process ownership
record. Every normal operation in a different PID fails closed. The owning
parent alone performs explicit unlock and ownership cleanup.

Each successful `STARTED` append returns one opaque, live
`ShadowAttemptPermit`. The permit is bound to the issuing store instance,
owner PID, and exact attempt digest; it is one-shot and is required for either
terminal transition. It is never persisted and cannot be reconstructed on
restart. Possession of an attempt identity, envelope, or inherited Python
object is insufficient. A restarted `STARTED` attempt therefore remains an
orphan and cannot be terminalized or retried.

#### Decision — durable cross-store window finalization

Detector events, real-detector attestations, source acknowledgements, and
shadow attempts remain separate authoritative stores. They do not share, and
this ADR does not simulate, a cross-SQLite transaction. Instead, the
prospective v2 runner uses a private append-only finalization journal whose
sole purpose is crash reconciliation. Before detector persistence, it records
one immutable `PREPARED` identity binding the complete stream identity, source
revision, environment-manifest digest, window sequence, exact source/shadow/
assembled-window digests, all 200 ordered source sequence/event/source/query
identities, the four completed attempt and trace identities, expected detector
status, and—only for an evaluated window—the canonical deterministic detector
decision and the minimum canonical pending evidence required to construct the
corresponding attestation.

The allowed per-window phase history is:

```
PREPARED
  -> DETECTOR_COMMITTED
  -> ATTESTATION_COMMITTED | ATTESTATION_NOT_REQUIRED
  -> SOURCE_ACKNOWLEDGED
  -> FINALIZED
```

Transitions are immutable, canonical, hash-chained, contiguous by window, and
reconstructively validated. The coordinator's claims are never sufficient by
themselves: every reconciliation step verifies the exact artifact in its
owning detector, attestation, source, or attempt store. A contradiction fails
closed; no store is overwritten and no history is skipped.

On startup and before a new window, reconciliation resumes the sole pending
window. `PREPARED` with no detector event invokes the governed detector path
once. If the exact detector event already exists, it is loaded and verified
without replaying `process_window`. A required missing attestation is rebuilt
from the persisted canonical pending evidence; an existing attestation is
verified and reused. A detector status without a head records
`ATTESTATION_NOT_REQUIRED` explicitly. Source acknowledgement occurs only
after the required detector and attestation artifacts verify. If the exact
contiguous acknowledgement already exists, it is reused rather than appended
again. `FINALIZED` is recorded only after the acknowledgement head and full
prefix match.

The next source window is derived from finalized/pending durable history, not
from a zero-initialized RAM cursor. Detector reference progression is loaded
from verified detector history; the reference bundle is reconstructed from
the exact committed source records and four persisted `COMPLETED` traces.
Reference recovery never executes a physical shadow search. Crash tests must
cover every boundary before and after detector, attestation,
acknowledgement, and finalization persistence, and prove exactly-once durable
artifacts, no source gap, and no repeated physical capture.

#### Consequences and compatibility

The worker performs two small durable commits per newly executed 50-query
trace; this latency is accepted because correctness and exact-once ambiguity
closure outrank Gate-C throughput. The implementation is prospective and adds
an attempt database and a reconciliation-only window-finalization database to
the v2 composition root. It never opens, migrates, repairs, imports, or rewrites
V1/V2/V3 runtime evidence. Existing trace payload,
assembled-window, detector, attestation, policy, grant, route, and actuation
schemas remain unchanged. The comparator tightens reconstructive set-only
acceptance while permitting only the reproducible precision-tie case; no new
candidate-capable authority is created.

#### Verification requirements

Offline tests must cover incomplete first/middle traces, exact reason
preservation, executor exception, orphan restart refusal, completed restart
reuse, four-trace durable assembly, trace/binding/schema/transition tamper,
fork-child close and inherited-permit refusal, cross-thread permit possession,
exact and tie-equivalent ordering, non-tie reordering, the observed source-372
precision shape, threshold and membership failures, capped-limit tie refusal,
all cross-store crash points, contradictory durable-artifact refusal, reference
reconstruction, durable cursor reconstruction, and proof that failed/orphaned
traces execute no later physical trace and never reach detector admission. A
complete repository suite is required before review, with no live service or
historical-evidence mutation.

### ADR-016: Committed Gate-C operator boundary, V4 retirement, and V5 preconditions

Status: Accepted — operator-authorized 2026-08-16
Date: 2026-08-16
Risk level: HIGH
Evidence status: no new live evidence. No Milvus search, workload replay,
detector execution, EXP-010 capture, grant, routing, activation, actuation, or
canary traffic is authorized by this ADR. V1/V2/V3/V4 remain immutable.

#### Context

`Exp010LiveRunner.process_ready_windows()` has always been the canonical Gate-C
call, but no committed code constructed the production composition around it.
Every historical campaign was therefore driven by an ad-hoc, unpersisted
composition, and a later preflight could not prove which path had actually run.
The V4 Gate-C preflight of 2026-08-15 halted on exactly this: the canonical path
could not be recovered from the repository.

Separately, that same preflight found the live stack no longer continuous with
Gate A #4 — `milvus-standalone` had exited (code 80) after an etcd session-lease
expiry following host sleep. Because `build_environment_manifest_sha256` binds
`observed_at_utc` and the live index identities, V4's stores are permanently
pinned to a digest whose continuity no longer holds.

#### Decision

**1. Gate B and Gate C are separate boundaries.** Gate B genuine ingress is
`vdbench.exp010_ingress.Exp010RequestIngress.admit` onto
`Exp010LiveRunner.serve`; a request is genuine solely because an external
application supplied it. Gate C never serves, never generates a query, and never
replays one.

**2. The canonical Gate-C entrypoint is `vdbench.exp010_gate_c_operator`.** It is
the only committed operator path to `process_ready_windows()`. It composes
`Exp010V2HostComposition`, `V2ShadowWorker`, `V2MilvusShadowCaptureExecutor`,
the durable shadow-attempt store, and the window-finalization machinery. The
historical `CaptureObserver` is not used.

**3. Preflight mode (`--mode preflight`)** validates the exact closed 23-key
operand set (a missing or unexpected key is refused, never defaulted),
re-derives `configuration_identity` from the serving operands rather than
trusting it, opens the already-initialized stores so their durable bindings are
verified, prints the resolved plan with a `plan_sha256`, and issues **zero**
physical searches and **zero** `serve()`/genuine requests. It injects executors
that raise on any call, so reaching Milvus is structurally impossible, and it
constructs no Milvus client at all. It refuses an uninitialized campaign: Gate C
advances a campaign, it never brings one into being.

**4. Execute mode requires a second explicit operator action** distinct from
choosing the mode: `--mode execute --confirm-physical-shadow-searches`. Execute
re-runs the entire preflight first, so a configuration, identity, or store
binding mismatch always fails with zero physical capture.

**5. Gate C advances only already-durable source windows.** Only whole
200-source windows already committed by Gate B are processed, in order. An
incomplete tail stays pending and is never acknowledged.

**6. Ambiguous STARTED attempts are non-retriable.** A durable STARTED without a
terminal record is `ORPHANED` / `EXECUTION_OUTCOME_UNKNOWN`. The entrypoint
contains no retry loop, no replay path, and no exception handler over the
canonical call: the reason code propagates verbatim to the operator.

**7. Gate C does not claim crash-safe physical exactly-once execution.** The
contract is: durably STARTED, then physical execution, then exactly one durable
terminal record (COMPLETED or FAILED). A STARTED attempt left without a terminal
record after interruption is execution-ambiguous and must never be replayed.

**8. V4 is retired from further live progression.** Its authoritative historical
state is Gate A #4 PASS, Gate B PASS, 600 genuine source records, and Gate C
NEVER STARTED — 0 physical shadow searches, 0 attempts, 0 acknowledgements, 0
detector events, 0 attestations, 0 finalization events. Environment continuity
later broke when Milvus exited. There is therefore **no** V4 environment
exception, **no** rebinding, **no** repair, **no** replay, and **no** V4 Gate C.
V4 evidence is not modified to encode this decision; this ADR records it.

**9. V5 requires a fresh Gate A and a fresh Gate B.** A new campaign needs a
newly observed environment manifest digest from a fresh Gate A #5 and its own
genuine Gate-B source ingest. A stale environment digest may not be carried
forward, because `observed_at_utc` and the live index identities are bound into
it precisely so that continuity cannot be silently asserted.

**10. The governed FLAT/oracle comparator contract** (see
`flat_oracle_agreement`) is six rules: exact capped membership; threshold
validity; raw returned FLAT metric ordering; distinguishable oracle score-group
order; permutation legal only inside one exact IEEE-754 binary32 oracle-score
tie group; and no capped-membership substitution. `NUMERIC_TOLERANCE` (1e-6) is
threshold-only — it is never applied to a FLAT-score-versus-oracle-score
comparison, and no direct score-magnitude equality contract exists.

**11. Canonical serialization is versioned.** `artifacts.canonical_json_bytes`
is the **frozen v1** historical authority: every registered artifact digest and
every V1-V4 campaign store digest is a SHA-256 over its exact bytes, so it must
never change — including its known, deliberately retained `allow_nan=True`
weakness. New governed schemas use the **strict v2** contract in
`canonical_serialization`: deterministic key order, UTF-8 without ASCII
escaping, non-finite floats refused, exactly one trailing newline, an exact
permitted value-type set, mandatory digest domain separation, and a decoder that
refuses duplicate keys and any non-canonical input.

#### Command contract

Preflight — issues zero physical searches:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_c_operator \
--operands /path/to/gate_c_operands.json \
--mode preflight
```

Execute — issues real FLAT and sentinel-`ef` searches:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_c_operator \
--operands /path/to/gate_c_operands.json \
--mode execute \
--confirm-physical-shadow-searches
```

The operand file's exact closed key set is `exp010_gate_c_operator.
OPERAND_FIELDS`. No V5 operand file exists yet, and none may be authored until a
fresh Gate A #5 has produced the environment manifest digest it must bind to.

#### Consequences

The canonical Gate-C path is now provable from the repository rather than
reconstructed from a session. An operator can inspect the resolved plan, and its
digest, before authorizing any physical work. No existing trace, assembled
window, detector, attestation, policy, grant, route, or actuation schema changes,
and no new candidate-capable authority is created.

#### Verification requirements

Offline tests must prove: preflight performs zero searches and zero serves; the
live command reaches `process_ready_windows()` exactly once; no `serve()` is
issued by Gate C; a configuration, revision, environment, or stream mismatch
fails before physical capture; the detector seed is explicit and has no default;
an ambiguous STARTED propagates rather than being retried; and the entrypoint
cannot replay source workload. A complete repository suite is required, with no
live service or historical-evidence mutation.

### ADR-017: Committed Gate-A operator, V5 campaign initialization, and environment-observation authority

Status: Accepted
Date: 2026-08-16
Risk level: HIGH
Evidence status: no new live evidence. This ADR authorizes no Milvus search, no
`serve()`, no Gate-B ingest, no Gate-C capture, and no V5 creation by itself.
V1/V2/V3/V4 remain immutable.

#### Context

ADR-016 item 9 requires a fresh Gate A #5 before any V5 campaign, but no
committed code performed one. The only Gate-A artifact was
`exp010_live_runner.build_environment_manifest_sha256`, a pure offline helper
with zero production callers: it hashes an observation an operator has *already*
made, and defines neither how that observation is obtained nor where the result
is persisted. Three gaps followed, and the 2026-08-16 Gate A #5 preflight halted
on them.

**First**, no operator existed. Gate C is provable from the repository; Gate A
was not.

**Second**, `deployment_identity` is one of the thirteen mandatory
`_ENVIRONMENT_FIELDS`, yet it had no authority domain anywhere. It appears in no
V4 durable store binding — the bound stream key is exactly seven fields and
`deployment_identity` is not among them — and its only committed occurrences are
mutually inconsistent test placeholders (`"deployment"`, `"offline-deployment"`,
`"env-001"`, `"offline-v2-engine"`). Its only validation is a non-empty string
check. `ARCHITECTURE.md` mentions it once, to *exclude* it from the serving
domain as belonging to "a separate authority domain" that was never defined.

**Third**, V5 campaign creation was unowned. Gate C refuses an uninitialized
campaign by design, so some earlier boundary must create one, and none did.

Separately, the preflight established by mechanical proof — not by campaign-name
inference — which values are stable project identities. Re-deriving
`derive_serving_configuration_identity` from candidate operands reproduces V4's
durably bound `configuration_identity`
(`exp010-serving-config-v1:sha256:825931cd…b366a9`) exactly, which is a SHA-256
preimage proof of eight serving operands at once. Re-deriving the DATASET-001
corpus identity from `artifacts/exp-001/dataset` reproduces V4's bound
`data_identity` (`DATASET-001-v1:sha256:b6cb56a3…51da9`) exactly.
`v2_milvus_shadow_capture` proves `flat_index_identity` and `hnsw_index_identity`
*are* `flat_binding_id` and `hnsw_binding_id`.

#### Decision

**1. What Gate A proves.** Gate A proves exactly one thing: that at a stated
instant, a live ENV-001 stack was observed to match a governed environment
description, and that this observation was reduced to one canonical digest. It
proves nothing about workload, sources, detectors, or drift. It is an
*observation* boundary, never a serving or capture boundary.

**2. Four field authority classes.** Every Gate-A field belongs to exactly one:

*Stable project identity* — a label for a governed object whose lifetime spans
campaigns. Reusable only because it is proven from committed authority:
`flat_binding_id`/`flat_index_identity`, `hnsw_binding_id`/`hnsw_index_identity`,
`threshold_stratum`, `threshold_radius`, `range_filter`, `limit`, `served_ef`,
`dimensions`, `consistency_level`, `metric`.

*Governed operator input* — supplied explicitly, never defaulted, never
inferred: `deployment_identity`, `stream_id`, `campaign_root`, `milvus_uri`,
`flat_collection_name`, `hnsw_collection_name`, the three container names,
`source_revision`, `expected_row_count`, `hnsw_m`, `hnsw_ef_construction`,
`dataset001_dir`.

*Freshly observed* — must come from the current container lifetime and may never
be carried forward: `observed_at_utc`, live FLAT/HNSW collection and index
metadata, and the Docker container lifetime identities.

*Derived* — computed, never accepted from the operator: `data_identity` (from
the verified DATASET-001 corpus), `configuration_identity` (re-derived from the
serving operands and required to match), `environment_manifest_sha256`, and the
evidence digest.

**3. `deployment_identity` is a governed operator input with no default.** No
committed authority assigns it a value, and this ADR deliberately does not
invent one. The operator refuses to run without it and never substitutes a
default, a campaign-name-derived string, or a historical placeholder. Choosing
its value is an operator act recorded in the evidence, not a code decision.

**4. The canonical Gate-A entrypoint is `vdbench.exp010_gate_a_operator`,** with
the same two-mode discipline as `exp010_gate_c_operator`. `--mode preflight`
validates the closed operand set, verifies the frozen source revision, performs
read-only Milvus metadata inspection, observes container lifetimes, re-derives
every derived field, builds the prospective observation and its manifest digest,
and prints the resolved plan. It creates nothing. `--mode execute` re-runs that
entire preflight and then, only after the separate explicit flag
`--confirm-initialize-v5`, initializes the campaign.

**5. Gate A owns V5 creation, exactly once.** Gate A is the only boundary that
brings a campaign into being. It creates the campaign root and the Gate-A
evidence beneath it, and nothing else — it never creates the five Gate-C stores,
which remain Gate B's to initialize through genuine ingest.

**6. Evidence format and location.** The campaign root is
`~/.local/share/vd/exp010/live-l2-target075-v5`, consistent with existing roots.
Gate-A evidence is one immutable file at `<campaign_root>/gate_a/
gate_a_environment_manifest.json`, serialized with the existing strict v2
contract (`canonical_serialization.strict_canonical_json_bytes`). No second
digest or serialization implementation is introduced: the environment digest is
`build_environment_manifest_sha256` unchanged, and the evidence digest is
`strict_canonical_digest` under a new domain constant.

**7. Exclusivity is decided by the creating syscall, not by an earlier check.**
`os.rename` has *replace* semantics: renaming onto an existing non-empty
directory fails with `ENOTEMPTY`, but renaming onto an existing **empty** one
silently removes and replaces it. Any publication design whose exclusivity rests
on a prior `exists()` check therefore carries a real TOCTOU window — a directory
appearing between the check and the rename would be destroyed. That is a genuine
integrity defect, not a documentation nuance.

Gate A closes the window with `os.mkdir`, which is unconditionally exclusive: it
fails with `EEXIST` against an empty directory, a non-empty directory, a regular
file, a symlink, and a dangling symlink alike. No platform-specific no-replace
rename (`renameat2` / `renamex_np`) and no `ctypes` binding is required, so the
guarantee costs no portability. The protocol is:

1. verify the parent is a safe, owned, non-world-writable directory;
2. `os.mkdir(root, 0o700)` — **exclusive reservation of the campaign root**;
3. write the incompleteness marker inside the reserved root, fsync;
4. `os.mkdir(root/gate_a, 0o700)` — **exclusive creation of the evidence
   directory**;
5. write the manifest to a temporary name inside `gate_a/`, fsync, then
   `os.link` it onto the canonical name and unlink the temporary;
6. unlink the marker, fsync root — **the commit point**.

Every step that creates a name uses a primitive that refuses to replace an
existing one, so nothing already present is destroyed anywhere in the tree.

**Same-UID threat model — mode bits are not a boundary.** `0o700` on the
reserved root excludes other *users*, but grants full rights to any process
sharing this UID. Directory permissions therefore provide no protection against
a hostile or buggy same-UID process, and this protocol does not rely on them.
Steps 2, 4, and 5 use `os.mkdir` and `os.link`, both unconditionally exclusive —
`EEXIST` against an empty directory, a non-empty directory, a regular file, a
symlink, and a dangling symlink alike, and `os.link` never writes through a
symlink. A same-UID process planting any of those at `root`, `root/gate_a`, or
the manifest path inside the window is refused with
`GATE_A_EVIDENCE_PATH_OCCUPIED` rather than silently replaced.

**The limit of that claim, stated so it is not over-read.** What Gate A
guarantees is that *its own implementation* and *ordinary competing
initializers* can never silently replace or rebind a path, and that it never
destroys content it did not create. It does **not** claim protection against a
malicious or arbitrary same-UID process that intentionally mutates evidence
after it has been created — such a process can delete the manifest, or remove
the incompleteness marker to make an interrupted campaign read as `COMPLETE`.
No userspace protocol can prevent that, because same-UID write access is total;
defending against it requires OS-level immutability or a separate privilege
domain, both outside this ADR's scope.

`gate_a/` consequently becomes visible while the campaign is still marked
INCOMPLETE. That is deliberate and safe: the marker is the sole transition to
COMPLETE, so a visible-but-uncommitted evidence directory is never a Gate-A
PASS. `os.link` additionally makes the manifest appear at its canonical name
exactly once and only fully written, so a torn manifest is never observable
there.

**8. Four campaign states, one initializable.** `inspect_campaign_state` is the
single authority on what a root is:

| state | meaning | initialize | Gate-A PASS |
|---|---|---|---|
| `ABSENT` | nothing at the path | yes | — |
| `INCOMPLETE` | reserved, marker present | refused (`GATE_A_CAMPAIGN_INCOMPLETE`) | **never** |
| `COMPLETE` | marker gone, evidence present | refused (`GATE_A_CAMPAIGN_ALREADY_INITIALIZED`) | yes |
| `FOREIGN` | anything else at the path | refused (`GATE_A_CAMPAIGN_PATH_OCCUPIED`) | **never** |

The classification is conservative: anything not positively recognized as
`COMPLETE` is not a PASS. Re-execution is therefore create-once and never
rebinds; preflight stays freely repeatable because it creates nothing.

**8a. Crash semantics.** A crash before step 2 leaves nothing (`ABSENT`). A
crash in the narrow interval after step 2 but before the marker is durable
leaves a bare reserved directory, which classifies `FOREIGN`. From the moment
the marker is durable until step 6 completes, a crash leaves `INCOMPLETE` —
including the conservative case where the manifest is in fact fully present but
step 6 did not run. After step 6, `COMPLETE`. Every pre-commit state is refused
and none is a PASS; the classification differs only in which reason code the
operator reports.

**Durability ordering.** The marker unlink of step 6 is issued only after the
manifest's *data* has been fsynced (step 5, before the link) and after both
`gate_a/` and the campaign root have been fsynced, which persists the canonical
manifest's directory entry and `gate_a/`'s entry respectively. The unlink's own
durability then requires the subsequent root fsync. **`COMPLETE` therefore
cannot become durable unless the canonical manifest is already durable.**

**8b. Cleanup removes only what the invocation created.** The release path is
never a recursive delete. Between the reservation and a failure, a same-UID
process can add files, directories, symlinks, or hard links under the campaign
root, and none of it is the operator's to destroy. Release therefore unlinks
only the specific paths this invocation created and removes directories with
`os.rmdir`, which refuses a non-empty directory rather than descending. If
anything foreign is present the marker is deliberately retained, so the root
stays `INCOMPLETE` and enters governed recovery instead of being silently
cleared. Once the campaign is committed, nothing is ever withdrawn.

**8c. Recovery is governed, manual, and deliberately not implemented.** An
`INCOMPLETE` root is **never** repaired, reused, overwritten, or removed by this
operator. Recovery is an explicit operator action — quarantine the root and
re-run Gate A — and **no tooling exists for it**; this ADR does not create any.
A partially created V5 can therefore persist on disk after a crash, but it is
positively identifiable and can never be interpreted as a successful Gate A.

**8d. What COMPLETE does and does not assert.** `COMPLETE` is a *structural*
classification: the marker is absent and a regular non-symlink file exists at
the canonical manifest path. It does not decode the manifest, validate it
against the strict canonical contract, or recompute either digest. That is sound
only because nothing currently consumes this evidence as authority — Gate C
takes `environment_manifest_sha256` from its own operand file. Any future
consumer treating the manifest as authority **must re-verify it itself**; a
`COMPLETE` classification is not evidence of validity.

**9. Direction of authority is one-way, and now enforced.** Gate A produces
`environment_manifest_sha256`; Gate B ingests genuine sources into the campaign
Gate A created; Gate C consumes both. A Gate-A artifact that Gate C cannot
consume is a Gate-A defect, never a reason to relax Gate C.

Persisting the artifact is not sufficient on its own: until it is *verified* by
a consumer, an operator can type any syntactically valid 64-hex digest into a
Gate-C operand file and build a downstream chain no Gate A ever attested.
`load_verified_gate_a_evidence` is therefore the mandatory authority path — it
requires `COMPLETE`, decodes under the strict canonical contract, recomputes
both `evidence_sha256` and `environment_manifest_sha256`, re-derives the serving
identity instead of trusting it, and cross-checks the digested observation
against every other section. `derive_downstream_authority` exposes the fields a
downstream operand set must inherit, so operands are built from evidence rather
than typed.

`build_gate_c_plan` calls that verifier on every run, before any Milvus client
can exist. The campaign root is derived from the existing `store_root`, so **no
operand is added and Gate C's closed 23-key contract is unchanged**, and the
operand `environment_manifest_sha256`, `source_revision`, and
`configuration_identity` must each equal what the artifact attests. Verification
is **mandatory and never skipped because evidence is absent**: missing,
malformed, incomplete, substituted, or mismatched Gate-A evidence all fail
closed with a distinct reason code. Any future legacy allowance must be an
explicit, versioned decision — never inferred from a missing file. This is the
one intentional change to Gate-C behavior in this ADR; its operand contract,
window semantics, detector contract, and scientific contracts are untouched.

**10. Zero searches, structurally.** Preflight and execute both reach Milvus
only through a metadata reader exposing exactly `describe_collection`,
`describe_index`, `get_collection_stats`, and `get_load_state`. The reader has no
`search` attribute at all, so issuing a search is not merely forbidden but
structurally impossible.

**11. Historical evidence is unreachable.** The operator refuses any
`campaign_root` that already exists, so no V1–V4 path can be targeted, and it
opens no historical store.

#### Command contract

Preflight — creates nothing, issues zero searches:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_a_operator \
--operands /path/to/gate_a_operands.json \
--mode preflight
```

Execute — initializes V5 exactly once:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_a_operator \
--operands /path/to/gate_a_operands.json \
--mode execute \
--confirm-initialize-v5
```

The operand file's exact closed key set is `exp010_gate_a_operator.
OPERAND_FIELDS`.

#### Consequences

Gate A becomes provable from the repository rather than reconstructed from a
session, and V5 acquires a single owner for its creation. The environment digest
helper, the serving-configuration identity, the canonical serializers, Gate C,
the detector contract, and every storage schema are unchanged; this ADR is
additive. No scientific or statistical contract is affected, because Gate A
observes an environment and measures nothing.

#### Verification requirements

Offline tests must prove: preflight creates nothing and issues zero searches;
execute requires the explicit confirmation flag; correct fresh live metadata is
accepted; and each of a source-revision, collection-name, row-count, dimension,
metric, FLAT-type, HNSW-type, `M`, `efConstruction`, governed-serving-config, and
stale-stable-identity mismatch is rejected. Incomplete and unexpected operands
must both fail closed. An existing campaign root must be refused, a partial
initialization must never report PASS, and an atomic-write failure must leave no
campaign root. The manifest digest must be deterministic for an identical
observation and must change with `observed_at_utc`. The source revision must be
bound into the evidence. No V1–V4 path may be targetable, and the produced
authority shape must be consumable by the existing Gate-C operand contract.

The reservation protocol of item 7 additionally requires adversarial tests that
open the window deliberately — planting a hostile target *after* the advisory
pre-check and *before* the reservation — for an empty directory, a non-empty
directory, a symlink, a dangling symlink, and a regular file, each proving the
planted target survives unmodified and is never followed or replaced. A genuinely
threaded test must show exactly one winner among concurrent initializers. The
`INCOMPLETE` state must be shown unreachable as a PASS, non-repairable, and
non-removable by the operator, including the conservative case where evidence is
present but uncommitted; and a `COMPLETE` root must be shown byte-identical after
a refused re-run.

The nested boundary needs the same treatment one level down: with the root
already reserved, planting an empty `gate_a/`, a non-empty `gate_a/`, a
symlinked `gate_a/`, a pre-existing manifest file, and a symlinked manifest path
must each be refused with nothing overwritten and nothing written through a
symlink, and the state must be observably INCOMPLETE at every nested step until
the marker is removed.

Because a test suite can pass vacuously here, the exclusivity tests must be
shown to fail when each exclusive create is weakened — the root reservation, the
evidence-directory creation, and the manifest publication alike.

Focused verification is sufficient: this change is additive and alters no shared
runtime semantics.

---

### ADR-018: Committed Gate-B live ingress operator, V5 retirement, and the end of operator-side hosting glue

#### Status

Accepted.

#### Context

ADR-016 established that Gate B's genuine ingress is
`exp010_ingress.Exp010RequestIngress.admit` onto `Exp010LiveRunner.serve`, and
ADR-017 committed the Gate-A operator. Both gates then had a committed operator
entrypoint with a `main()`. **Gate B did not.**

The committed repository supplied only the ingress *boundary* — the
`Exp010RequestIngress` class and the optional `Exp010StdlibSearchHandler`
adapter. Nothing committed ever *hosted* that boundary: constructing
`Exp010LiveRunner` with a real serving executor, opening the durable stores at
the campaign root, pinning the governed identities, and binding an HTTP port.
Every historical Gate-B campaign (V1–V4) was therefore driven by ad-hoc,
unpersisted operator-side glue, exactly the reproducibility gap ADR-016 closed
for Gate C. The V4 run receipt records only its symptom: an endpoint on
`127.0.0.1:59051` belonging to no committed module.

This was discovered when V5 reached Gate B. V5's Gate A completed and verified,
but there was no canonical path to ingest its 600 genuine source records, and
two operands the runner requires — `detector_seed`, plus the store and output
locations — had no committed derivation for a Gate-B host.

#### Decision

**1. `vdbench.exp010_gate_b_operator` is the canonical Gate-B operator.** It is
the committed, reproducible host for genuine external ingress. Like the Gate-C
operator it is deliberately the *smallest* auditable one: it invents no runner,
no ingress, no sequencing protocol, and no serving path, composing only
`Exp010LiveRunner`, `Exp010RequestIngress`, `Exp010StdlibSearchHandler`,
`MilvusRangeServingExecutor`, `DockerSocketHealthProbe`, the existing durable
stores, and a stdlib `HTTPServer`.

**2. Authority is inherited from Gate A, never typed.** The operator requires a
campaign root whose Gate-A evidence passes `load_verified_gate_a_evidence`, and
takes every governed identity from `derive_downstream_authority`: stream id,
metric, stratum, radius, range filter, limit, served ef, dimensions,
consistency level, configuration identity, both binding ids, source revision,
environment digest, Milvus URI, both collection names, and the dataset
directory. Deployment identity and data identity come from the verified evidence
document. The serving identity is re-derived and must equal what Gate A bound.
**No governed identity appears in the operand file at all**, so an operator
cannot host a campaign under an identity no Gate A attested — a stronger
position than Gate C's cross-check, which still accepts them as operands.

**3. The operand set is closed and deliberately tiny — seven keys.**
`campaign_root`, `detector_seed`, `host_address`, `host_port`,
`target_source_records`, `etcd_container`, `minio_container`. A missing or
unexpected key is refused, never defaulted.

**4. `detector_seed` remains a mandatory explicit operator decision.** It is
governed by the detector-contract domain, is deliberately excluded from
`configuration_identity` (ADR item: serving semantics only), and is frozen per
campaign. Gate A does not attest it and is not asked to. There is no default and
no inference, exactly as in the Gate-C operand contract. A new campaign requires
a new explicit operator decision; nothing here claims V1–V5 bound a future
campaign's value.

**5. `store_root` and `exp010_output_dir` are derived, not operands.** They are
`<campaign_root>/stores` and `<campaign_root>/output`, the layout every
historical campaign already used and the exact inverse of Gate C's
`store_root.parent` derivation. Exposing them as free-form input would add
operator degrees of freedom with no safe use.

**6. Gate separation is structural, not documentary.** `process_ready_windows`
is never called or imported, so Gate C/D progression is unreachable from this
entrypoint. Additionally the composition is built with a **refusing**
shadow-capture executor, so a Gate-C physical shadow search is impossible here
even under a future defect: the executor raises. This mirrors Gate C, which
injects a refusing *serving* executor for the same reason.

**7. The operator generates nothing.** No vector sampler, no random source, no
dataset or historical replay, no benchmark generator. It never constructs a
`query_vector`. Tests assert this over the module AST rather than its text, so a
generator cannot be added without failing the suite. The external application
remains the sole origin of genuine requests.

**8. Preflight is side-effect free with respect to Gate-B campaign state.** It
verifies authority, campaign state, the store set, the endpoint's bindability,
and the resolved plan, then prints it with a `plan_sha256`. It creates no store,
binds no listening server, and issues zero searches and zero `serve()` calls.
Because Gate B is the *first* writer, absent stores are the correct fresh state;
the composition is opened only when the stores already exist, so inspecting a
campaign cannot bring one into being.

**9. Execute requires a second explicit operator action.** `--mode execute
--confirm-gate-b-ingress`. Execute re-runs the entire preflight first, binds
**loopback only**, and serves genuine external requests through the committed
ingress until the governed target is durable.

**10. Sequencing is the store's, not the host's.** `source_sequence` is
allocated by `commit_response` as the durable membership length, so the host
never invents a sequencing protocol and a duplicate request id is refused by the
store (`HOST_SOURCE_QUERY_ID_DUPLICATE`). The governed target is 600 records —
three complete 200-source windows — and a target that is not a whole multiple of
`WINDOW_QUERY_COUNT` is refused.

**11. Restart is reopen, never repair.** An existing complete store set is
reopened and its durable bindings verified against inherited authority. A
*partially* present store set is ambiguous and is refused
(`GATE_B_STORE_SET_INCOMPLETE`) rather than repaired; nothing is deleted or
reset to recover from uncertainty, no replay path exists, and durable counts
above the governed target fail closed. STARTED/ambiguous semantics elsewhere are
untouched, and no physical exactly-once claim is made or implied.

**12. V5 is retired, truthfully.** V5's authoritative historical state is:
**Gate A COMPLETE** (evidence `bbf0c69a…bee40`, environment manifest
`e12f46dd…84b`), **Gate B NEVER STARTED**, **Gate C NEVER STARTED**, **zero
searches**, zero source records, zero attempts, zero acknowledgements, zero
detector events, zero attestations, zero finalization events. It is retired **not**
because anything failed or drifted, but because no committed canonical Gate-B
live host existed to ingest its sources. V5 evidence is not modified to encode
this decision; this ADR records it. There is no V5 Gate B, no V5 Gate C, and no
repair or reuse of the V5 root.

**13. This operator governs nothing retroactively.** It did not exist during
V1–V5 and makes no claim about how those campaigns were hosted beyond the fact
recorded here: V1–V4 Gate-B hosting relied on uncommitted operator-side glue,
and V5 never reached Gate B at all. Historical evidence is unchanged.

#### Command contract

Preflight — creates no store, accepts no request, issues zero searches:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_b_operator \
--operands /path/to/gate_b_operands.json \
--mode preflight
```

Execute — hosts genuine external ingress until the governed target is durable:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
python -m vdbench.exp010_gate_b_operator \
--operands /path/to/gate_b_operands.json \
--mode execute \
--confirm-gate-b-ingress
```

The operand file's exact closed key set is `exp010_gate_b_operator.
OPERAND_FIELDS`.

#### Consequences

Gate B becomes provable from the repository rather than reconstructed from a
session, closing the last gate that depended on unpersisted glue. All three
live gates — A, B, C — now have committed operators with preflight/execute
separation and explicit confirmation flags. The ingress boundary, the runner,
the source-event schema, the window semantics, the detector contract, and every
storage schema are unchanged; this ADR is additive. No scientific or statistical
contract is affected, because Gate B ingests genuine requests and measures
nothing.

A future campaign needs a fresh Gate A, a new explicit `detector_seed`, and this
operator for Gate B.

#### Verification requirements

Offline tests must prove: Gate-A evidence is required, and malformed,
substituted, and non-COMPLETE evidence each fail closed; no governed identity is
accepted as an operand; the operand set is closed and `detector_seed` is
mandatory and type-checked; store and output paths are derived; a non-loopback
address and a non-whole-window target are refused; preflight creates no store,
writes nothing, accepts no request, and never reaches the hosting seam; execute
requires the confirmation flag; the plan digest is load-bearing; the
shadow-capture and preflight-serving executors refuse; and the module's AST
contains no call to `process_ready_windows`, `trigger_state`, or
`capture_exp010_population` and no import of a random source or sampler.

Focused verification plus one full-suite run is sufficient: this change is
additive and alters no shared runtime semantics, but it defines the next live
source freeze.

#### Amendment (ADR-018a): V6 retirement and the Gate-B real-seam contract defect

The operator committed above carried one real defect, found on its first live
use and recorded here rather than quietly fixed.

**The defect.** `run_gate_b_host_from_cli` decided serving admission with
`getattr(admission, "admitted", False)`. The committed contract is
`ServingPreflightResult(complete, checked_stream_count, reason_codes)` — there
is no `admitted` field, so the `getattr` default made the check evaluate
`False` unconditionally and the Gate-B host could never start. The empty
`reason_codes` then surfaced as the invented string `UNKNOWN`. A read-only
reproduction of the exact construction returned
`ServingPreflightResult(complete=True, checked_stream_count=1, reason_codes=())`:
serving admission had in fact **succeeded**, and the operator refused a healthy
stack.

**Why the tests missed it.** Every Gate-B test stubbed this seam, and a stub is
free to expose whichever attribute the code happens to read. The regression
cover added with the fix therefore drives `run_gate_b_host_from_cli` with the
**real** frozen `ServingPreflightResult`, stubbing only true external
boundaries, and asserts that `complete=True` proceeds, `complete=False` fails
closed, and `reason_codes` propagate verbatim. Reintroducing the original
expression makes those tests fail. The lesson generalises: a defensive
`getattr(..., default)` across a committed contract boundary converts a
field-name mismatch into silent wrong behaviour instead of a loud error, and is
not used at this seam.

**V6 is retired, truthfully.** V6's authoritative historical state is: **Gate A
COMPLETE** (evidence `d0a688c3…e304c`, environment manifest `1a2d05bc…ae98`,
source revision `0464f290…`), **Gate B NEVER STARTED**, **Gate C NEVER
STARTED**, **0 source records**, **0 searches**, 0 attempts, 0 acknowledgements,
0 detector events, 0 attestations, 0 finalization events.

It is retired because the Gate-B operator contained this real-seam contract
defect. Critically, the defect **fired fail-closed before any effect**: the
refusal preceded runner construction, so no store directory, no store, no HTTP
listener and no client request ever existed. The external workload client was
never started and remains unmodified. Nothing about V6 was mutated, deleted or
rewritten, and V6 evidence is not modified to encode this decision; this
amendment records it.

Commit `0464f290…` is **not** amended or rewritten: it is published, and V6
evidence durably binds it. The fix is a separate commit and therefore a new
source revision, which is why V6 — whose Gate A attests the previous revision —
cannot proceed to Gate B and a fresh Gate A is required instead.

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

### ADR-019: Separate EXP-012-SCALE from the frozen EXP-010 campaign

Status: Accepted — offline foundation implemented and focused verification passed; no live scale run authorized

Date: 2026-08-20

Risk level: CRITICAL

#### Decision

EXP-010 and its V8 evidence remain immutable. Scale validation uses the distinct
experiment identity `EXP-012-SCALE`, distinct canonical schemas, and distinct
digest domains. The only supported v1 profiles are:

- `scale-2400`: exactly 2,400 durable sources, twelve exact 200-source windows,
  and 4,800 physical Gate-C searches;
- `scale-10000`: exactly 10,000 durable sources, fifty exact 200-source windows,
  and 20,000 physical Gate-C searches.

`WINDOW_QUERY_COUNT` remains exactly 200. Each source still receives exactly
one FLAT reference search and one HNSW sentinel search at `ef=100`. The existing
source membership, trace-attempt STARTED/terminal lifecycle, acknowledgement,
detector reference/current progression, real-detector attestation, and window
finalization semantics are reused without reinterpretation. Scale operators
consume a separately verified Gate-A authority root as a read-only upstream
input; the EXP-012 campaign root is distinct and contains no EXP-010
plan/evidence document. Scale operators emit only EXP-012 plan/result
documents. EXP-010 remains loopback-only; ENV-002 is a later decision.

Every EXP-012 campaign is mechanically marked by one immutable
`exp012-scale-campaign-v1` document under
`VD::EXP012_SCALE_CAMPAIGN::V1\0`, containing the exact scale contract, its
digest, and the externally verified Gate-A evidence digest. Scale Gate B
creates or exactly re-verifies that marker before hosting;
scale Gate C requires it. The legacy EXP-010 Gate-B/Gate-C operator entrypoints
refuse a marked campaign, preventing an EXP-012 store from emitting EXP-010
plan/result schemas. Unmarked EXP-010/V8 behavior is unchanged.

The scale contract schema is `exp012-scale-contract-v1`, detached under
`VD::EXP012_SCALE_CONTRACT::V1\0`. Gate-B plans and results use
`exp012-scale-gate-b-plan-v1` / `exp012-scale-gate-b-result-v1` under
`VD::EXP012_SCALE_GATE_B_PLAN::V1\0` /
`VD::EXP012_SCALE_GATE_B_RESULT::V1\0`. Gate-C plans and results use
`exp012-scale-gate-c-plan-v1` / `exp012-scale-gate-c-result-v1` under
`VD::EXP012_SCALE_GATE_C_PLAN::V1\0` /
`VD::EXP012_SCALE_GATE_C_RESULT::V1\0`.

#### Verified source-head/count rule

The host source store performs complete canonical source, outbox,
acknowledgement, binding, and schema verification at create/reopen and retains
the reconstructed immutable source prefix in memory. A normal append or target
check obtains a store-issued snapshot binding the derived count and maximum
sequence to the cached source head, cached outbox head, and exact store-binding
digest under `VD::HOST_RESPONSE_VERIFIED_HEAD::V1\0`. The caller supplies none
of those values. The durable source/outbox heads and SQLite data-version must
still match that verified state. Counter/head substitution fails closed.
Explicit audit/reopen continues to replay every canonical row; no mutable
counter table becomes authority. Therefore normal append/target checks are
independent of history length while restart verification remains O(N).

#### Additive per-search telemetry

Scale Gate C adds one separate append-only SQLite ledger. Its binding schema is
`exp012-shadow-search-telemetry-binding-v1`, under
`VD::EXP012_SHADOW_TELEMETRY_BINDING::V1\0`; records use
`exp012-shadow-search-telemetry-v1`, under
`VD::EXP012_SHADOW_TELEMETRY_RECORD::V1\0`. The binding fixes campaign, exact
scale-contract digest, stream/configuration/data/FLAT/HNSW identities, source
revision, and environment-manifest digest.

Each immutable record binds record sequence/hash-chain predecessor, window,
trace-attempt digest, source sequence, source digest, query-id digest, exact
role (`FLAT_REFERENCE` or `HNSW_SENTINEL`), monotonic start/end nanoseconds,
derived latency in nanoseconds and milliseconds, outcome, result count, and
error classification. `(source_sequence, role)` is unique. Exact successful
completion requires both roles for every governed source and exactly
4,800/20,000 records for the selected profile. The ledger is additive: existing
shadow trace, detector, attestation, and finalization schemas do not change.
A committed trace STARTED event precedes every physical trace. A crash or
telemetry failure after a search therefore leaves terminal/non-retriable trace
evidence; missing telemetry can never be treated as a complete scale run.

This telemetry is measurement evidence only. It creates no qualification,
policy, admission, grant, routing, activation, actuation, or production
authority. ADR-019 authorizes offline implementation and focused verification,
not a live 2,400/10,000 run and not ENV-002.

#### Accepted amendment to ADR-016 / ADR-019: bounded Gate-C checkpoints

Status: **Accepted 2026-08-23 for offline implementation and verification only;
real bounded physical execution still requires a new reviewed preflight and
separate explicit live authorization**

Date: 2026-08-23

Risk level: CRITICAL

The accepted contracts above authorize only the canonical unbounded
`Exp010LiveRunner.process_ready_windows()` transition and the exact full
EXP-012 Gate-C result. They do not authorize one-window or other partial live
execution. A full-campaign plan digest therefore cannot be treated as sufficient
authorization for a bounded checkpoint. Until this amendment is implemented,
verified, source-frozen, and separately authorized for a specific live bound,
any request to execute fewer than all ready Gate-C windows must continue to
fail before physical search.

The accepted additive contract introduces one immutable bound with exactly two
authoritative fields:

```
start_window_sequence: exact int >= 0 (bool forbidden)
window_count: exact int > 0 (bool forbidden)
```

`allowed_window_sequences` is derived exactly as
`tuple(range(start_window_sequence, start_window_sequence + window_count))`;
`expected_next_window_sequence` is derived exactly as
`start_window_sequence + window_count`. If either derived value is serialized
for audit readability, verification must reconstruct and compare it. No caller
may independently supply a different range, set, or postcondition. The runner
must require its durable next-window authority to equal
`start_window_sequence` before constructing a live client/executor or issuing a
physical search. Every authorized source window must already be complete,
ordered, identity-bound, and gap-free before the first authorized physical
search.

The canonical runner may accept this exact bound as an optional keyword-only
input. No bound preserves the accepted unbounded behavior byte-for-byte at the
public contract level. With a bound, one shared single-window transition must
process and reconcile only the derived allowed sequences, stop normally after
exactly `window_count` canonical transitions, verify the durable next-window
authority equals the derived postcondition, and return before polling,
preparing, constructing execution state for, or issuing a search for the next
unauthorized window. Existing acknowledgement, attempt, detector, attestation,
finalization, orphan-attempt, and no-physical-retry semantics remain the sole
canonical Gate-C state machine.

A bounded execution requires a detached envelope with schema
`exp012-scale-gate-c-bounded-execution-envelope-v1` and digest domain
`VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V1\0`. Its strict
canonical payload binds the existing full Gate-C `plan_sha256`, campaign
identity and campaign-binding digest, scale-contract payload/digest, Gate-A
evidence digest, verified source-store binding/head, verified outbox head,
source revision, mechanically reconstructed producer-run identity, source
count, metric, threshold stratum, configuration identity, data identity, FLAT
binding, HNSW binding, environment-manifest digest, and the two authoritative
bound fields. The derived allowed sequence tuple and expected next sequence may
also appear, but are never independent authority. The detached envelope digest
is computed over the exact payload excluding its own digest. Mutation of any
identity or bound changes the digest; malformed, mixed, or sequence-inconsistent
producer identities fail before live-client/executor construction. The existing
full-campaign Gate-C plan schema and digest domain do not change.

Bounded execution is a distinct operator action and result, not partial success
under the existing full result. The accepted CLI action is
`--mode checkpoint-execute --execution-envelope <path>
--confirm-bounded-physical-shadow-searches`. Its result schema is
`exp012-scale-gate-c-checkpoint-result-v1` under
`VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V1\0`. A checkpoint result must bind
the verified envelope digest, canonical pre/post durable heads and next-window
sequences, exact processed sequence tuple, and reconstructively verified
acknowledgement/attempt/detector/attestation/finalization/telemetry effects. It
must never deserialize or verify as `exp012-scale-gate-c-result-v1`, satisfy the
full 50-window result, imply campaign completion, or create Gate-D, policy,
qualification, admission, grant, routing, activation, actuation, or rollback
authority.

One additive checkpoint audit ledger is justified to distinguish authorization
issuance from execution recovery. Its binding/event schemas are
`exp012-scale-gate-c-checkpoint-binding-v1` and
`exp012-scale-gate-c-checkpoint-event-v1`, under
`VD::EXP012_SCALE_GATE_C_CHECKPOINT_BINDING::V1\0` and
`VD::EXP012_SCALE_GATE_C_CHECKPOINT_EVENT::V1\0`. It may append only
`CHECKPOINT_STARTED` and `CHECKPOINT_COMPLETED`. The ledger is never execution
authority: every completion must be reconstructed from the existing canonical
Gate-C stores. On reopen, a STARTED checkpoint with zero canonical effects may
resume only after full envelope/store verification; one whose authorized
windows are unambiguously complete may append completion without repeating any
physical search; an orphan or ambiguous attempt, effect outside the authorized
range, or inconsistent canonical state must refuse for forensic review. A
checkpoint event can neither repair nor override canonical state.

For the accepted SCALE10000 C1 checkpoint contract, the bound is `(0, 1)`, the derived
allowed sequence is exactly `(0,)`, and the derived postcondition is exactly
`1`. Current executable constants derive—not independently assert—the expected
effects: `WINDOW_QUERY_COUNT = TRACE_COUNT * TRACE_QUERY_COUNT = 4 * 50 = 200`
source acknowledgements; four canonical trace attempts; and two telemetry roles
(`FLAT_REFERENCE`, `HNSW_SENTINEL`) for each source, hence 400 physical searches
and 400 telemetry records. These cardinalities must be reconstructed from the
governed source window, attempts, and telemetry ledger before checkpoint
completion.

Implementation under this acceptance is limited to: one neutral immutable bound and
envelope contract; a shared single-window transition inside
`Exp010LiveRunner`; a distinct EXP-012 checkpoint operator/result path; the
non-authorizing checkpoint audit ledger; exact telemetry-prefix verification;
and focused offline/adversarial/restart tests. Tests must prove one- and
two-window bounds, resume from durable next sequence, default unbounded
compatibility, exact pre-search refusal for malformed bounds/envelopes and all
identity/head drift, zero activity outside the authorized range, reconstructive
recovery for zero-effect and already-complete STARTED checkpoints, forensic
refusal for ambiguous attempts, inability of a partial result to satisfy full
completion, and an entirely fake 50-ready-window run that advances only window
0 under `(0, 1)`. Historical EXP-010/V8, failed SCALE2400 evidence, completed
SCALE2400 evidence, existing full plan/result schemas, and canonical SQLite
stores require no migration or reinterpretation.

Human acceptance on 2026-08-23 authorizes implementation and offline
verification only. It does not authorize the real SCALE10000 C1 physical
searches; those require a new bounded preflight, a reviewed envelope digest,
and separate explicit live execution authorization after the implementation is
source-frozen.

#### Accepted dual-revision provenance amendment to ADR-016 / ADR-019

Status: **Accepted 2026-08-25 for offline implementation and verification
only; no live C1 execution is authorized**

The original `source_revision` meaning is unchanged. It identifies the exact
repository revision that produced and attested the immutable Gate-A/Gate-B
upstream evidence. A prospective bounded Gate-C checkpoint additionally binds
`execution_source_revision`: the exact committed repository revision whose
runtime code may verify, recover, or execute that checkpoint. These revisions
are independent provenance identities. Neither supplies window-range,
qualification, admission, routing, actuation, Gate-D, or other downstream
authority.

Prospective bounded envelopes use schema
`exp012-scale-gate-c-bounded-execution-envelope-v2` and digest domain
`VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V2\0`. The v2 payload is
the strict v1 payload plus `execution_source_revision`; it continues to bind
the unchanged upstream `source_revision`, all upstream evidence identities,
and the exact derived execution bound. Prospective checkpoint results use
`exp012-scale-gate-c-checkpoint-result-v2` under
`VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V2\0`. They explicitly bind both
revisions and the v2 envelope digest. Prospective checkpoint events use
`exp012-scale-gate-c-checkpoint-event-v2` under
`VD::EXP012_SCALE_GATE_C_CHECKPOINT_EVENT::V2\0`; every STARTED and COMPLETED
event explicitly binds `execution_source_revision`. The existing v1 checkpoint
binding remains an upstream/campaign binding and is unchanged. Historical v1
envelope, result, and event schemas retain their original interpretation and
canonical bytes. Mixed v1/v2 documents or event chains fail closed.

The execution revision is derived mechanically; it is never a caller-supplied
CLI authority field. Before any live-capable client or capture executor is
constructed, the bounded operator must, in order: verify the immutable
Gate-A/Gate-B authority under `source_revision`; reconstruct and verify the
complete v2 envelope and bound; then verify that repository `HEAD` is the exact
`execution_source_revision`, that every tracked runtime byte beneath
`src/vdbench` equals that commit, that no untracked executable module can
shadow the runtime package, and that loaded `vdbench` modules resolve beneath
that repository's `src/vdbench` tree. Unrelated documentation, reports, and
artifacts are outside this executable identity and do not cause rejection.
Wrong upstream revision and wrong execution revision fail independently.

Checkpoint recovery and completion must reconstruct the same v2 envelope,
event chain, result, and exact `execution_source_revision`. Reopening under a
different executor revision or with executable-source drift refuses before a
physical search; it cannot migrate, complete, or replay the old checkpoint.
Gate-A/Gate-B evidence is never rewritten or rerun merely because bounded
Gate-C code changed. Existing unbounded Gate-C plans, historical evidence, and
full-campaign source-revision semantics remain unchanged. Acceptance of this
amendment authorizes only offline implementation and verification; it does not
authorize a live envelope, physical search, C1 execution, or any later gate.

#### Accepted dual-environment provenance amendment to ADR-016 / ADR-019

Status: **Accepted 2026-08-26 for offline implementation and verification
only; no live C1 execution is authorized**

Prospective bounded Gate-C v3 separates four non-interchangeable identities.
`source_revision` and `environment_manifest_sha256` retain their historical
upstream Gate-A/Gate-B meanings unchanged. `execution_source_revision` retains
the exact bounded-executor-code meaning accepted above. The new
`execution_environment_identity_sha256` identifies the stable, currently
observed execution runtime and data plane. It does not absorb either source
revision, historical Gate-A/environment digests, observation or health time,
transient health, load/readiness, or policy, qualification, admission, routing,
authorization, or actuation state.

The stable identity schema is
`exp012-scale-gate-c-execution-environment-identity-v1`, detached under
`VD::EXP012_SCALE_GATE_C_EXECUTION_ENVIRONMENT_IDENTITY::V1\0`. Its closed
canonical payload binds the normalized effective Milvus endpoint; exact
etcd/MinIO/Milvus container IDs, image IDs, sorted repository digests,
`StartedAt`, restart count, OOM state, approved non-secret labels, mounts,
network attachments, and published ports; and exact FLAT/HNSW collection,
namespace, canonical schema, metric, dimension, integer entity count, index
name/type/metric/parameters, and derived index identity. For ENV-001 the two
entity counts must be exact non-boolean integers, equal each other, and equal
the governed expected count. Approximate or noncanonical counts fail closed.
Load/readiness is deliberately excluded from stable identity.

The observation schema is
`exp012-scale-gate-c-execution-environment-attestation-v1`, detached under
`VD::EXP012_SCALE_GATE_C_EXECUTION_ENVIRONMENT_ATTESTATION::V1\0`. It directly
binds the complete stable identity document/digest, executor revision,
`observed_runtime`, `governed_bindings`, `observation_metadata`, and a
successful compatibility result. Governed bindings include unchanged upstream
source/environment/Gate-A, campaign/scale, dataset, configuration,
FLAT/HNSW, serving, and collection identities. Observation metadata carries
UTC observation time, Docker/service health, Milvus healthz, collection load,
and index readiness. Reobserving the same stable runtime at a new time keeps
the stable identity digest but may change the attestation digest. Eligibility
always reevaluates the transient predicates independently.

The opaque Gate-A FLAT/HNSW binding IDs are not redefined as v3-specific
metadata digests. Each governed binding also carries the exact verified Gate-A
`live` metadata record to which that ID was historically attested. V3
mechanically compares the current stable Gate-A projection—collection, index
name/type/metric, exact row count and dimension, plus HNSW `M` and
`efConstruction`—to that preserved record before issuing an eligible
attestation. Index/load completion remain transient readiness predicates. A
self-consistent new execution-environment identity cannot override a mismatch,
and execute reloads the verified Gate-A authority and repeats the comparison
before `CHECKPOINT_STARTED`.

Prospective bounded artifacts use only new schemas and domains:

- `exp012-scale-gate-c-bounded-execution-envelope-v3` under
  `VD::EXP012_SCALE_GATE_C_BOUNDED_EXECUTION_ENVELOPE::V3\0`;
- `exp012-scale-gate-c-checkpoint-result-v3` under
  `VD::EXP012_SCALE_GATE_C_CHECKPOINT_RESULT::V3\0`;
- `exp012-scale-gate-c-checkpoint-event-v3` under
  `VD::EXP012_SCALE_GATE_C_CHECKPOINT_EVENT::V3\0`.

V3 retains all v2 upstream, executor, exact two-field bound, canonical
pre/post/effect, and per-window telemetry semantics and additionally binds the
stable execution-environment identity, complete attestation, and detached
attestation digest. The only independent range authority remains
`start_window_sequence` plus `window_count`; environment evidence cannot
select or alter windows. V1/v2 documents, domains, readers, and completed
historical chains remain unchanged. One chain is homogeneous; cross-version
envelopes/results/events and silent downgrade fail closed.

Read-only v3 preflight may reconstruct upstream authority, range eligibility,
executor provenance, runtime metadata, compatibility, and future-suffix state,
but writes no envelope/checkpoint/campaign evidence and issues no search. A
separate preparation action may persist exactly one canonical v3 envelope with
mode `0600`. The persisted bytes are the reviewed authorization handoff.
Execute consumes those exact bytes and accepts no replacement start/count or
regenerated envelope. It verifies upstream evidence and executor provenance,
reobserves metadata-only runtime identity, requires exact stable-identity
equality and current health/load/readiness, and only then may append durable
`CHECKPOINT_STARTED`. Search-capable capture construction and physical attempt
STARTED remain later boundaries.

All prospective legacy and v3 authority creation/resumption contends on the
same historical checkpoint sidecar lock. The shared lock remains nonblocking,
exclusive, inode/PID/path checked, same-process registered, and fork safe.
Under that lock, both generations are inspected. At most one unfinished
checkpoint may exist across them. An unfinished legacy chain blocks v3 and an
unfinished v3 chain blocks prospective legacy creation; completed historical
evidence does not block a new v3 checkpoint merely by existing.

`CHECKPOINT_STARTED` proves only that bounded checkpoint authority durably
began. Physical ambiguity begins at canonical attempt STARTED. If runtime
authority becomes invalid after checkpoint STARTED but before any local
attempt/effect, v3 may append terminal
`CHECKPOINT_ABORTED_PRE_SEARCH` with fixed reason
`EXECUTION_AUTHORITY_INVALIDATED_PRE_SEARCH`. That event requires exact
checkpoint-local pre/post equality, zero STARTED/COMPLETED/ORPHANED attempt
deltas, unchanged acknowledgement/attempt/detector/attestation/finalization/
telemetry heads and counts, unchanged next sequence, and no pending or prepared
finalization. Missing, drifting, partial, or ambiguous proof refuses the abort.
The old envelope becomes permanently terminal and non-reusable; a fresh
attestation and envelope may later propose the still-unconsumed range.

After any physical attempt STARTED, runtime change never authorizes blind retry
or replacement execution authority. Existing orphan/ambiguous recovery remains
fail closed. Conversely, if canonical durable effects already prove all
bounded work complete but checkpoint COMPLETE is missing, v3 may reconstruct
completion without contacting or recreating the old runtime and with zero
physical searches. Current runtime cannot substitute for the original search
provenance during that completion-only recovery.

The observer surface is metadata-only: Docker container and image inspection,
Milvus collection/index/count/load metadata, and healthz. It has no governed
vector-probe or mutation operation. These digests provide local
integrity/provenance, not hostile-host cryptographic attestation; the trust
boundary still includes the Docker daemon/socket, local filesystem, process
environment, same-user state, and OS metadata APIs. This acceptance authorizes
offline implementation/tests only. It creates no full-campaign completion,
Gate-D, qualification, admission, grant, routing, activation, actuation, or
rollback authority and does not authorize live C1.
