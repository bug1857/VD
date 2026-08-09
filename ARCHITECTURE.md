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
