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
- **H4 — HYPOTHESIS:** With pinned software and resources, repeated measurements will be stable enough for p95-latency coefficient of variation to remain at or below 30% for each configuration.

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

- [x] Milvus server version recorded exactly.
- [x] PyMilvus version recorded exactly and confirmed compatible with the server version.
- [x] Milvus Docker image tag and immutable digest recorded.
- [x] etcd image tag, immutable digest, and effective configuration recorded.
- [x] MinIO image tag, immutable digest, and effective configuration recorded.
- [x] Docker Engine/Desktop and Docker Compose versions recorded.
- [x] Compose file, Milvus configuration, and environment-file SHA-256 checksums recorded.
- [x] Container CPU quota/cpuset and RAM limit recorded for Milvus, etcd, and MinIO.
- [x] Host CPU model/architecture, core counts, RAM, storage, OS, and kernel recorded.
- [x] Python and NumPy versions plus complete lockfile/environment export recorded.
- [ ] Dataset seed `20260801`, derived ordering seeds, generator algorithm, and artifact checksums recorded.
- [ ] Milvus collection schema, consistency level, metric, index parameters, query parameters, and result `limit` recorded.
- [x] Background workloads disabled or disclosed; container health and pre-run resource snapshot captured.
- [ ] Post-run container health and resource snapshot captured.
- [x] Git commit hash and clean/dirty working-tree state recorded for pre-run evidence collection.

Checklist audit (2026-08-01): checked items are backed by the ENV-001 provisioning transcript and the pinned vendor/override Compose files, with PyMilvus compatibility established by the isolated probe appended to that transcript. Remaining unchecked items require run-time artifacts that do not yet exist: standalone Milvus/environment-file checksums; host storage and kernel details; the benchmark Python/NumPy lock/export; generated DATASET-001 checksums and derived seeds; the immutable collection/query run manifest; background-workload disclosure plus post-run resources; and the execution commit/dirty-state capture. The temporary PyMilvus compatibility environment is not the benchmark lockfile and does not satisfy that separate checklist item.

Pre-run environment evidence update (2026-08-01):

The audit paragraph immediately above is retained as the historical pre-completion audit; the evidence below supersedes its statements about then-missing host, configuration, Python-environment, background-workload, and pre-run Git artifacts.

- **Dedicated Python environment:** repository-root `.venv-exp001` was created fresh with CPython `3.14.5` on macOS `arm64`; it is persistent and gitignored, and is not the temporary PyMilvus compatibility probe. `artifacts/exp-001/environment/benchmark/requirements.lock` is a complete 16-package transitive resolution generated by uv `0.10.4` for `aarch64-apple-darwin`, with distribution SHA-256 hashes. The synchronized environment contains NumPy `2.5.1`, PyMilvus `3.0.1`, and pip `26.1.1`; `uv pip check` reported all 17 installed packages compatible, the committed `pip-freeze.txt` matched the live environment exactly, and all 20 offline harness tests passed in this environment. Lock SHA-256: `6599b1cffa9a64fad7655b18818d1ba1b11719323f4fb5066afa83bc5adab21e`; freeze SHA-256: `c7fd159738d069c648a2da4bec803fba239d6237ab79d1941746c3046d02225e`; runtime-export SHA-256: `82943f6b97d4be7f547b973e2c6043756439fb5a3f8ccdc2451035e0e7b0c84b`.
- **Host:** Apple M1 `arm64`, 8 physical cores (4 performance + 4 efficiency), 8 logical cores, and 8 GiB RAM. Storage is an internal `APPLE SSD AP0256Q`, 245.1 GB, Apple Fabric protocol, APFS, S.M.A.R.T. verified. OS is macOS `26.5.2` build `25F84`; kernel is Darwin `25.5.0`, `RELEASE_ARM64_T8103`. Sanitized raw evidence is `artifacts/exp-001/environment/benchmark/HOST_ENVIRONMENT.md` (SHA-256 `5c70af698c749e3e8153bd4e4053f182aab1815f65aa2db4f3171c707373200c`).
- **Compose/config/environment artifacts:** vendor Compose SHA-256 `4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1`; override SHA-256 `bd97b91052ac642593c0af33aa7e90519e472a168d4ada48ba71f0846a4ee8c6`; regenerated `vd-exp001` effective Compose SHA-256 `76310aee683a1dab714679f0f9202bc193ad87019e2e8bbf3c25fb46454ea217`; stock `/milvus/configs/milvus.yaml` copied from the running pinned Milvus image SHA-256 `b35bbf05c06d806621c1d98432b176e1ba819d1649ebd3e290a76b53ad1aa4bd`; explicit Compose environment file `infra/milvus/env-001/env001.env` SHA-256 `ea333e5bbe76b205dc51515b4426d4b8104aa2088662d5f78f9303e22f430d56`; Docker Desktop settings export SHA-256 `7893848604c0c441b89bfa964d60ec1b2b69c6212d6db594fbe06e70ff375688`. `ENVIRONMENT_SHA256SUMS` reverified all 12 evidence files and has SHA-256 `192de4d452317f2de0e1a7d6019daad3cb1eb432049b5d5187baf68a080820e9`.
- **Background workloads and pre-run resources:** background workloads were running and were not disabled; the host was not quiescent. At `2026-08-01T14:25:06Z`, load averages were `2.61 3.40 4.36`, and macOS services, the Docker virtual machine, ChatGPT/Codex, and other daemons were active. All three containers were `running` and `healthy`; Milvus used 8.00% CPU and 328.4 MiB/4 GiB, etcd 1.16% and 18.79 MiB/512 MiB, and MinIO 0.19% and 119.4 MiB/1 GiB. Evidence: `artifacts/exp-001/environment/benchmark/PRE_RUN_RESOURCE_SNAPSHOT.md` (SHA-256 `211f22df13c96109c6bc6130c29af6bae3d72144571011e1564229705ef7886c`). This is pre-run evidence only; latency interpretation is unauthorized until workloads are stabilized or re-disclosed immediately before execution and the post-run snapshot is captured.
- **Git state:** at the same pre-run evidence capture, `HEAD` was `417dfeb52562bf259e02c38fbb0ef3bb94dac319` on `main`, synchronized with `origin/main`; the working tree was **DIRTY** solely because `artifacts/exp-001/environment/volumes/` was untracked. EXP-002 execution must record its own later commit and clean/dirty state in the immutable run manifest.
- **Evidence-workflow scope:** this evidence-collection workflow did not invoke `execute_live`, create a Milvus collection, issue a Milvus search, or call a Milvus API. At `2026-08-01T14:31:43Z`, after the pre-run snapshot, an unrelated untracked `artifacts/exp-001/run-live/run_manifest.json` appeared and the containers were observed to have been recreated. Its manifest declares an `execute_live` invocation from another process at commit `417dfeb52562bf259e02c38fbb0ef3bb94dac319`, but no raw-query output or completed-run evidence exists. The artifact is preserved unmodified and excluded from this commit; it is not accepted as EXP-002 evidence. EXP-001 remains `NOT RUN`, and EXP-002 remains harness-only/offline-tested.

Unchecked items remain unchecked because their complete evidence does not yet exist: the realized derived ordering seeds and immutable run manifest, actual collection/index/query metadata, and the post-run health/resource snapshot.

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
- p95-latency coefficient of variation is at most 30% per configuration; otherwise EXP-001 is inconclusive and the environment must be stabilized before performance interpretation.
  - **Justification for the 30% ceiling:** EXP-001 runs on a shared laptop through Docker Desktop, not on dedicated bare-metal hardware. The two progressively controlled reruns each placed exactly one different configuration marginally above the former 20% ceiling (`21.66%` for `L2:target-075:HNSW:ef=400` and `26.02%` for `L2:target-075:HNSW:ef=800`), with no repeated configuration-level failure, query failure, threshold violation, or index-identity change. This supports treating up to 30% as the observed environment noise floor rather than evidence of a harness defect. The original uncontrolled run does **not** support the marginal-breach pattern: 16 of 36 configurations exceeded 20%, seven exceeded 30%, and its maximum was 47.21%; it therefore remains inconclusive under this revised criterion. This change affects only the variance acceptance ceiling; dataset, index, search, and measurement parameters remain frozen.
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

### EXP-002: EXP-001 benchmark harness implementation

Status: HARNESS IMPLEMENTED — OFFLINE UNIT TESTED; LIVE MILVUS EXECUTION NOT RUN
Date: 2026-08-01
Risk level: HIGH (research-validity implementation; no live benchmark evidence)

Objective:

Implement the EXP-001 contract without changing its dataset, index, search, or measurement parameters. This entry records implementation and offline verification only; it is not an empirical Milvus benchmark result.

Hypothesis:

No EXP-001 hypothesis is evaluated by this implementation-only entry. H1–H4 retain the statuses recorded in EXP-001 until a separately authorized live execution produces reviewable evidence.

Configuration:

- DATASET-001 default generator: NumPy `Generator(PCG64(20260801))`, 10,000 base vectors, 50 calibration queries, 200 disjoint measured queries, 128 dimensions, little-endian `float32`, independent standard-normal draws.
- Independent oracle: stored `float32` inputs converted for `float64` accumulation; squared Euclidean L2 and COSINE; strict metric-specific range boundaries; deterministic score/ID ordering; full and capped cardinality.
- Collections: separate L2/COSINE × FLAT/HNSW collections; explicit primary-key/vector schema; batched ingestion; entity-count and first/last-vector read-back; synchronous index build and load checks.
- HNSW build parameters: `M=16`, `efConstruction=200`; query sweep `ef in [100, 200, 400, 800, 1600]`; `limit=100`; `Strong` consistency; no timed payload/vector output.
- Protocol: all FLAT/oracle semantic checks precede HNSW timing; 50-query unmeasured warm-up per configuration; five measured repetitions × 200 measured queries; deterministic randomized configuration and query orders; one synchronous outstanding request; timing begins immediately before search and ends after full response materialization; oracle work, diagnostics, and writes occur outside the timing boundary.
- Artifacts: immutable NumPy/JSON outputs, canonical generation manifest, per-artifact SHA-256 entries, `SHA256SUMS`, immutable run manifest containing timestamp/Git/environment image references and derived ordering seeds, raw JSONL, and post-timing summaries.
- Deliberate local failures: `ef < limit` is rejected before a backend request; an injected unreachable endpoint emits only an expected-failure record.

Dataset ID:

`DATASET-001` contract implemented but the production 10k/250 dataset was not generated in this task. Unit tests use small deterministic temporary datasets and the required semantic micro-fixtures only.

Hardware:

Not applicable to benchmark interpretation. Tests ran locally, but no Milvus search, performance measurement, container resource sample, or hardware comparison was performed.

Git commit:

Implementation working tree based on `0750017739b8d56f7c9337e33aa6a400afc841c8`; dirty/uncommitted at verification time. A benchmark execution must record the eventual committed harness revision and clean/dirty state in its immutable run manifest.

Random seed:

Production default `20260801`. Ordering streams derive explicit `uint64` seeds through `numpy.random.SeedSequence([20260801, stream_id])`; every seed and realized order is included in the future run manifest.

Metrics implemented:

Raw capped recall@threshold, client-observed latency, returned/full cardinality, cap state, failed queries, and threshold violations; post-timing aggregation computes recall distribution and 95% CI, per-repetition p50/p95 latency, QPS validity and distribution, cardinality diagnostics, p95 coefficient of variation, and five-repetition confidence intervals. These functions emitted no live benchmark values in this task.

Raw output location:

No `artifacts/exp-001/<UTC-run-id>/` benchmark directory was created. Offline verification output was emitted to the task terminal only. Exact command:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /tmp/vd-env001-pymilvus-compat.G6sKog/venv/bin/python -m unittest discover -s tests -v
```

Exact output:

```text
test_required_categories_and_exact_outputs (test_boundary_fixtures.BoundaryFixtureTests.test_required_categories_and_exact_outputs) ... ok
test_result_cap_reports_uncapped_cardinality (test_boundary_fixtures.BoundaryFixtureTests.test_result_cap_reports_uncapped_cardinality) ... ok
test_ef_below_limit_is_rejected_before_backend_call (test_config_schedule.ConfigurationScheduleTests.test_ef_below_limit_is_rejected_before_backend_call) ... ok
test_exact_36_configuration_matrix_and_ef_sweep (test_config_schedule.ConfigurationScheduleTests.test_exact_36_configuration_matrix_and_ef_sweep) ... ok
test_schedule_is_deterministic_and_has_exact_protocol_counts (test_config_schedule.ConfigurationScheduleTests.test_schedule_is_deterministic_and_has_exact_protocol_counts) ... ok
test_calibration_emits_three_finite_thresholds_per_metric (test_dataset_artifacts.DatasetArtifactTests.test_calibration_emits_three_finite_thresholds_per_metric) ... ok
test_every_written_artifact_is_checksummed_and_tampering_is_detected (test_dataset_artifacts.DatasetArtifactTests.test_every_written_artifact_is_checksummed_and_tampering_is_detected) ... ok
test_generator_is_deterministic_disjoint_and_little_endian_float32 (test_dataset_artifacts.DatasetArtifactTests.test_generator_is_deterministic_disjoint_and_little_endian_float32) ... ok
test_any_failed_query_invalidates_qps_comparison (test_metrics.MetricSummaryTests.test_any_failed_query_invalidates_qps_comparison) ... ok
test_required_metrics_are_derived_from_five_complete_repetitions (test_metrics.MetricSummaryTests.test_required_metrics_are_derived_from_five_complete_repetitions) ... ok
test_flat_setup_has_no_hnsw_build_parameters (test_milvus_adapter.MilvusAdapterTests.test_flat_setup_has_no_hnsw_build_parameters) ... ok
test_hnsw_setup_uses_pinned_schema_build_params_and_readback (test_milvus_adapter.MilvusAdapterTests.test_hnsw_setup_uses_pinned_schema_build_params_and_readback) ... ok
test_search_request_has_exact_range_params_limit_and_no_payload (test_milvus_adapter.MilvusAdapterTests.test_search_request_has_exact_range_params_limit_and_no_payload) ... ok
test_cosine_known_values_use_float64_output (test_oracle.ExactOracleTests.test_cosine_known_values_use_float64_output) ... ok
test_cosine_rejects_zero_norm_vectors (test_oracle.ExactOracleTests.test_cosine_rejects_zero_norm_vectors) ... ok
test_l2_is_squared_euclidean_with_float64_output (test_oracle.ExactOracleTests.test_l2_is_squared_euclidean_with_float64_output) ... ok
test_timing_stops_after_materialization_and_writes_after_timer (test_protocol.ProtocolTests.test_timing_stops_after_materialization_and_writes_after_timer) ... ok
test_unreachable_probe_records_expected_failure_and_never_success (test_protocol.ProtocolTests.test_unreachable_probe_records_expected_failure_and_never_success) ... ok

----------------------------------------------------------------------
Ran 18 tests in 0.076s

OK
```

Result:

The offline unit suite passed. This result verifies the tested harness behavior only. DATASET-001 artifacts were not generated, no collection was created, no request was sent to Milvus, and no EXP-001 acceptance criterion that requires live evidence is marked PASSED or VERIFIED.

Conclusion:

The harness is ready for human review and a separately authorized live smoke execution. The live run remains the research-validity gate for FLAT/Milvus agreement, HNSW behavior, index-identity stability, latency/QPS/cardinality evidence, and H1–H4 evaluation.

Follow-up actions:

1. Review the implementation diff and commit it after human approval.
2. In a separate authorized task, create DATASET-001 artifacts and verify all SHA-256 values before ingestion.
3. Complete the remaining EXP-001 runtime environment checklist, execute the live smoke run against verified ENV-001, and preserve raw evidence under a unique UTC run directory.

Post-review verification addendum (2026-08-01):

The offline suite was rerun after enforcing one shared measured-query permutation per repetition, per-segment HNSW index-identity checks, live boundary-preflight orchestration, complete pinned digest fields in the future run manifest, and exact 36-configuration schedule validation. No live Milvus request was made. Exact output:

```text
test_required_categories_and_exact_outputs (test_boundary_fixtures.BoundaryFixtureTests.test_required_categories_and_exact_outputs) ... ok
test_result_cap_reports_uncapped_cardinality (test_boundary_fixtures.BoundaryFixtureTests.test_result_cap_reports_uncapped_cardinality) ... ok
test_dataset_defaults_are_exactly_the_exp001_contract (test_config_schedule.ConfigurationScheduleTests.test_dataset_defaults_are_exactly_the_exp001_contract) ... ok
test_ef_below_limit_is_rejected_before_backend_call (test_config_schedule.ConfigurationScheduleTests.test_ef_below_limit_is_rejected_before_backend_call) ... ok
test_exact_36_configuration_matrix_and_ef_sweep (test_config_schedule.ConfigurationScheduleTests.test_exact_36_configuration_matrix_and_ef_sweep) ... ok
test_schedule_is_deterministic_and_has_exact_protocol_counts (test_config_schedule.ConfigurationScheduleTests.test_schedule_is_deterministic_and_has_exact_protocol_counts) ... ok
test_calibration_emits_three_finite_thresholds_per_metric (test_dataset_artifacts.DatasetArtifactTests.test_calibration_emits_three_finite_thresholds_per_metric) ... ok
test_every_written_artifact_is_checksummed_and_tampering_is_detected (test_dataset_artifacts.DatasetArtifactTests.test_every_written_artifact_is_checksummed_and_tampering_is_detected) ... ok
test_generator_is_deterministic_disjoint_and_little_endian_float32 (test_dataset_artifacts.DatasetArtifactTests.test_generator_is_deterministic_disjoint_and_little_endian_float32) ... ok
test_any_failed_query_invalidates_qps_comparison (test_metrics.MetricSummaryTests.test_any_failed_query_invalidates_qps_comparison) ... ok
test_required_metrics_are_derived_from_five_complete_repetitions (test_metrics.MetricSummaryTests.test_required_metrics_are_derived_from_five_complete_repetitions) ... ok
test_flat_setup_has_no_hnsw_build_parameters (test_milvus_adapter.MilvusAdapterTests.test_flat_setup_has_no_hnsw_build_parameters) ... ok
test_hnsw_setup_uses_pinned_schema_build_params_and_readback (test_milvus_adapter.MilvusAdapterTests.test_hnsw_setup_uses_pinned_schema_build_params_and_readback) ... ok
test_search_request_has_exact_range_params_limit_and_no_payload (test_milvus_adapter.MilvusAdapterTests.test_search_request_has_exact_range_params_limit_and_no_payload) ... ok
test_cosine_known_values_use_float64_output (test_oracle.ExactOracleTests.test_cosine_known_values_use_float64_output) ... ok
test_cosine_rejects_zero_norm_vectors (test_oracle.ExactOracleTests.test_cosine_rejects_zero_norm_vectors) ... ok
test_l2_is_squared_euclidean_with_float64_output (test_oracle.ExactOracleTests.test_l2_is_squared_euclidean_with_float64_output) ... ok
test_timing_stops_after_materialization_and_writes_after_timer (test_protocol.ProtocolTests.test_timing_stops_after_materialization_and_writes_after_timer) ... ok
test_unreachable_probe_records_expected_failure_and_never_success (test_protocol.ProtocolTests.test_unreachable_probe_records_expected_failure_and_never_success) ... ok
test_all_boundary_fixtures_are_loaded_and_compared_before_timing (test_runner_boundary_preflight.BoundaryPreflightTests.test_all_boundary_fixtures_are_loaded_and_compared_before_timing) ... ok

----------------------------------------------------------------------
Ran 20 tests in 0.052s

OK
```

### EXP-003: EXP-001 live smoke execution — environment-noise inconclusive

Status: **INCONCLUSIVE — PERFORMANCE INTERPRETATION PROHIBITED**
Date: 2026-08-01
Risk level: HIGH (live benchmark evidence with failed variance acceptance criterion and uncontrolled host workloads)

Objective:

Execute the EXP-001 range/threshold-query smoke contract against the verified Milvus 3.0.0 stack using the fixed Milvus adapter at commit `9f233e9bca598b5707d8dbdc7703be86fa3c3ad2`. Preserve the completed run even if an acceptance criterion fails; do not overwrite EXP-001 or the EXP-002 harness record.

Hypothesis:

The H4 reproducibility hypothesis is not supported by this run: 16 of 36 configurations exceeded the contract's maximum 20% p95-latency coefficient of variation. H1–H3 are not promoted to VERIFIED by this entry. Semantic and integrity observations remain preserved for review, but performance interpretation is prohibited.

Configuration:

- EXP-001's unchanged 36-configuration matrix: L2 and COSINE; thresholds `target-005`, `target-025`, and `target-075`; FLAT plus HNSW with `M=16`, `efConstruction=200`, and `ef in [100, 200, 400, 800, 1600]`.
- `limit=100`, `Strong` consistency, one synchronous client, 50 unmeasured warm-up queries per configuration, five measured repetitions, and 200 measured queries per repetition in the manifest-recorded deterministic randomized order.
- Milvus `3.0.0`, PyMilvus `3.0.1`, NumPy `2.5.1`, CPython `3.14.5`; Milvus image `milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862`; etcd image `quay.io/coreos/etcd:v3.5.25@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366`; MinIO image `minio/minio:RELEASE.2024-05-28T17-19-04Z@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08`.
- Docker Desktop `4.84.0`, Docker Engine `29.6.2`, Docker Compose `v5.3.1`; Docker VM limited to 6 vCPU, 6 GiB RAM, and 2 GiB swap; service limits remained those recorded by ENV-001.
- Manifest timestamp: `2026-08-01T15:43:56.078229+00:00` (`2026-08-01 21:13:56.078229 +05:30`). Manifest-recorded argv: `-c run --repository . --dataset-dir artifacts/exp-001/dataset --run-dir artifacts/exp-001/run-20260801T154343Z --collection-prefix exp001_20260801T154343Z`.

Dataset ID:

`DATASET-001-v1`: 10,000 base vectors; 50 calibration and 200 measured queries; 128-dimensional little-endian `float32`; NumPy `Generator(PCG64(20260801))`; independent standard-normal project-generated data. The run manifest embeds the verified dataset manifest and artifact SHA-256 values.

Hardware:

Apple M1 `arm64`, 8 logical CPU cores, 8 GiB host RAM, internal Apple SSD, macOS `26.5.2` build `25F84`, Darwin `25.5.0`. The host was not quiescent during this run.

Git commit:

`9f233e9bca598b5707d8dbdc7703be86fa3c3ad2`. The manifest records `dirty: true` because experiment/evidence paths were untracked; no tracked source modification was used for the run.

Random seed:

Primary seed `20260801`; realized configuration/query ordering and derived seeds are preserved in `run_manifest.json`.

Metrics measured:

The EXP-001 contract metrics for all 36 configurations: capped recall@threshold, per-repetition p50/p95 client latency, QPS, result cardinality, failed-query and threshold-violation diagnostics, p95 coefficient of variation, and five-repetition confidence intervals. Boundary semantics and per-segment/final index identity were also checked.

Raw output location:

`artifacts/exp-001/run-20260801T154343Z/` is preserved and was not overwritten or discarded.

- `run_manifest.json`: SHA-256 `975097ade292537bc69234a1712c9053c99570d4d584e72fa998b28eee8e31d9`
- `boundary_results.json`: SHA-256 `ce248654c0bd7027b68c01256f4826f98854fa988ae1d06a70a9a5ddc2e5d321`
- `raw_queries.jsonl`: SHA-256 `dc950432eb6bcd3712e38a907a8fc547fceb2d269541834dead8b18ea1fe5dbf`
- `summary.json`: SHA-256 `c913c0b976fd096b54860b3f44b5e8838f1c4309f06694818c2dc2ef93760529`

Result:

**INCONCLUSIVE.** The contract requires p95-latency coefficient of variation at or below 20% for every configuration. Sixteen of 36 configurations violated that criterion: **44.44%** of the matrix.

| Configuration | p95 coefficient of variation |
|---|---:|
| `COSINE:target-005:HNSW:ef=1600` | 33.5032% |
| `COSINE:target-025:HNSW:ef=100` | 22.9807% |
| `COSINE:target-025:HNSW:ef=200` | 26.8506% |
| `COSINE:target-025:HNSW:ef=400` | 31.0222% |
| `COSINE:target-025:HNSW:ef=800` | 39.4551% |
| `COSINE:target-075:HNSW:ef=800` | 38.1726% |
| `COSINE:target-075:HNSW:ef=1600` | 21.3579% |
| `L2:target-005:FLAT:ef=none` | 35.3366% |
| `L2:target-005:HNSW:ef=1600` | 20.4075% |
| `L2:target-025:HNSW:ef=100` | 22.2272% |
| `L2:target-025:HNSW:ef=200` | 26.5429% |
| `L2:target-025:HNSW:ef=400` | 38.4593% |
| `L2:target-025:HNSW:ef=1600` | 47.2053% |
| `L2:target-075:HNSW:ef=100` | 24.9446% |
| `L2:target-075:HNSW:ef=400` | 24.8700% |
| `L2:target-075:HNSW:ef=1600` | 21.0112% |

The run otherwise recorded zero failed measured queries, zero threshold violations, valid QPS comparisons for all 36 configurations, no per-segment or final HNSW index-identity mismatch, and FLAT/oracle agreement for every measured query. These are preserved observations, not a VERIFIED experiment status and not authority for latency/QPS comparisons.

Environment-noise audit:

- The committed pre-run resource snapshot was captured at `2026-08-01T14:25:06Z`, **78 minutes 50 seconds before** the run manifest timestamp. It honestly disclosed that the machine was not quiescent and explicitly prohibited latency interpretation until workloads were stabilized or re-disclosed immediately before execution. It was therefore honest as a timestamped preliminary disclosure, but it was not a valid snapshot of this specific run.
- No comprehensive host-process, browser, load, or thermal snapshot was taken immediately before or after this run. The specific run conditions were under-disclosed at execution time.
- A post-run audit of the macOS unified log for `2026-08-01 21:13:50` through `21:18:00 +05:30` proves that Google Chrome, Safari, Codex, Docker Desktop, Amphetamine, and a clipboard application were active. Chrome was actively playing media during the benchmark: playback was reported `Playing` at `21:14:45`, changed between paused/playing, and was again `Playing` at `21:17:19`. Safari held foreground/network-process assertions. Docker Desktop and the benchmark's Docker VM/services were necessarily active.
- The same log-window query found no ChatGPT or Antigravity event. Absence from the filtered unified log is not proof that every unlisted process was absent.
- No thermal/performance-warning event appeared in the audited window, and `pmset -g therm` shortly after the run reported no recorded warning. Because no run-adjacent temperature, frequency, fan, or thermal-pressure telemetry was captured, the actual thermal state remains **unknown** and cannot be treated as nominal evidence.

Conclusion:

EXP-003 is retained as an **INCONCLUSIVE** live run. It is not VERIFIED and is not discarded. The 44.44% configuration-level CV violation, confirmed concurrent browser/media activity, stale pre-run snapshot, and missing run-adjacent resource/thermal evidence prohibit performance interpretation, backend-tuning conclusions, or claims of superiority/optimality. The semantic and index-integrity outputs remain reviewable evidence only.

Follow-up actions:

1. Do not rerun until separately authorized.
2. Define and enforce a quiescence procedure: stop browser media and nonessential applications, disclose all retained workloads, and verify background indexing/update activity.
3. Capture immediate pre-run and post-run process, load, container CPU/RAM, power-source, and thermal-pressure snapshots tied to the next immutable run ID.
4. Repeat the unchanged EXP-001 contract under a new EXP ID and run directory; never overwrite `artifacts/exp-001/run-20260801T154343Z/` or this entry.
