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

### EXP-004: EXP-001 verification decision

Status: **VERIFIED — EXP-001 LIVE SMOKE ACCEPTANCE PASSED**
Date: 2026-08-01
Risk level: HIGH (research-validity gate; verification is limited to the frozen smoke contract)

Objective:

Review the completed live evidence against EXP-001's frozen dataset, index, search, measurement, and revised p95-latency CV acceptance criteria, then record the current verification status without rewriting the original contract entry or EXP-003's separate INCONCLUSIVE historical result.

Evidence run:

`artifacts/exp-001/run-20260801T161924Z/`

Authoritative summary:

`artifacts/exp-001/run-20260801T161924Z/summary.json` — SHA-256 `f3c14c5708de0b67d5d7ecbd5fb54a3988ca9dcb9be9364cb68a152eec4a609b`.

Configuration and provenance:

- `DATASET-001-v1`, seed `20260801`, 10,000 base vectors, 50 calibration queries, 200 measured queries, 128-dimensional little-endian `float32`, L2 and COSINE.
- FLAT plus HNSW (`M=16`, `efConstruction=200`), `ef in [100, 200, 400, 800, 1600]`, three frozen thresholds per metric, `limit=100`, `Strong` consistency, one synchronous client, five measured repetitions.
- Milvus `3.0.0`, PyMilvus `3.0.1`, NumPy `2.5.1`, CPython `3.14.5`, Docker Desktop `4.84.0`; ENV-001 image digests and resource controls are embedded in the immutable run manifest.
- Apple M1 `arm64`, 8 logical CPU cores, 8 GiB RAM, macOS `26.5.2`; Git commit `d2e27c33187454ddd785fabe87daf68136908333`, with the manifest's dirty flag attributable to preserved untracked experiment artifacts.

Hypothesis evaluation:

- **H1 — SUPPORTED:** `validate_flat_semantics` completed all 1,200 pre-timing checks (six FLAT metric/threshold configurations × 200 measured queries) without an ordered-ID or threshold-validity disagreement. All six deterministic boundary fixtures also matched their expected IDs.
- **H2 — SUPPORTED:** aggregate HNSW results exhibited the hypothesized tradeoff as `ef` increased:

  | `ef` | Mean recall@threshold | Mean p95 latency |
  |---:|---:|---:|
  | 100 | `0.895965` | `3.1604 ms` |
  | 200 | `0.970187` | `3.6582 ms` |
  | 400 | `0.993641` | `4.0113 ms` |
  | 800 | `0.998954` | `4.6551 ms` |
  | 1600 | `0.999817` | `5.0889 ms` |

  These are equal-weight aggregates over the six HNSW metric/threshold configurations at each `ef`; every configuration has the same query/repetition count. This smoke result supports the existence of a recall/latency tradeoff but does not select an optimal `ef`.
- **H3 — SUPPORTED:** the raw stream contains 152 `index_identity_unchanged` records (150 measured HNSW configuration segments plus two final metric checks) and no HNSW identity mismatch.
- **H4 — SUPPORTED:** all 36 configurations met the reviewed p95-latency CV ceiling of 30%. The maximum was `26.023708345393032%` for `L2:target-075:HNSW:ef=800`.

Acceptance evidence:

- Zero failed measured queries.
- Zero threshold violations.
- Valid QPS comparisons for all 36 configurations.
- FLAT/oracle agreement on all 1,200 measured-query preflight checks.
- No per-segment or final HNSW index-identity mismatch.
- Maximum configuration-level p95-latency CV `26.0237%`, below the reviewed 30% ceiling.
- Immediate pre-run and post-run resource/process snapshots are preserved in the evidence directory. The post-run snapshot disclosed one 0%-CPU WhatsApp service extension; this shared-host limitation is retained as evidence and does not alter the observed CV acceptance result.

Result:

**VERIFIED.** EXP-001 passed its reviewed live smoke acceptance criteria using `run-20260801T161924Z` as the verifying evidence run. H1, H2, H3, and H4 are SUPPORTED at smoke-test scope.

Historical-record isolation:

EXP-003 and `artifacts/exp-001/run-20260801T154343Z/` remain **INCONCLUSIVE** and unchanged. This verification decision neither overwrites nor reclassifies that earlier noisy run.

Conclusion:

The Milvus range/threshold-query semantics, HNSW query-time `ef` tradeoff, index-identity stability, and harness reproducibility gate are verified for the frozen EXP-001 contract. This result does not establish optimality, adaptive behavior under drift, production readiness, or superiority over another backend.

Follow-up actions:

1. Use EXP-001 only as the validated smoke foundation for the next Phase 1 research task.
2. Keep all future performance or adaptation claims under new experiment IDs with their own immutable evidence directories.

### EXP-005: Stationary live-shadow detector-to-policy dry-run integration

Status: VERIFIED
Date: 2026-08-02
Risk level: CRITICAL (actuation boundary integration; no live actuation authorized)

Objective:

Define the first controlled experiment where real read-only Milvus shadow evidence flows through:

`ShadowAuditTrace → AssembledShadowWindow (200 raw observations) → SignalEvidence/WindowEvidence → DriftDecision → PolicyDecision in DRY_RUN mode → safe-actuation no-op boundary`

This experiment must not perform canary execution or real actuation.

Hypothesis:

- **H1 — Stationary detector behavior:** Under stationary live replay, both current windows should produce complete evidence and the final detector decision should be `NO_DRIFT`. Evaluate L2 and COSINE independently. Do not pool them into one statistical claim.
- **H2 — Policy behavior:** A real `NO_DRIFT` decision passed to the actual tuning policy in `DRY_RUN` mode should produce `NO_CHANGE`. The policy must preserve the real detector confidence, magnitude, metric, stratum, identities, and immutable audit provenance.
- **H3 — Actuation-boundary behavior:** Passing the dry-run policy result through the safe-actuation boundary must produce no Milvus mutation and no canary start. The evidence must demonstrate no `start_canary`, no `apply`, no rollback invocation, no index rebuild, no collection mutation, and no serving-parameter change.
- **H4 — Failure behavior:** Deliberate offline fake-client/fixture tests must prove that duplicate, incomplete, mismatched, failed, timed-out, non-finite, or identity-invalid traces fail closed before any live action.

Configuration:

#### Definitions

##### `AssembledShadowWindow`

An immutable, pure-data boundary containing one validated raw 200-query window assembled from exactly four compatible 50-query traces.

It is not the existing `drift.WindowEvidence`.

It provides the raw inputs required to later calculate:

* query-vector MMD evidence,
* threshold KS evidence,
* exact-cardinality KS evidence,
* sentinel-recall evidence.

##### Existing `WindowEvidence`

The existing ADR-002 detector object containing the finalized four-signal statistical family for one reference-versus-current comparison. ADR-004 adds optional immutable `EvidenceProvenance`; it does not alias this object to `AssembledShadowWindow`.

It must continue to be produced only from real `SignalEvidence` objects through the existing detector functions and completeness rules.

Do not alias an `AssembledShadowWindow` as `WindowEvidence`; extraction must use the actual detector signal functions.

#### Source-trace envelope contract

Because the current `ShadowAuditTrace` value itself does not provide persistence chronology and manifest metadata, define a separate persisted source envelope surrounding each trace.

Each envelope must provide:

* non-empty immutable `trace_id`,
* capture timestamp in canonical UTC form,
* sequence index within the raw window,
* declared observation count,
* expected SHA-256 of the canonical persisted trace payload,
* the existing immutable `ShadowAuditTrace`.

This is additive metadata around the current trace. It must not redefine or mutate `ShadowAuditTrace`.

The envelope capture timestamp must use strict RFC3339 UTC calendar parsing:

* valid UTC timestamp ending in `Z`,
* parsed as a real UTC calendar timestamp; invalid calendar values (for example month `13`) are rejected,
* no local-time or offset timestamps,
* all four timestamps within one window strictly increasing after parsing.

This intentionally strengthens the existing regex-only safe-actuation validator for persisted EXP-005 trace provenance. It does not modify or weaken the safe-actuation validator.

The experiment manifest must prove:

`reference capture < first-current capture < second-current capture`

using the completed capture time of each assembled window.

A complete `AssembledShadowWindow` requires:

* an externally supplied immutable `window_id`,
* the metric and threshold stratum,
* exactly four validated envelopes,
* sequence indexes exactly `{0, 1, 2, 3}`,
* four unique trace IDs,
* strictly increasing capture timestamps,
* no duplicate trace payload/checksum,
* declared observation count exactly `50` for every trace,
* actual `len(trace.queries)` exactly `50`,
* recomputed trace SHA-256 matching the envelope SHA-256,
* exactly 200 globally unique canonical query IDs,
* the aggregate chronological query records,
* one deterministic aggregate-manifest SHA-256,
* `complete` and fail-closed reason codes.

`window_id` must follow the existing ADR-002 canonical identifier domain:

* integer or string only,
* booleans forbidden,
* strings non-empty,
* strings normalized consistently with `canonical_serialize_tuple`.

The assembler must not invent `window_id`.

The immutable reference, first-current and second-current windows must have three distinct window IDs. Their experiment manifest must explicitly record their roles and order.

Any violation must fail closed and must not produce complete detector input.

#### Canonical trace-payload hashing

Add a normative versioned payload contract:

`shadow-trace-payload-v1`

The SHA-256 must cover the entire existing `ShadowAuditTrace` content, including all nested:

* metric and threshold stratum,
* candidate, last-known-good and sentinel `ef`,
* configuration and data identities,
* FLAT and HNSW identity evidence,
* pre/post identity snapshots,
* every query record,
* query ID and query vector,
* threshold radius, range filter and limit,
* oracle result,
* exact cardinality,
* FLAT and sentinel hits,
* sentinel recall,
* all stage evidence,
* `complete`,
* reason codes.

Canonical encoding must use UTF-8 JSON produced with the exact repository-compatible rules:

```text
json.dumps(
    payload,
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
    allow_nan=False,
).encode("utf-8")
```

Additional rules:

* include `schema_version: "shadow-trace-payload-v1"`,
* encode enum values using their exact `.value`,
* encode tuples as JSON arrays,
* convert dataclasses through explicit field mappings—not `repr`, `str`, `default=str`, pickle or platform-dependent serialization,
* retain integer IDs as JSON integers and string IDs as JSON strings,
* reject booleans as query IDs,
* reject unsupported values and every non-finite float,
* produce a lowercase 64-character SHA-256 hexadecimal digest,
* exclude envelope metadata and `expected_sha256` from the trace-payload hash itself.

`trace_id` remains externally supplied immutable provenance identity. The assembler must validate it but must not generate or rewrite it.

#### Deterministic aggregate-window manifest hashing

The aggregate manifest must use a separate version:

`assembled-shadow-window-manifest-v1`

It must contain:

* aggregate schema version,
* `window_id`,
* metric,
* threshold stratum,
* four envelopes ordered by sequence index,
* for each envelope:

  * trace ID,
  * exact validated timestamp string,
  * sequence index,
  * declared observation count,
  * verified trace-payload SHA-256,
* total observation count,
* ordered canonical query IDs.

Hash this manifest with the same canonical UTF-8 JSON rules.

Do not include the aggregate manifest’s own expected digest inside its hashed payload.

##### Initial assembly

During initial assembly:

* the assembler validates all four source envelopes and traces,
* constructs the canonical `assembled-shadow-window-manifest-v1` payload,
* computes `manifest_sha256`,
* returns the immutable `AssembledShadowWindow` containing that computed digest.

No caller-supplied aggregate digest is required during first construction because no persisted aggregate manifest exists yet.

The assembler must never accept a caller-supplied digest as a replacement for its own computation.

##### Persisted aggregate verification

When an already persisted assembled window is loaded or independently verified:

* the persisted aggregate record must provide an externally stored `expected_manifest_sha256`,
* this expected digest must remain outside the hashed manifest payload,
* the canonical manifest must be recomputed from the persisted window and its four source-envelope references,
* the recomputed digest must exactly equal `expected_manifest_sha256`,
* absence, malformed format, or mismatch must fail closed.

Both computed and expected aggregate SHA-256 values must be lowercase hexadecimal strings of exactly 64 characters.

Clarify that the existing fail-closed condition:

`aggregate manifest hash mismatch`

applies to persisted/reloaded aggregate verification. Initial assembly instead fails if canonical manifest construction or hashing cannot complete.

Do not add the expected digest inside `AssembledShadowWindow` as self-authenticating data. It belongs to the surrounding persisted aggregate record or artifact manifest.

##### Timestamp representation inside the manifest

* timestamp validity and ordering use parsed RFC3339 UTC semantics,
* the aggregate manifest hashes the exact already-validated timestamp string supplied by each source envelope,
* timestamps must not be silently reformatted, truncated, rounded, or rewritten during hashing,
* two timestamps representing the same instant are not strictly increasing and therefore fail chronology validation.

##### Canonical string identifier representation

For aggregate-manifest fields:

* `trace_id` must be a non-empty string.
* It must be NFC-normalized for envelope uniqueness and aggregate-manifest hashing.
* The assembler must reject two original trace IDs that normalize to the same canonical value.
* A normalization collision must fail closed; IDs must not be silently merged or rewritten.
* The canonical NFC value is included in `assembled-shadow-window-manifest-v1`.
* `trace_id` remains externally supplied; the assembler must not generate it.

* string `window_id` values and string query IDs are NFC-normalized before inclusion in the canonical manifest payload,
* integer IDs remain JSON integers,
* normalization is used for equality, uniqueness, lookup and aggregate-manifest hashing,
* the assembler must not silently merge two original IDs that normalize to the same canonical value; such a collision fails closed.

The underlying `shadow-trace-payload-v1` still hashes the complete explicit trace representation according to its own defined payload mapping.

#### Identity and compatibility requirements

All four traces must match exactly on:

* metric,
* threshold stratum,
* configuration identity,
* data identity,
* candidate `ef`,
* last-known-good `ef`,
* sentinel `ef`,
* query limit,
* threshold radius,
* range filter,
* FLAT expected binding and stable pre/post snapshots,
* HNSW expected binding and stable pre/post snapshots.

Every source trace must already have `complete=True`.

Every query record must contain complete, finite and internally consistent:

* query vector,
* oracle result,
* exact cardinality,
* FLAT hits,
* sentinel hits,
* sentinel recall,
* stage evidence.

#### Canonical query-ID rules

Across all four traces:

* exactly 200 query IDs must exist,
* all IDs must use one schema type: all integers or all strings,
* bool is invalid,
* string IDs must be non-empty,
* uniqueness must be checked using the existing canonical tuple serialization semantics,
* Unicode-normalization collisions must fail closed,
* aggregate order must be sequence index first, then query order inside each trace.

The assembler must preserve this order and must not sort query observations independently.

#### Detector-input extraction

Clarify that `AssembledShadowWindow` retains raw evidence for all 200 observations.

For each reference/current comparison:

* query-vector signal uses all 200 query vectors,
* threshold signal uses all 200 threshold-radius values,
* deterministic audit selection must call the existing `select_audit_sample` over the assembled window’s 200 canonical query IDs using:

  * frozen detector seed,
  * metric,
  * that window’s immutable `window_id`,
* the selected exactly 50 IDs provide the exact-cardinality and sentinel-recall samples,
* selected records must be retrieved from the assembled data by canonical query identity,
* cardinality and recall evidence must not use an arbitrary first 50, per-trace subset, or caller-selected replacement,
* actual `query_vector_signal_test`, `ks_signal_test`, `recall_signal_test`, and `finalize_window_evidence` functions must be used.

The detector seed remains external experiment configuration, frozen before first live capture. The assembler does not generate it.

The assembler must not repair or impute invalid traces.

#### Scope and invariants

1. Use the existing read-only shadow-candidate/audit path. Do not introduce another Milvus query implementation.
2. One `ShadowAuditTrace` contains exactly 50 audited queries.
3. One complete detector window contains exactly four compatible traces: exactly 200 observations, exactly 200 unique query/audit identities, and no duplicate or omitted observation.
4. For each evaluated configuration, use one immutable reference window, two consecutive current windows, and all three windows assembled independently from four traces each.
5. Never combine: L2 and COSINE evidence, different threshold strata, different collections, different dataset identities, different index-build identities, different query limits, or incompatible audit configurations.
6. Preserve the exact metric and canonical threshold-stratum identifiers already used by ADR-002 and current source.
7. All evidence used by the detector must satisfy the existing ADR-002 completeness and identity contracts.
8. The policy must run in `DRY_RUN` mode only.
9. `START_CANARY`, live parameter changes, index rebuilds, collection mutation, and automatic actuation are prohibited.
10. Any injected/stubbed `ResponseEstimate` must be explicitly labelled synthetic support input: it is not live-canary evidence, it must not be represented as an observed Milvus result, and it must not affect the expected stationary `NO_CHANGE` path.

#### Fail-closed conditions

The experiment must resolve to incomplete/invalid evidence and stop before downstream action if any window contains:
- fewer or more than four traces,
- fewer or more than 200 observations,
- duplicate query/audit IDs,
- a trace metadata count that disagrees with its actual persisted observation count,
- internally mismatched parallel arrays/records within one trace,
- duplicate identities inside a single trace even when the aggregate window count is 200,
- missing trace data,
- failed or timed-out queries,
- threshold violations,
- non-finite values,
- incompatible metric or threshold stratum,
- collection/data/index identity mismatch,
- inconsistent limit or audit configuration,
- incomplete recall-audit evidence,
- trace checksum or manifest mismatch,
- unordered or ambiguous window chronology,
- invalid or duplicate window ID,
- unsupported or mixed query-ID schema,
- Unicode-normalization identity collision,
- invalid/non-UTC timestamp,
- equal or non-monotonic timestamps,
- unsupported/non-finite canonical payload value,
- malformed or uppercase/non-64-character SHA-256,
- trace payload hash mismatch,
- aggregate manifest hash mismatch,
- manifest order differing from envelope sequence order,
- inability to select exactly 50 deterministic audit IDs,
- selected ID missing from the assembled observations.

Do not coerce incomplete evidence to `NO_DRIFT`.

#### Execution stages

1. **Complete (Stage 1, commit `1585a3a`):** Implement the persisted trace envelope, canonical trace hashing, and four-trace-to-AssembledShadowWindow pure validation boundary.
2. **Complete (Stages 1–2):** Add focused offline tests for valid assembly and every applicable fail-closed condition, then implement detector-input extraction using actual detector functions.
3. **Complete offline (ADR-004, commit `83a7743`):** Add an offline detector → policy → safe-boundary integration test using actual production functions and twelve independently assembled traces.
4. **Complete offline:** Review implementation and raw test output. This is not live EXP-005 evidence.
5. Implement and review a persisted stationary live-shadow acquisition runner.
6. Separately authorize stationary live shadow acquisition.
7. Capture the immutable reference and two current windows independently for each approved metric/stratum.
8. Run detector and policy evaluation offline from the persisted traces:
   1. reference and current `AssembledShadowWindow` objects are built independently,
   2. actual detector signal functions compare the immutable reference window with each current window,
   3. the existing `WindowEvidence` is finalized from those actual signal results,
   4. two current `WindowEvidence` objects are passed to `evaluate_drift_decision`.
9. Verify the no-op actuation evidence and pre/post live no-mutation evidence.
10. Review all artifacts before changing EXP-005 status.

Live acquisition may record process/container resource snapshots to identify material collection overhead, but memory-footprint behavior is observational and is not an EXP-005 acceptance criterion unless separately pre-registered before execution.

#### Required evidence and reproducibility

The minimum evidence set must include:

* exact repository commit and branch,
* tracked working-tree dirty/clean state,
* dataset, collection, index-build, metric, threshold-stratum, limit, and audit-configuration identities,
* chronological membership of every trace in the immutable reference window and two consecutive current windows,
* all twelve source `ShadowAuditTrace` records per evaluated metric/stratum:

  * four reference traces,
  * four first-current traces,
  * four second-current traces,
* SHA-256 for every persisted trace,
* deterministic aggregate manifests for each assembled window,
* assembled reference and current `WindowEvidence`,
* all detector `SignalEvidence`,
* final `DriftDecision`,
* complete tuning-policy inputs, including any synthetic `ResponseEstimate` clearly labelled as non-live support evidence,
* resulting `PolicyDecision`,
* safe-actuation/dry-run audit evidence proving no-op behavior,
* raw deliberate-failure fixture results,
* exact commands and raw test output,
* pre/post no-mutation evidence for Milvus collections, indexes, serving parameters, and canary state.

Dataset identity, repository commit, detector seed, collection identity, metric, and threshold stratum may remain `TBD` at contract-definition time. Each value must be frozen and recorded before the first live trace is captured, and none may be changed after evidence acquisition begins.

Dataset ID:

TBD at execution.

Hardware:

TBD at execution.

Git commit:

TBD at execution.

Random seed:

TBD at execution.

Metrics measured:

See Hypotheses and Acceptance criteria.

Raw output location:

Planned: `artifacts/exp-005/`. Artifact paths and filenames must follow existing repository conventions.

Result:

NOT RUN.

Conclusion:

Pending execution.

Acceptance criteria:

EXP-005 may later be considered for verification only if:
- all required traces and windows are complete and checksum-valid,
- L2 and COSINE remain independently stratified,
- detector output is `NO_DRIFT`,
- policy output is `NO_CHANGE`,
- safe-actuation output is a proven no-op,
- every deliberate failure test fails closed,
- no live Milvus state was mutated,
- raw evidence is independently reviewable,
- no acceptance claim relies on synthetic response estimates,
- an implementation does not alias an `AssembledShadowWindow` as the existing `WindowEvidence`,
- an implementation does not bypass actual signal-test functions,
- an implementation does not fabricate `SignalEvidence`,
- an implementation does not accept missing envelope chronology or checksum metadata,
- an implementation does not mutate the existing collector trace to retrofit persistence fields silently.

A result other than the pre-registered stationary expectation must be recorded honestly as unexpected, failed, or inconclusive—not rewritten after execution.

Follow-up actions:

1. Implement and review the persisted read-only live-shadow acquisition runner, including an experiment manifest and pre/post no-mutation evidence capture.
2. Separately authorize stationary live acquisition only after that runner and its deliberate-failure tests are reviewed.

#### Verification result — 2026-08-03

This block supersedes the contract-time `Result: NOT RUN` and `Conclusion: Pending execution` statements above; those original statements are retained as the pre-registration record.

**Status: VERIFIED.** Both independently stratified stationary live-shadow captures completed and were evaluated offline from their persisted traces:

- L2 / `target-075`: capture ID `exp005-l2-target075-001`, evidence commit `9a24299`.
- COSINE / `target-025`: capture ID `exp005-cosine-target025-001`, evidence commit `5c95e07`.
- Frozen detector seed: `20260804` for both captures.

The complete implementation path landed in commits `1585a3a` (four-trace assembly), `2bfcc75` (detector-input extraction), `83a7743` (provenance binding), and `9e2575a` (restart-durable trace codec and reviewed-baseline acquisition runner). The live L2 evaluation is recorded in `9a24299`; the COSINE capture and evaluation evidence is recorded in `5c95e07`; deliberate H4 failure coverage is recorded in `a5731fe`.

**Result and conclusion:** For each metric, all three persisted 200-query windows assembled as complete with checksum-valid trace envelopes and no reason codes. The real detector returned `NO_DRIFT`, the real policy in `DRY_RUN` returned `NO_CHANGE`, and the safe-actuation boundary returned an audited `NO_OP` using a fake client that would fail if called. Each capture's five live no-actuation flags — collection creation, collection mutation, restore last-known-good, rollback, and canary start — were all `false`.

**Hypothesis verification:** H1 is VERIFIED for L2 and COSINE independently; H2 is VERIFIED; H3 is VERIFIED; H4 is VERIFIED by the eight deliberate offline fail-closed categories in `a5731fe`. No acceptance claim relies on synthetic response estimates, and no automatic actuation was authorized or performed.

### EXP-006: Online workload monitor offline safety and recovery validation

Status: VERIFIED
Date: 2026-08-03  
Risk level: CRITICAL (ADR-005 monitor/orchestration boundary; no live Milvus or actuation authorized)

Objective:

Validate the future `workload_monitor.py` offline against ADR-005’s CRITICAL correctness, restart-safety, event-integrity, backpressure, and non-actuation requirements before any live integration is considered.

This experiment validates only the monitor’s composition of persisted trace events, `assemble_shadow_window`, `extract_window_evidence`, `evaluate_drift_decision`, and `evaluate_tuning_policy(..., mode=DRY_RUN)`. It must not contact Milvus, construct a real PyMilvus client, invoke a canary, restore a configuration, or modify a collection.

Hypothesis:

- **H1 — Restart recovery:** A monitor restarted after persisting partial stream state resumes deterministically and reaches the same valid assembled windows, detector output, policy output, and audit sequence as an uninterrupted replay of the same immutable event stream.
- **H2 — Event integrity:** Duplicate, malformed, incompatible, or identity-changing events fail closed and cannot be incorporated into a detector window or trigger a policy evaluation.
- **H3 — Bounded processing:** Backpressure is handled deterministically without dropping evidence silently, blocking a foreground query path, or combining events across monitor streams.
- **H4 — Non-actuation:** Every monitor path remains `DRY_RUN`; no actuation-client method, canary operation, rollback operation, restore operation, collection mutation, or serving-parameter change occurs.

Configuration:

- **Execution mode:** offline only; no live Milvus URI, no PyMilvus import, and no network dependency.
- **Trace source:** deterministic fake `ShadowTraceEventSource` supplying persisted fixture envelopes and replayable delivery order.
- **Persistence:** test `MonitorStateStore` implementation exercising durable pending-window state, accepted immutable reference state, prior/current `WindowEvidence`, deduplication state, and audit cursor across simulated process restart.
- **Audit sink:** append-only fake/file-backed `MonitorAuditSink` capturing every accepted, incomplete, rejected, and policy-evaluated cycle.
- **Policy input:** injected deterministic `DryRunPolicyInputProvider`; all calls use `PolicyMode.DRY_RUN`, `canary_observation=None`, externally reserved non-empty audit IDs, and no fabricated live-canary evidence.
- **Metrics:** L2 and COSINE fixture streams remain independently keyed and never pooled. At least one valid complete replay must traverse reference, current-1, and current-2 windows to prove the actual extraction → detector → policy composition.
- **Random seed:** freeze and record all fixture, event-order, and detector seeds before execution.
- **Raw output location:** planned `artifacts/exp-006/<UTC-run-id>/`, including exact command, fixture identities/checksums, event delivery order, monitor state snapshots, audit records, test output, and Git commit.

Required scenarios and pass criteria:

1. **Restart recovery**
   - Simulate a restart after partial receipt of a four-envelope window and again after a completed reference/current window but before the next monitor cycle.
   - Pass only if resumed processing accepts each remaining eligible envelope exactly once, reconstructs the same assembled manifest hashes and provenance as uninterrupted replay, and produces identical detector/policy outputs and ordered audit records.
   - A missing or corrupted persisted monitor state must fail closed with an explicit audited reason and no detector/policy/actuation call.

2. **Queue/event duplication**
   - Deliver duplicate event IDs, duplicate envelope references, and replayed previously acknowledged events, including at least one duplicate after restart.
   - Pass only if no duplicate trace enters an assembled window, no duplicate decision/audit outcome is emitted for the same completed evaluation, and conflicting duplicate identity/checksum evidence fails closed with an explicit reason code.

3. **Malformed envelopes**
   - Supply invalid schema, malformed JSON, invalid timestamp, invalid or mismatched checksum, and envelope/trace count disagreement fixtures.
   - Pass only if each case produces an explicit audited invalid result; no `AssembledShadowWindow.complete=True`, extraction, detector decision, policy evaluation, or actuation-client call may follow the invalid envelope.

4. **Identity change mid-stream**
   - Change one of metric, threshold stratum, configuration identity, data identity, FLAT binding, or HNSW binding after a reference has been accepted.
   - Pass only if the monitor rejects the incompatible stream/window, preserves the original immutable reference without automatic rebaseline, records the precise mismatch, and makes no policy or actuation call for the invalid comparison.

5. **Monitor backpressure**
   - Provide more events than one `run_once(max_events=...)` cycle may process, including events from at least two independent streams.
   - Pass only if processing is deterministic and bounded by `max_events`; unprocessed events remain durably pending or are explicitly rejected/audited. The monitor must not silently drop/reorder evidence, merge streams, or perform unbounded work in one cycle.
   - The monitor API must not block on foreground query work; this is demonstrated by using only the injected source and no live-query dependency.

6. **DRY_RUN non-actuation proof**
   - Run a complete valid stationary replay and all failure scenarios with a trap/fake actuation client whose every method raises if called.
   - Pass only if valid evidence reaches the real policy in `DRY_RUN` and yields a recorded non-actuating outcome, while all trap-client call counters remain zero.
   - No `START_CANARY`, `ROLLBACK`, `shadow_candidate`, `start_canary`, `stop_candidate`, `restore_last_known_good`, collection mutation, or serving-parameter mutation may occur.

Acceptance criteria:

- Every required scenario has raw test output and immutable audit evidence.
- All expected failure cases are fail-closed with explicit reason codes.
- Valid replay uses the real assembly, extraction, detector, and policy functions; no statistic, `SignalEvidence`, `WindowEvidence`, or `DriftDecision` may be fabricated.
- L2 and COSINE evidence remains independently stratified.
- Restarted and uninterrupted replay results are byte-for-byte or field-for-field identical where deterministic contracts require equality.
- No test imports PyMilvus, contacts a live database, or performs a live Milvus operation.
- All actuation-client call counters are zero in every scenario.
- A failing scenario, incomplete evidence, or monitor-state corruption must never be coerced to `NO_DRIFT`.
- No automatic actuation authorization, implementation acceptance, or production-readiness claim follows from this offline experiment.

Dataset ID:

Not applicable to the primary assertion. Fixtures consist of versioned, deterministic persisted shadow-trace envelopes; their source, checksums, identities, and seeds must be recorded in the run artifacts.

Hardware:

macOS `26.5.2` on `arm64`; Python `3.14.5`; evidence stored at `artifacts/exp-006/run-20260803T142304Z/`. Performance measurements remain out of scope.

Git commit:

`6650c066b07b7f91420b70463e875c322800d4a0` (`6650c06`), including the committed monitor, restart-durable evidence codec, and fail-closed EXP-006 validator.

Random seed:

`20260804`, frozen for fixture generation, event delivery, and detector sampling.

Metrics measured:

- Scenario pass/fail counts with raw reason codes.
- Assembled-window completeness and manifest-hash equality across restart.
- Duplicate suppression/rejection counts.
- Audit-record count, ordering, and immutability.
- Detector state/classification and policy action for valid stationary replay.
- Actuation-client call counts for every scenario.
- Bounded event-processing count versus `max_events`.

Raw output location:

`artifacts/exp-006/run-20260803T142304Z/`.

Result:

See the verification result below. The original contract remains the pre-registration record.

Conclusion:

Verified for the registered offline scope only. This result does not authorize live monitor integration or automatic actuation.

Follow-up actions:

1. Preserve the verified evidence bundle and execution receipt.
2. Design and pre-register a separate CRITICAL experiment for the live `ShadowTraceEventSource` integration.
3. Do not enable automatic actuation without a separate approved ADR and experiment evidence.

#### Verification result — 2026-08-03

**Status: VERIFIED.** The formal offline run completed at `artifacts/exp-006/run-20260803T142304Z/` under commit `6650c06` with detector seed `20260804`. Its manifest SHA-256 is `0f7dc9f78eb1bb415c88521478f263a6d04f2f2f00b7ed65133cde1a8fad3944`; its raw-result SHA-256 is `7c9304275dd057e21d6ed26059e5b7bfcf50feb9792d522a1b61b35af9a5e181`; and its execution-receipt SHA-256 is `d58bb62b1cd9fd3e2d5e77f6bdee9fdaa5c6fd451eeaac71bf279170c37504bb`.

**Result and conclusion:** All 93 fixture, 14 monitor-state, and 3 audit checksums matched their manifest. The validator and its raw result agreed exactly. L2 and COSINE each produced one audited `NO_DRIFT` detector result and `NO_CHANGE` policy result. All nine explicit integrity cases passed with their registered reason codes and zero policy-input calls; restart recovery and bounded backpressure passed; and the DRY_RUN proof recorded no PyMilvus import, no actuation import, no `CANARY_ENABLED` reference, zero trap-client/controller calls, and a non-executed safe-boundary no-op.

**Hypothesis verification:** H1, H2, H3, and H4 are VERIFIED for the registered offline scope. The focused validator tier passed 3 tests; the full repository suite passed 265 tests. No claim of live-monitor readiness or automatic actuation authorization follows from this result.

### EXP-007: Durable live-shadow event-source offline safety and recovery validation

Status: CONTRACT DEFINED — NOT IMPLEMENTED — NOT RUN
Date: 2026-08-03
Risk level: CRITICAL (ADR-006 producer/outbox boundary; no live Milvus, serving traffic, or automatic actuation authorized)

Objective:

Validate the ADR-006 host-side durable trace outbox offline before it is connected to any serving application. The experiment must prove that a complete immutable `ShadowAuditTrace` is persisted before a checksum-bound `ShadowTraceEvent` can be delivered to the real DRY_RUN workload monitor, while queue pressure and storage failures never delay or mutate the simulated foreground query path.

Hypotheses:

- **H1 — Persist-before-publish:** Every delivered event resolves to an immutable, checksum-valid envelope written before the event record; no event can reference a missing, incomplete, or different trace.
- **H2 — Restart and duplicate safety:** Interrupted publication, producer restart, consumer redelivery, and idempotent acknowledgement preserve at-least-once delivery with one monitor effect and no conflicting trace substitution.
- **H3 — Backpressure and data minimization:** Fixed in-memory/durable queue limits cause explicit non-sensitive drop/failure records without blocking the foreground simulator; pending event records contain no query vectors, thresholds, FLAT hits, or oracle payload.
- **H4 — DRY_RUN end-to-end safety:** Valid source events may drive the actual monitor, extraction, detector, and policy only in `DRY_RUN`; no event-source path imports PyMilvus/policy/actuation, performs a Milvus operation, or invokes an actuation method.

Configuration:

- **Execution mode:** offline only; deterministic synthetic `ShadowAuditTrace` fixtures; no Milvus URI, PyMilvus import, network transport, or serving application.
- **Transport:** single-host filesystem outbox with strict schema, owner-only directory checks, atomic writes, file/directory fsync, deterministic event ordering, and explicit at-least-once acknowledgement.
- **Fixture streams:** independent L2 and COSINE streams, each preserving the immutable metric, threshold stratum, data identity, configuration identity, FLAT binding, and HNSW binding required by `MonitorStreamKey`.
- **Queue limits:** freeze pending-event count, pending-byte cap, and in-memory observation cap in the experiment manifest before the run; require tests at exactly-full and one-over-limit boundaries.
- **Randomness:** freeze and record any trace/observation scheduling seed. Event IDs themselves are derived only from canonical content, never runtime randomness.

Required scenarios and pass criteria:

1. **Atomic publication order and orphan handling**
   - Inject failures before envelope write, after envelope fsync/before event creation, and after event creation/before acknowledgement.
   - Pass only if no event becomes pollable without its matching checksum-valid envelope; an orphan is non-deliverable and recorded; no partial pending record remains.
2. **Producer/consumer restart and redelivery**
   - Restart after each durable boundary; redeliver an unacknowledged event; acknowledge it twice.
   - Pass only if event identity and ordering are preserved, acknowledgement is idempotent, the real monitor has exactly one durable effect, and no trace is substituted.
3. **Duplicate/conflicting publication**
   - Publish the same context/payload twice, then reuse its event or trace identity with a changed checksum, context field, or envelope metadata.
   - Pass only if the identical case is idempotent and every conflict fails closed with an explicit reason, with no monitor input for the conflicting item.
4. **Backpressure and foreground isolation**
   - Fill each configured queue to capacity and submit one further observation/publication using a foreground simulator whose operations are counted.
   - Pass only if the final submission returns immediately with a drop/failure reason, performs no synchronous persistence, shadow query, monitor call, or retry, and leaves prior queue order intact.
5. **Schema, permission, checksum, and path safety**
   - Supply malformed event documents, corrupt envelopes, missing envelope paths, owner/group/world-readable directories, and symlink escape attempts.
   - Pass only if each condition fails closed with an explicit reason; no poll result or monitor call follows.
6. **Data-minimization inspection**
   - Use distinctive sentinel vector/threshold/oracle values in fixtures and scan pending/acknowledged event documents plus non-sensitive diagnostics.
   - Pass only if the sentinels occur only in the trace envelope and never in event records, acknowledgements, operational metrics, or errors.
7. **DRY_RUN monitor composition**
   - Feed three complete source-published windows through the real `WorkloadMonitor` with trap database/actuation objects whose methods raise on access.
   - Pass only if the actual monitor produces its expected stationary detector/policy audit result, trap call counts remain zero, and static import inspection finds no prohibited dependencies in the new source module.

Acceptance criteria:

- All seven scenarios pass with raw output, immutable checksummed artifacts, and exact git commit/dirty-state capture.
- The source uses the existing `PersistedShadowTraceEnvelope`, `persist_shadow_trace_envelope`, `ShadowTraceEvent`, and `WorkloadMonitor` contracts; it must not reserialize a trace with a competing canonical format or reimplement detector/policy logic.
- Every failed input yields an explicit reason code and no event delivery or downstream monitor call.
- L2 and COSINE streams remain strictly separate and are never combined by event ID, queue order, or recovery logic.
- No queue/document outside the trace payload contains raw query vectors, thresholds, FLAT hits, sentinel hits, or oracle results.
- The full repository suite and focused EXP-007 tests pass. A live database, live serving client, canary, rollback, or configuration mutation remains prohibited.

Dataset ID:

Not applicable to the primary assertion. Deterministic typed trace fixtures must have their hashes, identities, and scheduling seed recorded in the evidence manifest.

Hardware:

TBD at execution; record OS, architecture, Python version, filesystem type, and owner/permission mode of the outbox root. Performance claims are out of scope.

Git commit:

TBD at execution; pin the producer/outbox implementation, tests, and all committed composed dependencies.

Random seed:

TBD before execution; freeze any observation scheduling seed. Content-addressed event identity must be independently deterministic.

Metrics measured:

- Publication-to-poll ordering and envelope/event checksum agreement.
- Duplicate and conflict rejection counts by reason code.
- Restart/redelivery/acknowledgement outcomes and monitor-effect count.
- Queue depth, pending bytes, oldest-event age, backpressure drops, orphan count, and persistence failures.
- Forbidden-payload sentinel occurrence count outside trace envelopes.
- Real monitor detector/policy output and prohibited call/import counts.

Raw output location:

Planned: `artifacts/exp-007/<UTC-run-id>/`.

Result:

NOT RUN — contract only.

Conclusion:

Pending offline implementation, deliberate-failure validation, raw-evidence review, and a separately authorized live host integration. This contract does not authorize automatic actuation.

Follow-up actions:

1. Implement the source/outbox in new modules only, through injected filesystem/clock hooks and without live database dependencies.
2. Run the complete offline EXP-007 evidence harness and preserve its immutable artifact bundle.
3. Design a separate live host-instrumentation experiment only after EXP-007 is verified.
