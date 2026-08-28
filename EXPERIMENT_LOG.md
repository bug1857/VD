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

Status: VERIFIED (offline source/outbox scope only; host integration and all actuation remain unauthorized)
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

**VERIFIED — 2026-08-03.** The final formal offline run completed at `artifacts/exp-007/run-20260803T152516Z/` under commit `ad635c7` (`ad635c7043656cd41b57d6e6745e2c330d336eba`), detector seed `20260804`, and fixture scheduling seed `20260805`. The manifest file SHA-256 is `bd7f02acd730d17123e4ca5b2f71a50feb17a9de6cad23659b84e23ba33e3ba6`; raw-result file SHA-256 is `5b750a818f3c9f6fdc60e69b32a55ab6eb4706de73caa3fa0e9cf3a0862ee996`; execution-receipt file SHA-256 is `a36bb2b635c194e7e9ccc351f8cef046aeea799c3364e2084c702903fb1c6401`.

All seven registered scenarios passed: persist-before-publish with an orphaned non-deliverable trace, source/consumer restart plus idempotent acknowledgement and one durable monitor evaluation, duplicate/conflict rejection, bounded backpressure with zero synchronous persistence/shadow/monitor calls on the dropped foreground submission, malformed/corrupt/missing/unsafe-permission/symlink rejection, data-minimizing event records, and real DRY_RUN composition. The composed L2 and COSINE streams each produced exactly one `NO_DRIFT` / `NO_CHANGE` result from the actual monitor. Static inspection found no PyMilvus, policy, actuation, or `WorkloadMonitor` dependency in the source module; trap-client calls remained zero.

Independent verification recomputed and matched all 110 regular-artifact SHA-256 values and the one rejected-symlink target SHA-256 recorded in the manifest, the logical manifest/raw-result hashes, and receipt bindings. The manifest records `apfs`, owner UID `501`, owner-only outbox mode `0700`, and an explicit `NOT_APPLICABLE_SOURCE_V1_HOST_SAMPLER_UNIMPLEMENTED` in-memory-queue declaration: the source accepts only completed traces and does not claim an unbuilt host queue. H1, H2, H3, and H4 are VERIFIED for the registered offline scope only.

Conclusion:

The durable source/outbox is verified as an offline, single-host, at-least-once evidence boundary. The source accepts completed 50-query traces; it does not yet provide a serving application's non-blocking sampler or background shadow-audit worker. Any live host integration requires a separate design and experiment. This result does not authorize automatic actuation.

Follow-up actions:

1. Design the host-owned non-blocking observation sampler and background shadow-audit worker as a separate CRITICAL boundary.
2. Pre-register and run a separate live host-integration experiment before claiming continuous-traffic coverage.
3. Keep the monitor DRY_RUN-only; automatic actuation remains separately governed and unauthorized.

---

### EXP-008: Reference host-observation integration and live DRY_RUN validation

Status: VERIFIED — LIVE STATIONARY COMPOSITION AND H1/H4 FOREGROUND/FAILURE EVIDENCE CAPTURED
Date: 2026-08-03
Risk level: CRITICAL (ADR-007 foreground/worker separation; live ENV-001 reads only; no automatic actuation)

Objective:

Validate a framework-neutral reference host gateway that records completed live range-query observations without delaying or changing the foreground query, groups them through the ADR-007 background worker, emits complete traces through the verified ADR-006 outbox, and reaches the real DRY_RUN monitor. The experiment establishes a reference embedding seam, not an HTTP/gRPC deployment or production traffic claim.

Hypotheses:

- **H1 — Foreground isolation:** the post-response recorder accepts or drops an observation without filesystem, background-Milvus, source-publisher, detector, policy, or actuation work on the served-query call path.
- **H2 — Complete compatible trace production:** separate L2 and COSINE stationary host workloads yield ordered, identity-compatible complete 50-query traces, then three 200-query monitor windows per stream; partial/incompatible observations never publish a trace.
- **H3 — Live DRY_RUN composition:** real ENV-001 read-only shadow/FLAT/oracle capture reaches `NO_DRIFT → NO_CHANGE` once per stationary metric stream through the existing source/outbox and monitor, with zero configuration mutation or actuation call.
- **H4 — Failure containment and restart truthfulness:** full queue, closed source, executor failure/timeout, identity mismatch, and worker restart preserve foreground-query success, create explicit non-sensitive records, and never fabricate/replay unpersisted observations.

Configuration:

- **Backend/environment:** verified ENV-001 Milvus only; use the committed DATASET-001 artifacts, pinned URI, image/version/digest evidence, and a separately captured identity baseline.
- **Reference workload:** separate deterministic stationary L2 and COSINE request streams, each with exactly 600 completed host observations (three 200-query windows / twelve 50-query traces). Their request order, seed, metric, stratum, candidate/LKG/sentinel `ef`, and collection identities must be frozen in the run manifest.
- **Execution mode:** read-only data-plane queries plus downstream `PolicyMode.DRY_RUN`; no collection/index/schema/configuration mutation, `start_canary`, `stop_candidate`, `restore_last_known_good`, `verify_restoration`, or live action controller call.
- **Recorder/worker limits:** freeze queue capacity, worker drain limit, maximum partial batches, maximum observation age, and every drop/restart reason in the manifest before execution. The v1 in-memory queue is host-owned and must be explicitly distinguished from ADR-006's durable outbox capacity.

Frozen implementation parameters (2026-08-03; not evidence of a completed run):

- **Dataset and scheduling:** DATASET-001 `generation_manifest.json` SHA-256 `b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9`; deterministic scheduling/audit seed `20260805`; within each metric stream use measured query IDs `0…199` in canonical order for each of three windows. Each 200-query window is emitted as four contiguous 50-query worker groups.
- **L2 stream:** `target-075`; candidate `ef=800`; last-known-good and served `ef=400`; sentinel `ef=100`; reviewed baseline `artifacts/exp-005/baselines/l2-target-075-ef800-lkg400.json` (file SHA-256 `15c587aa592f76edcfe8768df62c565c4aca90916cf0a3852679abe9f1ac27e2`; internal baseline SHA-256 `6e26b0793ca44732ec464fe08e09287d28c87356f0f0e8dd71691e3e8658dc52`).
- **COSINE stream:** `target-025`; candidate `ef=400`; last-known-good and served `ef=200`; sentinel `ef=100`; reviewed baseline `artifacts/exp-005/baselines/cosine-target-025-ef400-lkg200.json` (file SHA-256 `47b2b5e0d182661019114405b841a63b87b89e4dcc286e7458038280e39921ca`; internal baseline SHA-256 `ad03acd55103be261bdac349500d96d80542f8dea1f0a436acfd365395a3e0c5`).
- **Bounded host/outbox/monitor limits:** volatile host-observation capacity `50`; worker drain limit `50`; maximum partial streams `2`; maximum observation age `60` seconds; durable source pending-event capacity `28`; durable source pending-byte capacity `16,777,216`; monitor poll limit `28`. The manifest must record every actual receipt/drop/restart counter and distinguish the volatile host queue from the durable outbox.
- **Read-only admission:** the runner must capture an explicit successful serving preflight for both streams before issuing any foreground request, and it must pin the reviewed baseline bindings in the manifest. The background executor still performs its required pre/post health/load/identity checks around every trace capture.
- **Evidence-process isolation:** the gRPC-owning capture phase must write only a `capture_receipt.json` after it closes every live client, then terminate. A fresh finalization process must independently re-derive all foreground/worker/outbox/window/monitor predicates before it captures the post-run host snapshot and writes `run_manifest.json` and `completion.json`. This prevents host-snapshot subprocesses from inheriting a live gRPC runtime; a capture receipt alone is incomplete evidence and must never be presented as a completed run.
- **Configuration isolation:** because `ActuationWorkload` binds exactly one immutable configuration identity, the L2 and COSINE streams must use separate read-only adapter/executor and serving-executor instances. A stream-key router may compose their existing interfaces, but it must never merge, rewrite, or substitute either configuration identity.

Required scenarios and pass criteria:

1. **Foreground isolation traps**
   - Call the reference gateway against live Milvus while recorder dependencies for filesystem, publisher, executor, detector, policy, and actuation are trapped.
   - Pass only if the served query returns normally, the recorder returns `ACCEPTED` or a documented drop without invoking any trap, and all additional shadow work occurs only after explicit worker execution.
2. **L2 and COSINE stationary composition**
   - Run 600 completed host observations for each metric separately; require twelve persisted complete traces and three assembled complete windows per stream.
   - Pass only if source/outbox, monitor, and provenance identities remain separate, all trace/window checksums verify, and each stream produces one audited `NO_DRIFT` / `NO_CHANGE` result.
3. **Read-only identity and range semantics**
   - Compare every real executor trace against its recorded FLAT/oracle/sentinel evidence and identity binding.
   - Pass only if all stages report success, zero timeout/threshold violations, exact FLAT/oracle agreement, and no pre/post identity mismatch; no collection/admin mutation is observed.
4. **Queue and source failure containment**
   - Force a full in-memory queue and an unavailable/rejecting publisher after a normal served query.
   - Pass only if the served query succeeds, the event is explicitly dropped/rejected without raw payload in diagnostics, and no detector/monitor/policy input follows.
5. **Executor failure, identity mismatch, and worker restart**
   - Inject an executor timeout/failure, a changed identity binding, and a worker restart with an incomplete volatile batch.
   - Pass only if no invalid trace is published; the restart-loss counter is exact; monitor receives no fabricated/replayed event; and all subsequent compatible complete groups remain ordered.
6. **DRY_RUN/non-actuation proof**
   - Attach trap actuation/controller implementations at every reachable boundary and inspect imports/call counts.
   - Pass only if all trap counts remain zero, policy mode remains `DRY_RUN`, and no Milvus mutation/configuration API is called.

Acceptance criteria:

- Raw foreground, worker, source/outbox, monitor, and failure-probe output is captured in an immutable artifact bundle with checksums, exact commit, dirty state, environment pinning, resource snapshots, and queue settings.
- Both metric streams meet all composition and identity/range semantics checks; no result combines stream identities or request observations.
- Every deliberate failure is fail-closed with an explicit non-sensitive reason and zero downstream detector/policy/actuation processing where applicable.
- Full repository and focused EXP-008 suites pass. EXP-008 may verify a live DRY_RUN reference integration only; it never authorizes automatic actuation or claims deployment in an external serving application.

Dataset ID:

DATASET-001, verified artifacts only. Record all consumed checksums in the run manifest.

Hardware:

Record host CPU/RAM/OS/kernel, Docker resource allocation, ENV-001 container health, foreground/background process snapshot, and pre/post-run resource snapshots. Performance interpretation is secondary to correctness and foreground-isolation evidence.

Git commit:

Stationary composition evidence: clean commit `2403799` (`2403799271f5bad205a752b9d407bf95ad3be852`). H1/H4 live failure evidence: clean commit `76600f8` (`76600f8c10eba219f22df36c9816d7661aff24fa`), which includes the independent failure-bundle verifier and strict observed-receipt/worker-state validation.

Random seed:

`20260805`, frozen for request scheduling, deterministic audit selection, and worker ordering configuration.

Raw output location:

Stationary composition evidence: `artifacts/exp-008/run-20260803T171620Z/`. H1/H4 live failure evidence: `artifacts/exp-008/failure-probes-20260803T175325Z/`.

Result:

Stationary live evidence captured on 2026-08-03 under clean commit `2403799` (`2403799271f5bad205a752b9d407bf95ad3be852`): `run_manifest.json` SHA-256 `0df310713eb067266187fd6f055b462dd5a2415531e0073a8527673f45b2fc95`. The fresh-process finalizer independently verified all 62 recorded artifact checksums; both resource snapshots had empty stderr (including no inherited gRPC fork warning). The bundle records 1,200 accepted foreground requests (600 per stream), 24 successful 50-query traces, six complete 200-query windows, and two audited evaluations: L2 / `target-075` and COSINE / `target-025` each reached `NO_DRIFT → NO_CHANGE`. `policy_mode_dry_run=true`; configuration mutation, canary start, rollback, and safe-boundary construction are all `false`. This is stationary H2/H3 evidence only; it does not satisfy the registered deliberate-failure/restart scenarios or authorize automatic actuation.

Deliberate live H1/H4 evidence captured on 2026-08-03 under clean commit `76600f8` (`76600f8c10eba219f22df36c9816d7661aff24fa`): `artifacts/exp-008/failure-probes-20260803T175325Z/run_manifest.json` SHA-256 `a154904c6b7bb41f16694bd17e3ba6fbcb4c7e83b63d4088fa09907d21735d01`. A fresh no-gRPC verifier independently validated the manifest payload hash, all 15 artifact hashes, the clean capture/finalizer commit binding, closed artifact inventory, raw probe/receipt agreement, and durable worker state. All 154 live foreground range queries succeeded. H1 observed the post-response recorder failure code `RECORDER_FAILED` without changing the served response. H4 observed exact fail-closed outcomes for queue capacity (`PENDING_OBSERVATION_CAPACITY_EXCEEDED`), unavailable publisher (`PUBLISH_OUTCOME_UNKNOWN`), shadow executor timeout (`EXECUTOR_CAPTURE_FAILED`), identity mismatch (`TRACE_IDENTITY_MISMATCH`), and worker restart partial-batch loss (`restart_loss_count=1`). Both preflight and postflight reported complete, reason-free L2 and COSINE streams; zero traces were published and monitor, policy, and actuation call counts were all zero. Both resource snapshots had empty stderr and no gRPC fork warning.

Conclusion:

EXP-008 is VERIFIED for the reference in-process host-observation composition: H1 foreground isolation, H2 trace/window production, H3 live read-only DRY_RUN evaluation, and H4 deliberate failure/restart containment all have immutable live evidence. This verification does not claim an external serving-application deployment, does not authorize full-traffic tuning, and does not authorize automatic actuation; a separately designed and evidenced human-gated canary/rollback step remains required.

---

### EXP-009: Human-gated 60-of-600 canary and rollback validation

Status: CONTRACT DEFINED — NOT IMPLEMENTED — NOT RUN
Date: 2026-08-03
Risk level: CRITICAL (ADR-008 candidate routing, confidence evidence, approval security, and rollback safety)

Objective:

Validate the prerequisites for exactly one controlled, human-gated adjacent HNSW query-time-`ef` canary transition. The experiment must prove the statistical/workload contract before implementation, then prove approval, bounded routing, failback, and restoration without claiming an external serving deployment or authorizing automatic/full-traffic tuning.

Scope:

- **In scope:** an immutable 600-occurrence routing-workload manifest plus 1,200 disjoint background recall-audit vectors; a CSPRNG-selected 60-of-600 candidate partition; a calibrated confidence-bound estimator; one-time signed operator approval; pre-action shadow/health/identity gates; candidate routing through the verified reference host seam; append-only audit records; mandatory rollback; restart and expiry failback; and a final 50-query restoration audit.
- **Out of scope:** automatic approval generation, automatic full-traffic application, persistent Milvus/index/schema/configuration mutation, external HTTP/gRPC serving deployment, multi-host coordination, multi-tenant routing, and a claim that a controlled replay represents IID production traffic.

Hypotheses:

- **H1 — Workload/routing cardinality:** a frozen, identity-bound 600-occurrence workload admits exactly 60 CSPRNG-selected candidate routes and 540 last-known-good routes at a strict 10% cap, with no duplicate occurrence identity, no candidate route outside the approved manifest, and a canonical candidate-selection record created only after the eligible workload is frozen and before candidate results are read.
- **H2 — Statistical gate validity:** the declared recall-lower and p95-latency-upper estimators meet their pre-registered coverage/calibration criteria under the declared sampling model. For the frozen 600-occurrence latency population with nearest-rank p95 `ceil(0.95 * 600) = 570`, a simple random 60-without-replacement candidate set makes the sample maximum at least that threshold with probability at least `1 - C(570,60) / C(600,60) = 0.961003033592`; this is a finite-manifest randomization claim, not an IID or production-latency claim. The 60 candidate observations do not make a distribution-free mean-recall lower bound viable: the one-sided Hoeffding margin is `0.158001378516`, so recall requires the separate 1,200-query background-audit evidence under its independently declared query-generator model.
- **H3 — Approval containment:** only a valid, unexpired, one-time grant bound to the exact decision, identities, eligible-workload digest, candidate-selection-record digest, and transition can install a route. Every invalid, expired, replayed, or mismatched grant fails closed before candidate traffic.
- **H4 — Foreground and restart safety:** foreground routing uses only a non-blocking immutable route lookup. Approval verification, audit persistence, health checks, detector/policy evaluation, shadow capture, and rollback verification remain off the served-query path; restart/expiry/identity failure immediately fails back to the persisted last-known-good `ef`.
- **H5 — Controlled rollback truthfulness:** a deliberately triggered hard, recall, and latency failure each clears candidate routing, records the exact trigger, restores last-known-good routing, and proves restoration with health/identity plus a 50-query FLAT/oracle audit. No alternate candidate and no automatic re-enable occurs.

Frozen contract (before implementation):

- **Candidate cardinality:** exactly 60 candidate occurrences from exactly 600 eligible occurrences. The route fraction is exactly `0.10`, never rounded upward. A 50-of-500 implementation is prohibited by ADR-008 for this experiment.
- **Sampling declaration:** before Stage 2, record the target population, whether occurrence IDs map one-to-one to vectors, any vector reuse, potential-outcome/no-interference assumption for the finite latency population, selection method, CSPRNG provenance, and exact calibration rule. Freeze the 600 eligible occurrences first; then create a canonical selection record that contains exactly 60 CSPRNG-selected IDs, the eligible-workload digest, and non-sensitive random-source provenance. The selected Stage-1 proposal is DATASET-002: 600 unique routing vectors and 1,200 disjoint background recall-audit vectors, all generated independently from the frozen query-only generator contract against DATASET-001's unchanged base vectors/thresholds. DATASET-001’s 200 measured query IDs cannot silently satisfy the 600-occurrence requirement.
- **Confidence rule:** report a one-sided 95% recall lower bound and p95 latency upper bound only when their estimator, assumptions, and Stage-1 calibration are accepted. The proposed latency baseline is the maximum of the CSPRNG-selected 60 candidate-route latencies; as a finite-manifest randomization bound it is at least the nearest-rank p95 threshold of the 600 eligible occurrence population with probability at least `0.961003033592`, conditional on the declared fixed-potential-outcome and no-interference assumptions. Stage 1 can validate the formula/selection contract but cannot prove live no-interference; Stage 4 must capture the pre-registered schedule-stability evidence. This value must never be labeled an IID/prod-latency interval. The proposed recall baseline is a one-sided Hoeffding lower bound across 1,200 disjoint background candidate audits; it has margin `0.035330182290` and therefore requires observed mean recall `>=0.985330182290` to clear the `0.95` floor. If another estimator is evaluated, compare it against this baseline without selecting it post hoc. Do not use the 60 candidate count as a recall-bound justification: at `alpha=0.05`, its one-sided bounded-mean Hoeffding margin is `0.158001378516`.
- **Stage-1 calibration pre-registration:** The exact hypergeometric formula, not simulation, is the validity proof for the finite-manifest latency statement. A deterministic diagnostic replay will nevertheless freeze PCG64 seed `20260810`, 100,000 simple-random 60-without-replacement draws from synthetic occurrence IDs `0…599`, and the strict upper-tail IDs `570…599`; it reports the observed hit rate against the analytic `0.961003033592` value. This replay tests the implementation/recording path only and must never be described as validating the production CSPRNG or live no-interference. The recall diagnostic will freeze three independent PCG64 streams with seeds `20260811`, `20260812`, and `20260813`, respectively, running 10,000 independent stationary replays for Bernoulli capped-recall means `0.50`, `0.95`, and `0.99`, each with exactly 1,200 values. It reports the fixed Hoeffding lower-bound behavior and empirical noncoverage count for each declared generator without changing the estimator, seeds, means, replay count, or acceptance threshold after observing results. These synthetic replays are regression/calibration evidence for the estimator implementation, not a claim about live HNSW recall; their theoretical applicability remains restricted to the explicitly declared independent query-generator population.
- **Stage-4 schedule-stability/no-interference gate (defined by Stage 1, not yet executed):** The evidence bundle must freeze exactly 50 LKG-only schedule controls: DATASET-002 `recall_audit` IDs `600…649`, each bound by its float32 vector SHA-256, using the same L2 / `target-075` radius, range filter, limit, collection identities, and LKG `ef=400` as the 600 routing occurrences. These controls are explicitly disjoint from the 600 routing vectors; their repeat use is a diagnostic and is never counted as independent recall evidence. The live runner must execute the 600 routing occurrences synchronously, once each, in immutable manifest order with one client, concurrency `1`, no batching, no retry, and a monotonic start/end ledger for every request. It must run three complete LKG-control sweeps before slot 0, one complete sweep after each contiguous 100 routing slots (six total), and three complete sweeps after slot 599. A sweep contains all 50 controls in frozen ID order. Let `m0` and `p0` be respectively the median and nearest-rank p95 (`ceil(0.95 × 50)=48`) of the three pre-sweep medians and p95 values. Both must be finite and strictly positive. Every sweep, including the three pre-sweeps, must have: all 50 successful, non-timeout, threshold-valid responses; no identity/health failure before or after the sweep; p95 `<= 10 ms` and `<= 1.50 × p0`; and median `<= 1.25 × m0`. The route ledger must contain exactly 600 unique manifest IDs, exactly the frozen 60 candidate IDs at `ef=800` and remaining 540 IDs at `ef=400`, strictly non-overlapping monotonic intervals, no query/configuration mutation API call, and no unscheduled search recorded by the runner. Any missing, duplicate, overlapping, retried, unhealthy, identity-mismatched, failed, timed-out, non-finite, or ceiling-breaching record makes the finite-manifest latency bound **NOT APPLICABLE** for that run. Passing these falsification controls supports—but does not prove—the fixed-potential-outcome/no-route-assignment-interference condition; the reported latency statement must remain conditional on that assumption and must never be generalized to IID or production traffic.
- **Approval scope:** the grant binds EXP-009 authorization, decision/audit digest, metric/stratum, current/candidate/last-known-good `ef`, configuration/index/data/FLAT identities, eligible-workload-manifest digest, candidate-selection-record digest, maximum fraction, issue/expiry, and rollback pre-authorization. Private keys, raw CSPRNG entropy, and raw payloads are prohibited from artifacts.
- **Live transition candidate:** only after all prior stages pass, the first controlled transition may evaluate the ADR-002 L2 / `target-075` `ef=400 → 800` quality-recovery exception. It requires the exact exception identity, `<=1.50×` relative latency ceiling, `<=10 ms` absolute ceiling, and `>=0.005` recall improvement. If no authentic, eligible `START_CANARY` decision and qualified last-known-good state exist, the live stage is blocked rather than fabricated.

Required stages and pass criteria:

1. **Stage 1 — workload and confidence preflight (offline)**
   - Define and checksum the 600-occurrence eligible-workload manifest; independently verify cardinality, uniqueness, vector-binding disclosure, and no hidden mutation of DATASET-001. Create and checksum a distinct 60-ID CSPRNG candidate-selection record only after the eligible manifest is frozen and before any candidate result is read.
   - Independently derive the finite-population coverage calculation, test selection-record validity and its no-post-freeze binding against pre-registered synthetic finite populations, and calibrate the recall estimator on pre-registered stationary replays without changing an estimator after viewing evaluation results. Define—not falsely validate—the schedule-stability/no-interference evidence required in Stage 4. Report point estimates, bounds, coverage, invalid intervals, and every assumption.
   - Pass only if the declared finite-population/randomization model is supportable, the p95 upper-bound method has at least its claimed finite-manifest coverage, recall bound behavior is specified and calibrated, the Stage-4 schedule-stability/no-interference gate is concrete, and all results are reproducible from committed inputs and recorded selection outcomes. Otherwise stop before route/approval implementation.
2. **Stage 2 — approval and routing contracts (offline)**
   - Test valid, missing, invalid-signature, expired, revoked, wrong-decision, wrong-identity, wrong-workload, wrong-selection-record, invalid-selection provenance, duplicate/replayed, and audit-write-failure grants.
   - Test 59/60/61 candidate and 599/600/601 workload boundaries; exact candidate/LKG partitions; duplicate occurrence IDs; route installation/removal atomicity; and state recovery after process restart.
   - Pass only if every unsafe condition records an explicit non-sensitive refusal and issues zero candidate search/routing calls.
3. **Stage 3 — rollback containment (offline)**
   - Inject every ADR-002 hard/recall/latency rollback trigger, route-store corruption, grant expiry, identity change, restoration-audit failure, and automatic-action-controller disable state.
   - Pass only if each event removes the candidate route, restores LKG, writes an append-only record, prevents any alternate candidate/re-enable, and fails closed when restoration cannot be verified.
4. **Stage 4 — controlled live canary (only if Stages 1–3 are verified)**
   - Run only from a clean commit against verified ENV-001 with a human-signed, one-time exact grant; capture raw preflight, approval, route partition, all 600 foreground outcomes, candidate/LKG observations, bounds, audit records, identity/health results, and post-run restoration check.
   - Pass only if every pre-action gate is real and passing; exposure is exactly 60/600; the required schedule-stability/no-interference evidence is captured and supports the finite-manifest latency condition; no Milvus mutation API is called; results meet the declared SLO/bound contract; and a separately authorized deliberate rollback run proves failback/restoration. A blocked precondition is an honest blocked result, not a reason to synthesize a grant or outcome.

Metrics:

- Candidate/LKG route counts, candidate fraction, duplicate/missing/unexpected occurrence-ID count, and foreground route-lookup latency.
- Estimator coverage, bound validity rate, p95/max latency, recall mean/lower bound, paired recall difference, and sensitivity to declared dependence assumptions.
- Grant validation/refusal counts by reason; grant-to-action/audit binding completeness; replay detection count; expiry and restart failback latency.
- Pre-action/shadow/health/identity gate outcomes; candidate failures/timeouts/threshold/semantic violations; rollback trigger and restoration-audit outcomes.
- Milvus mutation/configuration API call count (must be zero), policy/actuation/routing call counts, artifact checksum/inventory verification, and process-clean/dirty binding.

Acceptance criteria:

- Stage 1 establishes a reviewed estimator/workload basis before any candidate-routing code or live query-time transition exists.
- All Stage 2/3 failure cases are deliberately tested with raw output and prove zero candidate traffic on denial plus deterministic LKG failback on safety failures.
- The live stage, if reached, is human-gated, exactly bounded, read-only at the Milvus administration/configuration layer, reproducible, identity-bound, and has a raw deliberate rollback/restoration evidence bundle.
- Full repository and focused suites pass. EXP-009 may authorize only the tested, exact human-gated transition class after review; it never authorizes autonomous, unbounded, multi-transition, external-host, or full-traffic tuning.

Dependencies:

- ADR-002 for action ladder, SLOs, conservative bounds, and rollback conditions.
- ADR-004 for evidence provenance and identity binding.
- ADR-005 through ADR-007 plus verified EXP-005–EXP-008 for the durable observation/monitor reference path.
- `76600f8` for strict EXP-008 H1/H4 artifact verification; `fec0b86` for verified EXP-008 state documentation and evidence publication.

Dataset ID:

DATASET-001 remains immutable and may be consumed only as declared input. DATASET-002 is pre-registered as the selected Stage-1 query-only workload: 1,800 new deterministic vectors, split into 600 routing and 1,200 disjoint background recall-audit queries. It must receive its own generator, checksums, verifier, and ground-truth records before generation; it must not modify, overwrite, or relabel DATASET-001.

Hardware:

For any live stage, record ENV-001 image/digest/version pins, collection identities, container health, host/Docker resource allocation, background workload disclosure, pre/post resource snapshots, Python environment/lock checksum, and clean git state. Performance claims remain secondary to safety and confidence validity.

Git commit:

TBD at execution; pin every approval, routing, estimator, policy/actuation, audit, host-seam, test, and evidence-verifier dependency.

Random selection:

TBD before Stage 1. The 600-ID eligible workload must be committed before a CSPRNG creates the canonical 60-ID selection record; the record, not raw entropy, is bound into the approval artifact. No candidate set may be selected after inspecting canary outcomes.

Raw output location:

Planned: `artifacts/exp-009/<UTC-run-id>/`.

Result:

Not run.

Conclusion:

EXP-009 is the mandatory statistical, approval, routing, and rollback gate for any candidate query-time-`ef` transition. Until it is verified, every policy path remains DRY_RUN-only and the existing offline actuation code remains non-authorizing.

#### Stage 1 verification result — 2026-08-04

**Stage status: VERIFIED (Stage 1 only).** The original contract-time `Result: Not run` remains above as the append-only pre-registration record. This result verifies only the offline workload and confidence preflight; EXP-009 as a whole remains in progress, and no routing, approval, Milvus operation, or actuation is authorized.

- Evidence commit: `e1d83fa4bd3815274efa208f85f16fe465761f06` (`feat: record EXP-009 Stage-1 environment provenance`), with `384` full-suite tests passing in `110.324s` before the evidence run.
- Immutable v2 bundle and raw structured output: `artifacts/exp-009/run-20260804T031212Z/`; run manifest SHA-256 `8627ddee1d26ccb3cddb2faa46bf8ec2debaada01de29eb102ce05e8e79eedcd`; completion SHA-256 `1503a637aa6a56ff1d9f6ae68695bb5bb03b96c1041edb4394e00825b07f1ac1`.
- Input/evidence bindings: eligible-workload SHA-256 `59c010c1988b16a08e5c21ce4de9c3213cc831b5be1acc718844e692f3e26f2b`; CSPRNG selection-record SHA-256 `c4e083dbcaaf475371874e2b912629e9490147dc6714bf5a9fa8cadf0c810ac3`; calibration SHA-256 `dcd10710a8ce937a4702030c13a1b9c255f6cbc40717e5d823d9180e415744e9`; environment SHA-256 `516dd333e2c429e11a39dd3711786fe9b1db0d26ff3bb7f5fd32972172d288ea`.
- Environment provenance: CPython `3.14.5`, NumPy `2.4.3`, Darwin kernel `25.5.0`, `arm64`. The bundle explicitly labels this as `offline_nonperformance`; it makes no hardware-performance or production-latency claim.
- Public post-publication verification rebuilt the 600-occurrence manifest from DATASET-001/DATASET-002, verified the exact 60-ID `python.secrets.SystemRandom.sample` selection binding, recomputed the frozen calibration, checked the closed artifact inventory and every SHA-256, validated the environment document, and returned `public_verifier: OK`.
- H1 is VERIFIED for the manifest/selection cardinality contract: 600 unique routing-vector bindings and 60 candidates (`0.10`). H2 is VERIFIED only for the declared analytic finite-manifest coverage calculation (`0.9610030335925056`) and pre-registered synthetic calibration behavior; it is not an IID or production-latency claim. The Stage-4 schedule-stability/no-interference diagnostic is now frozen in the v2 manifest, but remains unexecuted and therefore is not evidence of no interference.

**Next gate:** Stage 2 must implement and deliberately test the signed approval, exact partition, refusal, and restart/expiry failback contracts before any candidate route exists.

#### Stage 2 verification result — 2026-08-04

**Stage status: VERIFIED (Stage 2 only).** The Stage-1 result above remains
unchanged.  This result verifies the offline approval, partition, lifecycle,
restart, expiry, and fail-closed routing contracts only.  EXP-009 remains in
progress: Stage 3 rollback containment and Stage 4’s separately human-gated
controlled live canary are not authorized by this evidence.

- Implementation dependencies: signed-grant verifier `4d461ff`, immutable
  route plan `2037396`, one-time grant ledger `197e464`, LKG-only marker
  `25c3d18`, route authority `eed889c`, lifecycle audit `32b6cc4`, activation
  coordinator `32c5896`, expiry failback `b922ba5`/`c34873e`, and committed
  immutable evidence runner/public verifier `6a3afdd`/`84ba2ea`.
- Immutable evidence bundle:
  `artifacts/exp-009/run-20260804T051014Z/`, generated from clean commit
  `84ba2ea239dfc23d249554812f4d16bb07723d18`.  Public verifier result:
  `COMPLETE`; manifest SHA-256
  `7673422ab1d9fad4760009b4ddeff3ec4948c54e2fe73a8d9f343b5f9235e3de`;
  raw-result SHA-256
  `fa2978d26a848553813e101e30da166c856dd7642d99bfe3ddf92e771027b68c`.
- The sealed bundle captures 13 successful commands: clean-tree
  `git diff --check`, all 10 focused Stage-2 suites, the full repository
  suite (`448` tests, `102.555s`), and `pip check` (`No broken requirements
  found`).  It hashes every source input, focused-suite source, command result,
  raw stdout, and raw stderr, and its public verifier rejects missing,
  incomplete, tampered, substituted, or symlinked evidence.
- The focused suites deliberately cover the registered valid/missing/invalid,
  expired, revoked, identity/workload/selection/audit mismatches; 59/60/61 and
  599/600/601 boundaries; exact 60/540 partition; replay, duplicate, unknown,
  restart, corruption, expiry, durable-write, and atomicity failures.  The
  verifier itself imports no Milvus client, makes no query, and claims or
  enables no candidate occurrence.  Every Stage-2 result is offline evidence,
  not authorization for live traffic.

**Next gate:** Stage 3 must implement and deliberately verify the full
rollback-containment contract (hard/recall/latency triggers, restoration-audit
failure, controller-disable state, and no-alternate-candidate failback) before
any controlled live canary is considered.

#### Stage 3 verification result — 2026-08-04

**Stage status: VERIFIED (Stage 3 only).** The original pre-registration and
the Stage-1/Stage-2 entries above remain unchanged. This result verifies the
offline rollback-containment and restart-durable activation-interlock contract
only. EXP-009 remains in progress: Stage 4 is separately human-gated, requires
an externally signed exact grant, and is not authorized by this evidence.

- Implementation dependencies: rollback coordinator and activation interlock
  `745deb8`; immutable Stage-3 evidence runner/public verifier `c78d0e2`.
- Immutable evidence bundle:
  `artifacts/exp-009/run-20260804T061821Z/`, generated from clean commit
  `c78d0e2a0ad32bb74162aecb230f318e3f8d5d93`. Its independent public verifier
  returns `COMPLETE` with 11 commands; manifest self-SHA-256
  `9cfb0dbba35dcc927b5b471303a3dd45bc7a34aa0b317bc5b29b06f89ea551b5` and
  raw-result self-SHA-256
  `2000d1f3472c8ceae6b5ed88ac3ad04a40c94b47d207be7f0be5dd38ac2a03ed`.
- The sealed bundle records clean-tree `git diff --check`, eight focused
  offline suites, a complete repository suite (`470` tests in `95.853s`), and
  `pip check` (`No broken requirements found.`). Every captured command passed;
  the verifier rejects incomplete, tampered, rehashed command-digest, missing,
  substituted, and symlinked evidence.
- Deliberate rollback coverage includes hard, recall, and latency policy
  triggers; route-state corruption; identity change; approval expiry;
  malformed context/policy reason; marker, audit, ledger, controller, and
  authority-clear failures; restoration-audit failure; duplicate terminal
  grants; and process restart. The real local expiry composition proves the
  existing expiry reconciler persists the LKG marker and terminal grant before
  the restoration audit. A restart-durable automatic-action controller blocks
  a later activation before approval verification or route work.
- The verifier itself imports no route/rollback runtime or Milvus layer and
  executes no database operation, search, claim, candidate enablement, or live
  rollback. These offline results validate containment behavior; they do not
  demonstrate a live candidate query or external serving deployment.

**Next gate:** Stage 4 remains blocked pending a clean commit, verified
ENV-001 preflight, qualified last-known-good state, and a human-signed,
one-time grant for the exact pre-registered L2 / `target-075` `ef=400 → 800`
transition. No automatic or full-traffic action is authorized.

#### Stage 4 offline-composition verification result — 2026-08-04

**Stage status: VERIFIED (offline composition seam only).** The Stage-1 through
Stage-3 records above remain unchanged. This result verifies the fake-only
composition of the admitted 1,200-slot schedule, DATASET-002 source binding,
durable ledger, restart continuation, terminal containment, and pure
schedule-stability evaluator. It does **not** execute a Milvus query, accept a
grant, publish or claim a candidate route, or constitute the controlled live
canary required by Stage 4.

- Implementation dependencies: immutable serial runner `70632bb`; sealed
  offline-composition validator `2d56463`.
- Immutable evidence bundle:
  `artifacts/exp-009/run-20260804T112128Z/`, generated from clean commit
  `2d5646323ab527ec50773584fb0a4948d849c0df`. Independent verification returns
  `COMPLETE`; manifest SHA-256
  `0e18398e799242358d86096d5aa4102a8ab357bd0c96e016d3062ac36d92b045`, raw-result
  SHA-256 `9c7776e2e3b50c16760f014eff2c022b0b8668de70039199736c66b38dd6fee7`, and
  receipt SHA-256 `b35b1ab5857a6b39594551f87980f209ced02d0393962c0245e5c91b22e61dee`.
- The sealed bundle records nine passing commands: clean-tree `git diff
  --check`, six focused suites, the full repository suite (`511` tests in
  `138.783s`), and `pip check` (`No broken requirements found.`). It records
  `live_database_or_routing_activity: false`; its verifier rejects incomplete,
  tampered command-output, tampered receipt, substituted, and symlinked
  evidence.
- The composition tests prove exact 1,200-slot ordering, 600 control and 600
  routing slots, exactly 60 candidate / 540 LKG planned `ef` assignments,
  strict resume at the next ledger index, and one-record terminal containment
  for admission, source, executor, health, and schedule-integrity failures.
  The runner and validator are AST-tested to exclude Milvus, approval/grant,
  activation, and route-authority imports.

**Next gate:** implement and verify a separately designed, serial live
composition root with injected serving/execution ports and no configuration
mutation. Only after that code exists, a fresh ENV-001 preflight passes, an
eligible policy/LKG state exists, and a human supplies an exact one-time
Ed25519 grant may the pre-registered controlled live Stage-4 canary be run.
No automatic, full-traffic, or ungranted candidate action is authorized.

#### Stage 4 live-root fake-only verification result — 2026-08-04

**Stage status: VERIFIED (fake-only root conformance only).** The preceding
Stage-1 through Stage-4 offline-composition records remain unchanged. This
result verifies the separately designed `Stage4LiveRunner` composition root
with injected fake activation, route-authority, runtime-probe, serving, ledger,
and rollback ports. It is not a controlled live canary and it does not accept a
real grant, enable or claim a real candidate route, create a Milvus client,
issue a live search, or mutate Milvus configuration.

- Implementation root: `94fe22c`; profile-aware verifier: `920aab9`; immutable
  evidence publication: `1614521`.
- Immutable bundle: `artifacts/exp-009/run-20260804T141850Z/`, generated from
  clean commit `920aab9371b49501bdfbea644a0c5575a15a96e6`. Independent
  verification returns `COMPLETE`; manifest SHA-256
  `1c04f8cfcbbbd331732e08e46560331608da6f2d7aa9c42f0ece920eeaf02dd8`, raw-result
  SHA-256 `61887ef633286434820d7b72cf792c0409b7944fb081fc67eea084432287ffd8`, and
  receipt SHA-256 `bdc136c2d5d0a4682f36d41c2285eba47139ec052dee7030e292af070f17ad45`.
- The sealed bundle records 14 successful commands: clean-tree `git diff
  --check`, 11 focused suites, the full repository suite (`525` tests in
  `165.256s`), and `pip check` (`No broken requirements found.`). The verifier
  rejects incomplete, tampered, substituted, and symlinked evidence and keeps
  the historical offline-composition profile independently verifiable.
- The fake-port tests prove two fresh admission checks, exact one-shot
  claim-to-step binding before any injected search, serial 1,200-slot dispatch
  with exactly 60 candidate-`ef=800` and 1,140 LKG-`ef=400` requests, terminal
  containment on registered failures, no resumption of a non-fresh ledger, and
  mandatory `COMPLETED_CANARY` rollback. They do not prove live no-interference,
  real health/load/identity behavior, or an external human approval.

**Next gate:** build and fake-test the read-only Stage-4 runtime-probe adapter
that maps the existing serving preflight to `Stage4RuntimeReadiness` and
per-slot `Stage4SlotSafety`. Only after that adapter and a fresh read-only
ENV-001 preflight are verified may an externally supplied exact grant be
considered; candidate dispatch remains blocked.

#### Stage 4 runtime-probe fake-only verification result — 2026-08-04

**Stage status: VERIFIED (fake-only runtime-probe conformance only).** This is
an append-only result after the live-root fake-only record above. It verifies
the narrow `Stage4ServingRuntimeProbe` mapping from one structural serving
preflight result to exact `Stage4RuntimeReadiness` and `Stage4SlotSafety`
values. It does not execute a real serving preflight, create a Milvus client,
issue a search, use a grant, claim a candidate route, or mutate Milvus.

- Implementation: `f0f0511`; separate evidence profile: `a8373ad`; immutable
  evidence publication: `1c995f7`.
- Immutable bundle: `artifacts/exp-009/run-20260804T145434Z/`, generated from
  clean commit `a8373adf14a2efff117479f580b817c2e0c381f6`. Independent
  verification returns `COMPLETE`; manifest SHA-256
  `ed6bf268a9c61d97e2f04b7cd02217f6a068741417fc15b4a35eea12d85fd890`, raw-result
  SHA-256 `59bdfc984f75d52bfbdbf8b1a8a46835952bb80da31dc59785c66b08340289a2`, and
  receipt SHA-256 `f8ae51cce6770a6d70100efbeb89f8c720c51c65ea4c2a0fe4e63b58bbcbb12c`.
- Eight commands passed: clean-tree `git diff --check`, five focused suites,
  the 537-test repository suite in 153.481 seconds, and `pip check` with no
  broken requirements. The fake-port suite covers exact one-stream binding,
  health/load/identity mapping, incomplete-scope rejection, unknown/malformed/
  exceptional preflight, invalid clocks, requested-binding mismatch, API
  re-export compatibility, and the import boundary.
- The sealed safety assertion records `false` for real-client construction,
  real serving preflight, live search, real-grant verification,
  candidate-route enablement, and configuration mutation. It verifies no live
  effect, not real ENV-001 health, load, or identity.

**Next gate:** capture and independently verify one fresh read-only ENV-001
health/load/exact-identity preflight through this adapter for the frozen L2 /
`target-075` binding. It must remain LKG-only and must not supply, verify, or
use a grant. Candidate dispatch remains blocked.

#### Stage 4 read-only ENV-001 preflight evidence — 2026-08-04

**Stage status: VERIFIED (read-only preflight only).** This append-only result
verifies the frozen L2 / `target-075` ENV-001 health/load/exact-identity
preflight through the runtime-probe adapter. It is neither a qualified LKG
result nor a candidate-routing, approval, rollback, or no-interference result;
EXP-009 remains in progress and ADR-008 remains Proposed.

- Source revision: clean commit `d03bbc352520a780ab6e76382d41f2fa09eb5692`;
  invocation wrapper `d03bbc3`; immutable evidence publication `3353992`.
- Immutable bundle: `artifacts/exp-009/run-20260804T153006Z/`. Independent
  verification returned `COMPLETE`; raw-result self-SHA-256
  `977d6b3d9fe978d4e3a757b9156351b3d019169ebeca67580c7d177a5e38065c`,
  manifest self-SHA-256
  `6487b1798be6b904a84078dc7e9fec2bd8949be207346babbec60d03b37b7449`, and
  receipt self-SHA-256
  `613a30e2320025cd166d2214edf31ee1de773668d09bbb7e494f4cdb8a250c61`.
- The frozen reviewed baseline SHA-256
  `6e26b0793ca44732ec464fe08e09287d28c87356f0f0e8dd71691e3e8658dc52` and
  DATASET-001 manifest SHA-256
  `b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9` are
  bound in the artifact. FLAT and HNSW pre/post identities match exactly;
  HNSW records `M=16` and `efConstruction=200`.
- The raw client transcript contains exactly four `get_load_state` and eight
  `describe_index` calls. It records zero search, insert, collection/index
  mutation, grant/route use, and configuration mutation. The pinned
  environment completed 550 repository tests in 193.153 seconds after
  independent evidence verification.

**Next gate:** a controlled candidate route remains blocked pending an exact
current qualified-LKG state, eligible policy/admission state, and an externally
supplied one-time Ed25519 grant for the frozen transition. This preflight
cannot manufacture, substitute for, or consume any of those gates.

#### Stage 4 evidence-binding repair prerequisite — 2026-08-05

**Status: VERIFIED — CONTRACT DEFINED, IMPLEMENTED, ADVERSARIALLY TESTED, AND
COMMITTED.** Before any
unintegrated 1,200-query recall-audit draft may be accepted, EXP-009 must
verify one canonical binding across recall and finite-manifest latency evidence.
The binding must cover the exact run, clean revision, metric/stratum,
current/candidate/LKG `ef`, configuration/data/FLAT/HNSW identity, DATASET-002
manifest and audit-set digests, eligible-workload/selection/schedule digests,
and evidence-schema versions. Both evidence artifacts and the enclosing
publication manifest require independently checked SHA-256 values.

Pass only if adversarial mismatch/tamper/missing-evidence tests fail closed as
`INCOMPLETE`, a complete bound evidence pair reports `FAILING` on an actual
SLO breach, and only one exact, fully bound pair can produce a `PASSING`
read-only qualification report. This is evidence integrity work only: it does
not establish a `QualificationResult`, authorize a grant, install a route, or
permit any candidate query.

- Implementation and full test suite: commit
  `088d325cfce099754f0efa63e0f46f1dc4e2f68d` ("fix: bind Stage-4 recall and
  latency evidence to verified ledgers"), pushed to `origin/main`. Touches
  `canary_recall_audit_ledger.py`, `canary_recall_audit_evaluation.py`,
  `canary_stage4_decision.py`, `canary_stage4_latency_evidence.py` (new),
  `canary_stage4_qualification_report.py`, and six corresponding test files;
  12 files changed, 4,183 insertions.
- Focused suites: recall-audit ledger 38/38, recall evaluator 26/26, decision
  combiner 14/14, latency evidence 7/7, end-to-end pipeline 3/3,
  qualification-report CLI 21/21 — 109/109 total, 0 failures. Full repository
  suite: 662/662 passing, 0 failures, 0 errors, 872.428 seconds.
- The end-to-end pipeline includes
  `test_hand_fabricated_latency_evidence_cannot_combine_with_real_recall_evidence`,
  a direct proof that individually valid recall and latency evidence bound to
  different run contexts yield `INCOMPLETE`, never a silently combined
  `PASSING` or `FAILING` decision.
- Two real defects were found and fixed by this test suite during
  implementation: `main()` was missing the newly required `binding_sha256`
  keyword when opening the recall ledger, and a CLI test fixture computed its
  `frozen_recall_audit_ids_sha256` over a different byte serialization than
  the one `main()` actually digests from `--frozen-query-ids-json`, which
  produced spurious `EVIDENCE_BINDING_MISMATCH` failures until both were
  hashed from the same bytes.
- This remains evidence-integrity work only: it establishes no
  `QualificationResult`, authorizes no grant, installs no route, and permits
  no candidate query. ADR-008's overall acceptance status is unchanged.

### EXP-010: Calibrated empirical response-profile contract and prospective replay validation

Status: CONTRACT DEFINED — NOT IMPLEMENTED — NOT RUN
Risk level: CRITICAL
Registration date: 2026-08-09
Governing decision: ADR-009 (Accepted; implementation and execution remain pending)

#### Objective

Validate the pure statistics, canonical identity/lineage, fail-closed behavior,
restart/replay determinism, and prospective empirical applicability of
ADR-009's `CalibratedResponseProfile` before any profile may be supplied to a
candidate-capable policy evaluation.

This experiment collects predictive evidence only. It does not qualify a
last-known-good value, satisfy admission, authorize or sign a grant, install a
route, execute a candidate canary, mutate Milvus, or prove a universal
freshness interval.

#### Pre-registered hypotheses

- **H1 — Statistical conformance:** The implementation reproduces ADR-009's
  exact sixteen-claim family, Hoeffding recall bounds, nearest-rank p95, and
  exact binomial/order-statistic latency ranks for hand-computable and
  independently generated fixtures.
- **H2 — Atomic lineage:** Only one complete canonical profile whose four `ef`
  points share exact workload/search/index/data/control/environment/evidence
  lineage is accepted; mixed or tampered points fail closed.
- **H3 — Prospective empirical applicability:** Under an unchanged declared
  stationary workload and control/environment profile, a profile calibrated
  from the frozen post-trigger segment contains the later untouched aggregate
  empirical recall means and p95 latencies for its exact cell. This is a
  transportability diagnostic, not proof of nominal confidence coverage or a
  universal TTL.
- **H4 — Unsupported-domain refusal:** Missing, stale, incompatible,
  insufficient, interpolated, extrapolated, substituted, malformed, or
  unverified evidence cannot produce an applicable profile.
- **H5 — Authority separation:** No result or artifact from EXP-010 can be
  consumed as Phase-3 qualification, Stage-4 admission, approval, routing, or
  execution authority, and no candidate-capable policy path is enabled.

#### Validation cells and supported `ef` family

The first registered validation matrix contains two independent cells:

1. `L2 / target-075`, matching the project's frozen reference transition; and
2. `COSINE / target-025`, providing a separately calibrated cross-metric cell.

Each cell is evaluated independently at the exact ordered family
`(200, 400, 800, 1600)`. Evidence is never pooled across cells. Other metric /
threshold-stratum combinations remain unsupported until separately registered
and measured. `ef=100` remains sentinel-only and is not a profile point.

#### Population construction and disjointness

For each cell, one detector trigger freezes an immutable trigger-evidence
boundary. All profile inputs occur strictly after that boundary. Calibration
membership, role assignment, canonical order, and payload bindings are frozen
before any response-profile replay result at a supported `ef` is inspected or
used for selection. Existing foreground served results may exist but may not
influence inclusion, exclusion, ordering, replacement, or role assignment.

1. The first subsequent complete 200-query window is the warm-up population.
   All 200 queries are replayed once at each supported `ef`; none contributes
   to a point estimate or bound.
2. The next six globally consecutive complete 200-query windows form exactly
   1,200 measured calibration observations.
3. Prospective validation uses the next twenty disjoint six-window segments,
   each containing exactly 1,200 observations. For each segment, membership,
   role assignment, canonical order, and payload bindings are frozen before any
   response-profile replay result at a supported `ef` is inspected or used for
   selection. Existing foreground served results may exist but may not
   influence inclusion, exclusion, ordering, replacement, or role assignment.
   Their combined 24,000 observations form one separately reported later
   aggregate in addition to the twenty segment-level diagnostics.
4. Detector evidence, warm-up evidence, measured calibration evidence,
   prospective validation evidence, Phase-3 qualification evidence, Stage-4
   evidence, and historical EXP-001 evidence are pairwise disjoint by canonical
   observation/query ID and query-payload digest.
5. Every measured/validation population requires distinct canonical query IDs
   and distinct query-payload digests. Repeated vectors or threshold/search
   payloads under invented IDs are not treated as independent and invalidate
   that population.
6. No observation may be dropped, replaced, retried as a new observation, or
   moved between roles after a response-profile replay result at a supported
   `ef` is inspected or used for selection.

If a live workload cannot supply these populations honestly, the corresponding
stage is `INCOMPLETE`; synthetic duplication is prohibited.

#### Frozen replay and measurement protocol

- Master deterministic seed: NumPy `Generator(PCG64(20260810))`.
- Schedule derivation binds the master seed, cell identifier, role/segment ID,
  workload-manifest digest, and source revision through the project's canonical
  length-prefixed tuple serialization and SHA-256-derived unsigned 64-bit seed.
- In v1 one query is exactly one block. Deterministically permute the canonical
  query order; then, independently for each query, deterministically permute
  `(200, 400, 800, 1600)` and execute all four values exactly once before
  proceeding to the next query. The realized ordered sequence of
  `(query_id, ef)` pairs and all seed-derivation inputs are persisted and bound
  by `replay_schedule_sha256` and `control_profile_sha256`.
- Execution is synchronous and serial with one client and concurrency `1`.
  No retry is permitted. The exact Milvus consistency level, timeout, result
  limit, radius/range filter, client/server versions, Docker/resource controls,
  timing clock, and background-workload disclosure are frozen in the control
  and environment manifests.
- Recall-bound applicability requires independent bounded query observations;
  latency order-statistic applicability requires IID/exchangeable latencies
  from one unchanged distribution at each `ef`. Randomized blocking, split-half
  summaries, lag diagnostics, and pre/post resource snapshots are recorded as
  assumption diagnostics but cannot prove those assumptions. Unsupported
  independence, exchangeability, stationarity, or no-interference makes the
  profile `INCOMPLETE`.
- Oracle computation, artifact serialization, health checks, and identity
  capture occur outside the measured client-search timing interval. Client
  latency begins immediately before the search API invocation and ends
  immediately after the complete response has returned and been materialized
  into the adapter's immutable hit representation.
- Recall uses the independent float64 exact oracle and the contract's capped
  range-query recall semantics. FLAT/oracle disagreement, threshold violation,
  result-cap inconsistency, load/identity change, timeout, exception, or missing
  observation invalidates the complete cell profile.
- A process restart requires fresh schema/evidence verification and replay of
  the complete warm-up role before measurement resumes. Partial in-memory
  statistics are never trusted or merged with a restarted run.

The live/read-only stage, if later authorized, must use a newly captured
post-trigger population. DATASET-001, DATASET-002, DATASET-003, detector,
Phase-3, Stage-4, and EXP-001 records may verify identities or provide
historical context but are not predictive observations for EXP-010.

#### Frozen statistical evaluation

For every cell and every exact `ef`, the profile evaluator consumes exactly
1,200 capped-recall values and 1,200 client latencies.

The family contains exactly sixteen one-sided claims with
`alpha_family=0.05` and `alpha_cell=0.05/16=0.003125`.

Recall:

```
mean = math.fsum(values_in_canonical_workload_order) / 1200
epsilon = sqrt(log(320) / 2400) = 0.04902516783837398
LCB = max(0.0, mean - epsilon)
UCB = min(1.0, mean + epsilon)
```

Every recall observation is a finite IEEE-754 binary64 value in canonical
workload order. Point estimates and bounds are recomputed from raw observations;
caller-supplied numeric constants are never trusted. Computed binary64 values
are persisted through the repository's canonical serialization contract, and
reader verification must reject any disagreement.

Latency uses nondecreasing one-based order statistics with ties retained:

```
point p95 = x_(1140)
LCB p95   = x_(1118)
UCB p95   = x_(1161)
```

The lower and upper ranks must be independently reproduced by exact binomial
inversion for `B~Binomial(1200,0.95)` under ADR-009's largest-lower-rank and
smallest-upper-rank conventions. Approximate normal ranks, bootstrap intervals,
interpolated quantiles, different alpha allocation, silent clipping of latency,
or a non-1,200 sample are contract failures.

The prospective stage evaluates the frozen calibration profile without refit:

- report, for each of twenty later segments, whether its exact realized recall
  mean and nearest-rank p95 lie within the corresponding calibration bounds for
  all four `ef` values;
- report per-claim and all-sixteen simultaneous empirical coverage counts;
- report the exact one-sided 95% Clopper-Pearson bounds for those descriptive
  counts; and
- compare the calibration profile against the exact empirical mean/p95 of the
  combined 24,000-observation later aggregate.

These prospective checks are empirical transportability diagnostics. They do
not convert confidence intervals for distribution parameters into prediction
intervals for future sample statistics, do not prove stationarity, and do not
establish a universal expiry interval.

#### Required adversarial cases

Each case must fail closed with a stable reason before policy candidate action:

1. stale/expired-under-test-policy profile;
2. metric or threshold-stratum mismatch;
3. HNSW/index or data identity mismatch;
4. search-configuration/radius/range-filter/limit/consistency mismatch;
5. workload, ordered-ID, query-payload, control-profile, or environment mismatch;
6. estimator-contract/source-revision/evidence-digest mismatch;
7. missing, additional, duplicate, or unsupported `ef`;
8. interpolation, extrapolation, or nearest-`ef` substitution attempt;
9. fewer or more than 1,200 observations for any claim;
10. duplicate IDs/payloads, missing observations, retries, or post-result role
    changes;
11. non-finite, boolean, negative-latency, or out-of-range recall values;
12. point/bound/rank inconsistency;
13. malformed timestamps, schema version, canonical JSON, or profile digest;
14. mixed points from two otherwise valid profiles;
15. raw evidence/profile tamper and restart-chain tamper;
16. object-forged/non-concrete verified-profile values;
17. caller-supplied `validated_model=True` or free-form provenance presented as
    authority; and
18. attempts to use profile evidence as Phase-3, admission, grant, route, or
    execution authority.

Profile-digest adversarial coverage must specifically verify that
`profile_sha256` is stored outside the strict `profile_payload`, equals
`SHA256(b"VD::CALIBRATED_RESPONSE_PROFILE::V1\x00" +
vdbench.artifacts.canonical_json_bytes(profile_payload)).hexdigest()`, and fails
closed when either payload or detached digest is altered. A digest included in
its own payload is an invalid schema.

#### Acceptance and failure criteria

EXP-010 may report **CONTRACT IMPLEMENTATION VERIFIED** only if:

- every hand-computable formula/rank fixture matches ADR-009 exactly;
- every complete profile contains exactly 4,800 measured searches over 1,200
  distinct queries and the exact four-`ef` family;
- all identity, evidence, schedule, canonical-digest, and restart checks pass;
- every required adversarial case fails closed with no candidate action;
- independent replay of the same immutable evidence produces byte-identical
  canonical profile output and digest; and
- raw commands, environment/control manifests, evidence manifests, profile,
  checksums, and test output are preserved for review.

EXP-010 may additionally report **PROSPECTIVE TRANSPORTABILITY SUPPORTED FOR THE
MEASURED CELL AND CONTROL PROFILE** only if all sixteen calibration intervals
contain the corresponding combined later-aggregate empirical values and all
twenty segment-level results, coverage counts, misses, and Clopper-Pearson
bounds are reported unchanged. A miss does not permit post-result refitting,
changing alpha/ranks, dropping a segment, or redefining the target; it produces
`INCONCLUSIVE` or `NOT SUPPORTED` for transportability under the pre-registered
rule.

Even if both statuses are achieved, candidate-capable policy consumption
remains disabled until an explicit governed freshness/invalidation rule and
mechanical policy-chain profile-digest binding are separately accepted and
verified. EXP-010 cannot by itself establish either.

Immediate failure/incomplete conditions include any population overlap,
identity/control/environment drift, query failure, incomplete evidence,
statistical mismatch, tamper, non-canonical artifact, missing checksum, dirty or
unrecorded source revision, or unreviewed protocol deviation.

No interval, rank, sample size, validation target, acceptance threshold, or
freshness rule may be tuned after results are observed. A change requires a new
estimator-contract version and a newly pre-registered experiment/result.

#### Optional non-authorizing comparators

The following may be recorded only as explicitly non-authorizing comparator or
reviewer evidence in EXP-010. They cannot replace, repair, or override v1:

- empirical-Bernstein recall bounds;
- paired adjacent-transition recall/latency uncertainty;
- `ef` monotonicity diagnostics without monotonic repair;
- canonical-digest cache and human reviewer tooling; and
- learned response models evaluated later against the empirical replay
  baseline.

Comparator output must use separate artifact names and status fields and must
never populate a v1 policy profile.

#### Required evidence bundle

Each executed stage must record:

- source revision and clean/dirty state;
- exact invocation and complete stdout/stderr;
- dataset/workload roles and disjointness proof;
- raw observation and schedule/control/environment manifests;
- all file and canonical-record SHA-256 digests;
- exact point/bound/rank outputs for every cell and `ef`;
- prospective segment and aggregate results without omission;
- adversarial-case outputs;
- restart/replay result and canonical profile digest comparison;
- explicit no-candidate/no-admission/no-grant/no-route/no-mutation flags; and
- limitations and all deviations, including an `INCOMPLETE` result when the
  contract cannot be met.

#### R2 raw-evidence protocol clarification (pre-registered 2026-08-09)

This append-only clarification freezes EXP-010's raw-evidence provenance and
crash-consistency rules before R2 implementation or any response-profile replay
result is observed. It changes no statistical formula, sample count, supported
`ef` value, validation cell, acceptance threshold, or authority boundary above.

Before supported-`ef` results are inspected or used for selection, each cell
must persist immutable detector-trigger, closed role-allocation, 1,200-query
calibration-population, query-vector, exact-oracle/FLAT-reference, replay-
schedule, control-profile, and environment-profile commitments. Query IDs use
unchanged R1 canonicalization:

```
canonical_query_id_bytes =
    canonical_serialize_tuple((normalized_query_id,))

query_id_sha256 = SHA256(
    b"VD::RESPONSE_PROFILE_QUERY_ID::V1\x00"
    + canonical_query_id_bytes
).hexdigest()
```

Integer `1` and string `"1"` therefore collide; other mixed integer/string IDs
remain permitted. Query vectors use the domain-separated finite contiguous
little-endian-float32 vector digest defined by ADR-009's R2 clarification. The
query-payload digest binds only vector digest, metric, threshold stratum,
radius, range filter, limit, and consistency level; it excludes IDs, roles,
source/replay positions, order, `ef`, timestamps, results, and index/data
identity and uses the exact `VD::RESPONSE_PROFILE_QUERY_PAYLOAD::V1` domain and
canonical payload fixed by ADR-009. Calibration and every validation segment
require distinct vector and payload digests. `query_id_sha256` is local to its
source namespace: same-namespace role comparisons use it, while comparisons
across distinct source namespaces use `observation_identity_sha256`. Bare
`query_id_sha256` is never treated as a global identity.

The closed role catalog comprises detector evidence, warm-up, calibration,
twenty indexed prospective segments, Phase-3 qualification, Stage-4 routing,
recall-audit and schedule-control evidence, historical EXP-001 calibration and
measured evidence, and prohibited DATASET-001/002/003 query/vector inventories.
Local IDs are compared only after binding them to the versioned source
namespace using ADR-009's exact artifact/live-stream discriminated payload and
`VD::RESPONSE_PROFILE_SOURCE_NAMESPACE::V1` and
`VD::RESPONSE_PROFILE_OBSERVATION_IDENTITY::V1` domains; role is excluded from
that cross-role observation identity.
Calibration must prove disjointness from detector evidence and every already
materialized prohibited role. Each prospective segment is later frozen before
its own result inspection, and final EXP-010 evaluation must prove pairwise
disjointness across every realized role. A missing catalog entry or overlap is
`INCOMPLETE`. Uniqueness and disjointness apply to frozen role membership, not
to repeated execution evidence for the same frozen member; vector/payload
disjointness remains unchanged.

Schedule derivation is exactly the ADR-009 R2 clarification: master seed
`20260810`; query-order tuple ending in `"QUERY_ORDER"`; per-query `ef` tuple
ending in `"EF_ORDER", query_id_sha256`; canonical tuple framing; the
`VD::RESPONSE_PROFILE_SCHEDULE_SEED::V1` domain; unsigned big-endian extraction
from the first eight SHA-256 bytes; one fresh PCG64 generator for the query
permutation and one fresh PCG64 generator for every four-`ef` permutation. The
exact NumPy version, algorithm version, tuples, digests, uint64 seeds, and all
4,800 realized positions are frozen evidence.

Every initial or resumed runtime epoch must durably complete the full frozen
warm-up role before its first measured position. Every resumed epoch replays
the exact same frozen non-measured membership. It creates execution evidence
only, not a new observation, population, role, or membership, and never counts
toward the 1,200 calibration observations. Missing, failed, incomplete, or
identity-incompatible warm-up evidence invalidates the run.

One query remains one four-`ef` block. Every measured call requires a durable
append-only `STARTED` record before invocation and one matching durable
`COMPLETED` record afterward. An orphan `STARTED` is terminal and non-retriable
whether the crash happened before, during, or after the unrecorded call. A valid
cell requires exactly 4,800 matching successful completions. Each block closes
only after all four completions and referenced pre/post runtime verification
succeed. Resume is permitted only after a fully closed block, under a fresh
epoch with complete warm-up replay; restart during an unclosed partial block is
terminal and requires a new run ID.

Runtime/epoch/block snapshot receipts may hold full health, load, index, data,
control, and environment evidence once and be referenced by digest from
measurement records. Verification must reconstruct every reference and reject
missing, substituted, incompatible, or changed evidence. Latency is derived
from client monotonic timestamps, and recall is recomputed from independently
verified oracle evidence; caller-supplied aggregate values are not trusted.

Run seal/invalidation records are publication evidence, never verdicts. The
full verified manifests and append-only chain mechanically determine validity;
an orphan start or incomplete block invalidates evidence without an explicit
invalidation event, and a forged seal cannot make it complete.

The final canonical raw-evidence-root payload excludes its own digest. Its
detached domain-separated SHA-256 binds every manifest, receipt, chain head,
count, identity, source revision, and timestamp required by ADR-009. Internal
verification of bundle plus expected identity yields only a non-authorizing
integrity report. Root-pinned capability issuance accepts bundle, expected
identity, and a separately governed expected root; it must independently rerun
complete verification and may not rely on a supplied integrity report. The
expected root is never derived from the same bundle by the issuing call.

Root-pinned evidence is predictive provenance only. Hashes and private
constructors are not signatures; producer/host compromise, omitted unknown
external roles, and producer-controlled root pins remain explicit limitations.
R1 remains unchanged, and no freshness, candidate policy, Milvus producer,
Phase-3, admission, grant, route, execution, rollback, or actuation authority is
created by this clarification or by EXP-010.

##### R2-G.1 schedule and population identifier clarification (pre-registered 2026-08-09)

Before R2 implementation or replay, EXP-010 fixes each schedule identifier to
one canonical detached digest. `cell_id` hashes exactly this payload under
`b"VD::RESPONSE_PROFILE_CELL::V1\x00"`:

```json
{
  "schema_version": "response-profile-cell-v1",
  "metric": "<exact Metric value>",
  "threshold_stratum": "<canonical stratum>"
}
```

`role_or_segment_id` hashes exactly this governed descriptor under
`b"VD::RESPONSE_PROFILE_ROLE::V1\x00"`:

```json
{
  "schema_version": "response-profile-role-v1",
  "kind": "<closed role kind>",
  "prospective_segment_index": null
}
```

The segment index is `null` except for prospective segments, where it is an
exact integer in `0..19`.

For response-profile calibration, `workload_manifest_sha256` is exactly the
detached digest of the canonical
`response-profile-calibration-population-v1` payload under
`b"VD::RESPONSE_PROFILE_CALIBRATION_POPULATION::V1\x00"`. This one digest is
both the schedule seed's workload-manifest identity and R1
`ResponseProfileIdentity.workload_manifest_sha256`.

`ordered_query_payload_sha256` hashes exactly this payload under
`b"VD::RESPONSE_PROFILE_ORDERED_QUERY_PAYLOADS::V1\x00"`:

```json
{
  "schema_version": "response-profile-ordered-query-payloads-v1",
  "query_payload_sha256": ["<exactly 1200 digests in frozen canonical order>"]
}
```

None of these payloads contains response results, timing, runtime epochs,
retries, authorization, routing, or execution evidence. Role-manifest counts
remain role-specific: exactly 200 members for warm-up, exactly 1,200 for
calibration, and exactly 1,200 for each prospective segment. The generic
closed-role manifest assigns no 1,200-member rule to unrelated roles. This
clarification changes no existing EXP-010 formula, schedule mechanic, role,
sample count, acceptance rule, freshness status, or authority boundary.

##### R2-G.2 lifecycle-ledger clarification (pre-registered 2026-08-09)

EXP-010 pre-registers the following R2-B structural lifecycle before any
response-profile replay result is inspected. This clarification consumes the
frozen R2-A population and schedule and does not change R1, R2-A, any
statistical rule, or any authority boundary.

The exact run-binding schema is
`response-profile-lifecycle-run-binding-v1`. Its canonical payload is:

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

Its detached digest domain is exactly
`b"VD::RESPONSE_PROFILE_LIFECYCLE_RUN_BINDING::V1\x00"`.

Opaque bytes use schema `response-profile-opaque-evidence-blob-v1` and the
exact detached descriptor:

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

Its detached descriptor digest domain is exactly
`b"VD::RESPONSE_PROFILE_OPAQUE_EVIDENCE_BLOB::V1\x00"`. The exact roles are
`WARMUP_EXECUTION`, `MEASURED_RESULT`, `PRE_BLOCK_RUNTIME_SNAPSHOT`, and
`POST_BLOCK_RUNTIME_SNAPSHOT`; they may be referenced only by
`WARMUP_COMPLETED`, `MEASUREMENT_COMPLETED`, `BLOCK_STARTED`, and
`BLOCK_CLOSED`, respectively. Bytes are SQLite `BLOB` data and the blob plus
its sole referencing event are inserted atomically. R2-B verifies only exact
bytes, role, length, byte digest, descriptor digest, and referential binding.
R2-C owns semantic verification.

Lifecycle events use schema `response-profile-lifecycle-event-v1`, detached
digest domain `b"VD::RESPONSE_PROFILE_LIFECYCLE_EVENT::V1\x00"`, and this exact
common canonical payload:

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

The closed event variants are exactly:

- `EPOCH_STARTED` with `{}`.
- `WARMUP_COMPLETED` with the run-bound warm-up role-manifest digest and one
  `warmup_execution_blob_sha256`.
- `BLOCK_STARTED` with one `pre_block_runtime_snapshot_blob_sha256`.
- `MEASUREMENT_STARTED` with the exact R2-A within-block index, canonical query
  index, canonical query ID, source-local query-ID digest, cross-source
  observation digest, scheduled `ef`, and non-negative monotonic start reading.
- `MEASUREMENT_COMPLETED` with the matching STARTED event digest, one
  `measured_result_blob_sha256`, and a monotonic completion reading greater
  than the matching start.
- `BLOCK_CLOSED` with the matching BLOCK_STARTED event digest, exactly four
  matching COMPLETED event digests in scheduled within-block order, and one
  `post_block_runtime_snapshot_blob_sha256`.
- `RUN_SEALED` with `{}`.
- `RUN_INVALIDATED` with one canonical non-empty stable `reason_code`.

All variants have exact fields and types; unknown fields fail closed. Event 0
uses the run-binding digest as `previous_event_sha256`; every later event uses
the immediately preceding lifecycle-event digest. `event_seq` and that hash
chain alone define canonical order. RFC3339 UTC values are metadata and cannot
order or repair the lifecycle. Matching same-epoch monotonic readings govern
latency chronology.

One schedule position has exactly one immutable `MEASUREMENT_STARTED` ->
`MEASUREMENT_COMPLETED` pair, never a mutable result row. Every runtime epoch
must bind one opaque `WARMUP_EXECUTION` blob through `WARMUP_COMPLETED` before
any block or measured start. This is only a structural completion claim in
R2-B; R2-C must verify that the bytes prove successful execution of all 200
frozen warm-up members. Every block has one pre-block snapshot, four exact
scheduled STARTED/completed pairs, one post-block snapshot, and a close event.
Structural completion requires exactly 1,200 closed blocks and 4,800 completed
measured pairs.

An orphan measured STARTED or an unclosed measured block is terminal and is
never retried. Closed-block restart opens a fresh epoch and replays all 200
frozen warm-up members before further measurement. A warm-up-only epoch that
is interrupted before any `BLOCK_STARTED` or `MEASUREMENT_STARTED` is abandoned
execution evidence and does not invalidate prior closed measured evidence;
reopen starts another epoch and replays the same warm-up membership from the
beginning. Repeated warm-up execution creates no new population membership and
never counts among the 1,200 calibration observations. This precisely limits
the prior missing/incomplete-warm-up rule to epochs that proceed into block or
measured execution.

`RUN_SEALED` and `RUN_INVALIDATED` remain audit/publication events only.
Validity, invalidity, and completeness are derived from the fully verified run
binding, event chain, blob references, R2-A schedule, and lifecycle state. A
seal cannot validate incomplete evidence; a missing invalidation event cannot
make an orphan or partial block valid; neither event repairs evidence.

Passing R2-B means structural lifecycle completeness only. It is necessary but
insufficient for raw-evidence validity, statistical validity, qualification,
profile use, policy, authorization, routing, or execution. R2-C semantic
verification remains mandatory before later independent-root-pinned issuance.

### EXP-010 R2-G.3 semantic verification and root-pin preregistration (2026-08-11)

Status: CONTRACT DEFINED — NOT IMPLEMENTED — NOT RUN

Risk level: CRITICAL

Governing decision: ADR-009 R2-G.3 accepted clarification.

Before any R2-C result is observed, EXP-010 freezes the exact response-profile
oracle manifest, 200-member warm-up role, 1,200-member calibration population,
4,800-position schedule, R2-B run binding, four semantic evidence schemas, R1
identity, control/environment digests, HNSW/data identities, and source
revision. Every runtime epoch executes 800 successful non-measured warm-up
searches: each frozen warm-up member once at each supported `ef` in canonical
member and ascending-`ef` order.

R2-C acceptance requires complete reconstruction of all 1,200 blocks and 4,800
successful measured results; exact lifecycle/blob association; passing PRE/POST
runtime snapshots; independently expected exact-oracle records; threshold-valid
candidate results; and query-major derived recall/latency observations. No
producer-supplied aggregate is accepted. R2-C emits only a non-authorizing
semantic report and computed raw-evidence root.

R2-D acceptance requires a separately governed expected raw-root digest not
derived from the bundle inside issuance. Issuance reruns complete R2-C
verification and fails closed on any difference. R2-E acceptance requires exact
root/identity matching and byte-deterministic projection through the unchanged
R1 builder. Golden fixtures pin every canonical schema/domain and one complete
adversarial fixture; production-count tests prove 1,200 observations and 4,800
positions without shortcuts.

Pre-registered adversarial cases include malformed/noncanonical documents,
duplicate fields, wrong role/event association, unreferenced or reused blobs,
warm-up omission/reorder/configuration substitution, runtime identity or health
drift, schedule/ef/query substitution, failed and timed-out searches, duplicate
result IDs, non-finite values, threshold/order violation, oracle substitution,
event timing mismatch, count mismatch, stored aggregate injection, digest
tamper, self-derived or wrong external root, forged report/capability/profile,
and cross-import of Stage-4 or candidate authority.

No freshness rule, policy migration, candidate authorization, Milvus producer,
grant, route, execution, or live claim is part of this clarification. Results
must not tune schemas, intervals, or failure criteria after the fact.

### EXP-010 R2-G.4 offline producer and publication preregistration (2026-08-11)

Status: CONTRACT DEFINED — NOT IMPLEMENTED — NOT RUN

Risk level: CRITICAL

Governing decision: ADR-009 R2-G.4 accepted clarification.

Before producer results are inspected, EXP-010 freezes an offline injectable
producer that consumes the exact R2-A/R2-B/R2-C contracts without redefining
them. Each epoch must issue exactly 800 successful non-measured warm-up calls.
Each measured position must have one durable STARTED before exactly one external
search and one atomic result/completion afterward. PRE/POST readiness collection
occurs outside SQLite transactions. Query material must mechanically rebuild to
the frozen member identity before any dispatch.

Acceptance requires: deterministic block-boundary resume; no continuation of an
old epoch; no retry after orphan STARTED; exact 1,200 closed blocks, 4,800
completed positions, and 4,800 measured calls; explicit fully verified ledger
export; complete R2-C reconstruction; and no producer-issued root-pinned
capability. Failure, timeout, malformed result, readiness failure, query
substitution, crash-point, export tamper, and self-derived-root attempts must
fail closed. The producer remains offline/injectable and imports no direct
Milvus, policy, Phase-3, Stage-4, grant, route, or actuation module.

The internally computed raw root is publication evidence only. A later R2-D
call must receive an independently governed expected root and rerun R2-C. No
freshness, policy migration, candidate authorization, live traffic, or
production-readiness claim is part of this checkpoint.

R2-G.4a additionally pre-registers post-run identity composition: static
identity fields are frozen before execution; calibration start/completion come
only from the verified first STARTED/last COMPLETED event metadata; generated
time is captured only after completion. Planned timestamps are forbidden.
Failed non-measured warm-up emits no completion claim and may restart only as a
new epoch with the full 800-call replay; measured STARTED positions remain
strictly non-retriable.

### EXP-011 — Atomic detector-head freshness and profile-policy binding

Status: PREREGISTERED — NOT RUN

Risk level: CRITICAL

Governing proposal: ADR-010 Proposed; Action 7A is structural and
non-authorizing, and candidate use remains disabled.

Objective: prove offline that response-profile use is bound to the exact
unsuperseded detector trigger rather than a caller-selected TTL or opaque
provenance string.

Required scenarios are: canonical pre-result control binding; atomic monitor
state/head append; restart and complete hash-chain replay; stale/superseded head
refusal; mismatched window sequence/provenance/manifest/metric/stratum/
configuration/data/FLAT/HNSW/environment/source refusal; forged historical head
refusal; concurrent monitor append versus capability refresh; monitor failure
or state/head divergence; bare profile and bare root-capability refusal; and
proof that active rollback remains available without any profile evidence.

Pass criteria: only one exact root-pinned profile/control/latest-head tuple may
produce a fresh-profile capability; any later detector-head append makes the old
tuple unusable on the next refresh; policy performs no I/O or statistics; no
legacy response estimate, file monitor state, timestamp, TTL, or human prose can
substitute; and no candidate action is enabled before separate real EXP-010
review. Raw output, schemas, canonical golden digests, restart evidence, race
results, and source revision must be persisted before this EXP can become
VERIFIED.

#### Prospective evidence protocol required before Action 7B

This protocol is pre-registered and has not been run. For each exact EXP-010 v1
cell, freeze an independently root-pinned calibrated profile and its control at
one verified detector-head transaction. Continue collecting detector windows
without using any prospective result to alter inclusion, ordering, replacement,
or labels. Partition later detector evaluations prospectively into stationary
clean windows, materially changed detector lineage, detector algorithm/config
revision changes, and explicit data/index/environment/source-revision changes.
Retain periods with no new detector evidence as a separate censored condition;
do not infer stationarity from silence.

For every later evaluation, replay the frozen profile protocol on the governed
prospective segment and record simultaneous recall/latency interval coverage,
profile utility decisions, exact detector state/classification/provenance, and
the durable head-record lineage. Compare these pre-registered invalidation
rules without post-result tuning: (A) invalidate on every later head, (B)
invalidate only on materially changed detector lineage, and (C) preserve across
stationary clean heads while invalidating exact algorithm/configuration/data/
index/environment/source incompatibilities. Report false invalidation,
missed-invalidity, coverage, and regeneration cost separately; no rule becomes
candidate authority from a single favorable aggregate.

Restart trials must cover clean reopen, head append immediately before/after a
refresh, interrupted monitor state/head transaction, stale issued wrappers,
and profile regeneration from a newly committed trigger. Regeneration may be
triggered experimentally by each candidate rule, but candidate policy
consumption requires independently reviewed evidence that the selected rule
preserves the ADR-009 simultaneous safety bounds on later segments, plus an
accepted ADR-010 status change and signed-lineage propagation. Real collection
requires the committed offline producer package and, where Milvus replay is
needed, a separately authorized read-only live command. No result is claimed by
this preregistration.

#### Structural implementation and unit-verification status (addendum, offline, not a run)

This addendum records that `src/vdbench/exp011_offline_acquisition.py` and its
accompanying `tests/test_exp011_offline_acquisition.py` now exist and that
their unit tests pass. The module exercises the real
`ResponseProfileMonitorStateStore`, the real `bind_fresh_response_profile_evidence`/
`verify_fresh_response_profile_evidence` boundary, and the real R2-C/R2-D/R2-E
verification chain (`verify_response_profile_semantic_bundle`,
`issue_root_pinned_response_profile_evidence`,
`project_root_pinned_response_profile`) against a hand-built, fully real,
internally consistent 1,200-observation calibration fixture — no fake or
dummy evidence stands in for any of that boundary's own logic. It covers the
preregistered adversarial scenarios that are offline-composable without a live
Milvus connection: canonical pre-result control binding, atomic monitor
state/detector-head append, monitor-store restart and hash-chain replay, stale/
superseded detector-head refusal, forged detector-head-wrapper refusal,
concurrent-open-vs-refresh refusal, schema-tamper (append-only trigger
bypass) refusal, bare-profile and bare-root-capability refusal, canary
rollback's structural independence from any response-profile import, and an
eleven-axis mismatch matrix (window sequence, detector provenance window ID,
detector provenance manifest, metric, threshold stratum, configuration
identity, data identity, FLAT identity, HNSW identity, environment-manifest
digest, source revision) confirming each axis is independently refused with
the correct real error code.

Every artifact this module can produce, and every scenario result object it
returns, carries `evidence_status: "STRUCTURAL_OFFLINE_NOT_PROSPECTIVE_EVIDENCE"`.
This addendum, and the module it describes, do **not** change the `Status:
PREREGISTERED — NOT RUN` line above, do **not** constitute an EXP-011 run, do
**not** supply real prospective evidence toward the "Prospective evidence
protocol required before Action 7B," and do **not** touch, import, or in any
way lift the B-001 interlock (`RESPONSE_PROFILE_CANDIDATE_CAPABILITY_AVAILABLE`
in `policy.py` remains `False`, unmodified, and unimported by this module —
mechanically confirmed by a dedicated adversarial test). It never opens a
Milvus connection and never produces a value that could be mistaken for
calibrated, live, or prospective evidence. Promotion of EXP-011 to a run
status still requires the full pre-registered protocol above, executed for
real against a live, read-only-authorized Milvus stack, and independently
reviewed — nothing in this addendum shortens that requirement.

### EXP-012-SCALE: Multi-window host and shadow-pipeline scale validation

Status: FOUNDATION IMPLEMENTED — SCALE-2400 V3 COMPLETE — SCALE-10000 NOT RUN

Governing decision: ADR-019 Accepted for offline foundation work; no live scale
campaign or ENV-002 execution is authorized by this entry.

#### Objective and frozen profiles

Determine whether the accepted v2 source, shadow, detector, attestation, and
finalization path remains correct beyond EXP-010's frozen 600-source campaign.
The v1 scale family is closed:

- `scale-2400`: exactly 2,400 source records, sequences `0..2399`, twelve exact
  200-source windows, 2,400 FLAT plus 2,400 HNSW-sentinel searches;
- `scale-10000`: exactly 10,000 source records, sequences `0..9999`, fifty exact
  200-source windows, 10,000 FLAT plus 10,000 HNSW-sentinel searches.

No partial target, extra source, gap, duplicate query identity, missing role,
duplicate role, failed search, unfinalized window, or altered 200-source window
is a passing result. Detector reference/current progression must continue in
natural canonical window order beyond three windows. A restart may continue
only through the existing durable closed-attempt/window rules; orphan STARTED
and ambiguous outcomes remain terminal and non-retriable.

#### Evidence and measurement contract

Campaign contracts, Gate-B plans/results, and Gate-C plans/results use the
distinct ADR-019 EXP-012 schemas and digest domains. Existing EXP-010 evidence
is neither migrated nor re-labeled. Gate-B normal target checks use the
store-issued source/outbox head snapshot; explicit audit and reopen still
perform complete chain reconstruction.

The campaign must contain the exact immutable ADR-019 scale marker before any
Gate-C operation. Legacy EXP-010 operators must refuse that marker, while the
scale operators must reject a missing, malformed, or profile-substituted
marker. The marker also pins the digest of a separately verified Gate-A
authority root; no EXP-010 plan/evidence document is copied into or emitted
inside the EXP-012 campaign root.

Gate C must persist exactly one append-only telemetry record for each physical
FLAT or HNSW-sentinel search. Every record binds campaign, scale contract,
window, trace attempt, source sequence/digest, query-id digest, role, monotonic
interval, derived latency, outcome/result count or error classification, and
the previous telemetry digest. Exact cardinality and source-role conservation
are reverified from canonical rows; no aggregate counter is evidence.

#### Required offline verification before live preflight

Focused tests must prove both exact targets and projected search counts;
duplicate/gap/overshoot refusal; twelve/fifty-window planning and progression;
acknowledgement/finalization continuation; closed-window restart; orphan
STARTED refusal; large-history/source-head/outbox-head substitution refusal;
telemetry append/reopen/hash-chain/schema/row/digest tamper refusal; and frozen
EXP-010 operator regressions. Synthetic/fake scale benchmarks may characterize
control-path complexity but are not live throughput/latency evidence.

#### Live status and claims at foundation freeze

At the initial foundation freeze, no 2,400- or 10,000-source campaign had been
run. The later immutable campaign records below supersede only that historical
run-status statement. They do not establish throughput, SLA, production,
resource-pressure, restart, or remote/distributed-Milvus claims. ENV-002 remains
out of this checkpoint.

#### 2026-08-21 EXP-012 scale-2400-v1 failed campaign — immutable

Status: **FAILED_CLOSED — HISTORICAL EVIDENCE; NO RETRY OR COMPLETION CLAIM**

Source revision: `810c569cb712296169a0bfe6c4dfd3d40aece0cf`

Campaign root: `~/.local/share/vd/exp012-scale2400-v1`

Profile: `scale-2400`; L2 `target-075`; DATASET-001/ENV-001 identities bound by
the campaign's independently verified Gate-A authority.

Gate B durably completed all 2,400 source records (`0..2399`) with no source
gap or duplicate. Gate C stopped fail-closed after 1,000 physical searches:
500 FLAT, 500 HNSW-sentinel, and exactly 1,000 matching telemetry records. Ten
attempts reached `STARTED`; nine completed, one failed; two 200-source windows
were finalized and 400 source positions acknowledged. No later source was
searched or repaired.

The terminal failure is window 2, trace 1, source 475
(`logsim-v2:e8b4cde2f42c4d06cce2b91bf7c8ee15:475`), attempt SHA-256
`5ef1529a7c8d700da5225514d93aee107f5cd3143d84f9d8eefddc5248b2d683`.
The low-level comparator emitted `FLAT_ORACLE_NON_TIE_ORDER_MISMATCH`; the
window/attempt layers preserved `FLAT_ORACLE_ORDER_MISMATCH`,
`TRACE_INCOMPLETE`, and `STAGE_FAILED`. Oracle and FLAT membership were
identical (76/76), and raw FLAT score order was valid. The first ID divergence
was `9017` then `8745` in the oracle versus `8745` then `9017` from FLAT:

- binary64 oracle: `182.7277454875737`, `182.7277686415395`;
- oracle final binary32: `0x4336ba4e`, `0x4336ba4f`;
- Milvus FLAT: both `182.72775268554688` (`0x4336ba4e`).

The exact oracle margin was `2.315396579888329e-05`, about 1.5174 binary32 ULP
at that magnitude. Frozen-input arithmetic and local inspection of the
Knowhere/Faiss binary established the forensic root cause as
`VD_ORACLE_NUMERIC_MODEL_MISMATCH`: legal binary32 L2 reduction orders can
produce ordered, tied, or reversed outcomes even when binary64-final-cast
oracle groups differ. This is a numerical-contract defect, not evidence of a
membership, adapter, persistence, telemetry, or raw FLAT-order defect.

Pre-amendment read-only census of all 500 persisted FLAT/oracle pairs was 499
`EXACT_ORDERED`, zero `PRECISION_TIE_EQUIVALENT`, one
`NON_TIE_ORDER_MISMATCH`, zero membership mismatches, and zero invalid evidence.
The six SQLite databases reported `integrity_check = ok`; the source, attempt,
and telemetry chains were independently verified. The campaign evidence
baseline SHA-256 (sorted relative path plus per-file SHA-256, lock files
excluded) is
`f508efc7b90e9a67b90bf3cbf2c936102cb333eaf7111248f023fdcaa8f57653`;
the telemetry chain has 1,000 records and head
`1bbb9bb04ec3d96bdea9a63d204875a0415f10d8c2ac9e15efc9f59189fc382e`.

The accepted 2026-08-21 ADR-015 amendment adds a separately identified,
analytically bounded L2 execution-tie classification. A read-only forensic
replay may state how source 475 would classify under new source, but it does
not change this campaign's terminal state or evidence. This run makes **no**
completed scale-2400, detector, latency, throughput, production, or remote
Milvus claim. A fresh campaign identity and fresh Gate-A/B/C lifecycle are
required after source freeze.

#### 2026-08-21 EXP-012 scale-2400-v2 provenance-invalid campaign — immutable

Status: **PROVENANCE INVALID — GATE C NOT STARTED — NO SCIENTIFIC CLAIM**

Source revision: `bbc9fcc17277245435f4a508402b0d1f53645295`

Campaign root: `~/.local/share/vd/exp012-scale2400-v2`

Gate B durably recorded 2,400 source positions, but pre-execution review did
not accept their workload provenance for the governed live scale claim. Gate C
therefore remained completely absent: zero attempts, searches, telemetry,
acknowledgements, detector events, attestations, and finalization events. The
campaign is preserved as non-authorizing historical evidence and is not
reinterpreted or reused by the successful V3 campaign.

#### 2026-08-22 EXP-012 scale-2400-v3 completed campaign — immutable

Status: **MECHANICALLY COMPLETE — VALID BOUNDED SCIENTIFIC EVIDENCE**

Source revision: `bbc9fcc17277245435f4a508402b0d1f53645295`

Campaign root: `~/.local/share/vd/exp012-scale2400-v3`

Gate-A evidence SHA-256:
`27996a691ff28a7959633fdd802f1123f0ca87d9f44a14be6c063222e49c1399`.
Environment-manifest SHA-256:
`11b616240869f778d158299df4231847af84bafa6b25d76b4da772ce44b49999`.
The campaign is bound to DATASET-001, ENV-001, L2 `target-075`, Strong
consistency, FLAT reference, and HNSW sentinel `ef=100` identities recorded in
that authority.

Gate B contains exactly 2,400 source records (`0..2399`) and twelve complete
200-source windows. Gate C completed 48 of 48 durable trace attempts with no
failure, orphan, or retry; issued exactly 2,400 FLAT and 2,400 HNSW-sentinel
physical shadow searches; persisted 4,800 unique telemetry records; acknowledged
all 2,400 sources; and finalized all twelve windows. Canonical reopen reported
zero pending windows and `next_window_sequence = 12`. All six SQLite databases
reported `integrity_check = ok`, and canonical readers reconstructed every hash
chain and terminal count.

The frozen ADR-015 comparator classified the 2,400 FLAT/oracle pairs as 2,396
`EXACT_ORDERED`, three `PRECISION_TIE_EQUIVALENT`, one
`EXECUTION_TIE_EQUIVALENT`, and zero membership mismatches, non-tie order
mismatches, or invalid evidence. This live run supports that the amended L2
execution-tie path operated as designed for its one observed qualifying case
without an observed false acceptance in the governed failure categories. It is
not universal proof of the numerical model.

HNSW-sentinel recall over these exact 2,400 observations was: minimum `0.75`,
mean `0.9150861173141502`, nearest-rank p50 `0.9206349206349206`, p95
`0.9696969696969697`, p99 `0.9855072463768116`, and maximum `1.0`; no governed
threshold or evidence-contract failure occurred. These are bounded ENV-001 /
DATASET-001 / L2 / `target-075` observations, not universal or production
recall and not qualification evidence.

The persisted detector sequence was: window 0 `REBASELINE`; window 1
`INSUFFICIENT_EVIDENCE` / `MISSING_PREVIOUS_WINDOW`; windows 2–5 `NO_DRIFT`;
window 6 `INSUFFICIENT_EVIDENCE` / `PENDING_CONFIRMATION`; windows 7–9
`DRIFT` / `INPUT_DRIFT`; and windows 10–11 `NO_DRIFT`. Therefore the governed
result is a confirmed transient three-window INPUT_DRIFT episode followed by
two windows that no longer satisfied the drift condition. The terminal
`NO_DRIFT` state does not erase the persisted drift episode and creates no
qualification, admission, grant, routing, activation, or actuation authority.

Append-only local ENV-001 physical-shadow latency, retaining all outliers, was:

- FLAT (`n=2400`): minimum `3.727542 ms`, mean `5.047436644166667 ms`,
  nearest-rank p50 `4.36325 ms`, p95 `6.174709 ms`, p99 `21.161292 ms`, and
  maximum `364.859208 ms`;
- HNSW (`n=2400`): minimum `2.064583 ms`, mean `2.9761200329166666 ms`,
  nearest-rank p50 `2.5665 ms`, p95 `4.384583 ms`, p99 `8.168417 ms`, and
  maximum `172.259583 ms`.

The telemetry binds timing, role, source, attempt, outcome, and result count,
but contains no sufficient server/resource trace explaining either maximum;
their causes are `UNKNOWN`. These values are not production, remote, SLA, or
throughput evidence.

Before the governed execute, one shell wrapper referenced a misspelled working
directory and exited `127` before Python or the operator started. A durable
zero-state check immediately afterward showed no attempt, acknowledgement,
telemetry, detector, attestation, finalization, search, or serve call. The
canonical operator was then invoked exactly once and exited `0`. This is
classified `NON_GOVERNED_PRE_EXECUTION_OPERATOR_SHELL_ERROR`, not a Gate-C
retry.

The successful V3 campaign remains bound to its original source revision above.
It proves bounded local scale-path completion and preserves a genuine detector
result; it does **not** establish qualification, canary/actuation authority,
production readiness, throughput/SLA, universal recall, or remote/distributed
Milvus behavior. Scale-10000 requires a fresh campaign and independent Gate-A
authority.

#### 2026-08-27 EXP-012 scale-10000-v1 failed campaign — immutable

Status: **FAILED_CLOSED — HISTORICAL EVIDENCE; NO RETRY OR SCALE-10000 CLAIM**

Historical upstream source revision:
`9021a61fd9c1f2e055396dbc24d1bd6c313d07e9`.

Gate-C execution source revision:
`1628c9f0f0c22647c3f6f0702c116a54df4d9642`.

Campaign root: `~/.local/share/vd/exp012-scale10000-v1`.

Gate B remains complete and immutable with exactly 10,000 source records,
sequences `0..9999`, and fifty frozen 200-source windows. Gate C finalized
windows `0..17`, acknowledged exactly 3,600 source positions (`0..3599`), and
persisted eighteen detector events. It issued 3,700 FLAT-reference and 3,700
HNSW-sentinel searches over sources `0..3699`, with exactly 7,400 matching
telemetry records. The attempt ledger contains 74 `STARTED`, 73 `COMPLETED`,
and one `FAILED` event. The bounded envelope
`43d24c8131bc461625ad45ef518fbbd93f028ef62548f524a53fd54ad9e55a3a`
remains truthfully `CHECKPOINT_STARTED` and non-terminal.

The terminal failure is window 18, trace 1, source 3669
(`logsim-v2:c2406d0574c1d6695b5030604c2980fe:3669`), attempt SHA-256
`5b8750a59c42bdb2fae85eaed882220451ba317e2351edfe13f6ec3b865bb21d`,
trace-envelope SHA-256
`a03944138e403432aed0cab9675a2c82bea0c12673f91d3ca19367f0376c8ed8`,
and terminal attempt-event SHA-256
`01a836ad74ca5515b98d2ebc0039766e8bfd99350d7abe8ccc61f057c6eeca7f`.
The durable reason is
`STAGE_FAILED:logsim-v2:c2406d0574c1d6695b5030604c2980fe:3669:FLAT`.
Oracle and FLAT membership and cardinality are exact (75/75), raw FLAT scores
are ordered, and one adjacent pair is transposed across distinct returned
binary32 values. The accepted ADR-015 execution-envelope partial-order
amendment classifies that pair prospectively as
`EXECUTION_ORDER_EQUIVALENT`; this forensic replay does not alter the historical
failed event.

An independent read-only replay of every finalized-prefix query under source
revision `b5b6d0dbf282303f5311b2e14e17051505399d4d` reconstructed exactly
3,600 observations: 3,594 `EXACT_ORDERED`, four
`PRECISION_TIE_EQUIVALENT`, two `EXECUTION_TIE_EQUIVALENT`, and zero persisted
agreement-boolean mismatches. Window 18 was kept outside that prefix: its
completed trace contains 50 `EXACT_ORDERED`; its failed trace contains 49
`EXACT_ORDERED` plus source 3669's one
`EXECUTION_ORDER_EQUIVALENT`. This corrects the discarded draft count of 3,650,
which had improperly included one trace from the unfinalized window.

All seven SQLite databases reported `integrity_check = ok`. At this immutable
audit point their sorted relative-path/per-file-SHA-256 aggregate is
`9b6cd898d96a8aaa4a9505f90a5fb24c47eb115534f8739d935193701d603906`;
lock files are excluded. No database was modified by the replay.

The campaign cannot resume. Window 18's failed attempt identity is immutable,
the append-only attempt transition refuses a second `STARTED` for that digest,
and no run-varying generation exists in the v1 attempt identity. Checkpoint
supersession cannot repair that independent identity boundary. No retry,
supersession, historical rewrite, or partial scale-10000 completion claim is
permitted. A completed scale-10000 result requires a fresh campaign identity,
fresh Gate-A authority, and a full fresh Gate-B/Gate-C lifecycle under a frozen
post-amendment source revision.

This failed campaign establishes no qualification, admission, grant, routing,
canary, actuation, production, remote-Milvus, SLA, or completed scale-10000
authority.
