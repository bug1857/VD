# RESEARCH_PLAN.md — Hypotheses, Literature, Evidence, Publication

Governed by rules in `AGENTS.md`. This file is a **living document**. Not auto-loaded by Codex — read explicitly when a task touches research/scope.

---

## PROJECT OBJECTIVE

Build an **Online Adaptive Workload-Aware Vector Database Tuning System**, currently scoped (per Scope Control in `AGENTS.md`) to **range/threshold-query tuning under workload drift** as the Core deliverable. k-NN/ANN, hybrid, filtered, and multi-tenant tuning are Future Work — see Scope Control before expanding this.

The system's Core scope:
- monitors workload drift on range/threshold queries
- predicts and applies adaptive index/query-time configuration changes
- benchmarks against static-config and periodic-manual-retune baselines on at least one backend (Qdrant or Milvus)
- safely rolls back poor decisions (see Safety Rules in `AGENTS.md`)

This is a research project. Research quality > coding speed.

### Success Metrics

Target directions (each starts as **HYPOTHESIS** per Evidence Policy, moves to VERIFIED only once an EXP entry in `EXPERIMENT_LOG.md` demonstrates it):

Latency ↓ · Recall ↑ · QPS ↑ · Memory ↓ · Storage ↓ · Configuration/re-tuning time ↓ · Rollback time ↓ · Drift detection accuracy ↑ · False positive rate ↓

---

## RELATED WORK (verified against the actual papers, not memory)

| Work | Venue | Focus | How it differs from this project |
|---|---|---|---|
| VDTuner | ICDE 2024 | Multi-objective Bayesian optimization to auto-tune VDMS params, evaluated on top-k recall/speed | Top-k only, not range/threshold queries |
| Quake | OSDI 2025 | Adaptive multi-level partitioning index, drift-aware, cost-model-guided reconfiguration | Framed and evaluated on k-NN search, not range queries |
| UNIFY | VLDB 2025 | Unified index for range-*filtered* (attribute-constrained) ANN search | Different "range" — attribute/label filtering combined with top-k, not a pure distance-threshold query |
| Exqutor | arXiv 2025/2026 | Query optimizer that adaptively tunes range thresholds for vector-augmented analytical (SQL-style) queries | One-shot adaptive query planning, not a continuous online tuning system with drift detection and rollback |

**Current framing of the gap (label: INFERRED, not yet VERIFIED via exhaustive literature search):** the combination of (a) pure distance-threshold range queries, (b) continuous online adaptation to workload/data drift, and (c) a safe, production-grade actuation/rollback layer, has not been addressed together in one system. Each individual piece has prior art (see table above) — novelty rests on the combination and the range-query framing, not on an untouched category. Revisit this framing after a fuller literature search before finalizing the publication's novelty claim.

---

## DATASET GOVERNANCE

Every dataset used gets a numbered entry before it's used in any experiment:

```
### DATASET-XXX
Source:
License:
Dimensions:
Embedding model:
Number of vectors:
Metadata schema:
Ground truth method:
Version:
Checksum:
```

**Never benchmark against an undocumented dataset.** If a dataset changes, it gets a new DATASET ID.

### DATASET-001: Deterministic synthetic range-query smoke dataset

Source: Deterministic synthetic independent standard-normal samples generated locally with NumPy `Generator(PCG64(20260801))`; no external source data.
License: Project-generated data. No external copyright-bearing data is included. Public redistribution is blocked until the repository has an explicit license or the human assigns a dataset license.
Dimensions: 128; little-endian IEEE-754 `float32`.
Embedding model: None — synthetic vectors, not model embeddings.
Number of vectors: 10,000 base vectors; 250 query vectors split into 50 calibration and 200 measured queries; separate deterministic boundary-fixture micro-dataset.
Metadata schema: Integer vector ID; split (`base`, `calibration_query`, `measured_query`, `boundary_fixture`); generation seed; generator/version metadata. No payload attributes are used by EXP-001.
Ground truth method: Independent NumPy exact L2 and cosine computation with float64 accumulation over stored little-endian float32 vectors, cross-checked against Milvus FLAT using the same metric, threshold bounds, ordering, and result cap.
Prospective numerical-contract note (2026-08-28): for governed L2 comparisons after the ADR-015 execution-envelope amendment, exact membership, threshold validity, distinct identifiers, and raw returned-score ordering remain mandatory, while oracle/FLAT rank agreement is evaluated against the conservative binary32 execution-envelope partial order. `EXECUTION_ORDER_EQUIVALENT` therefore means envelope compatibility, not exact kernel reconstruction or proof that one reduction schedule jointly attains the returned list. Historical EXP-001 evidence and COSINE semantics are unchanged.
Version: `DATASET-001-v1`; primary seed `20260801`; generated with NumPy `2.5.1` on 2026-08-01.
Artifact status: GENERATED AND CHECKSUM-VERIFIED — no Milvus ingestion or search was performed during generation.
Artifact location: `artifacts/exp-001/dataset/`.

Frozen calibration thresholds:

| Metric | Label | Target cardinality | Frozen radius | Observed median calibration cardinality |
|---|---|---:|---:|---:|
| L2 | `target-005` | 5 | `172.2832095509522` | 4.5 |
| L2 | `target-025` | 25 | `183.2043932030936` | 24.5 |
| L2 | `target-075` | 75 | `191.85897352125554` | 74.0 |
| COSINE | `target-005` | 5 | `0.28621445964266823` | 4.5 |
| COSINE | `target-025` | 25 | `0.2478647769312102` | 24.5 |
| COSINE | `target-075` | 75 | `0.21448069482694262` | 74.5 |

Artifact checksums:

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `base_ids.npy` | 80,128 | `3e7d12429f219ff5b6814ff1948c5b0e771431218bfb55b619b183b1e1264c51` |
| `base_vectors.npy` | 5,120,128 | `4fe7eda30b45e66d169123063fba91ca5ca2078b8ed6f25f87b2b7260d5a1d30` |
| `calibration_queries.npy` | 25,728 | `5bf9e5f2564a7d2dde20d26adf7584012f1551176b33991f0697ad9de312caaf` |
| `measured_queries.npy` | 102,528 | `418a924d04187f0eb08ecc3846e30b607f8ab1bca38699446cb2bb7c13210a1a` |
| `thresholds.json` | 622 | `597200ee81de02c658cf92b99c8ed3e1a8b492ac54f5f17b94a0734f378ee2ef` |
| `boundary_fixtures.json` | 3,092 | `09cb7e3b975107ffe3d4b029aefdf3ab64c6b93961e90a66787642ab1e80cb7a` |
| `generation_manifest.json` | 1,421 | `b6cb56a3eee60f6728be1d08a465e2a2500eec4089b4466da76fe2e886b51da9` |
| `SHA256SUMS` | 601 | `81b987be67471b6f2bfb4f71aeccee10bd819bee24fe8e418b21f2ffe552d4b1` |

Checksum procedure and verification: `generation_manifest.json` records byte sizes and SHA-256 values for every generated data/threshold/fixture artifact. `SHA256SUMS` additionally records the generation-manifest digest. Immediately after generation, `verify_dataset_artifacts(Path("artifacts/exp-001/dataset"))` re-read the manifest and independently verified every recorded byte size and checksum, then reverified every `SHA256SUMS` entry. A separate `shasum -a 256 artifacts/exp-001/dataset/*` audit matched all recorded values; the `SHA256SUMS` digest is recorded in this registry because a checksum list cannot include its own digest without recursion.
Used by: EXP-001 contract in `EXPERIMENT_LOG.md`.

---

### DATASET-002: Deterministic canary-routing and recall-audit query workload

Source: Planned deterministic independent standard-normal query vectors generated locally with NumPy `Generator(PCG64(20260809))`; DATASET-001's 10,000 base vectors and frozen thresholds are consumed read-only and are not regenerated, copied, or relabeled.
License: Project-generated data. No external copyright-bearing data is included. Public redistribution remains blocked until the repository has an explicit license or the human assigns one.
Dimensions: 128; little-endian IEEE-754 `float32`.
Embedding model: None — synthetic vectors, not model embeddings.
Number of vectors: 1,800 query vectors: 600 unique routing queries for the 60-of-600 candidate partition and 1,200 disjoint background candidate-recall audit queries. No query vector or occurrence identifier may appear in both roles.
Metadata schema: Canonical integer query ID; role (`routing` or `recall_audit`); immutable DATASET-001 base/threshold identity; seed/generator/version metadata; byte size and SHA-256 for every artifact. After the 600-ID eligible routing manifest is frozen, a separate approval-bound canonical CSPRNG candidate-selection record records the exact 60-ID candidate/LKG partition, eligible-manifest digest, and non-sensitive random-source provenance before result collection. Raw CSPRNG entropy is never persisted.
Ground truth method: Independent float64 oracle range search over the checksum-verified DATASET-001 base vectors, using the inherited frozen metric/radius/range/limit configuration; FLAT agreement is required before any HNSW candidate-recall value is usable.
Version: `DATASET-002-v1`; primary seed `20260809`; generated with NumPy `2.5.1` on 2026-08-03.
Artifact status: GENERATED AND CHECKSUM-VERIFIED — no Milvus ingestion, search, candidate routing, or approval action was performed during generation.
Artifact location: `artifacts/exp-009/dataset/`.
Use restrictions: EXP-009 Stage 1 only until its workload, estimator, and calibration gates are verified. The 600 routing vectors support the finite-population 60-of-600 latency design: with nearest-rank p95 `ceil(0.95 * 600) = 570`, a CSPRNG simple-random 60-ID set has at least `1 - C(570,60)/C(600,60) = 0.961003033592` probability that its maximum is at least the frozen manifest's p95 threshold, conditional on the pre-registered fixed-potential-outcome/no-interference model. Stage 1 validates the calculation and selection contract only; the controlled live stage must supply schedule-stability/no-interference evidence. This does not imply IID latency or production-traffic coverage. The 1,200 disjoint recall-audit vectors support the proposed one-sided bounded-mean Hoeffding recall bound with margin `sqrt(log(20)/(2*1200)) = 0.035330182290`; passing the ADR-002 recall floor requires an observed audit mean at least `0.985330182290` under the explicitly tested independent-query-generator model.
Checksum procedure: Before use, write arrays, role/identity manifest, oracle records, and SHA-256 inventory atomically; independently reread and verify every byte size/hash plus the inventory hash. Record the inherited DATASET-001 generation-manifest SHA-256 and thresholds SHA-256 in DATASET-002's manifest. A separate verification command and unit tests must fail closed on role overlap, duplicate IDs, non-finite vectors, wrong dimensions/dtype, inherited-identity mismatch, missing oracle records, any checksum mismatch, a candidate-selection record created before the eligible-manifest freeze, a selection-record digest mismatch, duplicate selection IDs, or any selected ID outside the eligible routing population.
Artifact checksums:

| Artifact | SHA-256 |
|---|---|
| `routing_ids.npy` | `e780f21d20b3df6c2d4bc46c908018dd03adbb256be7224aee0bafd48afd065d` |
| `routing_queries.npy` | `459bf6186b6d9b25c1557f6941d4e4cb86b6e40f9d6de68141bc553c1a54ad7c` |
| `recall_audit_ids.npy` | `d6c84131fe438255935ba77c12fa8f8d38779c3ce026ef82de7a2368696e0f91` |
| `recall_audit_queries.npy` | `29c64e7a3128fd3e94345c7580a6aa73dfd79412f000d60db256566b5dfa94bb` |
| `inherited_dataset001.json` | `832d681185d674d3a6e0d55645907101f1064175e41d89b1925f6f11d27e9388` |
| `oracle_records.jsonl` (10,800 records) | `9c5c01fe4c47233dba58fb9e0735b2150403cb2d068eba58f7893de59e7906c4` |
| `dataset002_manifest.json` | `45ae2d754cd0e0923a6e2be38c0878c27665b5f0cbd9dce2b7de3c3c5ae77b01` |
| `SHA256SUMS` | `848de8c74377acc57fa5385caabe812629a4a63074d9fa31c5008f3bed81af30` |
Verification: `verify_dataset002_artifacts(Path("artifacts/exp-009/dataset"), dataset001_dir=Path("artifacts/exp-001/dataset"))` independently regenerated the vectors, validated inherited DATASET-001 identity, reread all artifact checksums, and recomputed all 10,800 exact-oracle records successfully.
Known evidence-portability item (unresolved, 2026-08-06): a later re-run of the same verification command in a different session's environment found environment-sensitive COSINE floating-point score reproduction, most plausibly caused by BLAS/Accelerate differences; the exact historical environmental trigger remains unresolved. Every manifest hash, `SHA256SUMS` entry, inherited DATASET-001 identity, and deterministically-regenerated array above still matches exactly in that later environment; only 4,984/10,800 `oracle_records.jsonl` entries (exclusively COSINE, zero L2) disagree with a fresh recomputation, exclusively at few-ULP score precision (max delta `1.665e-16`), with hit membership/order/`full_count`/`capped` unaffected in every case. This does not change this entry's acceptance status or checksums above; it is recorded here as an open item, not fixed, tolerated, or worked around. DATASET-003 (below) depends only on this dataset's query-identity contract, not its oracle-correctness contract, and is unaffected by this item.
Used by: Planned EXP-009 Stage 1; no other experiment may consume it without an explicit registry update.

---

### DATASET-003: Deterministic LKG-qualification query workload

Source: Deterministic independent standard-normal query vectors generated locally with NumPy `Generator(PCG64(20260806))`; DATASET-001's 10,000 base vectors and DATASET-002's 1,800 routing/recall-audit query IDs are consumed strictly read-only, through their own existing verifiers, and are neither regenerated, copied, relabeled, nor modified.
License: Project-generated data. No external copyright-bearing data is included.
Dimensions: 128; little-endian IEEE-754 `float32`.
Embedding model: None — synthetic vectors, not model embeddings.
Number of vectors: 2,400 query vectors, single `lkg_qualification` role. IDs occupy `[10000, 12400)` — immediately after DATASET-001's `base_ids` range `[0, 10000)` and disjoint from DATASET-002's `routing` (`[0, 600)`) and `recall_audit` (`[600, 1800)`) roles. The writer and verifier both assert this disjointness against the actually-loaded parent arrays, not merely the generation arithmetic.
Metadata schema: Canonical integer query ID; fixed `lkg_qualification` role; inherited DATASET-001 identity (`generation_manifest`/`thresholds`/`base_ids`/`base_vectors` SHA-256) and inherited DATASET-002 identity (dataset ID/version, manifest SHA-256, `routing_ids`/`recall_audit_ids` SHA-256, and a `verification_scope` tag fixed to `QUERY_IDENTITY_ONLY`); seed/generator/version metadata; byte size and SHA-256 for every artifact.
Ground truth method: None precomputed. The live LKG shadow-audit path computes its oracle result at audit time via `exact_range_search` against DATASET-001's base vectors directly (`MilvusActuationClient._oracle`); it never reads a dataset's `oracle_records.jsonl`. DATASET-003 therefore registers only query IDs and vectors.
Parent verification scope: DATASET-003 depends on DATASET-002 through `verify_dataset002_query_identity` only (manifest schema, every artifact hash, `SHA256SUMS`, exact file inventory, inherited DATASET-001 identity, deterministic routing/recall-audit array regeneration, role disjointness) -- never `verify_dataset002_artifacts`'s oracle-record semantic recomputation. This is a deliberate scope separation, not a relaxation: DATASET-002's own EXP-009 Stage 1 acceptance contract above is unchanged and still requires the complete strict verifier. See ARCHITECTURE.md's ADR-002 LKG qualification amendment for the governed statement of this separation.
Version: `DATASET-003-v1`; primary seed `20260806`; implemented and covered by 18 focused tests across `tests/test_dataset002.py` (7 new: narrow-verifier accept/reject adversarial coverage) and `tests/test_dataset003.py` (10: 8 original + 2 proving the narrow-verifier dependency), all passing.
Artifact status: GENERATED AND CHECKSUM-VERIFIED against the real accepted DATASET-001 and DATASET-002 artifacts -- no Milvus ingestion, search, candidate routing, or approval action was performed during generation.
Artifact location: `artifacts/exp-009/dataset003/`.
Artifact checksums:

| Artifact | SHA-256 |
|---|---|
| `lkg_qualification_ids.npy` | `59c4c5a2b079c3a5377c3a4e0ffd2524141f88cedb493cdcd3388affa19df177` |
| `lkg_qualification_queries.npy` | `8d7914eb6ebb7f3624defd0f958d1aaa1d5c34c02e149065ce53a4ce39f44215` |
| `inherited_dataset001.json` | `832d681185d674d3a6e0d55645907101f1064175e41d89b1925f6f11d27e9388` |
| `inherited_dataset002.json` | `1d89e894451fb35972d7a3bf3705a15b30f83dc128a61ddd7f194e2202a9ad9c` |
| `dataset003_manifest.json` | `be2b5c7c133b70913b17c5243f61e7f89400f6f3adc010401827172ffa62360d` |
| `SHA256SUMS` | `7cc2f2c78099aef62b82d748177f2100225f8b4aad24e9b8565124ee6530e980` |
Verification: `verify_dataset003_artifacts(Path("artifacts/exp-009/dataset003"), dataset001_dir=Path("artifacts/exp-001/dataset"), dataset002_dir=Path("artifacts/exp-009/dataset"))` independently regenerated the vectors, reverified all artifact checksums, and reconfirmed cross-dataset ID disjointness against DATASET-001's `base_ids` and both DATASET-002 roles. IDs occupy `[10000, 12400)`, contiguous, 2,400 total.
Use restrictions: Not usable for real LKG qualification until the raw per-query evidence capture, epoch assembly, and consumption-ledger code (ARCHITECTURE.md's ADR-002 LKG qualification amendment) is implemented. Not affected by the DATASET-002 oracle-reproduction item above, by design.
Used by: Planned LKG qualification (ADR-002 amendment); no other experiment may consume it without an explicit registry update.

---

## EXPERIMENT ENVIRONMENT REGISTRY

### ENV-001: Milvus range-query smoke environment

Status: VERIFIED — PERSISTENCE AND HEALTH GATES PASSED 2026-08-01
As-of date: 2026-08-01
Compatibility policy: The official Milvus 3.0.0 `milvus-standalone-docker-compose.yml` release asset is the source of truth for Milvus, etcd, and MinIO versions. Preserve its service versions and effective configuration unless a specific incompatibility or security issue is demonstrated. Any deviation requires an ENV-001 deviation record stating the evidence, risk, rollback, and human approval before use.

| Component | Executable pin | Immutable artifact | Selection basis |
|---|---|---|---|
| Milvus | `3.0.0` | `milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862` (`linux/arm64`: `sha256:bfab7739a0479cd81ffdf5e473f88c5b143678c2520a06a19f86f35ecd586cad`) | Latest non-prerelease Milvus release; published 2026-07-29. |
| PyMilvus | `3.0.1` | Python requirement `pymilvus==3.0.1`; lockfile hash deferred to the EXP-001 harness environment | Latest non-prerelease SDK release; release notes identify it as recommended for Milvus 3.0; Python >=3.9 required. |
| Docker Desktop | `4.84.0` | Apple Silicon installer build `234817`; SHA-256 `ed9e93bf2b71c53492eb80ef35e722e131222018cba8157973dfe3bb717952dd` | Latest stable Docker Desktop release; published 2026-07-27. |
| Docker Engine | `29.6.2` | Bundled/runtime version must equal `29.6.2` and be captured by `docker version` | Latest stable Engine release available as of the pin date; published 2026-07-16. |
| Docker Compose | `5.3.1` | CLI version must equal `v5.3.1` and be captured by `docker compose version` | Latest stable Compose release available as of the pin date. |
| etcd | `3.5.25` | Compose-specified tag `quay.io/coreos/etcd:v3.5.25`; resolved index digest `sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366`; `linux/arm64` digest `sha256:8da34a9df5dc1bd879bea716a301113c4e49b6bbdbe5778214707c6043ccf65d` | Exact version from the official Milvus 3.0.0 standalone Compose asset. |
| MinIO | `RELEASE.2024-05-28T17-19-04Z` | Compose-specified tag `minio/minio:RELEASE.2024-05-28T17-19-04Z`; resolved index digest `sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08`; `linux/arm64` digest `sha256:fa7be14ee3f914469274c5dfc05949e0092500a71de4681f1f1b6b39275a13b1` | Exact version from the official Milvus 3.0.0 standalone Compose asset. |
| Compose definition | Milvus `v3.0.0` standalone CPU asset | SHA-256 `4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1` | Authoritative `milvus-standalone-docker-compose.yml` release asset; all service-version pins above were fetched from this file, not inferred. |

Target platform and resource pins:

- Host baseline: Apple M1, `arm64`, 8 logical cores, 8 GiB RAM, macOS 26.5.2 (build 25F84). Record again at execution; hardware/OS changes create a new ENV ID.
- Container platform: `linux/arm64`; architecture-specific digests above must match after pull.
- Docker Desktop VM allocation: 6 vCPU, 6 GiB RAM, 2 GiB swap.
- Milvus container limit: 4 CPU, 4 GiB RAM.
- etcd container limit: 1 CPU, 512 MiB RAM.
- MinIO container limit: 1 CPU, 1 GiB RAM.
- Persist separate named/bind volumes for Milvus, etcd, and MinIO. Start every valid smoke run from explicitly empty experiment-scoped volumes.
- Docker was not installed when ENV-001 was first recorded (`docker: command not found`). Provisioning completed on 2026-08-01; the stock Compose versions, resolved image digests, limits, health checks, Compose checksum, and persistence-across-restart probe are captured in `artifacts/exp-001/environment/ENV-001_PROVISIONING.md`.

Documented deviations from the stock Compose asset:

1. Replace each mutable image tag with the **same version plus its immutable digest**. Reason: prevent registry tag drift without changing the vendor-selected version. Rollback: restore the original tag after verifying that it resolves to the recorded digest.
2. Add the CPU/RAM limits listed above. Reason: make latency and QPS runs comparable on the fixed host. Rollback: remove the limits only under a new ENV ID; do not mix limited and unlimited runs.
3. Use experiment-scoped empty volume paths/names instead of reusable default paths. Reason: prevent state leakage between runs. Rollback: retain the generated data as an artifact, then recreate fresh volumes for a new run.

No etcd or MinIO version, command, health check, endpoint, credential, or storage-layout deviation is currently authorized.

Environment pinning checklist:

- [x] Fetch the CPU Compose asset only from the Milvus `v3.0.0` GitHub release URL recorded below.
- [x] Verify the Compose SHA-256 equals `4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1` before use.
- [x] Verify the file names exactly `milvusdb/milvus:v3.0.0`, `quay.io/coreos/etcd:v3.5.25`, and `minio/minio:RELEASE.2024-05-28T17-19-04Z`.
- [x] Resolve each tag and verify its index digest and `linux/arm64` platform digest against the table above.
- [x] Apply only the three documented deviations; preserve and checksum the resulting Compose diff.
- [x] Reject any etcd or MinIO version override unless an ENV-001 deviation record contains evidence, risk, rollback, and explicit human approval.
- [x] Verify Docker Desktop `4.84.0`, Docker Engine `29.6.2`, and Docker Compose `5.3.1` from command output.
- [x] Verify the Apple M1 `arm64` host baseline and all Docker VM/container CPU and RAM limits.
- [x] Start from empty experiment-scoped Milvus, etcd, and MinIO volumes.
- [x] Capture service version output, image IDs/digests, effective Compose config, health checks, and pre-run resource snapshots in the EXP artifact manifest.
- [x] Mark ENV-001 VERIFIED only after Milvus starts, persists a probe record across restart, and all services pass health checks with the stock dependency versions.

Version sources:

- [Milvus 3.0.0 release](https://github.com/milvus-io/milvus/releases/tag/v3.0.0)
- [PyMilvus 3.0.1 release](https://github.com/milvus-io/pymilvus/releases/tag/v3.0.1)
- [Docker Desktop release notes](https://docs.docker.com/desktop/release-notes/)
- [Docker Engine 29 release notes](https://docs.docker.com/engine/release-notes/29/)
- [Docker Compose releases](https://github.com/docker/compose/releases/tag/v5.3.1)
- [Milvus 3.0.0 standalone Compose asset](https://github.com/milvus-io/milvus/releases/download/v3.0.0/milvus-standalone-docker-compose.yml)

---

## EXPERIMENT VERIFICATION REGISTRY

### EXP-001: Milvus range/threshold-query smoke benchmark

Status: **VERIFIED — LIVE SMOKE ACCEPTANCE PASSED 2026-08-01**

Verifying evidence: `artifacts/exp-001/run-20260801T161924Z/`; authoritative summary: `artifacts/exp-001/run-20260801T161924Z/summary.json`, SHA-256 `f3c14c5708de0b67d5d7ecbd5fb54a3988ca9dcb9be9364cb68a152eec4a609b`. The verification decision and hypothesis evaluation are recorded by EXP-004 in `EXPERIMENT_LOG.md` so the original contract and the separate `run-20260801T154343Z` INCONCLUSIVE record remain append-only historical evidence.

Hypothesis disposition from the verifying run:

- **H1 — SUPPORTED:** Milvus FLAT and the independent oracle agreed on all 1,200 measured-query preflight checks (six metric/threshold FLAT configurations × 200 measured queries).
- **H2 — SUPPORTED:** aggregate HNSW recall increased from `0.8960` at `ef=100` to `0.9998` at `ef=1600`, while aggregate p95 latency increased from `3.1604 ms` to `5.0889 ms`; the run therefore exhibited the hypothesized recall/latency tradeoff.
- **H3 — SUPPORTED:** all 150 per-segment checks and both final metric checks reported unchanged HNSW index identity; no mismatch was recorded.
- **H4 — SUPPORTED:** every configuration satisfied the reviewed p95-latency CV ceiling of 30%; the maximum was `26.0237%` for `L2:target-075:HNSW:ef=800`.

The verifying run recorded zero failed measured queries, zero threshold violations, and valid QPS comparisons for all 36 configurations. Verification is limited to the EXP-001 smoke contract; it does not establish optimal tuning, production readiness, workload-drift adaptation, or backend superiority.

---

## PUBLICATION TRACKER

- **Research gap:** see Related Work above.
- **Novel contributions:** *(TBD — each contribution claim should carry an Evidence Policy label)*
- **Threats to validity:** *(TBD)*
- **Related work:** see table above.
- **Evaluation results:** *(pull from `EXPERIMENT_LOG.md` once populated)*
- **Figures / tables produced:** *(TBD)*
- **Future work:** k-NN/ANN tuning, hybrid search tuning, multi-backend policy transfer (see Scope Control in `AGENTS.md`)
- **Writing status:** not started
