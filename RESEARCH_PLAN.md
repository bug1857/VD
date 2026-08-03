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
Metadata schema: Canonical integer query ID; role (`routing` or `recall_audit`); immutable DATASET-001 base/threshold identity; seed/generator/version metadata; byte size and SHA-256 for every artifact. The routing manifest additionally records the approval-bound routing seed and exact candidate/LKG partition only after the seed is committed before result collection.
Ground truth method: Independent float64 oracle range search over the checksum-verified DATASET-001 base vectors, using the inherited frozen metric/radius/range/limit configuration; FLAT agreement is required before any HNSW candidate-recall value is usable.
Version: Planned `DATASET-002-v1`; primary seed `20260809`; expected NumPy pin `2.5.1`. This entry is a registry contract only: artifacts are not generated and have no checksums yet.
Artifact location: Planned `artifacts/exp-009/dataset/`.
Use restrictions: EXP-009 Stage 1 only until its workload, estimator, and calibration gates are verified. The 600 routing vectors support the 60-of-600 latency tolerance-bound design. The 1,200 disjoint recall-audit vectors support the proposed one-sided bounded-mean Hoeffding recall bound with margin `sqrt(log(20)/(2*1200)) = 0.035330182290`; passing the ADR-002 recall floor requires an observed audit mean at least `0.985330182290`. These guarantees are conditional on the explicitly tested independent-query sampling model and do not imply IID latency or production-traffic coverage.
Checksum procedure: Before use, write arrays, role/identity manifest, oracle records, and SHA-256 inventory atomically; independently reread and verify every byte size/hash plus the inventory hash. Record the inherited DATASET-001 generation-manifest SHA-256 and thresholds SHA-256 in DATASET-002's manifest. A separate verification command and unit tests must fail closed on role overlap, duplicate IDs, non-finite vectors, wrong dimensions/dtype, inherited-identity mismatch, missing oracle records, or any checksum mismatch.
Used by: Planned EXP-009 Stage 1; no other experiment may consume it without an explicit registry update.

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
