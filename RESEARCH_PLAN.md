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

Source: Locally generated with NumPy `Generator(PCG64(seed))` from independent standard-normal samples; no external source data.
License: Internal research use. No external copyright-bearing data is included. Public redistribution is blocked until the repository has an explicit license or the human assigns a dataset license.
Dimensions: 128
Embedding model: None — synthetic vectors, not model embeddings.
Number of vectors: 10,000 base vectors; 250 query vectors split into 50 calibration and 200 measured queries; separate deterministic boundary-fixture micro-dataset.
Metadata schema: Integer vector ID; split (`base`, `calibration_query`, `measured_query`, `boundary_fixture`); generation seed; generator/version metadata. No payload attributes are used by EXP-001.
Ground truth method: Independent NumPy exact L2 and cosine computation with float64 accumulation over stored little-endian float32 vectors, cross-checked against Milvus FLAT using the same metric, threshold bounds, ordering, and result cap.
Version: `DATASET-001-v1` contract; primary seed `20260801`; NumPy version will be added to the immutable generation manifest.
Checksum: Pending generation. SHA-256 is required for every vector/query artifact and the manifest before ingestion; EXP-001 execution is blocked until populated.
Used by: EXP-001 contract in `EXPERIMENT_LOG.md`.

---

## EXPERIMENT ENVIRONMENT REGISTRY

### ENV-001: Milvus range-query smoke environment

Status: PINNED TARGET — NOT PROVISIONED OR VERIFIED
As-of date: 2026-08-01
Compatibility policy: The official Milvus 3.0.0 `milvus-standalone-docker-compose.yml` release asset is the source of truth for Milvus, etcd, and MinIO versions. Preserve its service versions and effective configuration unless a specific incompatibility or security issue is demonstrated. Any deviation requires an ENV-001 deviation record stating the evidence, risk, rollback, and human approval before use.

| Component | Executable pin | Immutable artifact | Selection basis |
|---|---|---|---|
| Milvus | `3.0.0` | `milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862` (`linux/arm64`: `sha256:bfab7739a0479cd81ffdf5e473f88c5b143678c2520a06a19f86f35ecd586cad`) | Latest non-prerelease Milvus release; published 2026-07-29. |
| PyMilvus | `3.0.1` | Python requirement `pymilvus==3.0.1`; lockfile hash pending environment provisioning | Latest non-prerelease SDK release; release notes identify it as recommended for Milvus 3.0; Python >=3.9 required. |
| Docker Desktop | `4.84.0` | Installer checksum pending installation; installation is blocked unless the vendor checksum is recorded | Latest stable Docker Desktop release; published 2026-07-27. |
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
- Docker was not installed when ENV-001 was recorded (`docker: command not found`). Provisioning must verify the stock Compose versions, resolved image digests, limits, health checks, and Compose checksum before EXP-001 can run.

Documented deviations from the stock Compose asset:

1. Replace each mutable image tag with the **same version plus its immutable digest**. Reason: prevent registry tag drift without changing the vendor-selected version. Rollback: restore the original tag after verifying that it resolves to the recorded digest.
2. Add the CPU/RAM limits listed above. Reason: make latency and QPS runs comparable on the fixed host. Rollback: remove the limits only under a new ENV ID; do not mix limited and unlimited runs.
3. Use experiment-scoped empty volume paths/names instead of reusable default paths. Reason: prevent state leakage between runs. Rollback: retain the generated data as an artifact, then recreate fresh volumes for a new run.

No etcd or MinIO version, command, health check, endpoint, credential, or storage-layout deviation is currently authorized.

Environment pinning checklist:

- [ ] Fetch the CPU Compose asset only from the Milvus `v3.0.0` GitHub release URL recorded below.
- [ ] Verify the Compose SHA-256 equals `4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1` before use.
- [ ] Verify the file names exactly `milvusdb/milvus:v3.0.0`, `quay.io/coreos/etcd:v3.5.25`, and `minio/minio:RELEASE.2024-05-28T17-19-04Z`.
- [ ] Resolve each tag and verify its index digest and `linux/arm64` platform digest against the table above.
- [ ] Apply only the three documented deviations; preserve and checksum the resulting Compose diff.
- [ ] Reject any etcd or MinIO version override unless an ENV-001 deviation record contains evidence, risk, rollback, and explicit human approval.
- [ ] Verify Docker Desktop `4.84.0`, Docker Engine `29.6.2`, and Docker Compose `5.3.1` from command output.
- [ ] Verify the Apple M1 `arm64` host baseline and all Docker VM/container CPU and RAM limits.
- [ ] Start from empty experiment-scoped Milvus, etcd, and MinIO volumes.
- [ ] Capture service version output, image IDs/digests, effective Compose config, health checks, and pre-run resource snapshots in the EXP artifact manifest.
- [ ] Mark ENV-001 VERIFIED only after Milvus starts, persists a probe record across restart, and all services pass health checks with the stock dependency versions.

Version sources:

- [Milvus 3.0.0 release](https://github.com/milvus-io/milvus/releases/tag/v3.0.0)
- [PyMilvus 3.0.1 release](https://github.com/milvus-io/pymilvus/releases/tag/v3.0.1)
- [Docker Desktop release notes](https://docs.docker.com/desktop/release-notes/)
- [Docker Engine 29 release notes](https://docs.docker.com/engine/release-notes/29/)
- [Docker Compose releases](https://github.com/docker/compose/releases/tag/v5.3.1)
- [Milvus 3.0.0 standalone Compose asset](https://github.com/milvus-io/milvus/releases/download/v3.0.0/milvus-standalone-docker-compose.yml)

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
