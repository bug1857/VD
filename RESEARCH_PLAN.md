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

*(Empty — populate as datasets are selected, e.g. SIFT1M/GIST1M subsets, synthetic drift generator output.)*

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
