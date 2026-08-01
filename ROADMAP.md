# ROADMAP.md — Phases, Milestones, Backlog

Governed by rules in `AGENTS.md`. This file is a **living document** — update at the end of every session. Not auto-loaded by Codex — read explicitly at the start of a session (per the State Sync rule in `AGENTS.md`).

---

## PROJECT PHASES

1. Research
2. Requirements
3. Architecture
4. Backend
5. Frontend
6. Adaptive Engine (drift detection, tuning policy)
7. Benchmarking
8. Optimization
9. Documentation
10. Publication

**Current phase:** Phase 1 — Research

### Phase exit criteria

A phase cannot be marked complete until: objectives met, tests complete, benchmarks complete (per `EXPERIMENT_LOG.md`), documentation updated, design reviewed, human approval received.

---

## MODULE STATUS

| Module | Status (not started / in progress / blocked / verified) | Risk level | Notes |
|---|---|---|---|
| Workload monitor | not started | — | |
| Drift detector | not started | CRITICAL | |
| Tuning policy | not started | CRITICAL | |
| Safe actuation layer | not started | CRITICAL | |
| Benchmark harness | not started | HIGH | EXP-001 contract, DATASET-001, ENV-001, and configuration registry are defined; implementation is next. |

---

## TECHNICAL DEBT

```
### DEBT-XXX
Introduced in: (module / EXP / ADR)
Description:
Why it was taken:
Estimated effort to resolve:
```

*(None logged yet.)*

---

## NEXT HIGHEST-PRIORITY TASK

ADR-001 is accepted: Milvus is the selected primary backend, so backend selection is complete.

Next: design and implement the EXP-001 benchmark harness and independent NumPy oracle against the immutable contract in `EXPERIMENT_LOG.md`. Provision and verify ENV-001 first, then implement only deterministic DATASET-001 generation, FLAT/HNSW semantic checks, the approved `ef` sweep, required metrics/artifacts, and deliberate failure tests. Workload drift, tuning policy, IVF, and safe live actuation are not part of this next task.
