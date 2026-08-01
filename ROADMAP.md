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
| Benchmark harness | in progress | HIGH | Harness, DATASET-001, dedicated Python lock/export, host/config checksums, and pre-run resource evidence exist; host background load is disclosed; live execution and post-run evidence remain. |

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

Next: stabilize or re-disclose host background workloads immediately before execution and, only after separate authorization, run EXP-002 against verified ENV-001 using the immutable DATASET-001 artifacts. The run must capture realized ordering seeds, collection/index/query metadata, execution Git state, and post-run health/resources. Do not interpret H1–H4 or mark the harness verified until raw evidence is reviewed. Workload drift, tuning policy, IVF, and safe live actuation remain out of scope.
