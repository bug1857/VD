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
| Benchmark harness | in progress | HIGH | EXP-002 harness code and 20 passing offline unit tests are committed; DATASET-001 production artifacts are generated and checksum-verified; live Milvus execution remains. |

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

Next: complete the remaining runtime checklist and, only after separate authorization, execute EXP-002 against verified ENV-001 using the immutable DATASET-001 artifacts. Do not interpret H1–H4 or mark the harness verified until the live run and raw evidence are reviewed. Workload drift, tuning policy, IVF, and safe live actuation remain out of scope.
