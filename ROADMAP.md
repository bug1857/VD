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
| Safe actuation layer | in progress | CRITICAL | Offline boundary, restart-durable audit/controller stores, and the Milvus-backed actuation adapter are built and tested through commit `9442ea4` (`131/131` tests). The adapter is not yet integrated into the live benchmark harness, and no live automatic actuation is authorized. |
| Benchmark harness | in progress | HIGH | Harness, DATASET-001, dedicated Python lock/export, host/config checksums, pre/post-run resource evidence, and verified EXP-001 live evidence exist. ADR-002 actuation-adapter integration and dedicated live integration evidence remain pending. |

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

ADR-001 and ADR-002 are accepted. EXP-001 already supplies the verified Milvus range/threshold baseline; the Milvus-backed ADR-002 actuation adapter is built and fake-client-tested but not live-integrated.

Options considered:

- **Wire `milvus_actuation.py` into the existing EXP-001 benchmark harness next:** closes the current interface/evidence gap by exercising workload construction, deterministic 500-to-50 routing, paired candidate/last-known-good observations, health and identity bindings, durable audit output, and rollback verification. Because this is CRITICAL actuation work, first register a dedicated integration EXP contract and keep the path non-actuating/dry-run until deliberate failure and rollback evidence is reviewed.
- **Run EXP-001 unchanged first:** reconfirms the already-verified baseline but does not exercise the adapter or reduce its integration risk. Repeat EXP-001 only if environment identity or frozen baseline inputs have changed.

**Recommendation:** wire the adapter into the harness next under a new, pre-registered dry-run integration experiment. This produces new evidence against the highest-risk unverified boundary; an unchanged EXP-001 rerun would consume resources without validating the newly built module. Do not enable automatic actuation or mark the safe-actuation layer verified until the integration run, deliberate failures, rollback, restart persistence, and raw audit evidence pass review.
