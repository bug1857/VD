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
| Drift detector | in progress | CRITICAL | Implemented, offline tested, and empirically validated through stationary false-positive and drift-injection experiments. Not live integrated. |
| Tuning policy | in progress | CRITICAL | Implemented and offline tested, including detector-policy integration scenarios. Not live integrated. |
| Safe actuation layer | in progress | CRITICAL | Offline boundary, restart-durable audit/controller stores, optional `ShadowAuditTrace` collector, and the Milvus-backed actuation adapter are built and tested through commit `59a7655` (`138/138` tests). `ShadowAuditTrace` collects one read-only 50-query audit trace but does not yet assemble complete 200-query detector windows. The adapter is not yet integrated into the live benchmark harness, and no live automatic actuation is authorized. |
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

ADR-001, ADR-002, and ADR-004 are accepted. EXP-001 already supplies the verified Milvus range/threshold baseline. EXP-005's four-trace assembler, detector-input extraction, immutable evidence-provenance binding, and offline twelve-trace dry-run pipeline are implemented and tested. The experiment's real stationary live-shadow acquisition, persisted live artifacts, and live no-mutation evidence are not yet implemented or run.

Options considered:

- **Implement the EXP-005 live-shadow acquisition/persistence runner next:** closes the remaining evidence gap by capturing twelve independently persisted traces per metric/stratum, exercising workload construction, deterministic 500-to-50 routing, paired candidate/last-known-good observations, health and identity bindings, durable audit output, and no-mutation verification. Because this is CRITICAL actuation work, the path must remain non-actuating/DRY_RUN until raw stationary and deliberate-failure evidence is reviewed.
- **Run EXP-001 unchanged first:** reconfirms the already-verified baseline but does not exercise the adapter or reduce its integration risk. Repeat EXP-001 only if environment identity or frozen baseline inputs have changed.

**Recommendation:** Implement and review the EXP-005 live-shadow acquisition/persistence runner before separately authorizing a stationary live capture. An unchanged EXP-001 rerun would consume resources without validating the newly built integration boundary. No automatic actuation is authorized. Do not enable automatic actuation or mark the safe-actuation layer verified until the live integration run, deliberate failures, rollback, restart persistence, and raw audit evidence pass review.
