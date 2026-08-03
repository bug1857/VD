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
| Workload monitor | verified | CRITICAL | ADR-005 DRY_RUN monitor, restart-durable evidence persistence, and EXP-006 offline safety/recovery validation are complete through `6650c06`. It now has an EXP-007-verified offline event-source boundary; the host sampler/shadow worker and all live actuation remain separate and unbuilt. |
| Live shadow-trace event source | verified | CRITICAL | ADR-006's offline single-host durable source/outbox is verified through EXP-007 at `ad635c7`: persist-before-publish, at-least-once acknowledgement, bounded backpressure, path/permission hardening, data minimization, and real DRY_RUN monitor composition. The host query sampler/shadow worker and all live integration remain separate and unbuilt. |
| Host observation recorder and shadow worker | in progress | CRITICAL | ADR-007's offline framework-neutral recorder, metadata-only restart state store, 50-query worker, registered-parameter validation, and reference gateway are implemented and unit-tested. Shared stream/event/publication value types were moved below monitor/policy dependencies so the host boundary loads neither. The lazy read-only Milvus executor, full host→outbox→monitor composition, and EXP-008 live DRY_RUN evidence remain pending; no host application or automatic actuation is claimed. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 supplies verified L2 and COSINE stationary live-shadow evidence. EXP-006 verifies the standalone workload monitor's offline restart recovery, integrity rejection, bounded processing, and DRY_RUN non-actuation behavior at commit `6650c06`. EXP-007 verifies the durable single-host event-source/outbox at commit `ad635c7`. ADR-007's offline recorder/worker/reference-gateway boundary is implemented and under review; the remaining Core integration gap is its lazy read-only Milvus executor followed by complete host→outbox→monitor composition and EXP-008 live DRY_RUN evidence.

Options considered:

- **Implement the ADR-007 lazy read-only Milvus executor and offline composition test:** bind the reviewed offline recorder/worker to existing `MilvusActuationClient` shadow capture only through a new background adapter; prove it does not create/drop/load/index/mutate a collection, then run the pre-registered EXP-008 live DRY_RUN evidence path.
- **Rerun EXP-001 unchanged first:** reconfirms an already-verified benchmark baseline but neither creates an actual host-traffic observation path nor reduces host-integration risk. Repeat it only if the frozen environment or baseline inputs change.

**Recommendation:** Preserve the now-isolated recorder/worker boundary and implement only a lazy, read-only Milvus shadow executor plus focused fake-client composition tests. After that, run EXP-008 against ENV-001 in DRY_RUN only. No automatic actuation is authorized.
