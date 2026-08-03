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
| Workload monitor | verified | CRITICAL | ADR-005 DRY_RUN monitor, restart-durable evidence persistence, and EXP-006 offline safety/recovery validation are complete through `6650c06`. The live event source remains a separate unbuilt boundary; no live actuation is authorized. |
| Live shadow-trace event source | in progress | CRITICAL | ADR-006 source/outbox implementation is offline-tested in the working tree: persist-before-publish, at-least-once acknowledgement, bounded backpressure, path/permission hardening, and real DRY_RUN monitor composition. EXP-007’s reproducible evidence harness remains required; the host query sampler/shadow worker is explicitly separate. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 supplies verified L2 and COSINE stationary live-shadow evidence. EXP-006 verifies the standalone workload monitor's offline restart recovery, integrity rejection, bounded processing, and DRY_RUN non-actuation behavior at commit `6650c06`. ADR-006 and EXP-007 now pre-register the remaining Core event-source boundary: a host-side durable outbox that supplies persisted immutable trace events without delaying serving traffic.

Options considered:

- **Implement and validate the ADR-006 durable source/outbox offline:** add a single-host, at-least-once `ShadowTraceEventSource` plus publisher using the established trace-envelope persistence and monitor protocols. EXP-007 must prove atomic publication ordering, restart/redelivery, duplicate conflicts, backpressure, path/permission safety, data minimization, and DRY_RUN composition before a host integration is considered.
- **Rerun EXP-001 unchanged first:** reconfirms an already-verified benchmark baseline but does not supply continuous events to the monitor or reduce live-source integration risk. Repeat it only if the frozen environment or baseline inputs change.

**Recommendation:** Implement the ADR-006 source/outbox through new modules only, then run EXP-007's deliberate offline failures before any live host hook is enabled. The source must persist before publish, remain off the foreground request path, fail closed, enforce bounded queue/backpressure semantics, minimize sensitive event payloads, and remain DRY_RUN-only. No automatic actuation is authorized.
