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
| Host observation recorder and shadow worker | verified | CRITICAL | ADR-007's framework-neutral reference gateway, metadata-only restart state, 50-query worker, registered-parameter validation, lazy/read-only Milvus shadow executor, and real read-only HNSW serving adapter are verified through EXP-008. The clean-commit stationary run drove 1,200 served observations through worker → ADR-006 outbox → monitor → detector → DRY_RUN policy for separate L2/COSINE streams (`NO_DRIFT → NO_CHANGE`). The clean-commit H1/H4 run at `76600f8` then verified foreground recorder isolation plus queue, publisher, executor, identity, and restart containment across 154 successful live foreground requests; its independent verifier checks raw receipt/worker-state evidence and all artifact hashes. This is a reference in-process integration, not an external host deployment or automatic action authorization. |
| Drift detector | verified | CRITICAL | Offline statistical validation is complete; EXP-008 additionally exercised the real detector in the reference live read-only pipeline for L2 and COSINE stationary windows, each yielding `NO_DRIFT`. This does not validate drift response under production traffic or authorize actuation. |
| Tuning policy | verified | CRITICAL | Offline contract/integration tests are complete; EXP-008 additionally exercised the policy in the reference live read-only pipeline, each stationary stream yielding `NO_CHANGE` in `DRY_RUN`. Candidate canary/rollback application remains separately gated. |
| Safe actuation layer | in progress | CRITICAL | Offline boundary, restart-durable audit/controller stores, optional `ShadowAuditTrace` collector, and the Milvus-backed actuation adapter are built and tested through commit `59a7655` (`138/138` tests). ADR-008 / EXP-009 now identify a blocking correction: ADR-002’s 50-of-500 canary cannot support the promised distribution-free one-sided 95% p95-latency upper bound, and DATASET-001 has only 200 measured IDs. No candidate route may be implemented until EXP-009 validates a 60-of-600 workload, confidence estimator, signed human approval, and rollback contract. Automatic actuation remains unauthorized. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 verifies L2 and COSINE stationary live-shadow evidence. EXP-006 verifies offline monitor safety/recovery; EXP-007 verifies the durable single-host trace source/outbox; and ADR-007 / EXP-008 now verify the complete reference host-observation path through live read-only Milvus queries, detector, and `DRY_RUN` policy. EXP-008’s stationary bundle (`2403799`) and strict H1/H4 bundle (`76600f8`) together provide immutable evidence for reference foreground isolation and fail-closed containment. ADR-008 / EXP-009 record the next blocking correction: the current 50-of-500 canary cannot meet the stated distribution-free one-sided 95% p95-latency confidence promise, and DATASET-001 alone cannot provide the required 600 unique occurrences. No external host deployment, candidate route, full-traffic actuation, or automatic tuning is authorized.

Options considered:

- **Execute EXP-009 Stage 1 — workload and confidence preflight:** define a checksummed 600-occurrence workload and a pre-registered, calibrated sampling/bound estimator. This must resolve the current 50-of-500 / 95%-p95 conflict and DATASET-001’s 200-ID limit before routing or approval code exists.
- **Build approval, routing, or external host deployment first:** rejected for now. Each depends on the unresolved Stage-1 workload/statistics contract; writing it first would make the safety guarantee an implementation detail rather than a verified property.

**Recommendation:** Execute EXP-009 Stage 1 next, as an offline research/statistics task. If it validates neither a defensible 600-occurrence workload nor calibrated bounds, record that result and keep the system DRY_RUN-only rather than weakening the confidence claim. Only a verified Stage 1 may unlock the approval/routing design and later controlled rollback validation.
