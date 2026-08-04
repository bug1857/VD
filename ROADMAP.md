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
| Safe actuation layer | in progress | CRITICAL | Offline boundary, durable audit/controller stores, `ShadowAuditTrace` collector, and Milvus adapter remain built/tested. EXP-009 Stages 1–2 now verify the corrected 60-of-600 workload/statistics preflight plus Ed25519 human-approval, immutable partition, one-time lifecycle, restart/expiry LKG failback, and evidence contract (`84ba2ea`; bundle `run-20260804T051014Z`). Stage 3 still lacks complete rollback-containment/restoration evidence; no candidate route, live actuation, or automatic tuning is authorized. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 verifies L2 and COSINE stationary live-shadow evidence. EXP-006 verifies offline monitor safety/recovery; EXP-007 verifies the durable single-host trace source/outbox; and ADR-007 / EXP-008 verify the reference host-observation path through live read-only Milvus queries, detector, and `DRY_RUN` policy. EXP-009 Stages 1–2 now verify the corrected 60-of-600 finite-manifest workload, CSPRNG selection/calibration preflight, and the offline Ed25519 approval, exact route-partition, audit, restart, and expiry-failback contracts. The clean-commit Stage-2 bundle is `artifacts/exp-009/run-20260804T051014Z/` at `84ba2ea`; its public verifier reports `COMPLETE` after 13 commands and a 448-test full suite. No external host deployment, candidate route, full-traffic actuation, or automatic tuning is authorized.

Options considered:

- **Execute EXP-009 Stage 3 — rollback containment (offline):** compose the existing route authority, durable lifecycle/marker stores, restoration-audit seam, and automatic-action controller into one trigger-to-LKG failback boundary. Deliberately test hard/recall/latency triggers, route-store corruption, grant expiry, identity change, restoration-audit failure, and controller-disable state; every case must prove immediate candidate removal, durable LKG restoration, append-only audit, and no alternate candidate.
- **Skip to Stage 4 or external host deployment:** rejected. A valid approval/partition proves only that a bounded route can be authorized; it does not prove that adverse canary evidence is contained or that restoration is verified. Implementing live routing first would make failback a best-effort behavior rather than a verified safety invariant.

**Recommendation:** Execute EXP-009 Stage 3 next as a narrowly scoped offline rollback-containment task. Stages 1–2 establish the finite-manifest, approval, and bounded-route prerequisites, not live no-interference, candidate serving, or restoration correctness. Stage 3 must preserve those boundaries and fail closed to last-known-good routing for every safety trigger before any controlled live canary is considered.
