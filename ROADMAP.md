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
| Safe actuation layer | in progress | CRITICAL | Offline boundary, durable audit/controller stores, `ShadowAuditTrace` collector, and Milvus adapter remain built/tested. EXP-009 Stages 1–3 verify the 60-of-600 workload/statistics preflight, Ed25519 approval/partition/lifecycle, restart/expiry LKG failback, and rollback containment with a persistent activation interlock. Stage-4 admission, immutable 1,200-slot schedule, restart-durable execution ledger, and schedule-stability evaluator are built and tested offline through `29ee6f9`; the fake-only serial composition runner remains next. Stage 4 remains separately human-gated; no live candidate route, live actuation, or automatic tuning is authorized. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 verifies L2 and COSINE stationary live-shadow evidence. EXP-006 verifies offline monitor safety/recovery; EXP-007 verifies the durable single-host trace source/outbox; and ADR-007 / EXP-008 verify the reference host-observation path through live read-only Milvus queries, detector, and `DRY_RUN` policy. EXP-009 Stages 1–3 now verify the 60-of-600 finite-manifest workload and calibrated bounds, offline Ed25519 approval/exact routing partition/lifecycle, restart and expiry failback, rollback containment, and persistent automatic-action interlock. Stage-4 admission, immutable schedule, durable ledger, and pure control-sweep evaluation are implemented offline through `29ee6f9`. The clean-commit Stage-3 bundle is `artifacts/exp-009/run-20260804T061821Z/` at `c78d0e2`; its public verifier reports `COMPLETE` after 11 commands and a 470-test full suite. No external host deployment, candidate route, full-traffic actuation, or automatic tuning is authorized.

Options considered:

- **Complete the fake-only EXP-009 Stage-4 serial composition runner:** compose the already built admission, immutable schedule, DATASET-002 source, serving-executor protocol, ledger, and evaluator with deliberate failures. It must not accept a grant, claim a candidate route, or create a live client. This closes the final offline orchestration gap before any human-gated live preflight.
- **Prepare the pre-registered EXP-009 Stage 4 controlled live-canary preflight:** only after the offline serial runner is verified and a human operator supplies a one-time Ed25519-signed grant for the exact L2 / `target-075` `ef=400 → 800` transition. Reconfirm clean revision, verified ENV-001, qualified LKG state, exact identities, 60/600 immutable partition, schedule-stability controls, health checks, and restoration-audit readiness before dispatching any candidate query.
- **Proceed without the exact human grant or expand beyond the frozen transition:** rejected. Stage-3 evidence proves only offline containment and blocks automatic re-activation; it cannot authorize a live candidate route, manufacture an external approval, or generalize to another metric, stratum, or `ef` transition.

**Recommendation:** Do not start a candidate query automatically. Stage 4 is the next Core gate but requires a human-signed exact grant and a fresh verified live preflight. Until those human-only prerequisites exist, the correct operational state is DRY_RUN/LKG-only; no safer autonomous substitute can prove controlled live no-interference or create external approval authority.
