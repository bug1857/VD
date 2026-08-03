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
| Host observation recorder and shadow worker | in progress | CRITICAL | ADR-007's offline framework-neutral recorder, metadata-only restart state store, 50-query worker, registered-parameter validation, reference gateway, lazy/read-only Milvus shadow executor, and real read-only HNSW serving adapter are implemented and fake-client tested. A deterministic two-stream offline composition test drives 1,200 served observations through the actual worker, ADR-006 durable outbox, assembly/extraction, detector, and DRY_RUN monitor to `NO_DRIFT → NO_CHANGE`, with zero automatic-action calls. Serving is explicitly preflight-gated but executes exactly one HNSW search per accepted request; shadow capture validates all 50 observations and pre/post health/load/identity state, owns an exclusive temporary trace sink, and calls only `shadow_candidate`. The EXP-008 composition root now loads only DATASET-001 plus both reviewed baselines, keeps their configuration identities in separate read-only adapter/serving instances, writes owner-only durable monitor audit records, and is fake-component tested for the complete 1,200-observation path and preflight fail-closed behavior. Live DRY_RUN evidence and deliberate live failure probes remain pending; no host application or automatic actuation is claimed. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 supplies verified L2 and COSINE stationary live-shadow evidence. EXP-006 verifies the standalone workload monitor's offline restart recovery, integrity rejection, bounded processing, and DRY_RUN non-actuation behavior at commit `6650c06`. EXP-007 verifies the durable single-host event-source/outbox at commit `ad635c7`. ADR-007's offline recorder/worker/reference-gateway, lazy read-only Milvus executor, real read-only serving adapter, strict monitor-audit sink, and complete two-stream host→outbox→monitor composition are implemented and under review. The EXP-008 runner is now fake-component tested and pins DATASET-001 plus reviewed L2/COSINE baselines; the remaining Core integration gap is its commit-pinned live DRY_RUN capture and deliberate live failure-probe evidence.

Options considered:

- **Run the commit-pinned EXP-008 stationary capture and deliberate live failure probes:** use the reviewed DATASET-001/ENV-001 configuration and frozen baselines, capture raw evidence and immutable run metadata, independently verify every checksum and no-actuation flag, then decide whether EXP-008 can be marked verified. No policy actuation is permitted.
- **Rerun EXP-001 unchanged first:** reconfirms an already-verified benchmark baseline but neither creates an actual host-traffic observation path nor reduces host-integration risk. Repeat it only if the frozen environment or baseline inputs change.

**Recommendation:** Commit the reviewed runner, then execute the pre-registered live stationary capture only after an explicit fresh ENV-001/container and dataset-integrity preflight. Treat its evidence as incomplete until the registered live failure probes and independent artifact review are also complete. No automatic actuation is authorized.
