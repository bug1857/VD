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
| Safe actuation layer | in progress | CRITICAL | Offline boundary, durable audit/controller stores, `ShadowAuditTrace` collector, and Milvus adapter remain built/tested. EXP-009 Stages 1–3 verify the 60-of-600 workload/statistics preflight, Ed25519 approval/partition/lifecycle, restart/expiry LKG failback, and rollback containment with a persistent activation interlock. The Stage-4 admission/schedule/ledger/evaluator seam remains independently verified by `2d56463`; `Stage4LiveRunner` is fake-tested/sealed at `1614521`; and the runtime-probe adapter is fake-tested/sealed at `1c995f7`. The fresh frozen L2 / `target-075` ENV-001 health/load/exact-identity preflight is now independently verified at `3353992` (`artifacts/exp-009/run-20260804T153006Z/`; four load-state and eight index-description calls, no search/mutation/grant/route use). That is point-in-time read-only evidence only: a qualified LKG, eligible admission/policy state, and externally supplied exact grant remain required. Stage 4 remains human-gated; no live candidate route, live actuation, or automatic tuning is authorized. **The Stage-4 recall/latency evidence-binding repair is now implemented and verified at `088d325`, documented at `66563c0`:** a canonical `Stage4EvidenceBinding` now binds recall and latency evidence before combination, the recall-audit ledger persists its binding digest through a verified append-only hash chain, the free-form `--latency-evaluation-json` CLI path is removed, and the decision combiner forces `INCOMPLETE` on any binding mismatch even when both sides individually report `PASSING`. 109/109 focused tests (ledger, evaluator, decision combiner, latency evidence, end-to-end pipeline, qualification-report CLI) plus the 662/662 full repository suite pass. No deterministic fake-client 1,200-query recall-audit **producer** exists yet — the ledger/evaluator/binding machinery can accept and evaluate recall observations, but nothing yet executes the 1,200 DATASET-002 queries against a fake client and populates a ledger with real observations under the new binding contract. |
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

EXP-001 supplies the verified Milvus range/threshold baseline. EXP-005 verifies L2 and COSINE stationary live-shadow evidence. EXP-006 verifies offline monitor safety/recovery; EXP-007 verifies the durable single-host trace source/outbox; and ADR-007 / EXP-008 verify the reference host-observation path through live read-only Milvus queries, detector, and `DRY_RUN` policy. EXP-009 Stages 1–3 verify the 60-of-600 finite-manifest workload and calibrated bounds, offline Ed25519 approval/exact routing partition/lifecycle, restart and expiry failback, rollback containment, and a persistent automatic-action interlock. The sealed Stage-4 offline-composition bundle at `artifacts/exp-009/run-20260804T112128Z/`, generated at `2d56463`, verifies the original fake-only schedule/source/ledger/evaluator seam. The separate candidate-capable `Stage4LiveRunner` has fake-only evidence at `1614521`; the runtime-probe adapter has fake-only evidence at `1c995f7`; and the required fresh frozen L2 / `target-075` ENV-001 preflight is now independently verified at `3353992` (`artifacts/exp-009/run-20260804T153006Z/`). Its transcript contains four load-state and eight index-description calls with no search, mutation, grant, or route use. The Stage-4 recall/latency evidence-binding repair called for below is now implemented and verified at `088d325`/`66563c0` (109/109 focused tests, 662/662 full suite). The immediate Core gate is now the remaining exact candidate-route preconditions, not another preflight: a current qualified LKG state, eligible policy/admission state, and an externally supplied one-time Ed25519 grant for the frozen `ef=400 → 800` transition. No external host deployment, candidate route, full-traffic actuation, or automatic tuning is authorized.

Options considered:



- **Establish a current qualified-LKG state and eligible admission/policy evidence:** this is still required before any future candidate route and must remain read-only until every other gate is satisfied.
- **Prepare the pre-registered controlled live canary:** only after a clean revision, the current qualified LKG/admission state, and an externally supplied one-time Ed25519 grant for the exact L2 / `target-075` `ef=400 → 800` transition.
- **Proceed without the exact human grant, expand beyond the frozen transition, or automatically dispatch a candidate query:** rejected. Existing evidence proves offline containment and blocks automatic re-activation; it cannot authorize a live candidate route, manufacture external approval, or generalize to another metric, stratum, or `ef` transition.

**Recommendation (updated 2026-08-06):** Do not start a candidate query automatically. The Stage-4 recall/latency evidence-binding repair is complete, and the deterministic fake-client 1,200-query recall-audit **producer** is now implemented and tested (`7a6ad2a`) — the ledger/evaluator/binding machinery now has a real production caller. The remaining qualified-LKG/admission-evidence path required a statistical-contract decision before any implementation: ADR-002's `ResponseEstimate`/`QualificationWindow` never had a defined confidence-bound estimator, and the natural reuse (ADR-008's Hoeffding estimator) is mathematically infeasible at the original 200-query window size. That decision is now made and recorded in ARCHITECTURE.md's ADR-002 "LKG qualification amendment": two globally adjacent 1,200-query epochs (six 200-query FR-021 windows each), qualified on the **exact observed** mean recall and nearest-rank p95 of the realized workload, with no population confidence claim. This is separate from, and does not resolve, the `ResponseEstimate` predictive per-`ef` confidence-bound contract used for `START_CANARY` authorization elsewhere in ADR-002, which remains unaddressed. That amendment's required prerequisite — a dedicated ≥2,400-query DATASET-003 `lkg_qualification` workload, disjoint from DATASET-001's base vectors and both DATASET-002 roles — is now implemented and tested; DATASET-001 and DATASET-002 were not modified. What remains, in order: (1) raw per-query LKG evidence capture (recall + latency from the same live sentinel call), the constituent/epoch evidence types, and the SQLite-transactional constituent-consumption ledger — none of this exists in code yet; (2) `policy.py`/`last_known_good.py`/`canary_admission.py` integration; (3) an exact human-signed Ed25519 grant, a separate human-only prerequisite regardless of how complete the other evidence becomes. Until every gate is satisfied, the correct operational state is DRY_RUN/LKG-only; no safer autonomous substitute can prove controlled live no-interference or create external approval authority.

---

## PROJECT COMPLETION PUNCH LIST (added 2026-08-06)

Consolidated, tiered view of everything remaining before this project could be called ready/done. Each item cross-references its detailed source elsewhere in this file or in ARCHITECTURE.md/SRS.md; this list does not restate their full rationale.

### Tier 0 — build now, no live Milvus needed

- **Stage-4 evidence-binding v2 repair.** Investigation (2026-08-06) found and empirically reproduced a real gap: `Stage4EvidenceBinding` binds metric/threshold-label/candidate-ef/opaque-identity/several unrelated SHA-256 digests, but never the candidate `SearchConfiguration`'s `radius` — so a differently-configured (different-radius) 1,200-observation recall-audit run can still evaluate `PASSING` under an unchanged binding, with a matching `evidence_binding_sha256`. Design (not implemented): add `candidate_search_configuration_sha256` to a new binding schema v2, gate the evaluator on it before any observation is read, bump `schema_version` so a v1 binding can never satisfy a v2 evaluator. Affects `canary_stage4_evidence_binding.py`, `canary_recall_audit_evaluation.py`, and (same gap class) `canary_stage4_latency_evidence.py`'s binding-mismatch check, plus ~8 dependent test files. No new evidence needs preserving-vs-discarding decision; old v1 evidence stays readable as history, just not accepted by a v2 evaluator.

### Tier 1 — build, requires live Milvus reads to produce real (non-fake-client) evidence

- **LKG qualification, Phase 1 — raw per-query evidence capture.** New `LkgQueryObservation` record, constructed at the existing shadow-audit sentinel search call site (no second live query), threading through the already-computed `latency_ms`/`start_ns`/`end_ns` currently discarded by `_SearchOutcome`. Requires a typed `RollbackReadinessEvidence` binding, not a bare externally-supplied bool.
- **LKG qualification, Phase 2 — constituent/epoch evidence and ledger.** `LkgConstituentEvidence` → `QualificationEpochEvidence` → `QualificationEpochEvaluation` (evidence/evaluation separation, no premature statistical gate at the 200-query constituent level), plus a SQLite-transactional `LkgConstituentConsumptionLedger` (atomic six-window epoch commit, `BEGIN IMMEDIATE`, mirroring `canary_recall_audit_ledger.py`'s proven idiom) enforcing globally consecutive, non-overlapping window sequencing and zero query-ID reuse across all 2,400 observations.
- **LKG qualification, Phase 3 — policy/persistence/admission integration.** `policy.py::qualify_last_known_good` signature change to `Sequence[QualificationEpochEvaluation]`; `last_known_good.py` schema 1→2; `canary_admission.py`'s qualification check updated; legacy `QualificationWindow` removed (zero production callers today).
- Full design for all three phases is recorded in this session's history and should be re-derived/summarized into ARCHITECTURE.md before implementation begins, per the project's design-before-code convention.

### Tier 2 — design + build, live Milvus only needed for final validation

- **`ResponseEstimate` predictive per-`ef` confidence-bound contract.** Separate from LKG qualification; feeds `_pre_action_gates`'s `START_CANARY` authorization (one predicted estimate per rung on `ACTUATION_LADDER`). The statistical contract is now settled by ADR-009 (accepted 2026-08-09): one atomic `CalibratedResponseProfile` per metric/stratum, built from exactly 1,200 post-trigger disjoint HNSW replay queries, with Bonferroni-allocated Hoeffding recall LCB/UCB and exact order-statistic p95 latency LCB/UCB. The ADR-009 §Policy consumption rule 4 fail-closed gate (B-001) was implemented 2026-08-10: `evaluate_tuning_policy` in `CANARY_ENABLED` mode now returns `RECOMMEND_EF` with reason `RESPONSE_PROFILE_AUTHORITY_UNAVAILABLE` unless `type(profile_authority) is CalibratedResponseProfile`. 58 policy tests pass, including 12 new `ResponseProfileAuthorityTests` cases. What remains: R2-C semantic evidence verifier, R2-D independent root-pin capability, R2-E deterministic profile projection, R2-F adversarial closure, and EXP-010 offline producer/replay. No profile has been run or measured. A repository-precedent injection point already exists to model the estimator after: `CanaryBoundEstimatorLike` in `milvus_actuation.py`.


### Tier 3 — separate track, does not block Tiers 0-2

- **DATASET-002 COSINE score-reproduction discrepancy.** Recorded in RESEARCH_PLAN.md and ARCHITECTURE.md as an unresolved evidence-portability item: environment-sensitive COSINE floating-point score reproduction, most plausibly BLAS/Accelerate-caused; exact historical trigger unresolved. DATASET-003 already depends only on DATASET-002's query-identity contract (via `verify_dataset002_query_identity`) and is unaffected. Only matters for DATASET-002's own EXP-009 Stage 1 acceptance evidence if that evidence needs to be re-verified in a new environment.

### Tier 4 — human-gated, cannot be automated regardless of code completeness

- A live Milvus run to capture **real** (not fake-client) LKG qualification evidence, per Tier 1 — requires your explicit approval each time, per this project's standing high-risk-action rules.
- A one-time **human-signed Ed25519 grant** for the exact frozen L2/`target-075` `ef=400 → 800` transition — cannot be generated or substituted by any amount of implementation work.
- A first live candidate canary — its own separately authorized, monitored, rollback-ready event, only reachable after every gate above is satisfied.

Until Tier 4 is satisfied, the correct operational state remains DRY_RUN/LKG-only, as stated in the Recommendation above.
