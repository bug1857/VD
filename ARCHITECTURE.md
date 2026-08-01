# ARCHITECTURE.md — Design Decisions & System Structure

Governed by rules in `AGENTS.md`. This file is a **living document** — append, never silently rewrite. Not auto-loaded by Codex — read explicitly when a task touches architecture.

---

## DESIGN DECISION LOG (ADRs)

Every major architectural decision gets a numbered entry. Template:

```
### ADR-XXX: <title>
Status: Proposed | Accepted | Superseded by ADR-YYY
Date:
Risk level: LOW | MEDIUM | HIGH | CRITICAL

Problem:
Alternatives considered:
Chosen solution:
Reasoning:
Tradeoffs accepted:
Consequences for future modules:
Modules affected:
Research references:
```

**Never silently revise a past decision.** If a prior ADR turns out to be wrong or outdated, write a new ADR that explicitly supersedes it — the old one stays in the log with a "superseded by ADR-XXX" marker.

*(No ADRs recorded yet — add the first one once the core architecture for range/threshold-query adaptive tuning is decided.)*

---

## BACKEND COMPATIBILITY MATRIX

| Backend | Index types | Distance metrics | Filter support | Update/delete | Persistence | GPU | Limitations | Implementation status | Benchmark status |
|---|---|---|---|---|---|---|---|---|---|
| Qdrant | — | — | — | — | — | — | — | not started | not run |
| Milvus | — | — | — | — | — | — | — | not started | not run |

**Never assume feature parity across backends.** Check this matrix before generalizing a tuning policy across backends. Fill in rows as each backend is actually integrated and verified — checked directly, not from documentation alone.

---

## CONFIGURATION REGISTRY

Per Configuration Governance in `AGENTS.md` — every tunable parameter gets an entry here before the policy is allowed to set it.

```
### <parameter name>
Type:
Default:
Valid range:
Validation rule:
Dependencies:
Risk level:
Rollback behavior:
Research reference:
```

*(Empty — populate as parameters are formally defined.)*
