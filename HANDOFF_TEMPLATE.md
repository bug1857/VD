# HANDOFF_TEMPLATE.md — Standard Implementation Task Format

Governed by `AGENTS.md` → Quality before token efficiency, Codex + Antigravity
coordination, Design/risk/safety gates, and Testing/review gate. Use for a
delegated review or implementation proposal. The local bridge returns proposals
from supplied text; it does not edit VD or execute the listed tests.

---

```
## HANDOFF: <task name>
Classification: Core | Important | Future Work | Out of Scope
Risk level: LOW | MEDIUM | HIGH | CRITICAL   (justify using AGENTS.md design/risk/safety gates)
Mode: supplied-text proposal | authorized repository review | explicitly scoped edit
Delegation reason: <required independent gate, or concrete benefit after handoff/review overhead>
Division of work: <worker deliverable; Codex's remaining work and verification>
Revision / dirty diff / evidence identities: <exact inspected inputs>
Authorized effects and output location: <response by default; no implied runtime permission>
Required coverage and boundaries: <affected files, callers, invariants, assumptions>
Model / effort / rationale: <adequate for risk; no silent downgrade>
Related follow-up: <exact conversation/finding IDs; changed evidence and affected boundaries>
Fresh independence requirement: <new reviewer context when the gate requires it>

> Before implementing, surface material ambiguity, conflicting requirements,
> missing authorization, or unresolved design/safety prerequisites. Resolve
> routine details from source without weakening the contract. HIGH/CRITICAL work
> follows AGENTS.md → Design, risk, and safety gates before implementation.
> Token/word/context targets cannot remove required quality or proof. If inputs
> are insufficient, request the exact missing context or return PARTIAL/BLOCKED.

### 1. Objective
One sentence, unambiguous.

### 2. Exact interface/contract
Function signatures, types, file paths, module boundaries. No "figure it out."

### 3. Constraints
What NOT to touch (unrelated files). Performance/memory bounds if relevant.

### 4. Required tests
What must be written, what must pass, exact commands to run them.
Include required full, integration/restart, failure, and empirical checks.
Inspect side effects and current permission; unmet required checks block acceptance.

### 5. Required proof-of-work
The exact command whose *actual printed output* must be pasted back verbatim.
Not summarized. Not "passed ✅."
Separate actual executions from supplied historical results and proposed checks.
Record exit codes, relevant stdout/stderr, source/artifact identity, and limitations.

### 6. Explicit non-goals
What this task is NOT responsible for (prevents scope creep into other modules).

### 7. Acceptance and coverage
Criteria, COMPLETE/PARTIAL/BLOCKED status for the exact reviewed scope, omitted
paths, unresolved findings, and independent review required before acceptance.
No fixed answer length may omit a necessary finding, derivation, or failure case.

### 8. Failure and rollback
Expected refusal behavior, effect bounds, recovery plan, and evidence required.
For proposal-only work, no runtime effects are authorized.
```

---

## AFTER IMPLEMENTATION RETURNS — CHECKLIST

Run this before accepting any output as done (`AGENTS.md` → Testing and review gate):

- [ ] Ambiguities and risks were surfaced before implementation began (not patched around silently)
- [ ] Raw output pasted, not summarized
- [ ] Diff verified with `git diff` / `git show HEAD:<file> | grep <keyword>`
- [ ] Correctness re-derived independently where it matters
- [ ] Delegation served its stated purpose; follow-up covered findings and affected dependencies
- [ ] Unsupported worker assertions corrected; any required independent gate still satisfied
- [ ] No worker commit/merge/push/deploy; Codex follows AGENTS.md delivery authorization
- [ ] Complete relevant context/coverage and adequate review depth preserved
- [ ] Required tests/evidence met; unavailable required checks remain blockers
- [ ] No shorter output, lower model, or weaker guarantee substituted to save tokens
- [ ] Any efficiency report separates providers, cumulative/per-call usage, and worker/end-to-end time; savings have a comparable baseline
- [ ] Manual click-by-click / CLI test steps written for the human
- [ ] Code review checklist passed
- [ ] `ARCHITECTURE.md` / `ROADMAP.md` / `EXPERIMENT_LOG.md` updated as applicable
