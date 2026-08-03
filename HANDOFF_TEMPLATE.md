# HANDOFF_TEMPLATE.md — Standard Implementation Task Format

Governed by the Two-Agent Protocol in `AGENTS.md`. Use this whenever Codex is defining a task for a second implementation agent (e.g. Antigravity) — or use it as a self-check even when Codex implements solo, since writing the spec down this precisely catches ambiguity before code gets written.

---

```
## HANDOFF: <task name>
Risk level: LOW | MEDIUM | HIGH | CRITICAL   (see Risk Classification)

> **PRE-IMPLEMENTATION GATE:** Before implementing: if you see ambiguity or risk,
> state it and stop — don't silently choose or add scope.
> See `AGENTS.md` → PRE-IMPLEMENTATION GATE for the full rule.

### 1. Objective
One sentence, unambiguous.

### 2. Exact interface/contract
Function signatures, types, file paths, module boundaries. No "figure it out."

### 3. Constraints
What NOT to touch (unrelated files). Performance/memory bounds if relevant.

### 4. Required tests
What must be written, what must pass, exact commands to run them.

### 5. Required proof-of-work
The exact command whose *actual printed output* must be pasted back verbatim.
Not summarized. Not "passed ✅."

### 6. Explicit non-goals
What this task is NOT responsible for (prevents scope creep into other modules).
```

---

## AFTER IMPLEMENTATION RETURNS — CHECKLIST

Run this before accepting any output (own or another agent's) as done (full detail in `AGENTS.md` → Verification Gate):

- [ ] Ambiguities and risks were surfaced before implementation began (not patched around silently)
- [ ] Raw output pasted, not summarized
- [ ] Diff verified with `git diff` / `git show HEAD:<file> | grep <keyword>`
- [ ] Correctness re-derived independently where it matters
- [ ] No commit made without explicit "approved, commit"
- [ ] Manual click-by-click / CLI test steps written for the human
- [ ] Code review checklist passed
- [ ] `ARCHITECTURE.md` / `ROADMAP.md` / `EXPERIMENT_LOG.md` updated as applicable
