# README — Adaptive Vector DB Tuning Project Docs

Start here. `AGENTS.md` is the only file Codex loads automatically — the other five are reference files you or Codex read explicitly when a task touches them. Don't duplicate content between files; if something feels like it belongs in two places, put it in the more specific one and cross-reference.

## Files

| File | What it is | Auto-loaded by Codex? | How often it changes |
|---|---|---|---|
| **AGENTS.md** | The rules: roles, verification gate, safety rules, priorities, scope control | **Yes, every session** | Rarely — treat as fixed unless a rule is actively causing problems |
| **RESEARCH_PLAN.md** | Objective, current scope, related work, evidence policy, publication tracker | No — reference explicitly | On every research update |
| **ARCHITECTURE.md** | ADR log, backend compatibility matrix, config registry | No — reference explicitly | On every architecture decision |
| **ROADMAP.md** | Current phase, module status, tech debt, next task | No — reference explicitly | Every session |
| **EXPERIMENT_LOG.md** | Benchmark rules + EXP-ID entries | No — reference explicitly | Every experiment run |
| **HANDOFF_TEMPLATE.md** | Task template for a second implementer agent, or a solo self-check | No — reference explicitly | Rarely — used as-is |

## Where these files go (Codex-specific)

Put all seven files in your **repo root** (same directory as `.git`). Codex CLI and the Codex Mac/desktop app both discover `AGENTS.md` automatically by walking from your git root down to your current working directory, concatenating every `AGENTS.md` found along the way (closest to your working directory wins on conflicts). The companion files aren't part of that auto-load chain — tell Codex to read them ("read ROADMAP.md and ARCHITECTURE.md before we start") at the start of a session, or reference them by name mid-task.

If you want these defaults applied across *all* your repos, not just this one, an identical or trimmed-down `AGENTS.md` can also go in `~/.codex/AGENTS.md` (global). For the ChatGPT web/app version of Codex, the equivalent is Settings → Personalization → Custom Instructions, which maps to your personal `AGENTS.md`.

Practical commands:
- `codex --print-instructions` — dumps exactly what Codex loaded for the session, useful to confirm `AGENTS.md` was picked up and wasn't truncated (32 KiB default cap — this file is well under that).
- `/init` inside a Codex session — auto-generates a starter `AGENTS.md` if you don't already have one (not needed here, you already have a complete one).

## Starting a new Codex session

1. Make sure `AGENTS.md` is at the repo root — Codex loads it automatically.
2. Tell Codex explicitly which companion files are relevant to today's task (e.g. "read ROADMAP.md, we're starting the drift-detector module").
3. Codex rebuilds state from those files per the State Sync rule in `AGENTS.md`, rather than assuming it remembers a prior session.

## If you also use a second agent (e.g. Antigravity) for implementation

1. Codex fills out `HANDOFF_TEMPLATE.md` for the specific task.
2. The second agent implements and returns real output (not a summary).
3. Codex runs the post-return checklist at the bottom of `HANDOFF_TEMPLATE.md`.
4. Codex updates `ARCHITECTURE.md` / `ROADMAP.md` / `EXPERIMENT_LOG.md` as applicable.
5. Human gives explicit "approved, commit" before anything merges.

## Current project scope

**Core:** online adaptive tuning for range/threshold vector queries under workload drift, on one backend (Qdrant or Milvus), with a safe rollback/actuation layer.
**Explicitly not core right now:** k-NN/ANN tuning, hybrid search, multi-tenant tuning, multi-backend policy transfer — Future Work (see `RESEARCH_PLAN.md`). Don't build these before Core is done — see Scope Control in `AGENTS.md`.

## Team

Rudra Pratap Singh (lead), Swastik Anurag Vyas, Divayom Sengar — Tata Technologies InnoVent hackathon, Round 2, Aerospace / Edge AI for Sustainable Aviation & Energy Optimization track.

## A note on scope, for whoever's reading this later

This governance layer is intentionally thorough because it's meant to keep a small team consistent over weeks, not to impress anyone reading it. If updating these files ever starts eating more time than the actual build, that's a signal to stop updating them for a bit — the win condition is a working, benchmarked tuner, not perfect documentation.
