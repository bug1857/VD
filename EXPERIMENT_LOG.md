# EXPERIMENT_LOG.md — Empirical Runs & Benchmark Results

Governed by rules in `AGENTS.md`. **Append-only** — never overwrite a past result. Not auto-loaded by Codex — read explicitly when a task touches benchmarking.

---

## BENCHMARK GOVERNANCE (rules — apply to every entry below)

Every benchmark result must be reported alongside: dataset used (Dataset ID, see `RESEARCH_PLAN.md`) · hardware specs · software versions (DB engine, driver, OS) · random seed · full configuration used · metrics measured · number of runs · confidence interval/variance · statistical significance where comparing · location of raw output · git commit hash · Docker image/environment identifier, OS, CPU, RAM.

**Never claim an improvement without a pasted, real measurement.** Never compare results from different benchmark environments without disclosing the difference.

---

## EXPERIMENT LOG

Template per entry — ADRs (in `ARCHITECTURE.md`) record *decisions*; EXPs record *empirical runs*, keep them separate:

```
### EXP-XXX: <short title>
Date:
Objective:
Hypothesis:
Configuration:
Dataset ID:
Hardware:
Git commit:
Random seed:
Metrics measured:
Raw output location:
Result:
Conclusion:
Follow-up actions:
```

**Never overwrite a past experiment's result.** If repeated (new seed, fixed bug, different config), it gets a new EXP ID even if "basically the same test."

*(No experiments logged yet.)*
