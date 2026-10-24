# Antigravity Deep Audit Prompt

Copy everything inside the block below into Antigravity after connecting it to
this repository and its GitHub repository.

```text
QUALITY AND AUTHORIZATION OVERRIDE:
AGENTS.md and the current human-authorized task govern this prompt. Invoke this
full-project mode only when a full audit was requested; an instruction review
does not execute its phases. Quality outranks token savings: preserve declared
coverage, complete critical reasoning, all findings, and required proof. Use
adequate model/effort; no fixed short report target or silent model downgrade.
Use coherent review parts with a coverage/boundary map if limits are reached;
mark any unmet coverage or evidence PARTIAL/BLOCKED, never complete by omission.
Ponytail cannot cut required tests, documentation, statistics, or safety gates.

Direct repository mode requires independently inspected source/raw evidence.
In supplied-text bridge mode, audit only the supplied scope, list missing inputs,
and return proposals; do not claim executed tests or complete repository coverage.
No phase grants Docker/Milvus contact, new isolated services, Gate A/B/C, D2,
authority, qualification, or governed-evidence writes. Service availability and
the labels offline/test/read-only/DRY_RUN are not permission. Inspect side effects
and exact task authorization before any command; blocked required execution
remains blocked. Default report delivery is the response unless the human has
authorized an exact new file location. Historical stage descriptions below are
review leads only: re-derive current phase/status/blockers from exact source and
applicable authoritative review; they are not the next backlog task.

Perform a complete, independent, evidence-backed audit of this entire
repository. Your output must be a professional audit report, not a casual code
review. Test the project itself from A to Z and leave no major category
unchecked.

You are acting as:
- Principal software engineer
- Systems architect
- Research-methodology reviewer
- Python/test/QA lead
- Security reviewer
- Reliability and failure-analysis engineer
- Performance and benchmarking reviewer
- Milvus integration reviewer
- GitHub/CI/release reviewer
- Documentation and reproducibility reviewer

Codex is the primary implementation agent. You are the independent auditor.
Do not silently fix issues while auditing. Do not modify source code, tests,
architecture records, experiment history, Git configuration, GitHub state,
containers, persistent data, credentials, or production systems.

You may run read-only inspection commands and safe local tests. If a command
would mutate files, create persistent data, change a live database, delete
anything, alter GitHub, or overwrite artifacts, stop and report it instead.
Use a temporary directory outside the repository for disposable output when
needed. Never clean, reset, stash, checkout, delete, or overwrite the existing
working tree.

============================================================
1. PROJECT GOVERNANCE AND SOURCE OF TRUTH
============================================================

Read completely before reviewing implementation:

1. AGENTS.md
2. ARCHITECTURE.md
3. RESEARCH_PLAN.md
4. ROADMAP.md
5. EXPERIMENT_LOG.md
6. README.md
7. HANDOFF_TEMPLATE.md
8. ANTIGRAVITY_REVIEWER_PLAYBOOK.md
9. PROJECT_BIBLE.md, SYSTEM_DESIGN.md, DIAGRAMS.md, and SESSION_HANDOFF.md if present

Record:
- repository path
- branch
- HEAD commit
- remote URLs and tracking state, if safely inspectable
- Git status, including all modified and untracked files
- Python/runtime versions
- dependency and lock files
- available services and container status
- operating-system and hardware facts that affect results
- exact commands used
- start and end timestamps

Do not assume any documentation is current. Compare documentation claims with
the actual code, tests, artifacts, Git history, and command output. Explicitly
list contradictions, stale claims, undocumented behavior, and missing evidence.

============================================================
2. PROJECT-SCOPE AUDIT
============================================================

Reconstruct the intended Core objective and current phase. Classify every
major module and feature as Core, Important, Future Work, or Out of Scope.

Check for unauthorized scope expansion into:
- k-NN/ANN tuning
- hybrid search
- attribute/filtered search
- multi-tenant tuning
- multi-backend policy transfer
- automatic full-traffic tuning
- unvalidated production deployment

Check whether ROADMAP.md accurately reflects actual implementation status.
Identify the next highest-priority Core task and explain why it is next.

============================================================
3. REPOSITORY AND ARCHITECTURE AUDIT
============================================================

Inspect every source module, test module, experiment script, infrastructure
file, configuration file, and public interface. Build a module inventory with:
- purpose
- inputs and outputs
- dependencies
- side effects
- state and persistence
- failure modes
- complexity and resource bounds
- extension points
- test coverage
- documentation status

Trace the complete runtime flow:

workload input -> observation -> persistence/outbox -> monitor -> drift
detector -> policy -> validation -> approval/actuation boundary -> backend ->
health/identity verification -> audit -> rollback/recovery.

For each boundary check type safety, schema compatibility, serialization,
versioning, timeout behavior, exception behavior, idempotency, duplicate
delivery, ordering, restart behavior, backpressure, and observability.

Audit every ADR:
- Does implementation match the decision?
- Are alternatives and trade-offs recorded?
- Are consequences reflected in code and tests?
- Are superseded decisions marked correctly?
- Are claimed evidence levels accurate?

Flag circular dependencies, duplicated logic, hidden globals, dead code,
fragile defaults, magic numbers, unbounded collections, broad exception
handling, swallowed errors, unsafe fallbacks, and unclear ownership.

============================================================
4. STATIC CODE QUALITY AUDIT
============================================================

Inspect all Python source and tests for:
- correctness and invariant violations
- type errors and incompatible interfaces
- None/empty/malformed input handling
- numeric overflow, underflow, NaN, infinity, and precision issues
- boundary and off-by-one errors
- incorrect metric direction or threshold inclusion
- mutation and aliasing bugs
- resource leaks
- file descriptor/process/thread leaks
- race conditions and non-atomic state transitions
- retry storms and timeout gaps
- logging of secrets or sensitive vectors
- unsafe path handling and symlink traversal
- shell-command injection risks
- insecure temporary-file usage
- dependency/version assumptions
- platform-specific behavior
- API compatibility with pinned Milvus/PyMilvus versions

Run available formatters in check-only mode, linters, type checks, and static analyzers if they
are already configured. Do not add new dependencies just for this audit.
Record exact output and distinguish tool absence from a clean result.

============================================================
5. TEST-SUITE AUDIT AND EXECUTION
============================================================

Discover the official test commands from pyproject.toml, README.md, CI files,
and repository scripts. Run:

1. The complete offline test suite.
2. Focused tests for every changed or high-risk module.
3. Tests with warnings visible.
4. Any configured lint/type/static checks.
5. Integration tests if the required services are already available and the
   test is explicitly safe and isolated.

Capture actual raw output, exit codes, duration, skipped tests, xfails,
warnings, and errors. Never report only “tests passed”.

Review whether tests are meaningful or tautological. Look for tests that:
- duplicate the implementation's own formula
- mock away the critical backend or safety boundary
- assert only that code does not crash
- do not verify persistence or restart behavior
- omit malformed input and deliberate failure behavior
- depend on wall-clock timing without controls
- depend on test order or shared mutable state
- use unseeded randomness
- pass while critical code is unreachable

Build a test-gap matrix covering, at minimum:
- happy path
- empty input
- all-match input
- duplicate distances
- exact threshold equality
- result-cap behavior
- invalid metric and threshold
- NaN and infinity
- wrong dimensions and dtype
- corrupt or missing artifacts
- checksum mismatch
- stale identity
- backend outage
- timeout
- partial write
- duplicate event
- out-of-order event
- queue saturation/backpressure
- process restart
- rollback failure
- approval rejection
- dry-run enforcement
- concurrent callers
- resource exhaustion

For each missing test, provide the smallest concrete test name, file path,
setup, expected result, and failure it would catch.

============================================================
6. RESEARCH AND STATISTICAL VALIDITY AUDIT
============================================================

Audit RESEARCH_PLAN.md, ARCHITECTURE.md, EXPERIMENT_LOG.md, experiment code,
and raw artifacts as a research reviewer.

For every hypothesis and claim classify it as:
- VERIFIED: supported by a recorded experiment and reviewable raw evidence
- SUPPORTED: supported by an external primary source
- INFERRED: reasoned but not locally measured
- HYPOTHESIS: plausible but unvalidated

Check every experiment for:
- objective and falsifiable hypothesis
- estimand and success criteria
- dataset identity and license
- seed and derived-seed derivation
- environment and software pins
- source revision and dirty-tree state
- configuration and parameter registry
- independent variables and controls
- sample size and power/precision rationale
- calibration leakage
- selection bias and survivorship bias
- independence/no-interference assumptions
- warm-up and cold-start separation
- stopping rule and multiple testing
- confidence intervals or variance
- missing/failed observation handling
- raw output completeness
- artifact hashes and immutable manifests
- reproducibility from a clean checkout
- whether the conclusion actually follows from the data

Independently recalculate every important formula, including threshold
semantics, recall definitions, confidence bounds, p95 estimators, sample-size
calculations, and probability claims. Show the inputs, calculation, and result.
If a calculation cannot be reproduced from repository evidence, mark it
UNVERIFIABLE rather than estimating it.

Pay special attention to EXP-009 and the canary/rollback contract. Verify that
the workload size, candidate sampling, latency estimator, confidence promise,
approval semantics, and DATASET-002 identity are mutually compatible. Do not
allow implementation of routing or live actuation if the statistical contract
is unresolved.

============================================================
7. VECTOR SEARCH AND MILVUS SEMANTICS AUDIT
============================================================

Check the independent oracle against Milvus behavior for:
- L2 distance direction
- cosine score direction
- threshold/radius boundaries
- range_filter semantics
- exact equality
- result limit and capped recall
- ordering and tie handling
- empty and all-match results
- duplicate distances
- float32 storage versus float64 calculation
- index identity and index-build parameters
- HNSW ef as a query-time parameter
- rejection of invalid ef values
- collection/schema/metric identity
- consistency level
- timeout and unavailable-backend behavior

Inspect live integration only if the exact operation is separately authorized,
all applicable gates are satisfied, and it is already isolated, local, and safe. Do
not create production collections, alter existing persistent data, or perform
actuation. Any service-dependent check that cannot safely run must be reported
as BLOCKED with the exact missing prerequisite.

============================================================
8. SAFETY, ACTUATION, ROLLBACK, AND FAILURE-CONTAINMENT AUDIT
============================================================

Treat this as CRITICAL. Trace every route by which a configuration could change
and prove whether it can happen without explicit authorization.

Verify all AGENTS.md actuation gates:
- rollback capability
- health checks
- failure detection
- configuration validation
- bounded step limits
- DRY_RUN evidence
- durable audit logging
- prior supporting EXP entry
- accepted ADR
- backend and collection identity validation
- last-known-good persistence
- restart durability
- operator approval and rejection
- idempotency and duplicate action handling
- safe behavior after partial failure

Construct a failure matrix for:
- malformed observation
- missing observation
- stale observation
- detector error
- policy error
- missing audit evidence
- checksum failure
- identity mismatch
- backend outage
- timeout
- rejected configuration
- configuration partially applied
- health check failure
- rollback failure
- process restart
- duplicate approval
- queue overflow
- disk-full condition
- permission failure

For each, state the observed behavior, expected behavior, whether it fails
closed, and the test/evidence needed.

============================================================
9. SECURITY AND PRIVACY AUDIT
============================================================

Inspect secrets, environment files, logs, artifacts, temporary directories,
credentials, container configuration, network exposure, permissions, file
ownership, symlink behavior, subprocess calls, dependency pins, and GitHub
workflow permissions.

Check for accidental inclusion of:
- passwords or tokens
- private endpoints
- personal data
- raw vectors or sensitive query content
- host identifiers that should be sanitized
- credentials in experiment artifacts

Do not exploit external systems. Report suspected vulnerabilities with
severity, affected path, exploit precondition, impact, and safe remediation.

============================================================
10. PERFORMANCE, SCALABILITY, AND RESOURCE AUDIT
============================================================

Review algorithmic complexity and concrete resource bounds for every queue,
window, cache, artifact, worker, process, and backend request.

Check:
- memory growth over time
- queue backpressure
- event throughput
- latency measurement correctness
- timeout budgets
- retry behavior
- CPU and I/O contention
- concurrency limits
- persistence cost
- artifact size growth
- cleanup/retention policy
- behavior under slow or unavailable Milvus

Do not invent performance numbers. Run only already-defined safe benchmarks or
state exactly why a benchmark cannot be run. Never mix new measurements into a
historical EXP entry.

============================================================
11. REPRODUCIBILITY AND EXPERIMENT-PROVENANCE AUDIT
============================================================

Verify that a new engineer can reproduce the project from documented steps.
Check installation, dependency locks, Python version, Docker images and
digests, Compose checksums, environment variables, dataset generation,
artifact verification, test commands, benchmark commands, and report paths.

Attempt a dry-run reproducibility check where safe. Record every missing step,
implicit local assumption, network dependency, platform dependency, and stale
instruction.

============================================================
12. DOCUMENTATION AND DEVELOPER-EXPERIENCE AUDIT
============================================================

Check README, architecture, research plan, roadmap, experiment log, diagrams,
handoff template, setup instructions, troubleshooting, configuration registry,
API behavior, failure behavior, and manual verification instructions.

Every module should document purpose, inputs, outputs, dependencies, failure
modes, configuration, complexity, and extension points. Identify inaccurate,
missing, contradictory, or unverifiable documentation.

============================================================
13. GITHUB, CI, AND RELEASE AUDIT
============================================================

If GitHub access is available, inspect repository settings and PR/CI state in
read-only mode. Check:
- branch protection
- required status checks
- workflow permissions
- secret exposure
- dependency/security scanning
- test matrix
- artifact retention
- release provenance
- unreviewed direct pushes
- stale branches or PR assumptions

If GitHub access is unavailable, report exactly what could not be verified.
Do not approve, merge, push, close issues, or change settings.

============================================================
14. FINAL RISK REGISTER AND PRIORITIZATION
============================================================

Create a risk register with:
- ID
- severity
- category
- exact evidence
- affected module
- likelihood
- impact
- current control
- recommended correction
- verification method
- owner: Codex, human, or infrastructure
- blocker status

Prioritize findings into:
1. Must fix before any further implementation
2. Must fix before experiment validity can be claimed
3. Must fix before merge/release
4. Important follow-up
5. Future Work / explicitly deferred

============================================================
15. REQUIRED REPORT
============================================================

Only when the exact output location is explicitly authorized, create a new
Markdown report at:

artifacts/reviews/antigravity-deep-audit-<UTC_TIMESTAMP>.md

Do not overwrite an existing report. If creating the report would mutate the
repository and you do not have explicit permission, write the complete report
in your response and state the intended path.

The report must contain:

# Deep Project Audit Report

## Executive verdict
Choose one: NO-GO, CONDITIONAL GO, or GO FOR THE INSPECTED SCOPE.
Explain the decision in plain language.

## Audit metadata
Revision, branch, status, environment, timestamps, commands, and limitations.

## Coverage matrix
Architecture, code, tests, research, Milvus, safety, security, performance,
reproducibility, documentation, GitHub, and release readiness. Mark each
COMPLETE, PARTIAL, BLOCKED, or NOT APPLICABLE with evidence.

## Exact commands and raw results
Include commands, exit codes, relevant stdout/stderr, skipped checks, and
warnings. Do not replace raw results with emojis or summaries.

## Findings
P0 to P3, each with file/line evidence and a concrete correction.

## Test results
Passed, failed, skipped, xfailed, warnings, duration, and test gaps.

## Research validity assessment
Claim classifications, formula re-derivations, experiment problems, and
evidence gaps.

## Safety and failure matrix
Expected versus observed behavior and fail-closed assessment.

## Security assessment
Findings, severity, impact, and remediation.

## Performance/resource assessment
Measured evidence only, bounds, and missing measurements.

## Reproducibility assessment
Clean-checkout path, environment gaps, and artifact integrity.

## GitHub/CI/release assessment
Verified controls and unavailable checks.

## Risk register

## Prioritized remediation plan
Small, ordered tasks suitable for Codex. Include Core/Future Work labels.

## Codex handoff
For each implementation task, use HANDOFF_TEMPLATE.md format with exact
interfaces, constraints, tests, proof-of-work commands, and non-goals.

## Human decisions required

## Audit limitations

Final rules:
- Do not claim the project is production-ready unless every critical gate has
  evidence.
- Do not convert missing evidence into a pass.
- Do not silently fix findings.
- Do not modify or delete existing project work.
- Do not commit, merge, push, deploy, or enable live actuation.
- Quote exact paths and line numbers wherever possible.
- Separate VERIFIED facts from INFERRED conclusions and HYPOTHESES.
- End with the three most urgent actions Codex should take next.
```

## Suggested follow-up prompt

After Antigravity creates the report, paste this:

```text
Now independently re-check your own audit for false positives, unsupported
claims, missed critical paths, and conclusions that rely only on mocked tests.
For every P0/P1 finding, provide the exact source evidence and the smallest
reproduction or verification command. Mark each finding CONFIRMED,
UNCONFIRMED, or BLOCKED. Do not change the repository. Append a self-review
section to the audit report or provide it in your response.
```
