# Fresh Forensic Audit Prompt

Paste the complete prompt below into Antigravity. This version deliberately
does not trust the previous audit, previous test counts, previous experiment
claims, or existing documentation.

```text
QUALITY AND AUTHORIZATION OVERRIDE:
AGENTS.md and the current human-authorized task govern this prompt. Invoke this
full-project mode only when requested; reviewing its instructions does not
execute its phases. Quality outranks token savings. Preserve complete declared
coverage, critical reasoning, failure paths, all findings, and required evidence.
Use adequate model/effort and coherent parts with a coverage/boundary map when
limits are reached; report PARTIAL/BLOCKED rather than omit proof or downgrade
a model silently. Ponytail does not cap test depth or weaken project gates.

Direct repository mode requires independently inspected source and raw results.
In supplied-text bridge mode, review only supplied inputs, list missing evidence,
and propose checks; never imply fresh execution or full-repository coverage.
No phase grants Docker/Milvus contact, new isolated services, Gate A/B/C, D2,
authority, qualification, or governed-evidence writes. Availability and the labels
offline/test/read-only/DRY_RUN are not permission. Inspect effects and exact
authorization before execution. Required checks that cannot run remain blockers.
Historical EXP stage examples and package/version assertions below must be
checked against current pins, exact source, and authoritative task/review state;
they are not fixed current status or permission to start backlog work.

Perform a fresh forensic audit of the current repository from the actual
working tree. Do not rely on the previous Antigravity audit, Codex summaries,
README claims, EXPERIMENT_LOG claims, CI status, screenshots, cached output,
or previously reported test counts. Treat all previous results as unverified
until you independently reproduce them now.

You are an adversarial Principal Engineer, Research Scientist, QA Lead,
Security Engineer, Reliability Engineer, Milvus Integration Engineer, and
Release Reviewer.

The audit target is the current checkout at the moment you begin. Capture its
exact Git revision and state first. Do not modify source code, tests,
architecture files, experiment history, Git configuration, GitHub state,
containers, persistent data, credentials, or production systems. Never reset,
clean, checkout, stash, delete, overwrite, or reformat existing files.

Default report delivery is the response. Only if the human explicitly authorized
the exact new report location in Phase 10 may you create that report. Do not
create, edit, or delete any other repository file. If output cannot be written
safely within that authorization, return the full report in chat.

If a test or experiment would mutate the repository or persistent services,
run it only when separately authorized, in a newly-created temporary isolated
directory/service with an explicitly recorded cleanup path. If safe isolation is impossible, do not
pretend to run it: mark it BLOCKED and state the exact prerequisite.

Never run Docker Compose down, Docker prune, volume removal, index/collection
mutation APIs, candidate routing, or live actuation. Use existing test seams,
mocks, fixtures, or disposable isolated resources only.

============================================================
PHASE 0 — FREEZE THE AUDIT BASELINE
============================================================

Before reading conclusions from documentation, record the output of:

pwd
git rev-parse --show-toplevel
git branch --show-current
git rev-parse HEAD
git log -10 --oneline --decorate
git status --short --branch
git diff --stat
git diff --check
git ls-files | sort
git ls-files --others --exclude-standard | sort
git remote -v

Also record:

python3 --version
python3 -c "import sys, platform; print(sys.executable); print(platform.platform()); print(platform.machine())"
python3 -c "import numpy; print(numpy.__version__)"
docker version, if available
docker compose version, if available

Record exact timestamps, exit codes, stdout, stderr, and unavailable commands.
The working tree may be dirty. Preserve every modified and untracked path.

Audit the committed baseline and dirty working tree as separate targets:

1. **Committed baseline:** exactly `HEAD`, excluding untracked files.
2. **Current dirty tree:** all tracked modifications and untracked source/test
   files that Python test discovery can reach.

Report two independent test outcomes and identify exactly which tracked or
untracked paths explain any difference. Never let an untracked test, artifact,
or source module improve the verdict for the committed baseline.

Determine which paths are actually tracked, dirty, or untracked at this revision.
For example, historical EXP-009 routing WIP must not be called untracked if it
is now committed. Inspect genuine WIP separately and do not use it to improve
the committed baseline's verdict.

============================================================
PHASE 1 — REBUILD PROJECT STATE WITHOUT TRUSTING CLAIMS
============================================================

Read completely:

AGENTS.md
ARCHITECTURE.md
RESEARCH_PLAN.md
ROADMAP.md
EXPERIMENT_LOG.md
README.md
HANDOFF_TEMPLATE.md

Then inspect every source file, test file, experiment script, infrastructure
file, configuration file, artifact manifest, and GitHub workflow.

Build a claim ledger. For every important claim in the documentation record:

| Claim | File/section | Classification claimed | Fresh evidence | Status |

Use only these audit statuses:
- FRESHLY VERIFIED: reproduced during this audit with raw evidence
- REPRODUCIBLE BUT NOT EXECUTED: command/path exists but was not safely run
- HISTORICAL ONLY: prior artifact exists but current claim was not reproduced
- CONTRADICTED: current code/output disagrees
- UNVERIFIABLE: required evidence is missing
- BLOCKED: prerequisite unavailable

Do not call a historical EXP result fresh evidence.

============================================================
PHASE 2 — COMPLETE SOURCE AND ARCHITECTURE ANALYSIS
============================================================

Inspect all modules under src/ and map:

workload input -> query execution -> observation capture -> durable outbox ->
window assembly -> drift detector -> policy -> validation -> approval -> route
plan -> actuation boundary -> Milvus -> health/identity check -> audit ->
rollback/restart recovery.

For every function/class on this path, document:
- inputs and exact validation
- outputs and error types
- state transitions
- persistence and atomicity
- concurrency model
- timeout/retry behavior
- memory/queue/file bounds
- identity binding
- logging/audit behavior
- tests that exercise the real path
- untested failure modes

Independently inspect every ADR and compare its requirements with current
code. Identify architecture drift, stale status labels, missing configuration
registry entries, unregistered tunable parameters, dead code, hidden defaults,
duplicated validation, broad exception handling, fail-open paths, and scope
expansion.

============================================================
PHASE 3 — FRESH TEST EXECUTION
============================================================

Discover the official commands from pyproject.toml, README.md, scripts,
Makefiles, and CI. Do not assume the prior command is sufficient.

When authorized, create a clean temporary virtual environment outside the repository and test
the installed project—not the system interpreter or an accidentally importable
source tree. First materialize the exact audited source into an external
temporary checkout/archive; install from that copy to avoid build writes in VD.
Use current committed dependency pins/lock and record bootstrap versions; do not
upgrade tools or silently change dependencies merely to obtain a passing run.
The historical example below is schematic, not a command to execute blindly:

```text
AUDIT_ENV="$(mktemp -d /tmp/vd-audit-XXXXXX)"
python3 -m venv "$AUDIT_ENV"
"$AUDIT_ENV/bin/python" -m pip install /absolute/path/to/external/audited-source-copy
"$AUDIT_ENV/bin/python" -c "import cryptography; print(cryptography.__version__)"
"$AUDIT_ENV/bin/python" -m unittest discover -s tests -v
```

Capture complete installation and test output to a new external temporary file
and report every command, exit code, duration, resolved dependency version,
test count, failure, error, skip, xfail, warning, and environment detail. The
approval tests are BLOCKED—not passed—if `cryptography==49.0.0` is unavailable
in the clean environment.

First inspect whether a committed top-level dependency lock exists. If it does
not, record the absence as a reproducibility finding. Do not invent a lock,
install extra packages to make tests pass, or silently fall back to system
Python.

Then run every test module individually, or in the smallest safe groups, so a
single aggregate success cannot hide collection/import/order problems. Run the
committed-baseline module set separately from untracked WIP tests.

Run focused fresh tests for:
- drift detector
- policy
- actuation boundary
- persistence and restart
- workload monitor
- outbox/event source
- host observation recorder
- Milvus adapter and executor
- shadow extraction/window assembly
- DATASET-002 verification
- EXP-009 Stage 1
- EXP-009 signed approval verification
- EXP-009 route-plan WIP, if present
- canary statistics
- canary workload

Run tests twice where practical and compare results. Run in a different test
order or with randomized order if the framework supports it. Report any
non-determinism, timing sensitivity, leaked state, or order dependence.

Discover configured lint, format, type, import, syntax, and static-analysis
tools before running them. If the repository defines none, report
**NOT CONFIGURED** as a developer-experience/reproducibility finding. Do not
install or configure new tools merely to manufacture a green result.

============================================================
PHASE 4 — DELIBERATE FAILURE AND ADVERSARIAL TESTING
============================================================

Do not stop at happy-path tests. Locate existing failure tests and independently
execute them. Add no code; instead, use temporary copies/fixtures or existing
test seams to exercise failures safely.

Do not tamper with repository evidence, Docker volumes, persistent collections,
or existing services to create failures. If an existing safe test seam does not
exist, record the test gap rather than altering the project.

At minimum test and record behavior for:
- malformed JSON and duplicate JSON keys
- missing fields and unexpected fields
- wrong types, booleans where integers are expected
- NaN, infinity, negative, zero, and extreme numeric values
- wrong vector dimensions and dtype
- non-finite vectors
- checksum mismatch
- missing artifact
- truncated artifact
- changed artifact after manifest creation
- duplicate occurrence IDs
- missing occurrence IDs
- candidate count other than 60
- candidate ID outside eligible population
- candidate/LKG overlap
- post-freeze workload mutation
- wrong dataset identity
- wrong collection/index identity
- wrong metric or threshold
- wrong result limit
- stale timestamp
- future timestamp
- expired approval
- invalid signature
- replayed approval
- revoked approval
- missing approval
- policy exception
- detector exception
- backend unavailable
- backend timeout
- partial persistence
- disk-full or permission failure where safely simulatable
- queue full/backpressure
- duplicate event
- out-of-order event
- process restart
- rollback failure
- health-check failure
- identity-check failure
- DRY_RUN configuration

For each case prove whether it fails closed, whether state remains durable and
consistent, whether any backend call occurred, whether any candidate call
occurred, and whether an audit record was written.

============================================================
PHASE 5 — INDEPENDENT MATHEMATICAL AND STATISTICAL REVIEW
============================================================

Recalculate important formulas independently using a separate short script or
manual derivation, not by calling the project's implementation.

At minimum verify:
- L2 distance and cosine score semantics
- threshold boundary inclusion/exclusion
- capped recall definition
- MMD calculation
- permutation-test behavior and exchangeability
- Holm correction
- drift hysteresis
- finite-population 60-of-600 p95 coverage
- Hoeffding recall margin for 1,200 audit samples
- p95 nearest-rank index
- all sample-size and probability claims

Use independent standard-library or separately written calculations. Compare
the independent result with the project result. Record inputs, formula,
independent output, project output, tolerance, and conclusion.

Check for:
- leakage between calibration and evaluation
- post-hoc estimator selection
- multiple-testing problems
- unrecorded stopping rules
- selection bias
- hidden dependence
- route-assignment interference
- invalid IID or production generalization
- insufficient sample size
- mixing historical environments
- false precision

Every audit conclusion must use the audit statuses from Phase 1. You may record
the project's own VERIFIED/SUPPORTED/INFERRED/HYPOTHESIS labels separately, but
never substitute them for fresh audit evidence.

============================================================
PHASE 6 — FRESH ARTIFACT AND REPRODUCIBILITY VERIFICATION
============================================================

For every referenced dataset, environment, run, and evidence bundle:

1. Verify that the path exists.
2. Verify every declared checksum independently.
3. Verify byte sizes.
4. Parse manifests independently.
5. Check schema, timestamps, IDs, seeds, versions, and source revision.
6. Check that the artifact contents match the documentation.
7. Detect missing, extra, stale, or contradictory files.
8. Check whether the artifact was generated by the current code/revision.
9. Check for secrets, private data, raw vectors, and unsafe permissions.

Do not accept an artifact merely because a verifier inside the project accepts
it. Independently sample or recalculate critical values.

Attempt a clean reproducibility path in a temporary location if safe:
- create a fresh virtual environment only outside the repository
- install only from committed dependency configuration; if no top-level lock
  exists, record that gap explicitly
- regenerate or verify the deterministic datasets
- run the documented offline commands
- compare outputs and hashes

If blocked by network, Docker, hardware, or credentials, record the exact
blocker and do not label the result verified.

============================================================
PHASE 7 — MILVUS AND LIVE-INTEGRATION AUDIT
============================================================

Inspect pinned Milvus/PyMilvus versions, Compose files, digests, ports,
health checks, resource limits, volumes, and identity handling.

Only if the exact operation is separately authorized, applicable project gates
are satisfied, and an isolated local Milvus environment is already available
and safe, independently verify:
- service health
- empty experiment-scoped collections only
- FLAT versus independent oracle semantics
- HNSW threshold validity
- ef query-time variation
- index identity stability
- result limit behavior
- timeout and unavailable-service behavior
- no mutation API calls during read-only checks

Never use production services or existing persistent collections. Never claim
live verification if the environment is unavailable.

============================================================
PHASE 8 — CRITICAL SAFETY AND ACTUATION AUDIT
============================================================

Trace every possible path to a live configuration mutation. Prove whether it
can occur without all required gates:

- accepted ADR
- prior supporting EXP
- valid approval
- signature/trust validation
- expiry and replay prevention
- exact workload/selection/decision digest binding
- health check
- identity check
- configuration validation
- bounded step
- DRY_RUN evidence
- durable audit log
- last-known-good state
- rollback
- rollback verification
- restart recovery
- operator rejection handling

For every denial, prove zero candidate routing/backend mutation calls. For
every failure, prove LKG preservation or restoration. Review race conditions
between approval, route lookup, expiration, restart, and rollback.

Explicitly distinguish:
- Stage 1 offline contract verification
- Stage 2 approval/routing contract
- Stage 3 rollback containment
- Stage 4 controlled live canary

Do not treat frozen Stage-4 rules as executed Stage-4 evidence.
Do not mark Stage 2 complete merely because the signed-approval verifier exists.
It also requires a committed and freshly verified exact immutable 60/540 route
partition, atomic route-state lifecycle, refusal/audit boundary, expiry and
restart failback, and proof that every denial makes zero candidate/backend
mutation calls.

============================================================
PHASE 9 — SECURITY, PERFORMANCE, AND RELEASE AUDIT
============================================================

Inspect secrets, paths, permissions, subprocesses, shell use, dependencies,
containers, network bindings, logs, artifacts, and GitHub workflows.

Review algorithmic complexity, memory bounds, queue behavior, worker behavior,
timeouts, retry storms, disk growth, artifact retention, and concurrency.

Inspect GitHub/CI only read-only. Verify what is actually configured; report
unavailable access as BLOCKED.

============================================================
PHASE 10 — REPORT AND SELF-CHALLENGE
============================================================

Return the complete report in the response, or only when the exact new output
location was explicitly authorized create one report without overwriting prior reports:

artifacts/reviews/fresh-forensic-audit-<UTC_TIMESTAMP>.md

The report must contain:

# Fresh Forensic Audit Report
## Executive verdict
NO-GO, CONDITIONAL GO, or GO FOR INSPECTED SCOPE

## Audit baseline
Exact revision, branch, status, environment, timestamps, and limitations.

## Committed baseline versus dirty tree
List source/test/artifact differences and separate test results for each.

## Fresh-versus-historical evidence ledger

## Coverage matrix
Mark each area COMPLETE, PARTIAL, BLOCKED, or NOT APPLICABLE.

## Commands and raw outputs
Include exact commands, exit codes, durations, and relevant full outputs.

## Fresh test results
Do not quote previous test counts as current results.
Include clean-environment dependency-installation output and the exact
`cryptography` version, or the reason approval tests are BLOCKED.

## Failure-injection results
Include backend-call and candidate-call evidence.

## Independent mathematical verification

## Artifact and reproducibility verification

## Milvus/integration status

## Safety and actuation matrix

## Security findings

## Performance/resource findings

## P0-P3 findings
Every finding must include exact path/line, evidence, impact, correction,
verification command, and blocker status.

## Risk register

## Prioritized Codex plan
Separate immediate blockers, Core tasks, important cleanup, and Future Work.

## Human decisions required

## Audit limitations

## Auditor self-challenge
List at least five ways this audit could still be wrong and what evidence would
be needed to resolve each one.

## Final three actions

Do not report “all tests pass” unless you personally executed the tests during
this audit and include the current raw output. Do not report production
readiness while any CRITICAL gate is historical, unverified, or blocked.

Do not mark approval verification valid if `cryptography` was unavailable.
Do not mark Stage 2 complete if routing remains untracked, incomplete, or
untested. Do not mark Stage 3 as started because route-plan code exists. Do not
mark Stage 4 as verified because its schedule-stability contract is defined.
Do not mark live actuation safe without separately authorized, real live
evidence.
```

After the report is created, run this follow-up:

```text
Re-open the fresh forensic audit report and challenge every PASS, VERIFIED, and
COMPLETE label. For each one, identify the exact fresh command, raw output,
artifact, or independent calculation that supports it. Downgrade anything
based only on historical evidence, project self-verification, unavailable
infrastructure, or unexecuted live behavior. Then append a final correction
section to the report. Do not modify source code or tests.
```
