"""Seal fake-only evidence for EXP-009's human-gated Stage-4 live root.

This wrapper intentionally runs only focused fake-port suites, the repository
suite, and dependency integrity checks.  It neither creates a Milvus client,
performs a search, accepts a real grant, publishes a real route, nor mutates a
configuration.  The resulting profile is separate from the legacy offline
composition bundle so historical evidence remains independently verifiable.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from experiments.exp009_stage4_validate import (
    LIVE_ROOT_VALIDATION_SPEC,
    GitStateProvider,
    SuiteRunner,
)
from experiments.exp009_stage4_validate import (
    Exp009Stage4ValidationError as Exp009Stage4LiveRootValidationError,
)
from experiments.exp009_stage4_validate import (
    run_validation as _run_validation,
)
from experiments.exp009_stage4_validate import (
    verify_validation_bundle as _verify_validation_bundle,
)

__all__ = [
    "LIVE_ROOT_SUITE_FILENAMES",
    "Exp009Stage4LiveRootValidationError",
    "run_validation",
    "verify_validation_bundle",
]


LIVE_ROOT_SUITE_FILENAMES = LIVE_ROOT_VALIDATION_SPEC.focused_suite_filenames


def run_validation(
    *,
    output_dir: Path,
    repository: Path | None = None,
    git_state_provider: GitStateProvider | None = None,
    suite_runner: SuiteRunner | None = None,
) -> dict[str, object]:
    """Create one clean-commit sealed fake-only live-root evidence bundle."""

    options: dict[str, object] = {
        "output_dir": output_dir,
        "repository": repository,
        "spec": LIVE_ROOT_VALIDATION_SPEC,
    }
    if git_state_provider is not None:
        options["git_state_provider"] = git_state_provider
    if suite_runner is not None:
        options["suite_runner"] = suite_runner
    return _run_validation(**options)  # type: ignore[arg-type]


def verify_validation_bundle(
    output_dir: Path, *, require_complete: bool = True
) -> dict[str, object]:
    """Verify only the separate live-root fake-only evidence profile."""

    return _verify_validation_bundle(
        output_dir,
        require_complete=require_complete,
        spec=LIVE_ROOT_VALIDATION_SPEC,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(run_validation(output_dir=arguments.output_dir), sort_keys=True))


if __name__ == "__main__":
    main()
