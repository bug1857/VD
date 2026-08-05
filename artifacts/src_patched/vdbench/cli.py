"""Command line entry points for deterministic generation and explicit live runs."""

from __future__ import annotations

import argparse
from pathlib import Path

from .artifacts import verify_dataset_artifacts, write_dataset_artifacts
from .dataset import boundary_fixtures, calibrate_thresholds, generate_dataset
from .runner import execute_live


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vd-exp-bench")
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="generate immutable DATASET-001")
    generate.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run", help="explicitly execute EXP-002 against ENV-001")
    run.add_argument("--repository", type=Path, required=True)
    run.add_argument("--dataset-dir", type=Path, required=True)
    run.add_argument("--run-dir", type=Path, required=True)
    run.add_argument("--collection-prefix", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "generate":
        bundle = generate_dataset()
        thresholds = calibrate_thresholds(bundle.base_vectors, bundle.calibration_queries)
        write_dataset_artifacts(args.output, bundle, thresholds, boundary_fixtures())
        verify_dataset_artifacts(args.output)
        return 0
    execute_live(
        repository=args.repository,
        dataset_dir=args.dataset_dir,
        run_dir=args.run_dir,
        collection_prefix=args.collection_prefix,
    )
    return 0
