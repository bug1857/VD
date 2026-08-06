"""Load and validate DATASET-003's lkg_qualification workload before dispatch.

Purpose:
    Give the LKG-qualification producer one fail-closed entry point for the
    exact DATASET-003 query IDs, vectors, and identity it must have *before*
    any client dispatch -- ARCHITECTURE.md's ADR-002 amendment requires this
    contract to run against DATASET-003 (never a substitute), and this
    module is where that requirement is actually enforced for a real run.
Inputs:
    A published DATASET-003 output directory plus the DATASET-001/002
    directories its manifest was generated against.
Outputs:
    An ``LkgDataset003Workload``: validated query IDs (ascending, unique),
    their vectors, and the DATASET-003 identity fields
    (``dataset_id``/``dataset_version``/``manifest_sha256``/``query_role``)
    an ``LkgRunBinding`` and every ``LkgQueryObservation`` must carry.
Dependencies:
    ``dataset003.py``'s existing public ``verify_dataset003_artifacts`` --
    the complete strict verifier (manifest schema, every artifact checksum,
    inherited DATASET-001/002 identity, and a byte-exact re-comparison of
    the stored arrays against a fresh deterministic regeneration) -- plus
    NumPy to read the validated arrays. Never PyMilvus, routing, policy, or
    any canary/actuation module.
Failure modes:
    Any manifest/checksum/identity/array-shape/ordering/duplication problem
    ``verify_dataset003_artifacts`` or this module's own array checks catch
    raises ``ContractViolation`` before a single query ID or vector is
    returned. Nothing here is ever exposed as if partially trustworthy.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import numpy.typing as npt

from .artifacts import sha256_file
from .config import ContractViolation
from .dataset003 import verify_dataset003_artifacts


__all__ = ["LkgDataset003Workload", "load_dataset003_workload"]

# DATASET-003's own published, stable artifact filenames (dataset003.py's
# public contract, documented in its own module docstring). Not a private
# symbol reused from that module.
_MANIFEST_FILENAME = "dataset003_manifest.json"
_IDS_FILENAME = "lkg_qualification_ids.npy"
_QUERIES_FILENAME = "lkg_qualification_queries.npy"


@dataclass(frozen=True, slots=True)
class LkgDataset003Workload:
    """One validated, dispatch-ready DATASET-003 lkg_qualification population."""

    query_ids: tuple[int, ...]
    query_vectors: Mapping[int, npt.NDArray[np.float32]]
    dataset_id: str
    dataset_version: str
    manifest_sha256: str
    query_role: str
    query_id_array_sha256: str
    query_array_sha256: str


def load_dataset003_workload(
    output_dir: str | os.PathLike[str],
    *,
    dataset001_dir: str | os.PathLike[str],
    dataset002_dir: str | os.PathLike[str],
) -> LkgDataset003Workload:
    """Load and fully validate DATASET-003 before any query dispatch.

    Runs the complete strict verifier (``verify_dataset003_artifacts``)
    first -- manifest schema, every artifact SHA256SUMS/manifest checksum,
    exact file inventory, inherited DATASET-001/002 identity, and
    (critically) a byte-exact re-comparison of the stored ids/queries
    arrays against a fresh deterministic regeneration, which is the
    strongest available ordering guarantee: any reordering, duplication, or
    corruption of either array already fails that check. Only after
    verification succeeds are the arrays read into the returned workload,
    with this module's own shape/dtype/uniqueness/ordering checks applied
    as explicit, redundant defense-in-depth -- not trusting that the
    verifier above was actually called correctly by every caller.
    """

    output = Path(output_dir)
    manifest = verify_dataset003_artifacts(
        output, dataset001_dir=dataset001_dir, dataset002_dir=dataset002_dir
    )
    manifest_sha256 = sha256_file(output / _MANIFEST_FILENAME)
    query_id_array_sha256 = sha256_file(output / _IDS_FILENAME)
    query_array_sha256 = sha256_file(output / _QUERIES_FILENAME)

    dataset = manifest["dataset"]
    if not isinstance(dataset, Mapping):
        raise ContractViolation("DATASET-003 manifest dataset section is invalid")
    query_count = dataset.get("lkg_qualification_query_count")
    dimensions = dataset.get("dimensions")
    if (
        isinstance(query_count, bool)
        or not isinstance(query_count, int)
        or query_count <= 0
        or isinstance(dimensions, bool)
        or not isinstance(dimensions, int)
        or dimensions <= 0
    ):
        raise ContractViolation("DATASET-003 manifest dataset shape fields are invalid")

    try:
        ids = np.load(output / _IDS_FILENAME, allow_pickle=False)
        queries = np.load(output / _QUERIES_FILENAME, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise ContractViolation("DATASET-003 arrays are unreadable") from exc
    if (
        ids.ndim != 1
        or ids.shape != (query_count,)
        or ids.dtype.str != "<i8"
        or queries.shape != (query_count, dimensions)
        or queries.dtype.str != "<f4"
        or not np.all(np.isfinite(queries))
    ):
        raise ContractViolation("DATASET-003 arrays violate the expected schema")

    id_list = [int(value) for value in ids]
    if len(set(id_list)) != len(id_list):
        raise ContractViolation("DATASET-003 query IDs contain duplicates")
    if id_list != sorted(id_list):
        raise ContractViolation(
            "DATASET-003 query IDs are not in the expected ascending order"
        )

    query_vectors: dict[int, npt.NDArray[np.float32]] = {
        query_id: np.asarray(queries[index], dtype="<f4")
        for index, query_id in enumerate(id_list)
    }

    query_role = manifest.get("query_role")
    if not isinstance(query_role, str) or not query_role:
        raise ContractViolation("DATASET-003 manifest query_role is invalid")

    return LkgDataset003Workload(
        query_ids=tuple(id_list),
        query_vectors=query_vectors,
        dataset_id=dataset["dataset_id"],
        dataset_version=dataset["version"],
        manifest_sha256=manifest_sha256,
        query_role=query_role,
        query_id_array_sha256=query_id_array_sha256,
        query_array_sha256=query_array_sha256,
    )
