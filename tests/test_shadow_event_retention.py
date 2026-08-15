"""FINDING-004: rejected/quarantined traces are retained, not orphans.

`orphaned_trace_paths()` previously built its "referenced" set from the pending
and acknowledged directories only.  A trace whose event was quarantined still
has authoritative rejection evidence -- the event document plus its reason
ledger entry -- yet it was reported as unreferenced.  That is the wrong
classification for anything a retention policy might act on.

Nothing here deletes, and nothing in the repository deletes on this method's
behalf; the fix is purely to the classification.
"""

from __future__ import annotations

import inspect
import json
import tempfile
import unittest
from pathlib import Path

from tests.test_shadow_event_source import (  # reuse the committed fixtures
    _context,
    _trace,
)
from vdbench import shadow_event_source
from vdbench.shadow_event_source import FileShadowTraceEventSource


def _outbox(root: Path) -> FileShadowTraceEventSource:
    return FileShadowTraceEventSource(
        root / "outbox", max_pending_events=64, max_pending_bytes=64 * 1024 * 1024
    )


class RetentionClassificationTests(unittest.TestCase):
    def _publish(self, outbox: FileShadowTraceEventSource, *, index: int):
        context = _context(trace_sequence_index=index, trace_id=f"trace-{index}")
        return outbox.publish(trace=_trace(trace_offset=index), context=context)

    def test_pending_trace_is_not_an_orphan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            self._publish(outbox, index=0)
            self.assertEqual(outbox.orphaned_trace_paths(), ())

    def test_acknowledged_trace_is_not_an_orphan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            receipt = self._publish(outbox, index=0)
            outbox.poll(limit=4)
            outbox.acknowledge((receipt.event_id,))
            self.assertEqual(outbox.orphaned_trace_paths(), ())

    def test_quarantined_trace_is_retained_not_orphaned(self) -> None:
        """The regression: rejected evidence still references its trace."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            receipt = self._publish(outbox, index=0)
            envelope = root / "outbox" / "traces" / f"{receipt.event_id}.json"
            self.assertTrue(envelope.is_file())

            # Corrupt the envelope so `poll` quarantines the event. The event
            # document itself stays intact and moves to `rejected/`.
            envelope.write_text("{}", encoding="utf-8")
            self.assertEqual(outbox.poll(limit=4), ())

            rejected = root / "outbox" / "rejected" / f"{receipt.event_id}.json"
            self.assertTrue(rejected.is_file())
            self.assertTrue(
                (root / "outbox" / "rejected" / f"{receipt.event_id}.reason.json").is_file()
            )
            self.assertTrue(outbox.rejected_reason_codes())

            self.assertEqual(outbox.orphaned_trace_paths(), ())

    def test_a_true_orphan_is_still_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            orphan = root / "outbox" / "traces" / "unreferenced.json"
            orphan.write_text(json.dumps({"nothing": "refers to this"}), encoding="utf-8")
            self.assertEqual(outbox.orphaned_trace_paths(), (orphan,))

    def test_reason_sidecars_are_not_mistaken_for_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            receipt = self._publish(outbox, index=0)
            (root / "outbox" / "traces" / f"{receipt.event_id}.json").write_text(
                "{}", encoding="utf-8"
            )
            outbox.poll(limit=4)
            retained = outbox._retained_event_paths()
            self.assertTrue(retained)
            self.assertFalse(
                any(path.name.endswith(".reason.json") for path in retained)
            )

    def test_orphan_reporting_never_deletes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outbox = _outbox(root)
            orphan = root / "outbox" / "traces" / "unreferenced.json"
            orphan.write_text("{}", encoding="utf-8")
            outbox.orphaned_trace_paths()
            outbox.orphaned_trace_paths()
            self.assertTrue(orphan.is_file())


class NoAutomaticDeletionTests(unittest.TestCase):
    def test_the_module_has_no_bulk_or_targeted_removal_primitive(self) -> None:
        source = inspect.getsource(shadow_event_source)
        for forbidden in ("shutil.rmtree", "os.remove(", "os.unlink(", "os.rmdir("):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)

    def test_the_only_unlink_is_temporary_file_cleanup(self) -> None:
        """One `unlink` exists; it removes the write-time hardlink temp only."""

        source = inspect.getsource(shadow_event_source)
        unlinks = [line.strip() for line in source.splitlines() if ".unlink(" in line]
        self.assertEqual(unlinks, ["temporary.unlink()"])
        self.assertIn("def _write_new", source)

    def test_orphan_reporting_is_read_only_by_construction(self) -> None:
        for name in ("orphaned_trace_paths", "_retained_event_paths"):
            with self.subTest(method=name):
                body = inspect.getsource(
                    getattr(shadow_event_source.FileShadowTraceEventSource, name)
                )
                for forbidden in ("unlink", "rmtree", "remove(", "write_", "_move_durable"):
                    self.assertNotIn(forbidden, body)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
