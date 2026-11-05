"""Neutral read-only measurement fixtures; no model, network, or subprocesses."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from taskflow.stress_metrics import MetricsError, _bytes, runtime_metrics, usage_summary


class RuntimeMetricsTests(unittest.TestCase):
    def test_usage_totals_preserve_raw_counters_and_missing_is_unknown(self):
        rows = [{"line": 1, "value": {"input_tokens": 20, "cached_input_tokens": 8,
                                     "output_tokens": 3, "provider_extra": 77}},
                {"line": 2, "value": {"input_tokens": 10, "cached_input_tokens": 0,
                                     "output_tokens": 2}}]
        result = usage_summary(rows, 2)
        self.assertEqual((30, 8, 5), tuple(result[k] for k in
                         ("input_tokens", "cached_input_tokens", "output_tokens")))
        self.assertEqual(77, rows[0]["value"]["provider_extra"])
        self.assertIsNone(result["actual_billing"])
        for values, turns in ((rows, 3), ([], 0), ([], None),
                              ([{"value": {"input_tokens": True}}], 1),
                              ([{"value": {"input_tokens": -1}}], 1),
                              ([{"value": []}], 1)):
            with self.subTest(values=values, turns=turns):
                self.assertIsNone(usage_summary(values, turns)["input_tokens"])
        self.assertIsNone(usage_summary([{"value": {"input_tokens": 4}}], 1)["cached_input_tokens"])

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.write("manifest.json", {
            "mode": "native_role", "model": {"requested": "model-a", "reasoning_effort": "low",
                "served": None, "cli_version": "cli-one"},
            "inputs": [{"kind": "public"}, {"kind": "public"}],
        })
        self.termination = {"started_at": "2026-01-01T00:00:00+00:00",
                            "ended_at": "2026-01-01T00:02:03.5+00:00",
                            "returncode": 0, "timed_out": False}
        self.write("termination.json", self.termination)
        self.write("collection.json", {"status": "completed", "technical_accepted": True,
                                      "model_completed": True, "sid": "session-a",
                                      "termination": self.termination})
        self.trace([
            {"type": "thread.started", "thread_id": "session-a"},
            {"type": "item.completed", "item": {"type": "agent_message", "id": "a", "text": "DO_NOT_EXPOSE_ANSWER"}},
            {"type": "item.completed", "item": {"type": "command_execution", "id": "b", "command": "private command"}},
            {"type": "item.completed", "item": {"type": "file_change", "id": "c"}},
            {"type": "turn.completed", "usage": {"input_tokens": 40, "output_tokens": 12}},
        ])

    def write(self, name, value):
        (self.root / name).write_text(json.dumps(value) + "\n", encoding="utf-8")

    def trace(self, events):
        (self.root / "trace.jsonl").write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")

    def test_report_preserves_units_identity_original_usage_and_readonly_evidence(self):
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        result = runtime_metrics(self.root)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})
        self.assertEqual("model-a", result["requested_model"])
        self.assertEqual("low", result["effort"])
        self.assertIsNone(result["served"])
        self.assertEqual("cli-one", result["cli_version"])
        self.assertEqual("session-a", result["sid"])
        self.assertEqual("real_native", result["nature"])
        self.assertEqual(123.5, result["timing"]["gross_seconds"])
        self.assertNotIn("effective_seconds", result["timing"])
        self.assertNotIn("step_count", result)
        self.assertEqual(1, result["counts"]["agent_messages"])
        self.assertEqual(1, result["counts"]["command_executions"])
        self.assertEqual(1, result["counts"]["file_changes"])
        self.assertEqual(1, result["counts"]["tool_calls"])
        self.assertEqual({"input_tokens": 40, "output_tokens": 12}, result["usage"][0]["value"])
        self.assertEqual({"public": 2}, result["inputs_by_kind"])
        self.assertEqual(0, result["reconnections"])
        self.assertIsNone(result["actual_billing"])
        self.assertNotIn("DO_NOT_EXPOSE_ANSWER", json.dumps(result))
        self.assertNotIn("private command", json.dumps(result))
        trace_ref = next(row for row in result["evidence"] if row["path"].endswith("trace.jsonl"))
        self.assertEqual(hashlib.sha256(before["trace.jsonl"]).hexdigest(), trace_ref["sha256"])

    def test_missing_trace_is_unknown_not_zero_and_empty_trace_is_zero(self):
        (self.root / "trace.jsonl").unlink()
        result = runtime_metrics(self.root)
        self.assertIsNone(result["counts"]["agent_messages"])
        self.assertIsNone(result["reconnections"])
        self.assertIn("missing_trace.jsonl", result["limitations"])
        (self.root / "trace.jsonl").write_bytes(b"")
        result = runtime_metrics(self.root)
        self.assertEqual(0, result["counts"]["agent_messages"])
        self.assertEqual(0, result["counts"]["parsed_events"])
        self.assertEqual([], result["completion"]["terminal_events"])

    def test_missing_and_invalid_metadata_stay_unknown(self):
        (self.root / "manifest.json").unlink()
        (self.root / "termination.json").write_bytes(b"NaN")
        (self.root / "collection.json").write_bytes(b"[]")
        result = runtime_metrics(self.root)
        self.assertIsNone(result["requested_model"])
        self.assertIsNone(result["completion"]["status"])
        self.assertIsNone(result["timing"]["gross_seconds"])
        self.assertIsNone(result["inputs_by_kind"])
        self.assertEqual("session-a", result["sid"])
        self.assertIn("invalid_collection.json", result["limitations"])
        self.assertIn("invalid_termination.json", result["limitations"])

    def test_collection_termination_is_explicit_fallback_with_original_hash(self):
        (self.root / "termination.json").unlink()
        result = runtime_metrics(self.root)
        self.assertEqual(123.5, result["timing"]["gross_seconds"])
        self.assertIn("missing_termination.json", result["limitations"])
        changed = {**self.termination, "ended_at": "2026-01-01T00:03:00+00:00"}
        self.write("termination.json", changed)
        result = runtime_metrics(self.root)
        self.assertEqual(180, result["timing"]["gross_seconds"])
        self.assertIn("collection_termination_differs_from_original", result["limitations"])

    def test_duplicate_item_ids_count_once_and_unidentified_items_are_disclosed(self):
        self.trace([
            {"type": "item.completed", "item": {"type": "command_execution", "id": "same"}},
            {"type": "item.completed", "item": {"type": "command_execution", "id": "same"}},
            {"type": "item.completed", "item": {"type": "agent_message"}},
            {"type": "item.completed", "item": {"type": "mcp_tool_call", "id": "mcp"}},
            {"type": "item.completed", "item": {"type": "web_search", "id": "web"}},
        ])
        result = runtime_metrics(self.root)
        self.assertEqual(1, result["counts"]["command_executions"])
        self.assertEqual(3, result["counts"]["tool_calls"])
        self.assertEqual([{"id": "same", "first_line": 1, "line": 2}], result["counts"]["duplicate_item_completions"])
        self.assertIn("duplicate_completed_item_ids_counted_once", result["limitations"])
        self.assertIn("completed_items_without_ids_cannot_be_deduplicated", result["limitations"])

    def test_malformed_events_produce_partial_counts_with_line_numbers(self):
        (self.root / "trace.jsonl").write_text('\nnot-json\n[]\n{"type":1}\n{"type":"item.completed","item":[]}\n{"type":"turn.failed"}\n', encoding="utf-8")
        result = runtime_metrics(self.root)
        self.assertEqual([2, 3, 4, 5], result["counts"]["malformed_lines"])
        self.assertEqual(["turn.failed"], result["completion"]["terminal_events"])
        self.assertIn("trace_contains_malformed_events_counts_are_partial", result["limitations"])
        self.assertEqual(0, result["counts"]["agent_messages"])

    def test_invalid_utf8_trace_cannot_be_counted(self):
        (self.root / "trace.jsonl").write_bytes(b"\xff")
        result = runtime_metrics(self.root)
        self.assertIsNone(result["counts"]["events"])
        self.assertIn("trace_invalid_utf8", result["limitations"])

    def test_reconnection_timestamps_never_invent_a_complete_wait_ledger(self):
        self.trace([
            {"type": "error", "message": "Reconnecting... 1/3 (network error)", "timestamp": "2026-01-01T00:00:01Z"},
            {"type": "error", "message": "Reconnecting... waiting for network (Connection failed: error sending request)"},
            {"type": "error", "message": "Reconnecting... 4/3 (invalid numbered marker)"},
            {"type": "error", "message": "ordinary service error"},
            {"type": "turn.completed", "time": "2026-01-01T00:00:09Z"},
        ])
        result = runtime_metrics(self.root)
        self.assertEqual(2, result["reconnections"])
        self.assertEqual(2, result["counts"]["timestamped_events"])
        self.assertNotIn("effective_seconds", result["timing"])
        self.assertNotIn("effective_time_reason", result["timing"])

    def test_session_conflicts_are_not_resolved_by_picking_one(self):
        self.trace([{"type": "thread.started", "thread_id": "different"}])
        result = runtime_metrics(self.root)
        self.assertIsNone(result["sid"])
        self.assertIn("conflicting_session_ids", result["limitations"])
        self.write("collection.json", {})
        self.trace([{"type": "thread.started", "thread_id": "one"}, {"type": "thread.started", "thread_id": "two"}])
        self.assertIsNone(runtime_metrics(self.root)["sid"])

    def test_fixture_nature_and_invalid_input_membership_are_explicit(self):
        self.write("manifest.json", {"mode": "test_fixture", "model": "unsupported-shape", "inputs": [{"kind": 1}]})
        result = runtime_metrics(self.root)
        self.assertEqual("test_fixture", result["nature"])
        self.assertIsNone(result["requested_model"])
        self.assertIsNone(result["inputs_by_kind"])
        self.assertIn("input_kind_membership_unavailable", result["limitations"])
        self.write("manifest.json", {"mode": [], "inputs": []})
        self.assertEqual("other", runtime_metrics(self.root)["nature"])

    def test_invalid_timezone_and_reversed_time_are_not_durations(self):
        for started, ended in [(None, None), ("bad", "2026-01-01T00:00:02Z"),
                               ("2026-01-01T00:00:00", "2026-01-01T00:00:02Z"),
                               ("2026-01-01T00:00:02Z", "2026-01-01T00:00:01Z")]:
            with self.subTest(started=started, ended=ended):
                self.write("termination.json", {"started_at": started, "ended_at": ended})
                self.assertIsNone(runtime_metrics(self.root)["timing"]["gross_seconds"])
        self.write("termination.json", {"started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:00:00Z"})
        self.assertEqual(0, runtime_metrics(self.root)["timing"]["gross_seconds"])

    def test_symlink_file_or_parent_and_non_regular_file_are_rejected(self):
        (self.root / "manifest.json").unlink()
        external = self.root / "outside.json"
        external.write_text("{}", encoding="utf-8")
        (self.root / "manifest.json").symlink_to(external)
        with self.assertRaises(MetricsError):
            runtime_metrics(self.root)
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(MetricsError):
            runtime_metrics(alias)
        (self.root / "manifest.json").unlink()
        os.mkfifo(self.root / "manifest.json")
        with self.assertRaises(MetricsError):
            runtime_metrics(self.root)

    def test_size_limit_and_nonfinite_json_are_rejected_without_execution(self):
        p = self.root / "small"
        p.write_bytes(b"12345")
        with self.assertRaises(MetricsError):
            _bytes(p, 4)
        (self.root / "trace.jsonl").write_bytes(b'{"type":"turn.completed","usage":{"input_tokens":NaN}}\n')
        result = runtime_metrics(self.root)
        self.assertEqual([1], result["counts"]["malformed_lines"])
        self.assertEqual([], result["usage"])


if __name__ == "__main__":
    unittest.main()
