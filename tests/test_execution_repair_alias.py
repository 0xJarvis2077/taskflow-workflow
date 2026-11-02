"""Closed repair aliases carry ancestry, never an independent quality verdict."""

import copy
import unittest
from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_store import ExecutionStore
from tests import test_execution_store as store_fixtures


def make_alias_fixture(owner, *, closed=True, mutate=None):
    case = store_fixtures.ExecutionStoreTests("test_actual_material_repair_and_independent_package_recheck_close")
    case.setUp()
    owner.addCleanup(case.doCleanups)
    case.repaired()
    case.review("repair-whole", "candidate-b")
    if closed:
        case.store.close_defect("defect-a", "repair-whole", evidence=case.refs("original independent closure"))
    repaired = case.store.get_candidate("candidate-b")
    files = copy.deepcopy(repaired["files"])
    if mutate:
        mutate(case, files)
    case.store.register_candidate("item-a", "baseline-copy", **{key: repaired[key] for key in (
        "input_digest", "content_digest", "evaluation_digest")}, files=files, evidence=case.refs())
    author = case.complete("author", "baseline-copy")
    current_files = copy.deepcopy(files)
    current_files[0].update(author["evidence"][0])
    case.candidate("item-a", "new-current", parent_id="baseline-copy", author_attempt=author["attempt_id"], files=current_files)
    case.review("new-current-whole", "new-current")
    return case


class RepairAliasTests(unittest.TestCase):
    def test_closed_repaired_copy_and_its_new_child_use_only_their_own_whole(self):
        case = make_alias_fixture(self)
        before = case.store.get_defect("defect-a")
        review = case.store.quality_gate("new-current")
        self.assertEqual(review, case.store.quality_gate("new-current", require_repair_alias=True))
        self.assertEqual("new-current-whole", review["review_id"])
        self.assertEqual([], review.get("covered_defect_ids", []))
        self.assertEqual(before, case.store.get_defect("defect-a"))
        with self.assertRaisesRegex(TaskflowError, "missing/stale"):
            case.store.quality_gate("baseline-copy")
        case.review("baseline-own-whole", "baseline-copy")
        self.assertEqual("baseline-own-whole", case.store.quality_gate("baseline-copy")["review_id"])

    def test_unclosed_and_old_bad_byte_copy_still_reject(self):
        case = make_alias_fixture(self, closed=False)
        with self.assertRaisesRegex(TaskflowError, "unclosed"):
            case.store.quality_gate("new-current")
        case.store.close_defect("defect-a", "repair-whole", evidence=case.refs())
        original = case.store.get_candidate("candidate-a")
        case.store.register_candidate("item-a", "bad-copy", **{key: original[key] for key in (
            "input_digest", "content_digest", "evaluation_digest")}, files=original["files"], evidence=case.refs())
        case.review("bad-copy-whole", "bad-copy")
        with self.assertRaisesRegex(TaskflowError, "different/later"):
            case.store.quality_gate("bad-copy")

    def test_file_membership_and_metadata_must_match_in_addition_to_three_digests(self):
        changes = {
            "id": lambda c, f: f[0].update(id="other-member"),
            "path": lambda c, f: f[0].update(package_path="draft/input/other.md"),
            "role": lambda c, f: f[0].update(role="teacher"),
            "size": lambda c, f: f[0].update(size=0),
            "sha": lambda c, f: f[0].update(c.refs("different actual bytes")[0]),
            "extra": lambda c, f: f.append({**c.refs()[0], "package_path": "extra.md", "role": "teacher"}),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                case = make_alias_fixture(self, mutate=change)
                with self.assertRaisesRegex(TaskflowError, "file members differ"):
                    case.store.quality_gate("new-current")

    def test_all_three_digests_and_same_task_are_required_for_alias_ancestry(self):
        case = make_alias_fixture(self)
        original = case.store.get_candidate("candidate-a")
        repaired = case.store.get_candidate("candidate-b")
        for key in ("input_digest", "content_digest", "evaluation_digest"):
            identity = "changed-" + key
            digests = {k: repaired[k] for k in ("input_digest", "content_digest", "evaluation_digest")}
            digests[key] = original[key]
            author = case.complete("author", "candidate-a")
            case.store.register_candidate("item-a", identity, **digests, parent_id="candidate-a",
                                          author_attempt=author["attempt_id"], files=original["files"], evidence=case.refs())
            case.review(identity + "-whole", identity)
            with self.subTest(key=key), self.assertRaisesRegex(TaskflowError, "different/later"):
                case.store.quality_gate(identity)
        case.store.register_candidate("foreign-task", "foreign-copy", **{key: repaired[key] for key in (
            "input_digest", "content_digest", "evaluation_digest")}, files=repaired["files"], evidence=case.refs())
        with case.store._db() as db:
            self.assertNotIn("foreign-copy", ExecutionStore._quality_scope(db, "new-current"))
        author = case.complete("author", "new-current")
        with self.assertRaisesRegex(TaskflowError, "another task"):
            case.candidate("foreign-task", "wrong-parent", parent_id="new-current", author_attempt=author["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "no closed repair through"):
            case.store.quality_gate("candidate-b", require_repair_alias=True)

    def test_original_candidate_repair_and_closure_sources_are_rechecked(self):
        case = make_alias_fixture(self)
        defect = case.store.get_defect("defect-a")
        paths = [Path(case.store.get_candidate("candidate-b")["files"][0]["path"]),
                 Path(case.store.get_candidate("baseline-copy")["evidence"][0]["path"]),
                 Path(defect["repair_evidence"][0]["path"]), Path(defect["closure_evidence"][0]["path"]),
                 Path(case.store.get_attempt(defect["author_attempt"])["evidence"][0]["path"]),
                 Path(case.store.get_review("repair-whole")["evidence"][0]["path"])]
        for path in paths:
            raw = path.read_bytes()
            path.write_bytes(raw + b"changed")
            try:
                with self.subTest(path=path.name), self.assertRaisesRegex(TaskflowError, "evidence changed"):
                    case.store.quality_gate("new-current")
            finally:
                path.write_bytes(raw)

    def test_late_closure_and_later_negative_cannot_be_washed_by_aliases(self):
        case = make_alias_fixture(self, closed=False)
        case.store.close_defect("defect-a", "repair-whole", evidence=case.refs())
        with self.assertRaisesRegex(TaskflowError, "closure postdates"):
            case.store.quality_gate("new-current")
        case.review("fresh-current-whole", "new-current")
        case.store.quality_gate("new-current")

        # A later conflict exercises deep archival-source replay. Use actual
        # sealed role runtimes rather than the metadata-only fixture above.
        from taskflow.execution_runner import _validate, _validated_collection
        from tests import test_execution_composite_quality as composite
        from tests import test_execution_historical_closure_freshness as historical

        value = historical.closure_fixture(self, whole=True)
        store = value["store"]
        self.assertEqual(value["accepted"], store.quality_gate(value["candidate"]))
        accepted = store.get_attempt(value["accepted"]["attempt_id"])
        later = composite.CompositeQualityTests().later_negative(value)
        rejected = store.get_attempt(later["attempt_id"])
        self.assertNotEqual(accepted["cli_sid"], rejected["cli_sid"])
        for attempt in (accepted, rejected):
            runtime = Path(attempt["runtime_path"])
            collected = _validated_collection(runtime, _validate(runtime))
            self.assertEqual("completed", attempt["state"])
            self.assertEqual(attempt["cli_sid"], collected["sid"])
            self.assertTrue(collected["technical_accepted"])
        with store._db() as db:
            self.assertGreater(ExecutionStore._review_time(db, later),
                               ExecutionStore._review_time(db, value["accepted"]))
        with self.assertRaisesRegex(TaskflowError, "later independent"):
            store.quality_gate(value["candidate"])


if __name__ == "__main__":
    unittest.main()
