import json
import os
import unittest
from io import StringIO
from unittest.mock import patch

from FailureRecoveryManager.classes.FailureRecoveryManager import (
    FailureRecoveryManager as FRM,
)
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria

# Get the directory where this test file is located
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
WAL_PATH = os.path.join(TEST_DIR, "wal.log")
META_PATH = os.path.join(TEST_DIR, "last_checkpoint.json")


class TestRecover(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        FRM.buffer = []
        FRM.last_lsn = 0
        FRM.active_tx = []

        # Clean up test files
        for path in [WAL_PATH, META_PATH]:
            if os.path.exists(path):
                os.remove(path)

    def tearDown(self):
        """Clean up after each test method."""
        # Clean up test files
        for path in [WAL_PATH, META_PATH]:
            if os.path.exists(path):
                os.remove(path)

    def _make_exec(
        self,
        txid: int,
        query: str,
        table=None,
        key=None,
        old_value=None,
        new_value=None,
    ) -> ExecutionResult:
        """Helper to create ExecutionResult objects."""
        return ExecutionResult(
            transaction_id=txid,
            query=query,
            table=table,
            key=key,
            old_value=old_value,
            new_value=new_value,
        )

    @patch("sys.stdout", new_callable=StringIO)
    def test_undo_transaction_on_abort(self, mock_stdout):
        """Test that ABORT triggers undo for specific transaction."""
        # T1: BEGIN
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))

        # T1: UPDATE operation 1
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE employee SET salary = 5000 WHERE id = 1",
                table="employee",
                key=1,
                old_value=3000,
                new_value=5000,
            )
        )

        # T1: UPDATE operation 2
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE employee SET name = 'John' WHERE id = 1",
                table="employee",
                key=1,
                old_value="Alice",
                new_value="John",
            )
        )

        # Verify buffer has 3 records before abort
        self.assertEqual(len(FRM.buffer), 3)
        self.assertEqual(FRM.active_tx, [1])

        # T1: ABORT - this triggers recover
        FRM.write_log(self._make_exec(1, "ABORT"))

        # Check that UNDO messages were printed
        output = mock_stdout.getvalue()
        self.assertIn("UNDO: Restoring employee.1 from John to Alice", output)
        self.assertIn("UNDO: Restoring employee.1 from 5000 to 3000", output)

        # Transaction should be removed from active_tx
        self.assertEqual(FRM.active_tx, [])

        # Buffer should now have 4 records (START, OP, OP, ABORT)
        self.assertEqual(len(FRM.buffer), 4)

    @patch("sys.stdout", new_callable=StringIO)
    def test_undo_only_affects_target_transaction(self, mock_stdout):
        """Test that undo only affects the specified transaction."""
        # T1: BEGIN
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE employee SET salary = 5000 WHERE id = 1",
                table="employee",
                key=1,
                old_value=3000,
                new_value=5000,
            )
        )

        # T2: BEGIN
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                2,
                "UPDATE employee SET salary = 6000 WHERE id = 2",
                table="employee",
                key=2,
                old_value=4000,
                new_value=6000,
            )
        )

        # T1: ABORT (should only undo T1)
        FRM.write_log(self._make_exec(1, "ABORT"))

        output = mock_stdout.getvalue()
        # Should undo T1
        self.assertIn("UNDO: Restoring employee.1 from 5000 to 3000", output)
        # Should NOT undo T2
        self.assertNotIn("UNDO: Restoring employee.2", output)

        # T2 should still be active
        self.assertEqual(FRM.active_tx, [2])

    @patch("sys.stdout", new_callable=StringIO)
    @patch(
        "FailureRecoveryManager.classes.FailureRecoveryManager.FailureRecoveryManager._append_json_line"
    )
    @patch(
        "FailureRecoveryManager.classes.FailureRecoveryManager.FailureRecoveryManager._dump_json_file"
    )
    def test_crash_recovery_redo_committed_undo_uncommitted(
        self, mock_dump_json, mock_append_json, mock_stdout
    ):
        """Test crash recovery: redo committed, undo uncommitted transactions."""
        import builtins

        # Override mocks to write to test directory
        def append_to_test_dir(path, payload):
            test_path = os.path.join(TEST_DIR, os.path.basename(path))
            line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            with builtins.open(test_path, "a", encoding="utf-8") as f:
                f.write(line)

        def dump_to_test_dir(path, payload):
            test_path = os.path.join(TEST_DIR, os.path.basename(path))
            with builtins.open(test_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        mock_append_json.side_effect = append_to_test_dir
        mock_dump_json.side_effect = dump_to_test_dir

        # Correct ARIES scenario:
        # T1: BEGIN -> OPERATION (active at checkpoint)
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE employee SET salary = 5000 WHERE id = 1",
                table="employee",
                key=1,
                old_value=3000,
                new_value=5000,
            )
        )

        # Save checkpoint (LSN will be 3, T1 is active)
        FRM._save_checkpoint()

        # After checkpoint:
        # T1: COMMIT
        FRM.write_log(self._make_exec(1, "COMMIT"))

        # T2: BEGIN -> OPERATION (starts after checkpoint, no commit = crash)
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                2,
                "UPDATE employee SET salary = 7000 WHERE id = 2",
                table="employee",
                key=2,
                old_value=4000,
                new_value=7000,
            )
        )

        # Flush the post-checkpoint logs to disk manually
        for rec in FRM.buffer:
            append_to_test_dir("wal.log", rec.to_dict())

        # Simulate crash - clear in-memory state
        FRM.buffer = []
        FRM.active_tx = []
        FRM.last_lsn = 0

        # Patch the file operations to use test directory
        original_open = builtins.open

        def patched_open(path, *args, **kwargs):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            elif "last_checkpoint.json" in path and not path.startswith(TEST_DIR):
                path = META_PATH
            return original_open(path, *args, **kwargs)

        original_exists = os.path.exists

        def patched_exists(path):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            elif "last_checkpoint.json" in path and not path.startswith(TEST_DIR):
                path = META_PATH
            return original_exists(path)

        with patch("builtins.open", side_effect=patched_open):
            with patch("os.path.exists", side_effect=patched_exists):
                # Perform crash recovery
                FRM._crash_recovery()

        output = mock_stdout.getvalue()

        # At checkpoint (LSN 3): T1 was active (START, OP)
        # After checkpoint: T1 COMMIT (LSN 4), T2 START (LSN 5), T2 OP (LSN 6)

        # REDO phase: Replay T1 COMMIT and T2 operations after checkpoint
        # Note: T1's operation was before checkpoint, only T2's operation is after
        self.assertIn("REDO: Applying employee.2 = 7000", output)

        # T1's operation happened before checkpoint so it won't be redone
        # (it would have been flushed at checkpoint time)

        # UNDO phase: T2 is uncommitted (was active at checkpoint and never committed)
        # T1 was active at checkpoint but committed after, so no undo
        self.assertIn("UNDO: Restoring employee.2 from 7000 to 4000", output)

        # Should NOT undo T1 since it committed
        self.assertNotIn("UNDO: Restoring employee.1", output)

        # After recovery: T1 was in active_tx at checkpoint but committed, so removed
        # T2 was started after checkpoint but never committed, so still active
        self.assertEqual(FRM.active_tx, [2])

    def test_recover_criteria_transaction_id(self):
        """Test that RecoverCriteria with transaction_id triggers transaction undo."""
        # Add some operations for T1
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE employee SET salary = 5000 WHERE id = 1",
                table="employee",
                key=1,
                old_value=3000,
                new_value=5000,
            )
        )

        # Manually call recover with transaction_id
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            criteria = RecoverCriteria(transaction_id=1)
            FRM.recover(criteria)

            output = mock_stdout.getvalue()
            self.assertIn("UNDO: Restoring employee.1 from 5000 to 3000", output)

    def test_read_all_logs_from_disk(self):
        """Test reading logs from disk."""
        # Create a mock WAL file
        logs_data = [
            {
                "lsn": 1,
                "txid": 1,
                "log_type": "START",
            },
            {
                "lsn": 2,
                "txid": 1,
                "log_type": "OPERATION",
                "table": "employee",
                "key": 1,
                "old_value": 3000,
                "new_value": 5000,
            },
            {
                "lsn": 3,
                "txid": 1,
                "log_type": "COMMIT",
            },
        ]

        # Write mock data to test WAL file
        with open(WAL_PATH, "w", encoding="utf-8") as f:
            for log in logs_data:
                f.write(json.dumps(log, separators=(",", ":")) + "\n")

        # Patch path to use test directory
        with patch("os.path.exists", return_value=True):
            original_open = open

            def patched_open(path, *args, **kwargs):
                if "wal.log" in path and not path.startswith(TEST_DIR):
                    path = WAL_PATH
                return original_open(path, *args, **kwargs)

            with patch("builtins.open", side_effect=patched_open):
                logs = FRM._read_all_logs_from_disk()

        # Verify we read all logs correctly
        self.assertEqual(len(logs), 3)
        self.assertEqual(logs[0].lsn, 1)
        self.assertEqual(logs[0].log_type.value, "START")
        self.assertEqual(logs[1].lsn, 2)
        self.assertEqual(logs[1].log_type.value, "OPERATION")
        self.assertEqual(logs[1].table, "employee")
        self.assertEqual(logs[1].old_value, 3000)
        self.assertEqual(logs[1].new_value, 5000)
        self.assertEqual(logs[2].lsn, 3)
        self.assertEqual(logs[2].log_type.value, "COMMIT")

    def test_timestamp_based_recovery(self):
        """Test timestamp-based recovery to restore system to a specific point in time."""
        from datetime import datetime, timedelta

        # Create a scenario with operations at different times
        base_time = datetime(2024, 1, 1, 12, 0, 0)

        # T1: BEGIN and OPERATION at time T
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time
        FRM.write_log(
            self._make_exec(
                1,
                "UPDATE accounts SET balance = 5000 WHERE id = 1",
                table="accounts",
                key=1,
                old_value=3000,
                new_value=5000,
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=10)

        # T1: COMMIT at time T+20s
        FRM.write_log(self._make_exec(1, "COMMIT"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=20)

        # T3: BEGIN at T+12s (before T1 commits) but never commits
        FRM.write_log(self._make_exec(3, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=12)
        FRM.write_log(
            self._make_exec(
                3,
                "UPDATE accounts SET balance = 9000 WHERE id = 3",
                table="accounts",
                key=3,
                old_value=8000,
                new_value=9000,
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=15)

        # Target recovery point: T+25s (after T1 committed, T3 still active)
        target_timestamp = base_time + timedelta(seconds=25)

        # T2: BEGIN and OPERATION at time T+30s (after target)
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=30)
        FRM.write_log(
            self._make_exec(
                2,
                "UPDATE accounts SET balance = 7000 WHERE id = 2",
                table="accounts",
                key=2,
                old_value=4000,
                new_value=7000,
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=35)

        # T2: COMMIT at time T+40s (after target)
        FRM.write_log(self._make_exec(2, "COMMIT"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=40)

        # Write all logs directly to test directory
        for rec in FRM.buffer:
            line = json.dumps(rec.to_dict(), separators=(",", ":")) + "\n"
            with open(WAL_PATH, "a", encoding="utf-8") as f:
                f.write(line)

        # Clear in-memory state
        FRM.buffer = []
        FRM.active_tx = []

        # Perform timestamp-based recovery
        original_open = open

        def patched_open(path, *args, **kwargs):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            return original_open(path, *args, **kwargs)

        original_exists = os.path.exists

        def patched_exists(path):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            return original_exists(path)

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with patch("builtins.open", side_effect=patched_open):
                with patch("os.path.exists", side_effect=patched_exists):
                    criteria = RecoverCriteria(timestamp=target_timestamp)
                    FRM.recover(criteria)

            output = mock_stdout.getvalue()

        # Verify recovery output
        # T1 committed before timestamp, should be redone
        self.assertIn("REDO: Applying accounts.1 = 5000 (T1)", output)

        # T2 operations happened after timestamp, should be undone
        self.assertIn("UNDO: Restoring accounts.2 from 7000 to 4000 (T2)", output)

        # T3 started before timestamp but never committed, should be undone
        self.assertIn(
            "UNDO: Restoring accounts.3 from 9000 to 8000 (T3 uncommitted)", output
        )

        # After recovery, T3 should be active (started before timestamp but never committed)
        self.assertEqual(FRM.active_tx, [3])

        # Clean up
        for file in [WAL_PATH, META_PATH]:
            if os.path.exists(file):
                os.remove(file)


if __name__ == "__main__":
    unittest.main()
