import json
import os
import unittest
from datetime import datetime, timedelta
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


class TestRecoverSpecification(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        FRM.buffer = []
        FRM.last_lsn = 0
        FRM.active_tx = []

        # Clean up test files in test directory
        for path in [WAL_PATH, META_PATH]:
            if os.path.exists(path):
                os.remove(path)

        # Also clean up files in current directory (might be left from other tests)
        for filename in ["wal.log", "last_checkpoint.json"]:
            if os.path.exists(filename):
                os.remove(filename)

    def tearDown(self):
        """Clean up after each test method."""
        for path in [WAL_PATH, META_PATH]:
            if os.path.exists(path):
                os.remove(path)

        # Also clean up files in current directory
        for filename in ["wal.log", "last_checkpoint.json"]:
            if os.path.exists(filename):
                os.remove(filename)

        # Reset FRM state completely
        FRM.buffer = []
        FRM.last_lsn = 0
        FRM.active_tx = []

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
    def test_recover_until_transaction_in_buffer(self, mock_stdout):
        """Test recovering until a transaction when logs are in buffer (not yet flushed)."""
        # Build scenario:
        # T1: BEGIN -> OP -> COMMIT
        # T2: BEGIN -> OP (target)
        # T3: BEGIN -> OP

        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1, "UPDATE", table="accounts", key=1, old_value=100, new_value=200
            )
        )
        FRM.write_log(self._make_exec(1, "COMMIT"))

        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))  # Target transaction
        FRM.write_log(
            self._make_exec(
                2, "UPDATE", table="accounts", key=1, old_value=200, new_value=300
            )
        )

        FRM.write_log(self._make_exec(3, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                3, "UPDATE", table="accounts", key=1, old_value=300, new_value=400
            )
        )

        # Recover until T2 (should undo T3 and T2 operations, stop at T2 START)
        FRM.recover(RecoverCriteria(transaction_id=2))

        output = mock_stdout.getvalue()

        # Should undo T3's operation
        self.assertIn("UNDO LSN 7: accounts.1 from 400 to 300 (T3)", output)

        # Should undo T2's operation
        self.assertIn("UNDO LSN 5: accounts.1 from 300 to 200 (T2)", output)

        # Should NOT undo T1's operation (before T2 START)
        self.assertNotIn("UNDO LSN 2", output)

        # Should find T2 START
        self.assertIn("Reached transaction 2 START at LSN 4", output)

        # Should report 2 operations undone
        self.assertIn("Undoing 2 operations", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_until_transaction_from_disk(self, mock_stdout):
        """Test recovering until a transaction when logs are on disk."""
        import builtins

        # Build scenario and flush to disk
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1, "UPDATE", table="balance", key=1, old_value=1000, new_value=1500
            )
        )
        FRM.write_log(self._make_exec(1, "COMMIT"))

        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))  # Target
        FRM.write_log(
            self._make_exec(
                2, "UPDATE", table="balance", key=2, old_value=2000, new_value=2500
            )
        )
        FRM.write_log(self._make_exec(2, "COMMIT"))

        FRM.write_log(self._make_exec(3, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                3, "UPDATE", table="balance", key=3, old_value=3000, new_value=3500
            )
        )

        # Write to disk
        for rec in FRM.buffer:
            line = json.dumps(rec.to_dict(), separators=(",", ":")) + "\n"
            with builtins.open(WAL_PATH, "a", encoding="utf-8") as f:
                f.write(line)

        # Clear buffer to force reading from disk
        FRM.buffer = []

        # Patch file operations
        original_open = builtins.open
        original_exists = os.path.exists

        def patched_open(path, *args, **kwargs):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            return original_open(path, *args, **kwargs)

        def patched_exists(path):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            return original_exists(path)

        with patch("builtins.open", side_effect=patched_open):
            with patch("os.path.exists", side_effect=patched_exists):
                FRM.recover(RecoverCriteria(transaction_id=2))

        output = mock_stdout.getvalue()

        # Should undo T3's operation (after T2 START)
        self.assertIn("UNDO LSN 8: balance.3 from 3500 to 3000 (T3)", output)

        # Should undo T2's operations (T2 itself)
        self.assertIn("UNDO LSN 5: balance.2 from 2500 to 2000 (T2)", output)

        # Should NOT undo T1 (before T2 START)
        self.assertNotIn("balance.1 from 1500 to 1000", output)

        # Should report finding T2 START
        self.assertIn("Reached transaction 2 START at LSN 4", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_until_transaction_removes_all_after(self, mock_stdout):
        """Test that all transactions after target are removed, not just target."""
        # Scenario:
        # T1: complete
        # T2: target (START only, no operations)
        # T3: has operations
        # T4: has operations

        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(1, "UPDATE", table="x", key=1, old_value=1, new_value=2)
        )
        FRM.write_log(self._make_exec(1, "COMMIT"))

        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))  # Target (no ops)

        FRM.write_log(self._make_exec(3, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(3, "UPDATE", table="x", key=1, old_value=2, new_value=3)
        )

        FRM.write_log(self._make_exec(4, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(4, "UPDATE", table="x", key=1, old_value=3, new_value=4)
        )

        # Before recovery
        self.assertEqual(len(FRM.buffer), 8)
        self.assertIn(2, FRM.active_tx)
        self.assertIn(3, FRM.active_tx)
        self.assertIn(4, FRM.active_tx)

        FRM.recover(RecoverCriteria(transaction_id=2))

        output = mock_stdout.getvalue()

        # Should undo both T3 and T4 operations
        self.assertIn("UNDO LSN 8: x.1 from 4 to 3 (T4)", output)
        self.assertIn("UNDO LSN 6: x.1 from 3 to 2 (T3)", output)

        # Should report 2 operations undone
        self.assertIn("Undoing 2 operations", output)

        # Active transactions should be updated (T3 and T4 removed)
        self.assertNotIn(3, FRM.active_tx)
        self.assertNotIn(4, FRM.active_tx)

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_until_timestamp_from_buffer(self, mock_stdout):
        """Test timestamp-based recovery with buffer."""
        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # T1: complete before target time
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time
        FRM.write_log(
            self._make_exec(
                1, "UPDATE", table="data", key=1, old_value=10, new_value=20
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=5)
        FRM.write_log(self._make_exec(1, "COMMIT"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=10)

        # Target timestamp: 10:00:15
        target_time = base_time + timedelta(seconds=15)

        # T2: after target time
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=20)
        FRM.write_log(
            self._make_exec(
                2, "UPDATE", table="data", key=1, old_value=20, new_value=30
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=25)

        # T3: after target time
        FRM.write_log(self._make_exec(3, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=30)
        FRM.write_log(
            self._make_exec(
                3, "UPDATE", table="data", key=1, old_value=30, new_value=40
            )
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=35)

        # Recover to target time
        FRM.recover(RecoverCriteria(timestamp=target_time))

        output = mock_stdout.getvalue()

        # Should undo T3 and T2 operations (after target)
        self.assertIn("data.1 from 40 to 30 (T3", output)
        self.assertIn("data.1 from 30 to 20 (T2", output)

        # Should NOT undo T1 (before target)
        self.assertNotIn("UNDO LSN 2", output)

        # Should report reaching target timestamp
        self.assertIn("Reached target timestamp at LSN 3", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_until_timestamp_mid_transaction(self, mock_stdout):
        """Test timestamp recovery where target is in middle of a transaction."""
        base_time = datetime(2024, 1, 1, 12, 0, 0)

        # T1: starts before target, commits after target
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.buffer[-1].timestamp = base_time
        FRM.write_log(
            self._make_exec(1, "UPDATE", table="x", key=1, old_value=100, new_value=200)
        )
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=5)

        # Target: 12:00:10 (after T1's operation, before T1's commit)
        target_time = base_time + timedelta(seconds=10)

        FRM.write_log(self._make_exec(1, "COMMIT"))
        FRM.buffer[-1].timestamp = base_time + timedelta(seconds=15)

        # Recover to target
        FRM.recover(RecoverCriteria(timestamp=target_time))

        output = mock_stdout.getvalue()

        # T1's COMMIT happened after target, but operation was before
        # So no operations should be undone (operation was at 12:00:05, before target)
        self.assertIn("Undoing 0 operations", output)

        # T1 should be in active transactions (started before target, not committed before target)
        self.assertEqual(FRM.active_tx, [1])

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_transaction_not_found(self, mock_stdout):
        """Test recovery when target transaction doesn't exist in logs."""
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(1, "UPDATE", table="t", key=1, old_value=1, new_value=2)
        )
        FRM.write_log(self._make_exec(1, "COMMIT"))

        # Try to recover to non-existent transaction
        FRM.recover(RecoverCriteria(transaction_id=999))

        output = mock_stdout.getvalue()

        # Should show warning
        self.assertIn("Transaction 999 START not found in logs", output)

        # Should still undo all operations (went to beginning without finding target)
        self.assertIn("UNDO LSN 2: t.1 from 2 to 1 (T1)", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_crash_recovery_aries(self, mock_stdout):
        """Test full crash recovery with ARIES algorithm."""
        import builtins

        # Build scenario and create checkpoint
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1, "UPDATE", table="acc", key=1, old_value=100, new_value=200
            )
        )

        # Save checkpoint (T1 active)
        FRM._save_checkpoint()

        # After checkpoint
        FRM.write_log(self._make_exec(1, "COMMIT"))
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                2, "UPDATE", table="acc", key=2, old_value=200, new_value=300
            )
        )
        # T2 never commits (crash)

        # Flush to disk
        for rec in FRM.buffer:
            line = json.dumps(rec.to_dict(), separators=(",", ":")) + "\n"
            with builtins.open(WAL_PATH, "a", encoding="utf-8") as f:
                f.write(line)

        # Simulate crash
        FRM.buffer = []
        FRM.active_tx = []
        FRM.last_lsn = 0

        # Patch file operations
        original_open = builtins.open
        original_exists = os.path.exists

        def patched_open(path, *args, **kwargs):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            elif "last_checkpoint.json" in path and not path.startswith(TEST_DIR):
                path = META_PATH
            return original_open(path, *args, **kwargs)

        def patched_exists(path):
            if "wal.log" in path and not path.startswith(TEST_DIR):
                path = WAL_PATH
            elif "last_checkpoint.json" in path and not path.startswith(TEST_DIR):
                path = META_PATH
            return original_exists(path)

        with patch("builtins.open", side_effect=patched_open):
            with patch("os.path.exists", side_effect=patched_exists):
                # Perform crash recovery (no criteria)
                FRM.recover(RecoverCriteria())

        output = mock_stdout.getvalue()

        # Should show ARIES phases
        self.assertIn("=== Crash Recovery (ARIES) ===", output)
        self.assertIn("Phase 1: ANALYSIS", output)
        self.assertIn("Phase 2: REDO", output)
        self.assertIn("Phase 3: UNDO", output)

        # Should identify T1 as committed, T2 as active
        self.assertIn("T1 committed", output)
        self.assertIn("T2 started", output)

        # Should REDO T2's operation
        self.assertIn("REDO LSN 6: acc.2 = 300", output)

        # Should UNDO T2's operation (uncommitted)
        self.assertIn("UNDO LSN 6: acc.2 from 300 to 200 (T2)", output)

        # T2 should remain in active list
        self.assertEqual(FRM.active_tx, [2])

    @patch("sys.stdout", new_callable=StringIO)
    def test_empty_logs(self, mock_stdout):
        """Test recovery with empty logs."""
        # No logs in buffer
        FRM.buffer = []

        # Try to recover
        FRM.recover(RecoverCriteria(transaction_id=1))

        output = mock_stdout.getvalue()

        # Should handle gracefully
        self.assertIn("Starting backward recovery from LSN 0", output)
        self.assertIn("Undoing 0 operations", output)

    @patch("sys.stdout", new_callable=StringIO)
    def test_recover_preserves_committed_before_target(self, mock_stdout):
        """Test that transactions committed before target are preserved."""
        # T1: complete
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                1, "UPDATE", table="balance", key=1, old_value=0, new_value=100
            )
        )
        FRM.write_log(self._make_exec(1, "COMMIT"))

        # T2: target
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        FRM.write_log(
            self._make_exec(
                2, "UPDATE", table="balance", key=1, old_value=100, new_value=200
            )
        )

        # Recover until T2
        FRM.recover(RecoverCriteria(transaction_id=2))

        output = mock_stdout.getvalue()

        # T1's operation should NOT be undone
        self.assertNotIn("balance.1 from 100 to 0", output)

        # Only T2's operation should be undone
        self.assertIn("UNDO LSN 5: balance.1 from 200 to 100 (T2)", output)
        self.assertIn("Undoing 1 operations", output)


if __name__ == "__main__":
    unittest.main()
