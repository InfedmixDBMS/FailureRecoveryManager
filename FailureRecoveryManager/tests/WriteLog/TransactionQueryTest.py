import unittest

from FailureRecoveryManager.classes.FailureRecoveryManager import (
    FailureRecoveryManager as FRM,
)
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult


class TransactionQueryTest(unittest.TestCase):
    def setUp(self):
        FRM.buffer = []
        FRM.last_lsn = 0
        FRM.active_tx = []

    def test_transaction_lifecycle(self):
        # Transaction 123: BEGIN -> COMMIT
        begin_exec = ExecutionResult(
            message="Transaction started", transaction_id=123, query="BEGIN TRANSACTION"
        )

        commit_exec = ExecutionResult(
            message="Transaction committed", transaction_id=123, query="COMMIT"
        )

        # Transaction 124: BEGIN -> ABORT
        begin_exec1 = ExecutionResult(
            message="Transaction started", transaction_id=124, query="BEGIN TRANSACTION"
        )

        abort_exec = ExecutionResult(
            message="Transaction aborted", transaction_id=124, query="ABORT"
        )

        # Execute the transactions
        FRM.write_log(begin_exec)
        FRM.write_log(commit_exec)
        FRM.write_log(begin_exec1)
        FRM.write_log(abort_exec)

        # Verify buffer contents
        self.assertEqual(len(FRM.buffer), 4, "Buffer should contain 4 log records")

        # Check LSN sequence
        self.assertEqual(FRM.buffer[0].lsn, 1)
        self.assertEqual(FRM.buffer[1].lsn, 2)
        self.assertEqual(FRM.buffer[2].lsn, 3)
        self.assertEqual(FRM.buffer[3].lsn, 4)

        # Check transaction IDs
        self.assertEqual(FRM.buffer[0].txid, 123)
        self.assertEqual(FRM.buffer[1].txid, 123)
        self.assertEqual(FRM.buffer[2].txid, 124)
        self.assertEqual(FRM.buffer[3].txid, 124)

        # Check log types
        self.assertEqual(FRM.buffer[0].log_type.value, "START")
        self.assertEqual(FRM.buffer[1].log_type.value, "COMMIT")
        self.assertEqual(FRM.buffer[2].log_type.value, "START")
        self.assertEqual(FRM.buffer[3].log_type.value, "ABORT")

        # Verify string representations
        self.assertEqual(repr(FRM.buffer[0]), "1: <123, Start>")
        self.assertEqual(repr(FRM.buffer[1]), "2: <123, Commit>")
        self.assertEqual(repr(FRM.buffer[2]), "3: <124, Start>")
        self.assertEqual(repr(FRM.buffer[3]), "4: <124, Abort>")

        # Verify active_tx list (should be empty after commit and abort)
        self.assertEqual(FRM.active_tx, [], "No transactions should be active")

        # Verify last_lsn
        self.assertEqual(FRM.last_lsn, 4, "Last LSN should be 4")


if __name__ == "__main__":
    unittest.main()
