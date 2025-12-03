import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Add project root to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ==========================================
# MOCK ENVIRONMENT SETUP
# ==========================================
mock_qp = MagicMock()
sys.modules['QueryProcessor'] = mock_qp
sys.modules['QueryProcessor.interfaces'] = MagicMock()

class AbstractFailureRecoveryManager:
    pass
sys.modules['QueryProcessor.interfaces'].AbstractFailureRecoveryManager = AbstractFailureRecoveryManager

sys.modules['QueryProcessor.interfaces.concurrency_control_interface'] = MagicMock()
sys.modules['QueryProcessor.models'] = MagicMock()

sys.modules['QueryOptimization'] = MagicMock()
sys.modules['QueryOptimization.src'] = MagicMock()
sys.modules['QueryOptimization.src.optimizer'] = MagicMock()
sys.modules['QueryOptimization.src.optimizer.optimization_engine'] = MagicMock()
sys.modules['QueryOptimization.src.parser'] = MagicMock()
sys.modules['QueryOptimization.src.parser.parser'] = MagicMock()
sys.modules['QueryOptimization.src.tree'] = MagicMock()
sys.modules['QueryOptimization.src.tree.query_tree'] = MagicMock()
sys.modules['QueryOptimization.src.tree.nodes'] = MagicMock()

sys.modules['ConcurrencyControl'] = MagicMock()
sys.modules['ConcurrencyControl.src'] = MagicMock()
sys.modules['ConcurrencyControl.src.concurrency_control_manager'] = MagicMock()
sys.modules['ConcurrencyControl.src.lock_based_concurrency_control_manager'] = MagicMock()
sys.modules['ConcurrencyControl.src.row_action'] = MagicMock()
sys.modules['ConcurrencyControl.src.transaction_status'] = MagicMock()
sys.modules['ConcurrencyControl.src.concurrency_response'] = MagicMock()

sys.modules['StorageManager'] = MagicMock()
sys.modules['StorageManager.classes'] = MagicMock()
sys.modules['StorageManager.classes.API'] = MagicMock()

class ExecutionResult:
    def __init__(self, success=True, transaction_id=None, query=None, error=None):
        self.success = success
        self.transaction_id = transaction_id
        self.query = query
        self.error = error

sys.modules['QueryProcessor.models'].ExecutionResult = ExecutionResult

mock_frm_pkg = MagicMock()
sys.modules['FailureRecoveryManager'] = mock_frm_pkg
sys.modules['FailureRecoveryManager.FailureRecoveryManager'] = MagicMock()
sys.modules['FailureRecoveryManager.FailureRecoveryManager.classes'] = MagicMock()
sys.modules['FailureRecoveryManager.FailureRecoveryManager.classes.FailureRecoveryManager'] = MagicMock()
sys.modules['FailureRecoveryManager.FailureRecoveryManager.types'] = MagicMock()
sys.modules['FailureRecoveryManager.FailureRecoveryManager.types.RecoverCriteria'] = MagicMock()

MockBackendFRM = sys.modules['FailureRecoveryManager.FailureRecoveryManager.classes.FailureRecoveryManager'].FailureRecoveryManager
MockBackendFRM.buffer = []

# ==========================================
# IMPORT TARGET
# ==========================================
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager


class TestIntegratedFailureRecoveryManager(unittest.TestCase):
    
    def setUp(self):
        MockBackendFRM.reset_mock()
        MockBackendFRM.recover.side_effect = None
        MockBackendFRM.buffer = []
        
        self.manager = IntegratedFailureRecoveryManager()
        self.manager.setVerbose(False)

    def test_initialization(self):
        self.assertFalse(self.manager.verbose)
        self.assertEqual(self.manager.tag, "\033[93m[FRM]\033[0m")

    def test_log_transaction_start(self):
        tid = 123
        self.manager.log_transaction_start(tid)
        
        MockBackendFRM.write_log.assert_called_once()
        
        args, _ = MockBackendFRM.write_log.call_args
        exec_result = args[0]
        
        self.assertIsInstance(exec_result, ExecutionResult)
        self.assertEqual(exec_result.transaction_id, tid)
        self.assertEqual(exec_result.query, "BEGIN TRANSACTION")
        self.assertTrue(exec_result.success)

    def test_log_transaction_commit_triggers_checkpoint_when_buffer_exceeds_threshold(self):
        tid = 456
        # Arrange: buffer must exceed threshold for checkpoint to trigger
        MockBackendFRM.buffer = [MagicMock() for _ in range(12)]

        self.manager.log_transaction_commit(tid)

        # Verify write_log was called
        MockBackendFRM.write_log.assert_called_once()
        args, _ = MockBackendFRM.write_log.call_args
        self.assertEqual(args[0].query, "COMMIT")

        # Verify checkpoint was saved (only because buffer > 10)
        MockBackendFRM._save_checkpoint.assert_called_once()

    def test_log_transaction_abort(self):
        tid = 789
        self.manager.log_transaction_abort(tid)
        
        MockBackendFRM.write_log.assert_called_once()
        args, _ = MockBackendFRM.write_log.call_args
        self.assertEqual(args[0].query, "ABORT")

    def test_write_log_generic(self):
        exec_result = ExecutionResult(transaction_id=999, query="UPDATE table SET a=1")
        table = "users"
        key = 1
        old_val = {"name": "Alice"}
        new_val = {"name": "Bob"}
        
        self.manager.write_log(exec_result, table=table, key=key, old_value=old_val, new_value=new_val)
        
        MockBackendFRM.write_log.assert_called_once_with(
            exec_result, table, key, old_val, new_val
        )

    def test_recover_success(self):
        result = self.manager.recover()
        
        self.assertTrue(result)
        MockBackendFRM.recover.assert_called_once()

    def test_recover_failure(self):
        MockBackendFRM.recover.side_effect = Exception("Disk error")
        
        result = self.manager.recover()
        
        self.assertFalse(result)
        MockBackendFRM.recover.assert_called_once()

class TestWriteLogCheckpointPolicy(unittest.TestCase):
    CHECKPOINT_THRESHOLD = 10  # Expected threshold per line 25 comment

    def setUp(self):
        # Reset mock state
        MockBackendFRM.reset_mock()
        MockBackendFRM.buffer = []
        MockBackendFRM._save_checkpoint.reset_mock()
        MockBackendFRM.write_log.reset_mock()

        self.manager = IntegratedFailureRecoveryManager()
        self.manager.setVerbose(False)

    def test_commit_below_threshold_should_not_checkpoint(self):
        """
        Scenario 1: buffer size < THRESHOLD with COMMIT
        Expected: _save_checkpoint should NOT be called
        """
        # Arrange: buffer with 5 items (below threshold of 10)
        MockBackendFRM.buffer = [MagicMock() for _ in range(5)]

        exec_result = ExecutionResult(
            success=True,
            transaction_id=100,
            query="COMMIT"
        )

        # Act
        self.manager.write_log(exec_result)

        # Assert: checkpoint should NOT be triggered
        MockBackendFRM._save_checkpoint.assert_not_called()

    def test_commit_at_or_above_threshold_should_checkpoint(self):
        """
        Scenario 2: buffer size >= THRESHOLD with COMMIT
        Expected: _save_checkpoint should be called exactly once
        """
        # Arrange: buffer with 12 items (above threshold of 10)
        MockBackendFRM.buffer = [MagicMock() for _ in range(12)]

        exec_result = ExecutionResult(
            success=True,
            transaction_id=200,
            query="COMMIT"
        )

        # Act
        self.manager.write_log(exec_result)

        # Assert: checkpoint SHOULD be triggered exactly once
        MockBackendFRM._save_checkpoint.assert_called_once()

    def test_non_commit_queries_never_trigger_checkpoint(self):
        """
        Non-COMMIT queries should NEVER trigger _save_checkpoint regardless
        of buffer size.
        """
        non_commit_queries = [
            "BEGIN TRANSACTION",
            "UPDATE users SET name='Alice'",
            "INSERT INTO users VALUES (1, 'Bob')",
            "DELETE FROM users WHERE id=1",
            "SELECT * FROM users",
        ]

        for query in non_commit_queries:
            MockBackendFRM._save_checkpoint.reset_mock()

            MockBackendFRM.buffer = [MagicMock() for _ in range(15)]

            exec_result = ExecutionResult(
                success=True,
                transaction_id=300,
                query=query
            )

            # Act
            self.manager.write_log(exec_result)

            # Assert: checkpoint should NOT be triggered for non-COMMIT
            MockBackendFRM._save_checkpoint.assert_not_called(), \
                f"_save_checkpoint was unexpectedly called for query: {query}"


if __name__ == '__main__':
    unittest.main()