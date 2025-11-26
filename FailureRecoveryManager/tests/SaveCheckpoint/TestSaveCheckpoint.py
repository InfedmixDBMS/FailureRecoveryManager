import json
import os
import unittest
from unittest.mock import patch

from FailureRecoveryManager.classes.FailureRecoveryManager import (
    FailureRecoveryManager as FRM,
)
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult

# Get the directory where this test file is located
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
WAL_PATH = os.path.join(TEST_DIR, "wal.log")
META_PATH = os.path.join(TEST_DIR, "last_checkpoint.json")


class TestSaveCheckpoint(unittest.TestCase):
    def setUp(self):
        FRM.buffer = []
        FRM.last_lsn = 0
        FRM.active_tx = []

        for path in [WAL_PATH, META_PATH]:
            if os.path.exists(path):
                os.remove(path)

    def _make_exec(self, txid: int, query: str) -> ExecutionResult:
        return ExecutionResult(transaction_id=txid, query=query)

    @patch(
        "FailureRecoveryManager.classes.FailureRecoveryManager.FailureRecoveryManager._append_json_line"
    )
    @patch(
        "FailureRecoveryManager.classes.FailureRecoveryManager.FailureRecoveryManager._dump_json_file"
    )
    def test_save_checkpoint_flushes_buffer_and_writes_meta(
        self, mock_dump_json, mock_append_json
    ):
        # ------------------------------------------------------------------
        # Urutan log:
        #    1: T1 BEGIN
        #    2: T1 OPERATION
        #    3: T1 COMMIT
        #    4: T2 BEGIN
        #    5: T2 OPERATION

        # Override mock to write to test directory
        def append_to_test_dir(path, payload):
            test_path = os.path.join(TEST_DIR, os.path.basename(path))
            line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            with open(test_path, "a", encoding="utf-8") as f:
                f.write(line)

        def dump_to_test_dir(path, payload):
            test_path = os.path.join(TEST_DIR, os.path.basename(path))
            with open(test_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        mock_append_json.side_effect = append_to_test_dir
        mock_dump_json.side_effect = dump_to_test_dir

        # T1: BEGIN
        FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
        # T1: OPERATION
        FRM.write_log(
            self._make_exec(1, "UPDATE employee SET salary = 2000 WHERE id = 1")
        )
        # T1: COMMIT
        FRM.write_log(self._make_exec(1, "COMMIT"))

        # T2: BEGIN
        FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
        # T2: OPERATION
        FRM.write_log(
            self._make_exec(2, "UPDATE employee SET salary = 3000 WHERE id = 2")
        )

        self.assertEqual(FRM.last_lsn, 5)
        # active_tx harus hanya berisi T2
        self.assertEqual(FRM.active_tx, [2])
        self.assertEqual(len(FRM.buffer), 5)

        FRM._save_checkpoint()

        # Setelah checkpoint:
        # buffer harus kosong & last_lsn harus = 6
        self.assertEqual(FRM.buffer, [])
        self.assertEqual(FRM.last_lsn, 6)
        # active_tx T2 tetap aktif
        self.assertEqual(FRM.active_tx, [2])

        # cek wal.log
        self.assertTrue(
            os.path.exists(WAL_PATH), "wal.log should be created by _save_checkpoint"
        )

        with open(WAL_PATH, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f.readlines()]

        # Harus ada 6 baris: 5 log dari buffer + 1 CHECKPOINT
        self.assertEqual(len(lines), 6)

        # Cek field penting
        # 1: <T1, START>
        self.assertEqual(lines[0]["lsn"], 1)
        self.assertEqual(lines[0]["txid"], 1)
        self.assertEqual(lines[0]["log_type"], "START")

        # 3: <T1, COMMIT>
        self.assertEqual(lines[2]["lsn"], 3)
        self.assertEqual(lines[2]["txid"], 1)
        self.assertEqual(lines[2]["log_type"], "COMMIT")

        # 4: <T2, START>
        self.assertEqual(lines[3]["lsn"], 4)
        self.assertEqual(lines[3]["txid"], 2)
        self.assertEqual(lines[3]["log_type"], "START")

        # 5: <T2, OPERATION>
        self.assertEqual(lines[4]["lsn"], 5)
        self.assertEqual(lines[4]["txid"], 2)
        self.assertEqual(lines[4]["log_type"], "OPERATION")

        # 6: CHECKPOINT record
        ckpt = lines[5]
        self.assertEqual(ckpt["lsn"], 6)
        self.assertEqual(ckpt["log_type"], "CHECKPOINT")
        self.assertEqual(ckpt["active_transactions"], [2])

        # Cek last checkpoint
        self.assertTrue(
            os.path.exists(META_PATH), "last_checkpoint.json should be created"
        )

        with open(META_PATH, "r", encoding="utf-8") as f:
            meta = json.load(f)

        self.assertEqual(meta["checkpoint_lsn"], 6)
        self.assertEqual(meta["active_tx"], [2])


if __name__ == "__main__":
    unittest.main()
