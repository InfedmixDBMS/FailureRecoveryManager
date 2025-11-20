import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import json
import unittest
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.classes.FailureRecoveryManager import FailureRecoveryManager as FRM

WAL_PATH = "wal.log"
META_PATH = "last_checkpoint.json"


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

	def test_save_checkpoint_flushes_buffer_and_writes_meta(self):
		# ------------------------------------------------------------------
		# Urutan log:
		#    1: T1 BEGIN
		#    2: T1 OPERATION
		#    3: T1 COMMIT
		#    4: T2 BEGIN
		#    5: T2 OPERATION       

		# T1: BEGIN
		FRM.write_log(self._make_exec(1, "BEGIN TRANSACTION"))
		# T1: OPERATION 
		FRM.write_log(self._make_exec(1, "UPDATE employee SET salary = 2000 WHERE id = 1"))
		# T1: COMMIT
		FRM.write_log(self._make_exec(1, "COMMIT"))

		# T2: BEGIN
		FRM.write_log(self._make_exec(2, "BEGIN TRANSACTION"))
		# T2: OPERATION
		FRM.write_log(self._make_exec(2, "UPDATE employee SET salary = 3000 WHERE id = 2"))

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
		self.assertTrue(os.path.exists(WAL_PATH), "wal.log should be created by _save_checkpoint")

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
		self.assertEqual(ckpt["txid"], "CHECKPOINT")
		self.assertEqual(ckpt["log_type"], "CHECKPOINT")
		self.assertEqual(ckpt["active_transaction"], [2])

        # Cek last checkpoint
		self.assertTrue(os.path.exists(META_PATH), "last_checkpoint.json should be created")

		with open(META_PATH, "r", encoding="utf-8") as f:
			meta = json.load(f)

		self.assertEqual(meta["checkpoint_lsn"], 6)
		self.assertEqual(meta["active_tx"], [2])


if __name__ == "__main__":
	unittest.main()
