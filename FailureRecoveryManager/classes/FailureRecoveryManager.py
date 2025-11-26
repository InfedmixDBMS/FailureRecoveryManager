import json
import os
import time

from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.LogRecord import LogRecord
from FailureRecoveryManager.types.LogType import LogType
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria


class FailureRecoveryManager:
    buffer: list[LogRecord] = []
    last_lsn: int = 0

    # Active Transactions: [txid]
    active_tx: list[int] = []

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            "FailureRecoveryManager is a static class and cannot be instantiated"
        )

    @classmethod
    def _save_checkpoint(cls):
        if not cls.buffer and cls.last_lsn == 0:
            return

        for rec in cls.buffer:
            # TODO : Redirect filepath to actual one in disk
            cls._append_json_line("wal.log", rec.to_dict())

        flushed_lsn = cls.last_lsn + 1

        checkpoint_rec = LogRecord(
            lsn=flushed_lsn,
            txid="CHECKPOINT",
            log_type=LogType.CHECKPOINT,
            active_transactions=cls.active_tx,
        )

        # TODO : Redirect filepath to actual one in disk
        cls._append_json_line("wal.log", checkpoint_rec.to_dict())

        # TODO : Save checkpoint to disk

        cls.buffer.clear()
        cls.last_lsn = flushed_lsn

        # Save metadata for faster recovery
        meta_path = "last_checkpoint.json"
        meta_payload = {
            "checkpoint_lsn": flushed_lsn,
            "active_tx": cls.active_tx,
            "timestamp": int(time.time() * 1000),
        }

        cls._dump_json_file(meta_path, meta_payload)

    @classmethod
    def write_log(cls, execution_result: ExecutionResult):
        query = execution_result.query.strip().upper()
        cls.last_lsn += 1
        lsn = cls.last_lsn
        txid = execution_result.transaction_id

        if query.startswith("BEGIN"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.START)
            cls.buffer.append(log)
            if log.txid not in cls.active_tx:
                cls.active_tx.append(log.txid)
        elif query.startswith("COMMIT"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.COMMIT)
            if log.txid not in cls.active_tx:
                raise Exception(f"Transaction {txid} committed without BEGIN")
            cls.buffer.append(log)
            cls.active_tx.remove(log.txid)
        elif query.startswith("ABORT"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.ABORT)
            if log.txid not in cls.active_tx:
                raise Exception(f"Transaction {txid} abort without BEGIN")
            cls.buffer.append(log)
            cls.recover(RecoverCriteria(transaction_id=txid))
            cls.active_tx.remove(log.txid)
        else:
            log = LogRecord(
                lsn=lsn,
                txid=txid,
                log_type=LogType.OPERATION,
                table=execution_result.table,
                key=execution_result.key,
                old_value=execution_result.old_value,
                new_value=execution_result.new_value,
            )
            cls.buffer.append(log)
            # TODO : integrate the new execution result attributes

    @classmethod
    def recover(cls, criteria: RecoverCriteria):
        # TODO : READ LAST LSN
        # TODO : REPLAY LOG FROM LAST LSN
        pass

    @classmethod
    # HELPER JSON-WRITE methods:
    def _append_json_line(cls, path: str, payload: dict):
        # TODO : Make sure directory exists in disk
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush
            os.fsync(f.fileno())

    @classmethod
    def _dump_json_file(cls, meta_path: str, meta_payload: dict):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
