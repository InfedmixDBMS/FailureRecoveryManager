from FailureRecoveryManager.types.LogRecord import LogRecord
from FailureRecoveryManager.types.LogType import LogType
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria
import json, os, time

class FailureRecoveryManager:
    buffer: list[LogRecord] = []
    last_lsn: int = 0

    def __new__(cls, *args, **kwargs):
        raise TypeError("FailureRecoveryManager is a static class and cannot be instantiated")

    @classmethod
    def _save_checkpoint(cls):
        if not cls.buffer and cls.last_lsn == 0:
            return

        if cls.buffer:
            for rec in cls.buffer:
                # TODO : Redirect filepath to actual one in disk
                cls._append_json_line("wal.log", rec.to_dict()) 

        flushed_lsn = cls.last_lsn

        checkpoint_rec = {
            "lsn": flushed_lsn + 1,
            "txid": "CHECKPOINT",
            "log_type": LogType.CHECKPOINT.value,
            "timestamp": int(time.time() * 1000),
        }

        # TODO : Save checkpoint to disk

        cls.buffer.clear()
        cls.last_lsn = checkpoint_rec["lsn"]

    @classmethod
    def write_log(cls, execution_result: ExecutionResult):
        query = execution_result.query.strip().upper()
        cls.last_lsn += 1
        lsn = cls.last_lsn
        txid = execution_result.transaction_id

        if query.startswith("BEGIN"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.START)
            cls.buffer.append(log)
        elif query.startswith("COMMIT"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.COMMIT)
            cls.buffer.append(log)
        elif query.startswith("ABORT"):
            # Also write abort log (optional)
            cls.recover(RecoverCriteria(transaction_id=txid))
        else:
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.OPERATION)
            cls.buffer.append(log)
            #TODO : add table, key, old_val, new_val after integration 
            
        

    @classmethod
    def recover(cls, criteria: RecoverCriteria):
        # TODO : READ LAST LSN
        # TODO : REPLAY LOG FROM LAST LSN
        pass

    # HELPER JSON-WRITE methods:
    def _append_json_line(cls, path: str, payload: dict):
        # TODO : Make sure directory exists in disk
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush
            os.fsync(f.fileno())

    
