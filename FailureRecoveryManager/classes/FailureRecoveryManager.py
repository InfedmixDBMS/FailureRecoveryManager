from FailureRecoveryManager.types.LogRecord import LogRecord
from FailureRecoveryManager.types.LogType import LogType
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
    def write_log(cls, log: LogRecord):
        cls.buffer.append(log)
        cls.last_lsn = log.lsn
        

    @classmethod
    def recover(cls):
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

    
