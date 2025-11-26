from dataclasses import asdict, dataclass
from typing import Any, Optional

from .LogType import LogType


@dataclass
class LogRecord:
    lsn: int  # Log Sequence Number (monotonik)
    txid: str  # Tn
    log_type: LogType
    # OPERATION fields:
    table: Optional[str] = None
    key: Optional[Any] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    # CHECKPOINT field
    active_transactions: Optional[Any] = None  # pointer REDO (untuk CHECKPOINT)

    def __repr__(self):
        if self.log_type == LogType.START:
            return f"{self.lsn}: <{self.txid}, Start>"
        if self.log_type == LogType.OPERATION:
            return f"{self.lsn}: <{self.txid}, {self.table}.{self.key}, {self.old_value}, {self.new_value}>"
        if self.log_type == LogType.COMMIT:
            return f"{self.lsn}: <{self.txid}, Commit>"
        if self.log_type == LogType.CHECKPOINT:
            return f"{self.lsn}: <Checkpoint, T: {self.active_transactions}>"
        if self.log_type == LogType.ABORT:
            return f"{self.lsn}: <{self.txid}, Abort>"

        return f"{self.lsn}: <{self.txid}, Unknown>"

    def to_dict(self) -> dict:
        logdict = asdict(self)
        logdict["log_type"] = self.log_type.value
        # Remove None values to keep JSON clean
        return {k: v for k, v in logdict.items() if v is not None}
