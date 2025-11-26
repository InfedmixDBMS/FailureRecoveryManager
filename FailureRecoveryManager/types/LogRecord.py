from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Optional

from .LogType import LogType


@dataclass
class LogRecord:
    # Common fields
    lsn: int  # Log Sequence Number (monotonik)
    txid: Optional[int]  # Tn (None for CHECKPOINT)
    log_type: LogType
    timestamp: datetime = field(default_factory=datetime.now)  # Automatic timestamp

    # OPERATION fields
    table: Optional[str] = None
    key: Optional[Any] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None

    # CHECKPOINT field
    active_transactions: Optional[list[int]] = None # List of active transaction IDs

    def __post_init__(self):
        """Validate that non-checkpoint logs must have a txid"""
        if self.log_type != LogType.CHECKPOINT and self.txid is None:
            raise ValueError(f"txid is required for {self.log_type.value} logs")

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

        txid_str = self.txid if self.txid else "None"
        return f"{self.lsn}: <{txid_str}, Unknown>"

    def to_dict(self) -> dict:
        logdict = asdict(self)
        logdict["log_type"] = self.log_type.value
        # Convert timestamp to ISO format string for JSON serialization
        logdict["timestamp"] = self.timestamp.isoformat()
        # Remove None values to keep JSON clean
        return {k: v for k, v in logdict.items() if v is not None}
