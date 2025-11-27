from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any


class ExecutionResult:
    
    def __init__(
        self,
        success: bool,
        affected_rows: int = 0,
        message: str = "",
        error: Optional[str] = None,
        execution_time: float = 0.0,
        transaction_id: Optional[int] = None,
        query: Optional[str] = None
    ):
        self.success = success
        self.affected_rows = affected_rows
        self.message = message
        self.error = error
        self.execution_time = execution_time
        self.transaction_id = transaction_id
        self.query = query
        self.timestamp = datetime.now()
    
    def __repr__(self) -> str:
        if self.success:
            tx_info = f", tx={self.transaction_id}" if self.transaction_id else ""
            return f"ExecutionResult(success=True, affected_rows={self.affected_rows}, message='{self.message}'{tx_info})"
        else:
            return f"ExecutionResult(success=False, error='{self.error}')"