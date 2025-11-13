from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime = datetime.now()
    message: str = ""
    data: Optional[Any] = None # Rows | int
    query: str = ""