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
    table: Optional[str] = None
    key: Optional[Any] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None

# KELAS MERUPAKAN SEBAGIAN DARI KELAS ASLI EXECUTION RESULT, DAN HANYA DIGUNAKAN UNTUK PERCOBAAN QUERY TRANSAKSI