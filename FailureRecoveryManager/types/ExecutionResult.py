from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

@dataclass
class ExecutionResult:
    transaction_id: str
    timestamp: datetime = datetime.now()
    message: str = ""
    data: Optional[Any] = None # Rows | int
    query: str = ""

# KELAS MERUPAKAN SEBAGIAN DARI KELAS ASLI EXECUTION RESULT, DAN HANYA DIGUNAKAN UNTUK PERCOBAAN QUERY TRANSAKSI