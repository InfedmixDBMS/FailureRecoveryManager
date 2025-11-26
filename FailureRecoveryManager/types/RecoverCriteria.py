from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RecoverCriteria:
    timestamp: Optional[datetime] = None
    transaction_id: Optional[int] = None