from enum import Enum

class LogType(Enum):
    START = "START"
    OPERATION = "OPERATION"
    COMMIT = "COMMIT"
    CHECKPOINT = "CHECKPOINT"