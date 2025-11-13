from FailureRecoveryManager.types.LogRecord import LogRecord

class FailureRecoveryManager:
    buffer: list[LogRecord] = []
    last_lsn: int = 0

    def __new__(cls, *args, **kwargs):
        raise TypeError("FailureRecoveryManager is a static class and cannot be instantiated")

    @classmethod
    def _save_checkpoint(cls):
        # TODO : Flush LOG
        # TODO : Save checkpoint to disk
        pass

    @classmethod
    def write_log(cls, log: LogRecord):
        cls.buffer.append(log)
        cls.last_lsn = log.lsn
        

    @classmethod
    def recover(cls):
        # TODO : READ LAST LSN
        # TODO : REPLAY LOG FROM LAST LSN
        pass
