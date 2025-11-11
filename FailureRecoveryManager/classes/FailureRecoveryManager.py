from FailureRecoveryManager.types.LogRecord import LogRecord

class FailureRecoveryManager:
    def __init__(self):
        self.buffer : list[any] = []
        self.last_lsn : int = 0

    def _save_checkpoint(self):
        # TODO : Flush LOG
        # TODO : Save checkpoint to disk
        pass

    def write_log(self, log: LogRecord):
        self.buffer.append(log)
        self.last_lsn = log.lsn
        

    def recover(self):
        # TODO : READ LAST LSN
        # TODO : REPLAY LOG FROM LAST LSN
        pass
