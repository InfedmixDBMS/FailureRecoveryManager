import json
import os
import time
from datetime import datetime

from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.LogRecord import LogRecord
from FailureRecoveryManager.types.LogType import LogType
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria


class FailureRecoveryManager:
    buffer: list[LogRecord] = []
    last_lsn: int = 0

    # Active Transactions: [txid]
    active_tx: list[int] = []

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            "FailureRecoveryManager is a static class and cannot be instantiated"
        )

    @classmethod
    def _save_checkpoint(cls):
        if not cls.buffer and cls.last_lsn == 0:
            return

        for rec in cls.buffer:
            # TODO : Redirect filepath to actual one in disk
            cls._append_json_line("wal.log", rec.to_dict())

        flushed_lsn = cls.last_lsn + 1

        checkpoint_rec = LogRecord(
            lsn=flushed_lsn,
            txid=None,
            log_type=LogType.CHECKPOINT,
            active_transactions=cls.active_tx.copy(),
        )

        # TODO : Redirect filepath to actual one in disk
        cls._append_json_line("wal.log", checkpoint_rec.to_dict())

        # TODO : Save checkpoint to disk

        cls.buffer.clear()
        cls.last_lsn = flushed_lsn

        # Save metadata for faster recovery
        meta_path = "last_checkpoint.json"
        meta_payload = {
            "checkpoint_lsn": flushed_lsn,
            "active_tx": cls.active_tx,
            "timestamp": int(time.time() * 1000),
        }

        cls._dump_json_file(meta_path, meta_payload)

    @classmethod
    def write_log(cls, execution_result: ExecutionResult):
        query = execution_result.query.strip().upper()
        cls.last_lsn += 1
        lsn = cls.last_lsn
        txid = execution_result.transaction_id

        if query.startswith("BEGIN"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.START)
            cls.buffer.append(log)
            if log.txid not in cls.active_tx:
                cls.active_tx.append(txid)

        elif query.startswith("COMMIT"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.COMMIT)
            if log.txid not in cls.active_tx:
                raise Exception(f"Transaction {txid} committed without BEGIN")
            cls.buffer.append(log)
            cls.active_tx.remove(txid)

        elif query.startswith("ABORT"):
            log = LogRecord(lsn=lsn, txid=txid, log_type=LogType.ABORT)
            if log.txid not in cls.active_tx:
                raise Exception(f"Transaction {txid} abort without BEGIN")
            cls.buffer.append(log)
            cls.recover(RecoverCriteria(transaction_id=txid))
            cls.active_tx.remove(txid)

        else:
            log = LogRecord(
                lsn=lsn,
                txid=txid,
                log_type=LogType.OPERATION,
                table=execution_result.table,
                key=execution_result.key,
                old_value=execution_result.old_value,
                new_value=execution_result.new_value,
            )
            cls.buffer.append(log)
            # TODO : Integrate the new execution result attributes

    @classmethod
    def recover(cls, criteria: RecoverCriteria):
        if criteria.transaction_id is not None:
            # UNDO specific transaction (ABORT case)
            cls._undo_transaction(criteria.transaction_id)
        elif criteria.timestamp is not None:
            # Time-based recovery
            cls._recover_to_timestamp(criteria.timestamp)
        else:
            # Full crash recovery
            cls._crash_recovery()

    @classmethod
    def _undo_transaction(cls, txid: int):
        # Scan buffer in reverse to undo operations
        for i in range(len(cls.buffer) - 1, -1, -1):
            log = cls.buffer[i]

            # Only process OPERATION logs for this transaction
            if log.txid == txid and log.log_type == LogType.OPERATION:
                # Apply UNDO: restore old_value
                # In a real system, this would update the actual database
                # For now, we log the undo operation conceptually
                # The actual database update would happen via a callback or interface
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value}"
                )
                # TODO: Interface to actual database to restore old_value
                # e.g., database.update(log.table, log.key, log.old_value)

    @classmethod
    def _recover_to_timestamp(cls, timestamp):
        # Read all logs from disk
        all_logs = cls._read_all_logs_from_disk()

        # Phase 1: ANALYSIS - Determine transaction states at target timestamp
        committed_before_timestamp = set()
        active_at_timestamp = set()
        operations_before_timestamp = []
        operations_after_timestamp = []

        for log in all_logs:
            if log.timestamp <= timestamp:
                # Log occurred before or at target timestamp
                if log.log_type == LogType.START:
                    active_at_timestamp.add(log.txid)
                elif log.log_type == LogType.COMMIT:
                    if log.txid in active_at_timestamp:
                        active_at_timestamp.remove(log.txid)
                    committed_before_timestamp.add(log.txid)
                elif log.log_type == LogType.ABORT:
                    if log.txid in active_at_timestamp:
                        active_at_timestamp.remove(log.txid)
                elif log.log_type == LogType.OPERATION:
                    operations_before_timestamp.append(log)
            else:
                # Log occurred after target timestamp
                if log.log_type == LogType.OPERATION:
                    operations_after_timestamp.append(log)

        # Phase 2: REDO - Apply operations for committed transactions up to timestamp
        print(f"Recovering to timestamp: {timestamp.isoformat()}")
        print(
            f"Found {len(committed_before_timestamp)} committed transactions before timestamp"
        )
        print(f"Found {len(active_at_timestamp)} active transactions at timestamp")

        for log in operations_before_timestamp:
            if log.txid in committed_before_timestamp:
                # REDO: Apply new_value for committed transactions
                print(
                    f"REDO: Applying {log.table}.{log.key} = {log.new_value} (T{log.txid})"
                )
                # TODO: Interface to actual database
                # database.update(log.table, log.key, log.new_value)

        # Phase 3: UNDO - Roll back operations after timestamp
        # Scan in reverse to undo operations that happened after target time
        for log in reversed(operations_after_timestamp):
            # UNDO: restore old_value for all operations after timestamp
            print(
                f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value} (T{log.txid})"
            )
            # TODO: Interface to actual database
            # database.update(log.table, log.key, log.old_value)

        # Phase 4: UNDO - Roll back active (uncommitted) transactions at timestamp
        # These transactions were started but not committed before timestamp
        for log in reversed(operations_before_timestamp):
            if log.txid in active_at_timestamp:
                # UNDO: restore old_value for uncommitted transactions
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value} (T{log.txid} uncommitted)"
                )
                # TODO: Interface to actual database
                # database.update(log.table, log.key, log.old_value)

        # Update in-memory state
        cls.active_tx = list(active_at_timestamp)
        print(f"\nRecovery to timestamp complete. Active transactions: {cls.active_tx}")

    @classmethod
    def _crash_recovery(cls):
        # Try to read checkpoint metadata
        checkpoint_lsn = 0
        active_transactions_at_checkpoint = []

        meta_path = "last_checkpoint.json"
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                checkpoint_lsn = meta.get("checkpoint_lsn", 0)
                active_transactions_at_checkpoint = meta.get("active_tx", [])

        # Read all logs from disk
        all_logs = cls._read_all_logs_from_disk()

        # Phase 1: ANALYSIS - determine which transactions committed/aborted
        committed_tx = set()
        aborted_tx = set()
        active_tx = set(active_transactions_at_checkpoint)

        for log in all_logs:
            if log.lsn <= checkpoint_lsn:
                continue

            if log.log_type == LogType.START:
                active_tx.add(log.txid)
            elif log.log_type == LogType.COMMIT:
                if log.txid in active_tx:
                    active_tx.remove(log.txid)
                committed_tx.add(log.txid)
            elif log.log_type == LogType.ABORT:
                if log.txid in active_tx:
                    active_tx.remove(log.txid)
                aborted_tx.add(log.txid)

        # Phase 2: REDO - replay all operations from checkpoint forward (idempotent)
        # In ARIES, we REDO all operations to bring database to state before crash
        # This includes both committed and uncommitted transactions
        for log in all_logs:
            if log.lsn <= checkpoint_lsn:
                continue
            if log.log_type == LogType.OPERATION:
                # REDO: Apply new_value for all operations
                print(f"REDO: Applying {log.table}.{log.key} = {log.new_value}")
                # TODO: Interface to actual database
                # database.update(log.table, log.key, log.new_value)</parameter>

        # Phase 3: UNDO - rollback uncommitted transactions
        # Scan logs in reverse for active (uncommitted) transactions
        for i in range(len(all_logs) - 1, -1, -1):
            log = all_logs[i]
            if log.txid in active_tx and log.log_type == LogType.OPERATION:
                # UNDO: restore old_value
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value}"
                )
                # TODO: Interface to actual database
                # database.update(log.table, log.key, log.old_value)

        # Update in-memory state
        cls.active_tx = list(active_tx)
        if all_logs:
            cls.last_lsn = max(log.lsn for log in all_logs)

    @classmethod
    def _read_all_logs_from_disk(cls) -> list[LogRecord]:
        wal_path = "wal.log"
        if not os.path.exists(wal_path):
            return []

        logs = []
        with open(wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                log_dict = json.loads(line)

                # Parse timestamp from ISO format string
                timestamp = (
                    datetime.fromisoformat(log_dict["timestamp"])
                    if "timestamp" in log_dict
                    else datetime.now()
                )

                # Reconstruct LogRecord from dictionary
                log = LogRecord(
                    lsn=log_dict["lsn"],
                    txid=log_dict.get("txid"),
                    log_type=LogType(log_dict["log_type"]),
                    timestamp=timestamp,
                    table=log_dict.get("table"),
                    key=log_dict.get("key"),
                    old_value=log_dict.get("old_value"),
                    new_value=log_dict.get("new_value"),
                    active_transactions=log_dict.get("active_transactions"),
                )
                logs.append(log)

        return logs

    @classmethod
    # HELPER JSON-WRITE methods:
    def _append_json_line(cls, path: str, payload: dict):
        # TODO : Make sure directory exists in disk
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush
            os.fsync(f.fileno())

    @classmethod
    def _dump_json_file(cls, meta_path: str, meta_payload: dict):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
