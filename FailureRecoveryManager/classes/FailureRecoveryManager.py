import json
import os
import time
from datetime import datetime

from ..types.ExecutionResult import ExecutionResult
from ..types.LogRecord import LogRecord
from ..types.LogType import LogType
from ..types.RecoverCriteria import RecoverCriteria

from typing import Optional, Any

# Storage Manager
try:
    from StorageManager.classes.API import StorageEngine
    from StorageManager.classes.DataModels import (
        DataRetrieval,
        DataWrite,
        DataDeletion,
        Condition,
        Operation,
    )
except Exception:
    StorageEngine = None
    DataRetrieval = None
    DataWrite = None
    DataDeletion = None
    Condition = None
    Operation = None

class FailureRecoveryManager:
    buffer: list[LogRecord] = []
    last_lsn: int = 0
    _initialized: bool = False

    # Active Transactions: [txid]
    active_tx: list[int] = []

    def __new__(cls, *args, **kwargs):
        raise TypeError(
            "FailureRecoveryManager is a static class and cannot be instantiated"
        )
    
    @classmethod
    def _initialize(cls):
        if cls._initialized:
            return
        
        # Try to load from checkpoint metadata first
        meta_path = "storage/last_checkpoint.json"
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r") as f:
                    meta = json.load(f)
                    cls.last_lsn = meta.get("checkpoint_lsn", 0)
                    cls.active_tx = meta.get("active_tx", [])
                    cls._initialized = True
                    return
            except Exception as e:
                print(f"[FRM] Warning: Failed to load checkpoint metadata: {e}")
        
        # Fallback: scan WAL log to find highest LSN
        wal_path = "wal.log"
        if os.path.exists(wal_path):
            try:
                with open(wal_path, "r") as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            lsn = record.get("lsn", 0)
                            if lsn > cls.last_lsn:
                                cls.last_lsn = lsn
                        except:
                            continue
            except Exception as e:
                print(f"[FRM] Warning: Failed to read WAL log: {e}")
        
        cls._initialized = True

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

        cls._append_json_line("wal.log", checkpoint_rec.to_dict())

        cls.buffer.clear()
        cls.last_lsn = flushed_lsn

        meta_path = "storage/last_checkpoint.json"
        meta_payload = {
            "checkpoint_lsn": flushed_lsn,
            "active_tx": cls.active_tx,
            "timestamp": int(time.time() * 1000),
        }

        cls._dump_json_file(meta_path, meta_payload)

    @classmethod
    def write_log(cls, execution_result: ExecutionResult, table:str, key:Optional[Any], old_value:Optional[Any], new_value:Optional[Any]):
        # Initialize on first use
        cls._initialize()
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
            cls.recover(RecoverCriteria(transaction_id=txid))
            cls.buffer.append(log)
            cls.active_tx.remove(txid)

        else:
            log = LogRecord(
                lsn=lsn,
                txid=txid,
                log_type=LogType.OPERATION,
                table=table,
                key=key,
                old_value=old_value,
                new_value=new_value,
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
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value}"
                )
                try:
                    cls._apply_undo(log)
                except Exception as e:
                    print(f"[FRM] UNDO error: {e}")

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
                try:
                    cls._apply_redo(log)
                except Exception as e:
                    print(f"[FRM] REDO error: {e}")

        # Phase 3: UNDO - Roll back operations after timestamp
        # Scan in reverse to undo operations that happened after target time
        for log in reversed(operations_after_timestamp):
            # UNDO: restore old_value for all operations after timestamp
            print(
                f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value} (T{log.txid})"
            )
            try:
                cls._apply_undo(log)
            except Exception as e:
                print(f"[FRM] UNDO error: {e}")

        # Phase 4: UNDO - Roll back active (uncommitted) transactions at timestamp
        # These transactions were started but not committed before timestamp
        for log in reversed(operations_before_timestamp):
            if log.txid in active_at_timestamp:
                # UNDO: restore old_value for uncommitted transactions
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value} (T{log.txid} uncommitted)"
                )
                try:
                    cls._apply_undo(log)
                except Exception as e:
                    print(f"[FRM] UNDO error: {e}")

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
                try:
                    cls._apply_redo(log)
                except Exception as e:
                    print(f"[FRM] REDO error: {e}")

        # Phase 3: UNDO - rollback uncommitted transactions
        # Scan logs in reverse for active (uncommitted) transactions
        for i in range(len(all_logs) - 1, -1, -1):
            log = all_logs[i]
            if log.txid in active_tx and log.log_type == LogType.OPERATION:
                # UNDO: restore old_value
                print(
                    f"UNDO: Restoring {log.table}.{log.key} from {log.new_value} to {log.old_value}"
                )
                try:
                    cls._apply_undo(log)
                except Exception as e:
                    print(f"[FRM] UNDO error: {e}")

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
            f.flush()
            os.fsync(f.fileno())

    @classmethod
    def _dump_json_file(cls, meta_path: str, meta_payload: dict):
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

    # Helpers
    @classmethod
    def _get_non_system_columns(cls, table: str):
        if StorageEngine is None:
            raise RuntimeError("StorageEngine unavailable for recovery operations")
        cols = StorageEngine.load_schema_names(table)
        return [c for c in cols if c != "__row_id"]

    @classmethod
    def _read_rows(cls, table: str, conditions):
        req = DataRetrieval(table=table, column=[], conditions=[])
        setattr(req, "conditions", conditions)
        return StorageEngine.read_block(req)

    @classmethod
    def _delete_rows(cls, table: str, conditions) -> int:
        del_req = DataDeletion(table=table, conditions=conditions)
        return StorageEngine.delete_block(del_req)

    @classmethod
    def _insert_rows(cls, table: str, columns, rows) -> int:
        write_req = DataWrite(table=table, column=columns, conditions=[], new_value=rows)
        return StorageEngine.write_block(write_req)

    @classmethod
    def _merge_row(cls, col_names, base_row, updates):
        merged = []
        for c in col_names:
            if c in updates:
                merged.append(updates[c])
            else:
                merged.append(base_row.get(c))
        return merged

    @classmethod
    def _apply_redo(cls, log: LogRecord) -> None:
        if StorageEngine is None:
            raise RuntimeError("StorageEngine unavailable for recovery operations")

        table = log.table
        key = log.key
        new_vals = log.new_value or {}
        old_vals = log.old_value or {}

        cols = cls._get_non_system_columns(table)

        # locate row by 'id' if present, else by __row_id
        conditions = []
        
        if "id" in old_vals:
            conditions.append(Condition("id", Operation.EQ, old_vals["id"]))
        elif key is not None:
            conditions.append(Condition("__row_id", Operation.EQ, key))    

        current = cls._read_rows(table, conditions)

        # If nothing found and this is INSERT (old_value None), re-insert new_value
        if (not current or not current.data):
            if not old_vals and new_vals:
                row = [new_vals.get(c) for c in cols]
                cls._insert_rows(table, cols, [row])
            return

        # delete matched rows then insert merged rows
        updated_rows = []
        for row in current.data:
            row_dict = {col: row[idx] for idx, col in enumerate(current.columns)}
            base = {c: row_dict.get(c) for c in cols}
            merged = cls._merge_row(cols, base, new_vals)
            updated_rows.append(merged)

        # Delete matched rows (same conditions)
        cls._delete_rows(table, conditions)
        # Insert merged
        if updated_rows:
            cls._insert_rows(table, cols, updated_rows)

    @classmethod
    def _apply_undo(cls, log: LogRecord) -> None:
        if StorageEngine is None:
            raise RuntimeError("StorageEngine unavailable for recovery operations")

        table = log.table
        key = log.key
        new_vals = log.new_value or {}
        old_vals = log.old_value or {}

        cols = cls._get_non_system_columns(table)

        # kalo INSERT (no old_value), UNDO is delete the inserted row
        if not old_vals and new_vals:
            # Try __row_id first, else by values
            conditions = []
            if key is not None:
                conditions.append(Condition("__row_id", Operation.EQ, key))
            else:
                for k, v in new_vals.items():
                    conditions.append(Condition(k, Operation.EQ, v))
            cls._delete_rows(table, conditions)
            try:
                clr_exec = ExecutionResult(success=True, transaction_id=log.txid, query="UNDO")
                cls.write_log(clr_exec, table=table, key=key, old_value=new_vals, new_value=None)
            except Exception as e:
                pass
            return

        # For UPDATE/DELETE (have old_value):
        # delete any rows that match the "new" state (id plus new fields)
        delete_conditions = []
        if "id" in old_vals:
            delete_conditions.append(Condition("id", Operation.EQ, old_vals["id"]))
        for k, v in new_vals.items():
            delete_conditions.append(Condition(k, Operation.EQ, v))
        if delete_conditions:
            cls._delete_rows(table, delete_conditions)

        # re-insert the old row content
        if old_vals:
            old_row = [old_vals.get(c) for c in cols]
            cls._insert_rows(table, cols, [old_row])
            try:
                clr_exec = ExecutionResult(success=True, transaction_id=log.txid, query="UNDO")
                cls.write_log(clr_exec, table=table, key=key, old_value=old_vals, new_value=None)
            except Exception as e:
                pass
            
