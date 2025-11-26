# FailureRecoveryManager

## Data Types

- **LogRecord** - Individual log entries with LSN, transaction ID, operation details
- **LogType** - Enum for log types: START, OPERATION, COMMIT, ABORT, CHECKPOINT
- **ExecutionResult** - Database operation execution details
- **RecoverCriteria** - Specifies recovery parameters (transaction_id or timestamp)

## Main Class

- **FailureRecoveryManager** - Static class managing logging, checkpoints, and recovery

## File Structure

```
FailureRecoveryManager/
├── classes/
│   └── FailureRecoveryManager.py    # Main recovery manager
├── types/
│   ├── LogRecord.py                 # Log record structure
│   ├── LogType.py                   # Log type enumeration
│   ├── ExecutionResult.py           # Execution result data
│   └── RecoverCriteria.py           # Recovery criteria
├── interfaces/
│   └── __init__.py                  # Future database interface
├── tests/
│   ├── WriteLog/                    # Write log tests
│   ├── SaveCheckpoint/              # Checkpoint tests
│   ├── Recover/                     # Recovery tests
│   ├── RunTests.py                  # Test runner
│   └── Demo.py                      # Interactive demonstration
└── logs/                            # Runtime log files
```

## Usage

### Basic Transaction with Rollback

```python
from FailureRecoveryManager.classes.FailureRecoveryManager import FailureRecoveryManager as FRM
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult

# Start transaction
FRM.write_log(ExecutionResult(
    transaction_id=1,
    query="BEGIN TRANSACTION"
))

# Perform operation
FRM.write_log(ExecutionResult(
    transaction_id=1,
    query="UPDATE employee SET salary = 5000 WHERE id = 1",
    table="employee",
    key=1,
    old_value=3000,
    new_value=5000
))

# Abort - automatically triggers recovery
FRM.write_log(ExecutionResult(
    transaction_id=1,
    query="ABORT"
))
# Result: salary restored to 3000
```

### Crash Recovery

```python
# After system restart, recover from crash
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria

# Perform full crash recovery (ARIES algorithm)
FRM.recover(RecoverCriteria())
# This will:
# 1. Read checkpoint metadata
# 2. Analyze transaction states
# 3. REDO all operations from checkpoint
# 4. UNDO uncommitted transactions
```

### Point-in-Time Recovery (Transaction-Based)

```python
# Restore database to state BEFORE a specific transaction
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria

FRM.recover(RecoverCriteria(transaction_id=5))
# This will:
# 1. Start from the LAST entry in WAL
# 2. Go BACKWARD, undoing operations
# 3. Stop when reaching transaction 5's START
# Result: All operations from transaction 5 onward are undone
```

### Point-in-Time Recovery (Timestamp-Based)

```python
# Restore database to a specific point in time
from datetime import datetime
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria

target_time = datetime(2024, 1, 15, 14, 30, 0)
FRM.recover(RecoverCriteria(timestamp=target_time))
# This will:
# 1. Start from the LAST entry in WAL
# 2. Go BACKWARD, undoing operations with timestamp > target
# 3. Stop when reaching entries at or before target time
# Result: Database restored to state at target_time
```

### Checkpoint Creation

```python
# Periodically flush logs and create checkpoint
FRM._save_checkpoint()
# This writes:
# - All buffered logs to wal.log
# - Checkpoint record with active transactions
# - Checkpoint metadata to last_checkpoint.json
```

## Recovery Methods

### 1. Transaction-Specific Recovery
Rolls back a single transaction by scanning buffer in reverse and undoing operations.

**Use Case:** Called automatically when ABORT is issued

**Algorithm:** 
- Scan buffer backward from end
- Undo operations belonging to the aborted transaction
- Stop when reaching transaction START

### 2. Crash Recovery
Full system recovery implementing three phases:

1. **Analysis Phase** - Determine transaction states from checkpoint forward
2. **Redo Phase** - Replay all operations to restore pre-crash state
3. **Undo Phase** - Roll back uncommitted transactions

**Use Case:** System restart after unexpected failure

**Invocation:** `FRM.recover(RecoverCriteria())`

### 3. Transaction-Based Recovery
Restore system to state BEFORE a specific transaction began.

**Algorithm:**
1. Start from LAST entry in WAL
2. Process entries BACKWARD
3. UNDO each operation encountered
4. STOP when reaching the START of target transaction

**Use Case:** Rollback to a known checkpoint or transaction boundary

**Invocation:** `FRM.recover(RecoverCriteria(transaction_id=N))`

**Note:** This undoes ALL operations from the target transaction onward, including committed transactions. Different from ARIES crash recovery which preserves committed work.

### 4. Timestamp-Based Recovery
Restore system to a specific point in time.

**Algorithm:**
1. Start from LAST entry in WAL
2. Process entries BACKWARD
3. UNDO operations with timestamp > target
4. STOP when reaching entries at or before target timestamp

**Use Case:** Restore to a known good state at a specific time

**Invocation:** `FRM.recover(RecoverCriteria(timestamp=datetime_obj))`

**Note:** This undoes ALL operations after the target time, even if committed. Use for point-in-time restore scenarios.

## Testing

Run all tests:
```bash
python FailureRecoveryManager/tests/RunTests.py
```

Run interactive demonstration:
```bash
python FailureRecoveryManager/tests/Demo.py
```

### Test Coverage

- Transaction lifecycle (BEGIN → OPERATION → COMMIT/ABORT)
- Single transaction rollback on ABORT
- Multiple concurrent transactions with selective abort
- Checkpoint creation and metadata persistence
- Crash recovery with REDO and UNDO phases (ARIES)
- Specification-based recovery by transaction ID
- Specification-based recovery by timestamp
- WAL file reading and log reconstruction

## Log Files

### wal.log (Write-Ahead Log)
JSON lines format storing all log records:
```json
{"lsn":1,"txid":1,"log_type":"START","timestamp":1234567890000}
{"lsn":2,"txid":1,"log_type":"OPERATION","table":"employee","key":1,"old_value":3000,"new_value":5000,"timestamp":1234567890100}
{"lsn":3,"txid":1,"log_type":"COMMIT","timestamp":1234567890200}
```

### last_checkpoint.json (Checkpoint Metadata)
Fast recovery information:
```json
{
  "checkpoint_lsn": 6,
  "active_tx": [2],
  "timestamp": 1234567890000
}
```

## Implementation Status

### Completed
- Write-ahead logging with LSN
- Transaction lifecycle management (BEGIN/COMMIT/ABORT)
- Automatic rollback on ABORT
- Checkpoint mechanism with disk persistence
- Crash recovery with ARIES algorithm
- Specification-based recovery by transaction ID (backward undo)
- Specification-based recovery by timestamp (backward undo)
- Automatic timestamp tracking for all log entries
- Log file reading and reconstruction
- Comprehensive test suite
- Interactive demonstration with detailed scenarios

### TODO
- Database interface integration for actual UNDO/REDO operations
- Persistent storage of active transaction states