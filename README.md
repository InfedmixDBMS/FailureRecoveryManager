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
├── tests/
│   ├── WriteLog/                    # Write log tests
│   ├── SaveCheckpoint/              # Checkpoint tests
│   └── Recover/                     # Recovery tests
├── examples/
│   └── recovery_demo.py             # Interactive demonstration
└── RECOVERY_IMPLEMENTATION.md       # Detailed documentation
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

# Perform full crash recovery
FRM.recover(RecoverCriteria())
# This will:
# 1. Read checkpoint metadata
# 2. Analyze transaction states
# 3. REDO all operations from checkpoint
# 4. UNDO uncommitted transactions
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

### 1. Transaction-Specific Recovery (ABORT)
Rolls back a single transaction by scanning buffer in reverse and undoing operations.

**Use Case:** Called automatically when ABORT is issued

### 2. Crash Recovery (ARIES)
Full system recovery implementing three phases:

1. **Analysis Phase** - Determine transaction states from checkpoint forward
2. **Redo Phase** - Replay all operations to restore pre-crash state
3. **Undo Phase** - Roll back uncommitted transactions

**Use Case:** System restart after unexpected failure

### 3. Timestamp-Based Recovery
Restore system to a specific point in time.

**Algorithm:**
1. **Analysis Phase** - Categorize transactions and operations relative to target timestamp
2. **Redo Phase** - Apply operations for transactions committed before timestamp
3. **Undo Phase** - Roll back operations after timestamp and uncommitted transactions

**Use Case:** Restore to a known good state at a specific time

## Testing

Run all tests:
```bash
python FailureRecoveryManager/tests/RunTests.py
```

Run interactive demonstration:
```bash
python FailureRecoveryManager/examples/recovery_demo.py
```

### Test Coverage

- Transaction lifecycle (BEGIN → OPERATION → COMMIT/ABORT)
- Single transaction rollback on ABORT
- Multiple concurrent transactions with selective abort
- Checkpoint creation and metadata persistence
- Crash recovery with REDO and UNDO phases
- Timestamp-based point-in-time recovery
- WAL file reading and log reconstruction


## Log Files

### wal.log (Write-Ahead Log)
JSON lines format storing all log records:
```json
{"lsn":1,"txid":1,"log_type":"START"}
{"lsn":2,"txid":1,"log_type":"OPERATION","table":"employee","key":1,"old_value":3000,"new_value":5000}
{"lsn":3,"txid":1,"log_type":"COMMIT"}
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

# Implementation Status

## Completed
- Write-ahead logging with LSN
- Transaction lifecycle management (BEGIN/COMMIT/ABORT)
- Automatic rollback on ABORT
- Checkpoint mechanism with disk persistence
- Crash recovery with ARIES algorithm
- Timestamp-based recovery with automatic timestamp tracking
- Log file reading and reconstruction
- Comprehensive test suite

## TODO
- Database interface integration for actual UNDO/REDO operations
