#!/usr/bin/env python3
"""
Recovery Demonstration Script

This script demonstrates the recovery functionality of FailureRecoveryManager.
It shows three main recovery scenarios:
1. Transaction abort (single transaction rollback)
2. Multiple concurrent transactions with selective abort
3. Crash recovery simulation

Run this script to see the recovery mechanisms in action.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from FailureRecoveryManager.classes.FailureRecoveryManager import (
    FailureRecoveryManager as FRM,
)
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria


def print_separator(title):
    """Print a formatted section separator."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_buffer_state():
    """Print current buffer and active transactions."""
    print("\nCurrent state:")
    print(f"  Last LSN: {FRM.last_lsn}")
    print(f"  Active transactions: {FRM.active_tx}")
    print(f"  Buffer size: {len(FRM.buffer)} records")
    if FRM.buffer:
        print("  Buffer contents:")
        for log in FRM.buffer:
            print(f"    {repr(log)}")


def scenario_1_simple_abort():
    """Demonstrate simple transaction abort with rollback."""
    print_separator("SCENARIO 1: Simple Transaction Abort")

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    print("\n1. Starting transaction T1...")
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="BEGIN TRANSACTION",
            message="Transaction started",
        )
    )

    print("2. T1: Updating employee salary: 3000 → 5000")
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE employee SET salary = 5000 WHERE id = 1",
            table="employee",
            key=1,
            old_value=3000,
            new_value=5000,
        )
    )

    print("3. T1: Updating employee name: 'Alice' → 'Bob'")
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE employee SET name = 'Bob' WHERE id = 1",
            table="employee",
            key=1,
            old_value="Alice",
            new_value="Bob",
        )
    )

    print_buffer_state()

    print("\n4. T1: Issuing ABORT - This will trigger recovery...")
    print("\n--- Recovery Output ---")
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="ABORT",
            message="Transaction aborted",
        )
    )
    print("--- End Recovery Output ---")

    print("\n5. After ABORT:")
    print("   - Salary restored to 3000 (from 5000)")
    print("   - Name restored to 'Alice' (from 'Bob')")
    print("   - Transaction removed from active list")

    print_buffer_state()


def scenario_2_concurrent_abort():
    """Demonstrate multiple concurrent transactions with selective abort."""
    print_separator("SCENARIO 2: Concurrent Transactions with Selective Abort")

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    print("\n1. Starting transaction T1...")
    FRM.write_log(ExecutionResult(transaction_id=1, query="BEGIN TRANSACTION"))

    print("2. T1: Updating product price: 100 → 150")
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE products SET price = 150 WHERE id = 1",
            table="products",
            key=1,
            old_value=100,
            new_value=150,
        )
    )

    print("\n3. Starting transaction T2...")
    FRM.write_log(ExecutionResult(transaction_id=2, query="BEGIN TRANSACTION"))

    print("4. T2: Updating product stock: 50 → 45")
    FRM.write_log(
        ExecutionResult(
            transaction_id=2,
            query="UPDATE products SET stock = 45 WHERE id = 2",
            table="products",
            key=2,
            old_value=50,
            new_value=45,
        )
    )

    print("\n5. Starting transaction T3...")
    FRM.write_log(ExecutionResult(transaction_id=3, query="BEGIN TRANSACTION"))

    print("6. T3: Updating customer balance: 1000 → 900")
    FRM.write_log(
        ExecutionResult(
            transaction_id=3,
            query="UPDATE customers SET balance = 900 WHERE id = 1",
            table="customers",
            key=1,
            old_value=1000,
            new_value=900,
        )
    )

    print_buffer_state()

    print("\n7. T2: Committing transaction...")
    FRM.write_log(ExecutionResult(transaction_id=2, query="COMMIT"))

    print("\n8. T1: Aborting transaction...")
    print("\n--- Recovery Output (T1 only) ---")
    FRM.write_log(ExecutionResult(transaction_id=1, query="ABORT"))
    print("--- End Recovery Output ---")

    print("\n9. Final state:")
    print("   - T1: Aborted (price restored to 100)")
    print("   - T2: Committed (stock = 45)")
    print("   - T3: Still active (balance = 900)")

    print_buffer_state()


def scenario_3_crash_recovery():
    """Simulate crash recovery scenario."""
    print_separator("SCENARIO 3: Crash Recovery Simulation")

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    # Clean up any existing files
    for file in ["wal.log", "last_checkpoint.json"]:
        if os.path.exists(file):
            os.remove(file)

    print("\n1. Transaction T1: BEGIN and operation")
    FRM.write_log(ExecutionResult(transaction_id=1, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE accounts SET balance = 5000 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=3000,
            new_value=5000,
        )
    )

    print("\n2. Creating checkpoint (flushes T1 to disk)...")
    FRM._save_checkpoint()
    print(f"   Checkpoint created at LSN {FRM.last_lsn}")

    print("\n3. Transaction T1: COMMIT (after checkpoint)")
    FRM.write_log(ExecutionResult(transaction_id=1, query="COMMIT"))

    print("\n4. Transaction T2: BEGIN and operation")
    FRM.write_log(ExecutionResult(transaction_id=2, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=2,
            query="UPDATE accounts SET balance = 7000 WHERE id = 2",
            table="accounts",
            key=2,
            old_value=4000,
            new_value=7000,
        )
    )

    # Flush remaining logs to disk
    import json

    for rec in FRM.buffer:
        line = json.dumps(rec.to_dict(), separators=(",", ":")) + "\n"
        with open("wal.log", "a") as f:
            f.write(line)

    print("\n5. ** SIMULATING CRASH ** (T2 never committed)")
    print("   Clearing in-memory state...")

    # Simulate crash - lose all in-memory state
    FRM.buffer = []
    FRM.active_tx = []
    FRM.last_lsn = 0

    print("\n6. ** SYSTEM RESTART **")
    print("   Running crash recovery...")
    print("\n--- Recovery Output ---")
    FRM._crash_recovery()
    print("--- End Recovery Output ---")

    print("\n7. Recovery Complete!")
    print("\n   Analysis Phase:")
    print("     - Checkpoint LSN: 3 (T1 was active)")
    print("     - After checkpoint: T1 committed, T2 started but never committed")
    print("\n   Redo Phase:")
    print("     - Replayed T2's operation (LSN > checkpoint)")
    print("\n   Undo Phase:")
    print("     - Rolled back T2 (uncommitted)")
    print("     - T1 not rolled back (it committed)")

    print("\n   Final consistent state:")
    print("     - accounts.1 balance = 5000 (T1 committed)")
    print("     - accounts.2 balance = 4000 (T2 rolled back)")

    print_buffer_state()

    # Clean up
    for file in ["wal.log", "last_checkpoint.json"]:
        if os.path.exists(file):
            os.remove(file)


def scenario_4_timestamp_recovery():
    """Demonstrate timestamp-based recovery."""
    print_separator("SCENARIO 4: Timestamp-Based Recovery")
    from datetime import datetime, timedelta

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    # Clean up any existing files
    for file in ["wal.log", "last_checkpoint.json"]:
        if os.path.exists(file):
            os.remove(file)

    print("\n1. Recording base timestamp...")
    base_time = datetime.now()
    print(f"   Base time: {base_time.strftime('%H:%M:%S')}")

    print("\n2. Transaction T1: BEGIN and operation at T+0s")
    FRM.write_log(ExecutionResult(transaction_id=1, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE accounts SET balance = 5000 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=3000,
            new_value=5000,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=5)

    print("3. T1: COMMIT at T+10s")
    FRM.write_log(ExecutionResult(transaction_id=1, query="COMMIT"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=10)

    print("\n4. ** SAVING RECOVERY POINT ** at T+15s")
    recovery_point = base_time + timedelta(seconds=15)
    print(f"   Recovery point: {recovery_point.strftime('%H:%M:%S')}")

    print("\n5. Transaction T2: BEGIN and operation at T+20s")
    FRM.write_log(ExecutionResult(transaction_id=2, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=20)
    FRM.write_log(
        ExecutionResult(
            transaction_id=2,
            query="UPDATE accounts SET balance = 7000 WHERE id = 2",
            table="accounts",
            key=2,
            old_value=4000,
            new_value=7000,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=25)

    print("6. T2: COMMIT at T+30s")
    FRM.write_log(ExecutionResult(transaction_id=2, query="COMMIT"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=30)

    print("\n7. Transaction T3: BEGIN at T+7s but never commits")
    FRM.write_log(ExecutionResult(transaction_id=3, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=7)
    FRM.write_log(
        ExecutionResult(
            transaction_id=3,
            query="UPDATE accounts SET balance = 9000 WHERE id = 3",
            table="accounts",
            key=3,
            old_value=8000,
            new_value=9000,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=9)

    print("\n8. Current state (T+35s):")
    print("   - accounts.1 balance = 5000 (T1 committed)")
    print("   - accounts.2 balance = 7000 (T2 committed)")
    print("   - accounts.3 balance = 9000 (T3 uncommitted)")

    # Write logs to disk
    import json

    for rec in FRM.buffer:
        line = json.dumps(rec.to_dict(), separators=(",", ":")) + "\n"
        with open("wal.log", "a") as f:
            f.write(line)

    # Clear in-memory state
    FRM.buffer = []
    FRM.active_tx = []

    print(f"\n9. ** RECOVERING TO TIMESTAMP ** {recovery_point.strftime('%H:%M:%S')}")
    print("   This will restore the system to T+15s state...")
    print("\n--- Recovery Output ---")
    FRM.recover(RecoverCriteria(timestamp=recovery_point))
    print("--- End Recovery Output ---")

    print("\n10. Recovery Complete!")
    print("\n    Timeline Analysis:")
    print("    • T+0s:  T1 START")
    print("    • T+5s:  T1 OP (balance 1: 3000→5000)")
    print("    • T+7s:  T3 START")
    print("    • T+9s:  T3 OP (balance 3: 8000→9000)")
    print("    • T+10s: T1 COMMIT")
    print("    • T+15s: 🎯 RECOVERY POINT")
    print("    • T+20s: T2 START (after recovery point)")
    print("    • T+25s: T2 OP (balance 2: 4000→7000)")
    print("    • T+30s: T2 COMMIT")

    print("\n    Recovery Actions:")
    print("    ✓ REDO: T1's operation (committed before recovery point)")
    print("    ✓ UNDO: T2's operation (happened after recovery point)")
    print("    ✓ UNDO: T3's operation (uncommitted at recovery point)")

    print("\n    Final state at T+15s:")
    print("    • accounts.1 balance = 5000 (T1 committed)")
    print("    • accounts.2 balance = 4000 (T2 rolled back)")
    print("    • accounts.3 balance = 8000 (T3 rolled back)")

    print_buffer_state()

    # Clean up
    for file in ["wal.log", "last_checkpoint.json"]:
        if os.path.exists(file):
            os.remove(file)


def main():
    """Run all demonstration scenarios."""
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 15 + "FAILURE RECOVERY MANAGER DEMO" + " " * 24 + "║")
    print("║" + " " * 20 + "Recovery Mechanisms" + " " * 29 + "║")
    print("╚" + "═" * 68 + "╝")

    try:
        scenario_1_simple_abort()
        input("\n\nPress Enter to continue to next scenario...")

        scenario_2_concurrent_abort()
        input("\n\nPress Enter to continue to next scenario...")

        scenario_3_crash_recovery()
        input("\n\nPress Enter to continue to next scenario...")

        scenario_4_timestamp_recovery()

        print_separator("DEMO COMPLETE")
        print("\nAll recovery scenarios executed successfully!")
        print("\nKey Takeaways:")
        print("  • ABORT triggers automatic rollback of that transaction only")
        print("  • Recovery is isolated - other transactions are not affected")
        print("  • Crash recovery uses ARIES: Analysis → Redo → Undo")
        print("  • Timestamp recovery restores system to any point in time")
        print("  • System maintains ACID properties through WAL and recovery")

    except Exception as e:
        print(f"\n\nError during demo: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
