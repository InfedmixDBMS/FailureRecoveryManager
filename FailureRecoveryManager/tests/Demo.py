#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from FailureRecoveryManager.classes.FailureRecoveryManager import (
    FailureRecoveryManager as FRM,
)
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria


def print_separator(title):
    """Print a formatted section separator."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_buffer_state():
    """Print current buffer contents."""
    print("\nCurrent state:")
    print(f"  Last LSN: {FRM.last_lsn}")
    print(f"  Active transactions: {FRM.active_tx}")
    print(f"  Buffer size: {len(FRM.buffer)} records")
    if FRM.buffer:
        print("  Buffer contents:")
        for log in FRM.buffer:
            print(f"    {repr(log)}")


def scenario_1_recover_until_transaction():
    """Demonstrate recovery until a specific transaction."""
    print_separator("SCENARIO 1: Recover Until Transaction (Specification)")

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    print(
        "\nConcept: Undo everything BACKWARD from end UNTIL we reach target transaction's START"
    )
    print("   This restores database to state BEFORE target transaction began")

    print("\n--- Building Timeline ---")

    print("\n1. T1: Complete transaction")
    FRM.write_log(ExecutionResult(transaction_id=1, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE accounts SET balance = 200 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=100,
            new_value=200,
        )
    )
    FRM.write_log(ExecutionResult(transaction_id=1, query="COMMIT"))
    print("   ✓ T1: balance 100 → 200 (COMMITTED)")

    print("\n2. T2: Target transaction (we'll recover UNTIL here)")
    FRM.write_log(ExecutionResult(transaction_id=2, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=2,
            query="UPDATE accounts SET balance = 300 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=200,
            new_value=300,
        )
    )
    FRM.write_log(ExecutionResult(transaction_id=2, query="COMMIT"))
    print("   ✓ T2: balance 200 → 300 (COMMITTED) ← TARGET")

    print("\n3. T3: Transaction after target")
    FRM.write_log(ExecutionResult(transaction_id=3, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=3,
            query="UPDATE accounts SET balance = 400 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=300,
            new_value=400,
        )
    )
    FRM.write_log(ExecutionResult(transaction_id=3, query="COMMIT"))
    print("   ✓ T3: balance 300 → 400 (COMMITTED)")

    print("\n4. T4: Another transaction after target")
    FRM.write_log(ExecutionResult(transaction_id=4, query="BEGIN TRANSACTION"))
    FRM.write_log(
        ExecutionResult(
            transaction_id=4,
            query="UPDATE accounts SET balance = 500 WHERE id = 1",
            table="accounts",
            key=1,
            old_value=400,
            new_value=500,
        )
    )
    print("   ✓ T4: balance 400 → 500 (NOT COMMITTED YET)")

    print("\n--- Current State ---")
    print("  Timeline:")
    print("    LSN 1: T1 START")
    print("    LSN 2: T1 OP (balance: 100→200)")
    print("    LSN 3: T1 COMMIT")
    print("    LSN 4: T2 START           ← TARGET (recover until here)")
    print("    LSN 5: T2 OP (balance: 200→300)")
    print("    LSN 6: T2 COMMIT")
    print("    LSN 7: T3 START")
    print("    LSN 8: T3 OP (balance: 300→400)")
    print("    LSN 9: T3 COMMIT")
    print("    LSN 10: T4 START")
    print("    LSN 11: T4 OP (balance: 400→500)")
    print("\n  Current balance: 500")

    print("\n--- Executing Recovery ---")
    print("  Command: FRM.recover(RecoverCriteria(transaction_id=2))")
    print("\n  Algorithm:")
    print("    1. Start from LAST entry (LSN 11)")
    print("    2. Go BACKWARD")
    print("    3. UNDO each operation")
    print("    4. STOP when we reach T2 START (LSN 4)")
    print("\n--- Recovery Output ---")

    FRM.recover(RecoverCriteria(transaction_id=2))

    print("--- End Recovery Output ---")

    print("\n--- Recovery Result ---")
    print("  ✓ Undone: T4's operation (500→400)")
    print("  ✓ Undone: T3's operation (400→300)")
    print("  ✓ Undone: T2's operation (300→200)")
    print("  ✓ Stopped at: T2 START (LSN 4)")
    print("  ✗ NOT undone: T1's operation (remains 200)")
    print("\n  Final balance: 200 (state BEFORE T2 started)")
    print("  Active transactions: [] (all were after T2)")


def scenario_2_recover_until_timestamp():
    """Demonstrate timestamp-based recovery."""
    print_separator("SCENARIO 2: Recover Until Timestamp (Specification)")

    # Reset state
    FRM.buffer = []
    FRM.last_lsn = 0
    FRM.active_tx = []

    print(
        "\nConcept: Undo everything BACKWARD from end UNTIL we reach target timestamp"
    )
    print("   This restores database to state AT that specific time")

    base_time = datetime.now()
    print(f"\n--- Building Timeline (Base: {base_time.strftime('%H:%M:%S')}) ---")

    print("\n1. T1: Transaction at T+0s")
    FRM.write_log(ExecutionResult(transaction_id=1, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time
    FRM.write_log(
        ExecutionResult(
            transaction_id=1,
            query="UPDATE price SET value = 150 WHERE id = 1",
            table="price",
            key=1,
            old_value=100,
            new_value=150,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=5)
    FRM.write_log(ExecutionResult(transaction_id=1, query="COMMIT"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=10)
    print("   T+0s:  T1 START")
    print("   T+5s:  T1 OP (price: 100→150)")
    print("   T+10s: T1 COMMIT")

    target_time = base_time + timedelta(seconds=15)
    print("\n   T+15s: RECOVERY TARGET TIME")

    print("\n2. T2: Transaction at T+20s (after target)")
    FRM.write_log(ExecutionResult(transaction_id=2, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=20)
    FRM.write_log(
        ExecutionResult(
            transaction_id=2,
            query="UPDATE price SET value = 200 WHERE id = 1",
            table="price",
            key=1,
            old_value=150,
            new_value=200,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=25)
    FRM.write_log(ExecutionResult(transaction_id=2, query="COMMIT"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=30)
    print("   T+20s: T2 START (after target)")
    print("   T+25s: T2 OP (price: 150→200)")
    print("   T+30s: T2 COMMIT")

    print("\n3. T3: Transaction at T+35s (after target)")
    FRM.write_log(ExecutionResult(transaction_id=3, query="BEGIN TRANSACTION"))
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=35)
    FRM.write_log(
        ExecutionResult(
            transaction_id=3,
            query="UPDATE price SET value = 250 WHERE id = 1",
            table="price",
            key=1,
            old_value=200,
            new_value=250,
        )
    )
    FRM.buffer[-1].timestamp = base_time + timedelta(seconds=40)
    print("   T+35s: T3 START (after target)")
    print("   T+40s: T3 OP (price: 200→250)")

    print("\n--- Current State ---")
    print("  Current time: T+45s")
    print("  Current price: 250")
    print(f"  Target time: T+15s ({target_time.strftime('%H:%M:%S')})")

    print("\n--- Executing Recovery ---")
    print(
        f"  Command: FRM.recover(RecoverCriteria(timestamp={target_time.strftime('%H:%M:%S')}))"
    )
    print("\n  Algorithm:")
    print("    1. Start from LAST entry")
    print("    2. Go BACKWARD")
    print("    3. UNDO operations with timestamp > target")
    print("    4. STOP when we reach entry with timestamp ≤ target")
    print("\n--- Recovery Output ---")

    FRM.recover(RecoverCriteria(timestamp=target_time))

    print("--- End Recovery Output ---")

    print("\n--- Recovery Result ---")
    print("  ✓ Undone: T3's operation (250→200)")
    print("  ✓ Undone: T2's operation (200→150)")
    print("  ✗ NOT undone: T1's operation (before target time)")
    print(f"\n  Final price: 150 (state at {target_time.strftime('%H:%M:%S')})")
    print("  Active transactions at target time: []")


def scenario_3_comparison_with_aries():
    """Show the difference between specification and ARIES recovery."""
    print_separator("SCENARIO 3: Specification vs ARIES Comparison")

    print("\nUnderstanding The Difference:\n")

    print("SPECIFICATION INTERPRETATION (What we implemented):")
    print("  • Goal: Rollback TO a specific point")
    print("  • Method: Go backward, undo everything until criteria met")
    print("  • Effect: All operations after target are removed")
    print("  • Use case: Time travel / restore to checkpoint")

    print("\nARIES ALGORITHM (Previous implementation):")
    print("  • Goal: Recover FROM a crash")
    print("  • Method: Analysis → Redo → Undo")
    print("  • Effect: Preserve committed, remove only uncommitted")
    print("  • Use case: System crash recovery")

    print("\n--- Example Scenario ---")
    print("\nTimeline:")
    print("  T1: Complete (balance: 100→200)")
    print("  T2: Complete (balance: 200→300) ← Target")
    print("  T3: Complete (balance: 300→400)")
    print("  T4: Incomplete (balance: 400→500)")

    print("\nSpecification recover(transaction_id=T2):")
    print("  Result: balance = 200")
    print("  Reason: Undid everything from end until T2 START")
    print("  Affected: T4, T3, T2 all undone")

    print("\nARIES recover (if we had it):")
    print("  Result: balance = 400")
    print("  Reason: Preserve committed (T1,T2,T3), undo uncommitted (T4)")
    print("  Affected: Only T4 undone")

    print("\n--- When To Use Each ---")
    print("\n✓ Use Specification Approach When:")
    print("  • You want to restore to a known good state")
    print("  • You discovered an error and need to go back")
    print("  • You need point-in-time recovery")
    print("  • Example: 'Undo all changes after 3pm yesterday'")

    print("\n✓ Use ARIES Approach When:")
    print("  • System crashed unexpectedly")
    print("  • You want to preserve committed work")
    print("  • You need atomicity and durability")
    print("  • Example: 'Server rebooted, recover database'")


def main():
    """Run all demonstration scenarios."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print(
        "║" + " " * 15 + "SPECIFICATION-BASED RECOVERY DEMONSTRATION" + " " * 21 + "║"
    )
    print("║" + " " * 20 + "Backward Recovery Until Criteria Met" + " " * 22 + "║")
    print("╚" + "=" * 78 + "╝")

    try:
        scenario_1_recover_until_transaction()
        input("\n\nPress Enter to continue to next scenario...")

        scenario_2_recover_until_timestamp()
        input("\n\nPress Enter to continue to next scenario...")

        scenario_3_comparison_with_aries()

        print_separator("DEMONSTRATION COMPLETE")
        print("\nAll scenarios executed successfully!")

        print("\nKey Takeaways:")
        print("  1. Specification recovery is BACKWARD from end")
        print("  2. We UNDO everything UNTIL criteria is met")
        print("  3. transaction_id: Stop at that transaction's START")
        print("  4. timestamp: Stop at entries before that time")
        print("  5. This is different from ARIES (selective undo)")
        print("  6. Best for 'restore to checkpoint' scenarios")

        # Clean up
        for file in ["wal.log", "last_checkpoint.json"]:
            if os.path.exists(file):
                os.remove(file)

    except Exception as e:
        print(f"\n\n❌ Error during demo: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
