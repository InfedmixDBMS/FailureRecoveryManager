import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from FailureRecoveryManager.types.ExecutionResult import ExecutionResult
from FailureRecoveryManager.classes.FailureRecoveryManager import FailureRecoveryManager

def main():
    begin_exec = ExecutionResult(
        message="Transaction started",
        transaction_id=123,
        query="BEGIN TRANSACTION"
    )

    commit_exec = ExecutionResult(
        message="Transaction committed",
        transaction_id=123,
        query="COMMIT"
    )

    begin_exec1 = ExecutionResult(
        message="Transaction started",
        transaction_id=124,
        query="BEGIN TRANSACTION"
    )

    abort_exec = ExecutionResult(
        message="Transaction aborted",
        transaction_id=124,
        query="ABORT"
    )

    FailureRecoveryManager.write_log(begin_exec)
    FailureRecoveryManager.write_log(commit_exec)
    FailureRecoveryManager.write_log(begin_exec1)
    FailureRecoveryManager.write_log(abort_exec)

    print("FailureRecoveryManager.buffer:")
    for rec in FailureRecoveryManager.buffer:
        print(repr(rec))

if __name__ == "__main__":
    main()

# RESULTS SHOULD BE:
# FailureRecoveryManager.buffer:
# 1: <123, Start>
# 2: <123, Commit>
# 3: <124, Start>
# 4: <124, Abort>
