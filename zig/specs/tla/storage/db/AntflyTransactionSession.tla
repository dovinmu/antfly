\* Copyright 2026 Antfly, Inc.
\*
\* Licensed under the Apache License, Version 2.0 (the "License");
\* you may not use this file except in compliance with the License.
\* You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.

------------------------ MODULE AntflyTransactionSession ------------------------
(*
  Session/savepoint and transaction recovery overlay for the current Zig
  transaction implementation.

  Concrete contracts modeled:
    - writes and transforms stage intent data that is invisible until commit;
    - rollback-to-savepoint discards later staged data;
    - all participants prepare before a coordinator commit publishes;
    - finalized orphan recovery follows the durable commit/abort decision;
    - stale pending recovery aborts without publishing;
    - the stable-session handoff retains the transaction ID, coordinator group,
      coordinator table, and one exact terminal outcome:
        "committed", "committed_visibility_pending", or
        "committed_recovery_pending";
    - retry reads that durable handoff without staging writes or applying
      transforms again;
    - cleanup cannot discard a terminal handoff while propagation debt remains.
*)

EXTENDS Naturals, TLC

CONSTANTS
    BuggyRollbackExposeDiscarded,
    BuggyRecoveryWrongDecision,
    BuggyCleanupUnresolved

Participants == {"local", "remote"}
Statuses == {"idle", "open", "prepared", "committed", "aborted"}
TerminalOutcomes == {"none", "committed", "committed_visibility_pending",
                     "committed_recovery_pending"}
NoCoordinatorTable == "none"
StableCoordinatorTable == "docs"
NoCoordinatorGroup == 0
StableCoordinatorGroup == 7
MaxStage == 2
MaxVisible == 4
MaxSessions == 2
MaxRetry == 1

VARIABLES
    status,
    baseVisible,
    staged,
    intentCount,
    savepoint,
    participantPrepared,
    participantResolved,
    visible,
    identityRows,
    conflict,
    stalePending,
    rollbackDiscarded,
    cleanedWithUnresolved,
    txnId,
    writeApplications,
    transformApplications,
    coordinatorGroup,
    coordinatorTable,
    terminalOutcome,
    propagationPending,
    visibilityPending,
    terminalRetained,
    terminalTxnId,
    terminalWriteApplications,
    terminalTransformApplications,
    retryCount

vars == <<status, baseVisible, staged, intentCount, savepoint,
          participantPrepared, participantResolved, visible, identityRows,
          conflict, stalePending, rollbackDiscarded, cleanedWithUnresolved,
          txnId, writeApplications, transformApplications, coordinatorGroup,
          coordinatorTable, terminalOutcome, propagationPending,
          visibilityPending, terminalRetained, terminalTxnId,
          terminalWriteApplications, terminalTransformApplications, retryCount>>

Init ==
    /\ status = "idle"
    /\ baseVisible = 0
    /\ staged = 0
    /\ intentCount = 0
    /\ savepoint = 0
    /\ participantPrepared = [p \in Participants |-> FALSE]
    /\ participantResolved = [p \in Participants |-> FALSE]
    /\ visible = 0
    /\ identityRows = 0
    /\ conflict = FALSE
    /\ stalePending = FALSE
    /\ rollbackDiscarded = FALSE
    /\ cleanedWithUnresolved = FALSE
    /\ txnId = 0
    /\ writeApplications = 0
    /\ transformApplications = 0
    /\ coordinatorGroup = NoCoordinatorGroup
    /\ coordinatorTable = NoCoordinatorTable
    /\ terminalOutcome = "none"
    /\ propagationPending = FALSE
    /\ visibilityPending = FALSE
    /\ terminalRetained = FALSE
    /\ terminalTxnId = 0
    /\ terminalWriteApplications = 0
    /\ terminalTransformApplications = 0
    /\ retryCount = 0

AllPrepared(prepared) ==
    \A p \in Participants: prepared[p]

AllResolved(resolved) ==
    \A p \in Participants: resolved[p]

AllPreparedParticipantsResolved ==
    \A p \in Participants: participantPrepared[p] => participantResolved[p]

OutcomeForDebt(propagation, visibility) ==
    IF propagation THEN "committed_recovery_pending"
    ELSE IF visibility THEN "committed_visibility_pending"
    ELSE "committed"

BeginSession ==
    /\ status = "idle"
    /\ txnId < MaxSessions
    /\ status' = "open"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ intentCount' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ txnId' = txnId + 1
    /\ writeApplications' = 0
    /\ transformApplications' = 0
    /\ coordinatorGroup' = NoCoordinatorGroup
    /\ coordinatorTable' = NoCoordinatorTable
    /\ terminalOutcome' = "none"
    /\ propagationPending' = FALSE
    /\ visibilityPending' = FALSE
    /\ terminalRetained' = FALSE
    /\ terminalTxnId' = 0
    /\ terminalWriteApplications' = 0
    /\ terminalTransformApplications' = 0
    /\ retryCount' = 0
    /\ UNCHANGED <<visible, identityRows, cleanedWithUnresolved>>

StageWrite ==
    /\ status = "open"
    /\ staged < MaxStage
    /\ baseVisible + staged < MaxVisible
    /\ writeApplications < MaxStage
    /\ staged' = staged + 1
    /\ intentCount' = intentCount + 1
    /\ writeApplications' = writeApplications + 1
    /\ transformApplications' = transformApplications + 1
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved,
                  txnId, coordinatorGroup, coordinatorTable, terminalOutcome,
                  propagationPending, visibilityPending, terminalRetained,
                  terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

CreateSavepoint ==
    /\ status = "open"
    /\ savepoint' = staged
    /\ UNCHANGED <<status, baseVisible, staged, intentCount,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

RollbackToSavepoint ==
    /\ status = "open"
    /\ savepoint <= staged
    /\ staged' = savepoint
    /\ intentCount' = savepoint
    /\ rollbackDiscarded' = (staged > savepoint)
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, cleanedWithUnresolved, txnId,
                  writeApplications, transformApplications, coordinatorGroup,
                  coordinatorTable, terminalOutcome, propagationPending,
                  visibilityPending, terminalRetained, terminalTxnId,
                  terminalWriteApplications, terminalTransformApplications,
                  retryCount>>

BuggyRollbackToSavepoint ==
    /\ BuggyRollbackExposeDiscarded
    /\ status = "open"
    /\ savepoint < staged
    /\ staged' = savepoint
    /\ intentCount' = savepoint
    /\ visible' = baseVisible + staged
    /\ identityRows' = visible'
    /\ rollbackDiscarded' = TRUE
    /\ UNCHANGED <<status, baseVisible, savepoint, participantPrepared,
                  participantResolved, conflict, stalePending,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

PrepareParticipant(p) ==
    /\ p \in Participants
    /\ status = "open"
    /\ intentCount > 0
    /\ ~conflict
    /\ participantPrepared' = [participantPrepared EXCEPT ![p] = TRUE]
    /\ status' =
        IF AllPrepared([participantPrepared EXCEPT ![p] = TRUE])
        THEN "prepared"
        ELSE status
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantResolved, visible, identityRows, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved,
                  txnId, writeApplications, transformApplications,
                  coordinatorGroup, coordinatorTable, terminalOutcome,
                  propagationPending, visibilityPending, terminalRetained,
                  terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

DetectConflict ==
    /\ status = "open"
    /\ conflict' = TRUE
    /\ status' = "aborted"
    /\ intentCount' = 0
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, visible, identityRows, stalePending,
                  rollbackDiscarded, cleanedWithUnresolved, txnId,
                  writeApplications, transformApplications, coordinatorGroup,
                  coordinatorTable, terminalOutcome, propagationPending,
                  visibilityPending, terminalRetained, terminalTxnId,
                  terminalWriteApplications, terminalTransformApplications,
                  retryCount>>

Commit ==
    /\ status = "prepared"
    /\ status' = "committed"
    /\ visible' = baseVisible + intentCount
    /\ identityRows' = baseVisible + intentCount
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ coordinatorGroup' = StableCoordinatorGroup
    /\ coordinatorTable' = StableCoordinatorTable
    /\ terminalOutcome' = "committed_recovery_pending"
    /\ propagationPending' = TRUE
    /\ visibilityPending' = TRUE
    /\ terminalRetained' = TRUE
    /\ terminalTxnId' = txnId
    /\ terminalWriteApplications' = writeApplications
    /\ terminalTransformApplications' = transformApplications
    /\ retryCount' = 0
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications>>

Abort ==
    /\ status \in {"open", "prepared"}
    /\ status' = "aborted"
    /\ visible' = baseVisible
    /\ identityRows' = baseVisible
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

MarkStalePending ==
    /\ status = "open"
    /\ stalePending' = TRUE
    /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

RecoverStalePending ==
    /\ status = "open"
    /\ stalePending
    /\ status' = "aborted"
    /\ visible' = baseVisible
    /\ identityRows' = baseVisible
    /\ intentCount' = 0
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

CrashFinalizeCommittedOrphan ==
    /\ status = "prepared"
    /\ intentCount > 0
    /\ status' = "committed"
    /\ coordinatorGroup' = StableCoordinatorGroup
    /\ coordinatorTable' = StableCoordinatorTable
    /\ terminalOutcome' = "committed_recovery_pending"
    /\ propagationPending' = TRUE
    /\ visibilityPending' = TRUE
    /\ terminalRetained' = TRUE
    /\ terminalTxnId' = txnId
    /\ terminalWriteApplications' = writeApplications
    /\ terminalTransformApplications' = transformApplications
    /\ retryCount' = 0
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications>>

CrashFinalizeAbortedOrphan ==
    /\ status \in {"open", "prepared"}
    /\ intentCount > 0
    /\ status' = "aborted"
    /\ stalePending' = FALSE
    /\ UNCHANGED <<baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  terminalOutcome, propagationPending, visibilityPending,
                  terminalRetained, terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

RecoverFinalizedIntents ==
    /\ status \in {"committed", "aborted"}
    /\ intentCount > 0
    /\ visible' = IF status = "committed" THEN baseVisible + intentCount ELSE baseVisible
    /\ identityRows' = visible'
    /\ intentCount' = 0
    /\ UNCHANGED <<status, baseVisible, staged, savepoint, participantPrepared,
                  participantResolved, conflict, stalePending,
                  rollbackDiscarded, cleanedWithUnresolved, txnId,
                  writeApplications, transformApplications, coordinatorGroup,
                  coordinatorTable, terminalOutcome, propagationPending,
                  visibilityPending, terminalRetained, terminalTxnId,
                  terminalWriteApplications, terminalTransformApplications,
                  retryCount>>

BuggyRecoverWrongDecision ==
    /\ BuggyRecoveryWrongDecision
    /\ status = "aborted"
    /\ intentCount > 0
    /\ visible' = baseVisible + intentCount
    /\ identityRows' = visible'
    /\ intentCount' = 0
    /\ UNCHANGED <<status, baseVisible, staged, savepoint,
                  participantPrepared, participantResolved, conflict,
                  stalePending, rollbackDiscarded, cleanedWithUnresolved,
                  txnId, writeApplications, transformApplications,
                  coordinatorGroup, coordinatorTable, terminalOutcome,
                  propagationPending, visibilityPending, terminalRetained,
                  terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications, retryCount>>

ResolveParticipant(p) ==
    LET newResolved == [participantResolved EXCEPT ![p] = TRUE]
        allResolved == AllResolved(newResolved)
    IN /\ p \in Participants
       /\ status \in {"committed", "aborted"}
       /\ participantPrepared[p]
       /\ participantResolved' = newResolved
       /\ propagationPending' =
            IF status = "committed" THEN ~allResolved ELSE FALSE
       /\ terminalOutcome' =
            IF status = "committed"
            THEN OutcomeForDebt(~allResolved, visibilityPending)
            ELSE terminalOutcome
       /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                     participantPrepared, visible, identityRows, conflict,
                     stalePending, rollbackDiscarded, cleanedWithUnresolved,
                     txnId, writeApplications, transformApplications,
                     coordinatorGroup, coordinatorTable, visibilityPending,
                     terminalRetained, terminalTxnId,
                     terminalWriteApplications, terminalTransformApplications,
                     retryCount>>

ReachVisibility ==
    /\ status = "committed"
    /\ terminalRetained
    /\ ~propagationPending
    /\ visibilityPending
    /\ visibilityPending' = FALSE
    /\ terminalOutcome' = "committed"
    /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  propagationPending, terminalRetained, terminalTxnId,
                  terminalWriteApplications, terminalTransformApplications,
                  retryCount>>

StableRetry ==
    /\ status = "committed"
    /\ terminalRetained
    /\ retryCount < MaxRetry
    /\ retryCount' = retryCount + 1
    /\ terminalOutcome' = OutcomeForDebt(propagationPending, visibilityPending)
    /\ UNCHANGED <<status, baseVisible, staged, intentCount, savepoint,
                  participantPrepared, participantResolved, visible,
                  identityRows, conflict, stalePending, rollbackDiscarded,
                  cleanedWithUnresolved, txnId, writeApplications,
                  transformApplications, coordinatorGroup, coordinatorTable,
                  propagationPending, visibilityPending, terminalRetained,
                  terminalTxnId, terminalWriteApplications,
                  terminalTransformApplications>>

Cleanup ==
    /\ status \in {"committed", "aborted"}
    /\ intentCount = 0
    /\ AllPreparedParticipantsResolved
    /\ (status # "committed" \/ ~propagationPending)
    /\ status' = "idle"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ coordinatorGroup' = NoCoordinatorGroup
    /\ coordinatorTable' = NoCoordinatorTable
    /\ terminalOutcome' = "none"
    /\ propagationPending' = FALSE
    /\ visibilityPending' = FALSE
    /\ terminalRetained' = FALSE
    /\ terminalTxnId' = 0
    /\ terminalWriteApplications' = 0
    /\ terminalTransformApplications' = 0
    /\ retryCount' = 0
    /\ UNCHANGED <<intentCount, visible, identityRows, cleanedWithUnresolved,
                  txnId, writeApplications, transformApplications>>

BuggyCleanupBeforeResolved ==
    /\ BuggyCleanupUnresolved
    /\ status \in {"committed", "aborted"}
    /\ intentCount = 0
    /\ ~AllPreparedParticipantsResolved
    /\ status' = "idle"
    /\ baseVisible' = visible
    /\ staged' = 0
    /\ savepoint' = 0
    /\ participantPrepared' = [p \in Participants |-> FALSE]
    /\ participantResolved' = [p \in Participants |-> FALSE]
    /\ conflict' = FALSE
    /\ stalePending' = FALSE
    /\ rollbackDiscarded' = FALSE
    /\ cleanedWithUnresolved' = TRUE
    /\ coordinatorGroup' = NoCoordinatorGroup
    /\ coordinatorTable' = NoCoordinatorTable
    /\ terminalOutcome' = "none"
    /\ propagationPending' = FALSE
    /\ visibilityPending' = FALSE
    /\ terminalRetained' = FALSE
    /\ terminalTxnId' = 0
    /\ terminalWriteApplications' = 0
    /\ terminalTransformApplications' = 0
    /\ retryCount' = 0
    /\ UNCHANGED <<intentCount, visible, identityRows, txnId,
                  writeApplications, transformApplications>>

Next ==
    \/ BeginSession
    \/ StageWrite
    \/ CreateSavepoint
    \/ RollbackToSavepoint
    \/ BuggyRollbackToSavepoint
    \/ DetectConflict
    \/ Commit
    \/ Abort
    \/ MarkStalePending
    \/ RecoverStalePending
    \/ CrashFinalizeCommittedOrphan
    \/ CrashFinalizeAbortedOrphan
    \/ RecoverFinalizedIntents
    \/ BuggyRecoverWrongDecision
    \/ ReachVisibility
    \/ StableRetry
    \/ Cleanup
    \/ BuggyCleanupBeforeResolved
    \/ \E p \in Participants:
        \/ PrepareParticipant(p)
        \/ ResolveParticipant(p)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ status \in Statuses
    /\ baseVisible \in 0..MaxVisible
    /\ staged \in 0..MaxStage
    /\ intentCount \in 0..MaxStage
    /\ savepoint \in 0..MaxStage
    /\ participantPrepared \in [Participants -> BOOLEAN]
    /\ participantResolved \in [Participants -> BOOLEAN]
    /\ visible \in 0..MaxVisible
    /\ identityRows \in 0..MaxVisible
    /\ conflict \in BOOLEAN
    /\ stalePending \in BOOLEAN
    /\ rollbackDiscarded \in BOOLEAN
    /\ cleanedWithUnresolved \in BOOLEAN
    /\ txnId \in 0..MaxSessions
    /\ writeApplications \in 0..MaxStage
    /\ transformApplications \in 0..MaxStage
    /\ coordinatorGroup \in {NoCoordinatorGroup, StableCoordinatorGroup}
    /\ coordinatorTable \in {NoCoordinatorTable, StableCoordinatorTable}
    /\ terminalOutcome \in TerminalOutcomes
    /\ propagationPending \in BOOLEAN
    /\ visibilityPending \in BOOLEAN
    /\ terminalRetained \in BOOLEAN
    /\ terminalTxnId \in 0..MaxSessions
    /\ terminalWriteApplications \in 0..MaxStage
    /\ terminalTransformApplications \in 0..MaxStage
    /\ retryCount \in 0..MaxRetry

NoVisibleUncommittedWrites ==
    status \in {"open", "prepared", "aborted"} => visible = baseVisible

CommitRequiresAllPrepared ==
    status = "committed" => AllPrepared(participantPrepared)

CommitPublishesStagedWrites ==
    status = "committed" /\ intentCount = 0 => visible = baseVisible + staged

AbortPreservesVisibility ==
    status = "aborted" => visible = baseVisible

SavepointWithinStage ==
    savepoint <= staged

RollbackDiscardedNotVisible ==
    status = "open" /\ rollbackDiscarded => visible = baseVisible

IntentCountWithinStagedWrites ==
    intentCount <= staged

IdentityRowsMatchVisibleDocs ==
    identityRows = visible

ParticipantRecoveryDoesNotPublishAbortedData ==
    status = "aborted" => /\ visible = baseVisible /\ identityRows = baseVisible

CleanupRequiresAllResolved ==
    ~cleanedWithUnresolved

IdleHasNoDanglingIntents ==
    status = "idle" => intentCount = 0

TerminalHandoffIsComplete ==
    terminalRetained =>
        /\ status = "committed"
        /\ coordinatorGroup = StableCoordinatorGroup
        /\ coordinatorTable = StableCoordinatorTable
        /\ terminalOutcome \in TerminalOutcomes \ {"none"}
        /\ terminalTxnId = txnId

StableRetryPreservesIdentityAndWrites ==
    terminalRetained =>
        /\ terminalTxnId = txnId
        /\ terminalWriteApplications = writeApplications
        /\ terminalTransformApplications = transformApplications

TerminalOutcomeReflectsDebt ==
    terminalRetained =>
        terminalOutcome = OutcomeForDebt(propagationPending, visibilityPending)

PendingPropagationRetainsTerminal ==
    status = "committed" /\ propagationPending => terminalRetained

Safety ==
    /\ TypeOK
    /\ NoVisibleUncommittedWrites
    /\ CommitRequiresAllPrepared
    /\ CommitPublishesStagedWrites
    /\ AbortPreservesVisibility
    /\ SavepointWithinStage
    /\ RollbackDiscardedNotVisible
    /\ IntentCountWithinStagedWrites
    /\ IdentityRowsMatchVisibleDocs
    /\ ParticipantRecoveryDoesNotPublishAbortedData
    /\ CleanupRequiresAllResolved
    /\ IdleHasNoDanglingIntents
    /\ TerminalHandoffIsComplete
    /\ StableRetryPreservesIdentityAndWrites
    /\ TerminalOutcomeReflectsDebt
    /\ PendingPropagationRetainsTerminal

=============================================================================
