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

---------------- MODULE AntflyDistributedTransactionRecovery ----------------
(*
  Focused model of the durable distributed-transaction recovery boundary in:
    - pkg/antfly/src/api/distributed_txn.zig
    - pkg/antfly/src/api/transactions.zig
    - pkg/antfly/src/api/table_writes.zig
    - pkg/antfly/src/storage/transactions.zig

  The model separates a durable coordinator decision from phase-two delivery,
  visibility, retry, and cleanup. A topology epoch fences the initial attempt,
  while recovery may redeliver an already-durable decision after routing has
  changed. Legacy records keep unknown prepare/coordinator facts explicit.
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Participants,
    Coordinator,
    MaxEpoch,
    MaxRetry,
    BuggyCommitWithoutPrepare,
    BuggyCommitStaleEpoch,
    BuggyAbortAfterCommit,
    BuggyPreparedFollowerAbort,
    BuggyRetryNewTransaction,
    BuggyCleanupPendingPropagation,
    BuggyEarlySuccess

ASSUME Coordinator \in Participants
ASSUME Cardinality(Participants) >= 2
ASSUME MaxEpoch >= 2
ASSUME MaxRetry >= 2

ParticipantPhases == {"none", "begun", "prepared", "committed", "aborted"}
AmbiguousPhases == {"none", "begin", "prepare", "resolve"}
Decisions == {"none", "committed", "aborted"}
Responses == {"none", "committed", "committed_visibility_pending",
             "committed_recovery_pending"}

VARIABLES
    topologyEpoch,
    attemptEpoch,
    txnGeneration,
    transformApplications,
    retryCount,
    phase,
    preparedKnown,
    coordinatorKnown,
    everPrepared,
    protectedPending,
    delivered,
    acknowledged,
    presumedAborted,
    ambiguousPhase,
    decision,
    response,
    everCommitted,
    decisionEpochValid,
    retainTerminal,
    propagationPending,
    visibilityReached,
    cleaned

vars == <<topologyEpoch, attemptEpoch, txnGeneration, transformApplications,
          retryCount, phase, preparedKnown, coordinatorKnown, everPrepared,
          protectedPending, delivered, acknowledged, presumedAborted,
          ambiguousPhase, decision, response, everCommitted,
          decisionEpochValid, retainTerminal, propagationPending,
          visibilityReached, cleaned>>

Init ==
    /\ topologyEpoch = 1
    /\ attemptEpoch = 1
    /\ txnGeneration = 1
    /\ transformApplications = 1
    /\ retryCount = 0
    /\ phase = [p \in Participants |-> "none"]
    /\ preparedKnown = [p \in Participants |-> TRUE]
    /\ coordinatorKnown = [p \in Participants |-> TRUE]
    /\ everPrepared = {}
    /\ protectedPending = {}
    /\ delivered = {}
    /\ acknowledged = {}
    /\ presumedAborted = {}
    /\ ambiguousPhase = [p \in Participants |-> "none"]
    /\ decision = "none"
    /\ response = "none"
    /\ everCommitted = FALSE
    /\ decisionEpochValid = TRUE
    /\ retainTerminal = TRUE
    /\ propagationPending = FALSE
    /\ visibilityReached = FALSE
    /\ cleaned = FALSE

BeginParticipant(p) ==
    /\ p \in Participants
    /\ phase[p] = "none"
    /\ phase' = [phase EXCEPT ![p] = "begun"]
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  response, everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

BeginParticipantAmbiguous(p) ==
    /\ p \in Participants
    /\ phase[p] = "none"
    /\ phase' = [phase EXCEPT ![p] = "begun"]
    /\ ambiguousPhase' = [ambiguousPhase EXCEPT ![p] = "begin"]
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, decision, response,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

PrepareParticipant(p) ==
    /\ p \in Participants
    /\ phase[p] = "begun"
    /\ phase' = [phase EXCEPT ![p] = "prepared"]
    /\ everPrepared' = everPrepared \cup {p}
    /\ protectedPending' = protectedPending \cup {p}
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, delivered, acknowledged, presumedAborted,
                  ambiguousPhase, decision, response, everCommitted,
                  decisionEpochValid, retainTerminal, propagationPending,
                  visibilityReached, cleaned>>

PrepareParticipantAmbiguous(p) ==
    /\ p \in Participants
    /\ phase[p] = "begun"
    /\ phase' = [phase EXCEPT ![p] = "prepared"]
    /\ everPrepared' = everPrepared \cup {p}
    /\ protectedPending' = protectedPending \cup {p}
    /\ ambiguousPhase' = [ambiguousPhase EXCEPT ![p] = "prepare"]
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, delivered, acknowledged, presumedAborted,
                  decision, response, everCommitted, decisionEpochValid,
                  retainTerminal, propagationPending, visibilityReached,
                  cleaned>>

LoadLegacyPending(p) ==
    /\ p \in Participants
    /\ phase[p] = "none"
    /\ phase' = [phase EXCEPT ![p] = "begun"]
    /\ preparedKnown' = [preparedKnown EXCEPT ![p] = FALSE]
    /\ coordinatorKnown' = [coordinatorKnown EXCEPT ![p] = FALSE]
    /\ protectedPending' = protectedPending \cup {p}
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, everPrepared, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  response, everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

DurableCommitDecision ==
    /\ decision = "none"
    /\ (BuggyCommitWithoutPrepare \/
        \A p \in Participants : phase[p] = "prepared")
    /\ (BuggyCommitStaleEpoch \/ attemptEpoch = topologyEpoch)
    /\ decision' = "committed"
    /\ everCommitted' = TRUE
    /\ decisionEpochValid' = (attemptEpoch = topologyEpoch)
    /\ propagationPending' = TRUE
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, response,
                  retainTerminal, visibilityReached, cleaned>>

DurableAbortDecision ==
    /\ decision # "aborted"
    /\ (decision # "committed" \/ BuggyAbortAfterCommit)
    /\ decision' = "aborted"
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, response,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

AdvanceTopology ==
    /\ topologyEpoch < MaxEpoch
    /\ topologyEpoch' = topologyEpoch + 1
    /\ UNCHANGED <<attemptEpoch, txnGeneration, transformApplications,
                  retryCount, phase, preparedKnown, coordinatorKnown,
                  everPrepared, protectedPending, delivered, acknowledged,
                  presumedAborted, ambiguousPhase, decision, response,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

DeliverInitialDecision(p) ==
    /\ p \in Participants
    /\ decision \in {"committed", "aborted"}
    /\ attemptEpoch = topologyEpoch
    /\ phase' = [phase EXCEPT ![p] = decision]
    /\ delivered' = delivered \cup {p}
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  response, everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

DeliverRecoveryDecision(p) ==
    /\ p \in Participants
    /\ decision \in {"committed", "aborted"}
    /\ phase' = [phase EXCEPT ![p] = decision]
    /\ delivered' = delivered \cup {p}
    /\ \/ ambiguousPhase' = ambiguousPhase
       \/ ambiguousPhase' = [ambiguousPhase EXCEPT ![p] = "resolve"]
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending,
                  acknowledged, presumedAborted, decision, response,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

RecoverStalePending(p) ==
    LET requiresCoordinatorProof ==
            phase[p] = "prepared" \/ ~preparedKnown[p]
    IN /\ p \in Participants
       /\ decision = "none"
       /\ phase[p] \in {"begun", "prepared"}
       /\ (~requiresCoordinatorProof \/
           (coordinatorKnown[p] /\ p = Coordinator) \/
           BuggyPreparedFollowerAbort)
       /\ phase' = [phase EXCEPT ![p] = "aborted"]
       /\ presumedAborted' = presumedAborted \cup {p}
       /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                     transformApplications, retryCount, preparedKnown,
                     coordinatorKnown, everPrepared, protectedPending,
                     delivered, acknowledged, ambiguousPhase, decision,
                     response, everCommitted, decisionEpochValid,
                     retainTerminal, propagationPending, visibilityReached,
                     cleaned>>

AcknowledgeParticipant(p) ==
    LET newAcknowledged == acknowledged \cup {p}
    IN /\ p \in delivered
       /\ acknowledged' = newAcknowledged
       /\ propagationPending' = (newAcknowledged # Participants)
       /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                     transformApplications, retryCount, phase, preparedKnown,
                     coordinatorKnown, everPrepared, protectedPending,
                     delivered, presumedAborted, ambiguousPhase, decision,
                     response, everCommitted, decisionEpochValid,
                     retainTerminal, visibilityReached, cleaned>>

ReachVisibility ==
    /\ decision = "committed"
    /\ visibilityReached' = TRUE
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  response, everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, cleaned>>

ExpectedCommitResponse ==
    IF propagationPending THEN "committed_recovery_pending"
    ELSE IF ~visibilityReached THEN "committed_visibility_pending"
    ELSE "committed"

ReportCommitOutcome ==
    /\ decision = "committed"
    /\ IF BuggyEarlySuccess /\ (propagationPending \/ ~visibilityReached)
       THEN response' \in {ExpectedCommitResponse, "committed"}
       ELSE response' = ExpectedCommitResponse
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

StableRetry ==
    /\ retainTerminal
    /\ decision = "committed"
    /\ retryCount < MaxRetry
    /\ retryCount' = retryCount + 1
    /\ response' = ExpectedCommitResponse
    /\ IF BuggyRetryNewTransaction
       THEN /\ txnGeneration' = txnGeneration + 1
            /\ transformApplications' = transformApplications + 1
       ELSE /\ txnGeneration' = txnGeneration
            /\ transformApplications' = transformApplications
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached, cleaned>>

CleanupCoordinator ==
    /\ decision \in {"committed", "aborted"}
    /\ (BuggyCleanupPendingPropagation \/
        (acknowledged = Participants /\ ~propagationPending))
    /\ cleaned' = TRUE
    /\ UNCHANGED <<topologyEpoch, attemptEpoch, txnGeneration,
                  transformApplications, retryCount, phase, preparedKnown,
                  coordinatorKnown, everPrepared, protectedPending, delivered,
                  acknowledged, presumedAborted, ambiguousPhase, decision,
                  response, everCommitted, decisionEpochValid, retainTerminal,
                  propagationPending, visibilityReached>>

Next ==
    \/ DurableCommitDecision
    \/ DurableAbortDecision
    \/ AdvanceTopology
    \/ ReachVisibility
    \/ ReportCommitOutcome
    \/ StableRetry
    \/ CleanupCoordinator
    \/ \E p \in Participants:
        \/ BeginParticipant(p)
        \/ BeginParticipantAmbiguous(p)
        \/ PrepareParticipant(p)
        \/ PrepareParticipantAmbiguous(p)
        \/ LoadLegacyPending(p)
        \/ DeliverInitialDecision(p)
        \/ DeliverRecoveryDecision(p)
        \/ RecoverStalePending(p)
        \/ AcknowledgeParticipant(p)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ topologyEpoch \in Nat
    /\ attemptEpoch \in Nat
    /\ txnGeneration \in Nat
    /\ transformApplications \in Nat
    /\ retryCount \in Nat
    /\ phase \in [Participants -> ParticipantPhases]
    /\ preparedKnown \in [Participants -> BOOLEAN]
    /\ coordinatorKnown \in [Participants -> BOOLEAN]
    /\ everPrepared \subseteq Participants
    /\ protectedPending \subseteq Participants
    /\ delivered \subseteq Participants
    /\ acknowledged \subseteq Participants
    /\ presumedAborted \subseteq Participants
    /\ ambiguousPhase \in [Participants -> AmbiguousPhases]
    /\ decision \in Decisions
    /\ response \in Responses
    /\ everCommitted \in BOOLEAN
    /\ decisionEpochValid \in BOOLEAN
    /\ retainTerminal \in BOOLEAN
    /\ propagationPending \in BOOLEAN
    /\ visibilityReached \in BOOLEAN
    /\ cleaned \in BOOLEAN

DecisionNeverReverses ==
    everCommitted => decision = "committed"

CommitRequiresAllPrepared ==
    everCommitted => everPrepared = Participants

CommitUsesCurrentTopology ==
    everCommitted => decisionEpochValid

PreparedFollowerDoesNotPresumeAbort ==
    presumedAborted \intersect protectedPending \subseteq {Coordinator}

StableRetryPreservesIdentityAndWrites ==
    /\ txnGeneration = 1
    /\ transformApplications = 1

CleanupRequiresRecoveryComplete ==
    cleaned => acknowledged = Participants /\ ~propagationPending

SuccessReflectsPropagationAndVisibility ==
    /\ response # "none" => decision = "committed"
    /\ propagationPending =>
        response \in {"none", "committed_recovery_pending"}
    /\ ~visibilityReached => response # "committed"

Safety ==
    /\ TypeOK
    /\ DecisionNeverReverses
    /\ CommitRequiresAllPrepared
    /\ CommitUsesCurrentTopology
    /\ PreparedFollowerDoesNotPresumeAbort
    /\ StableRetryPreservesIdentityAndWrites
    /\ CleanupRequiresRecoveryComplete
    /\ SuccessReflectsPropagationAndVisibility

=============================================================================
