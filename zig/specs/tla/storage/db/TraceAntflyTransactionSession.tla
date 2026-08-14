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

--------------------- MODULE TraceAntflyTransactionSession ---------------------
(*
  Fixture-backed trace refinement for AntflyTransactionSession. In addition to
  savepoint, orphan-recovery, and stale-pending facts, each committed-session
  fixture carries the durable handoff coordinates, exact terminal outcome,
  stable transaction identity, write/transform application counts, and
  propagation-gated cleanup state.
*)

EXTENDS AntflyTransactionSession, Json, IOUtils, Naturals, Sequences, TLC

ASSUME TLCGet("config").mode = "bfs"
ASSUME TLCGet("config").worker = 1

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON ELSE "./txn-session-trace.ndjson"

OriginTraceLog ==
    SelectSeq(
        ndJsonDeserialize(JsonFile),
        LAMBDA line: "tag" \in DOMAIN line /\ line.tag = "txn-session-trace")

TraceLog ==
    TLCEval(
        IF "MAX_TRACE" \in DOMAIN IOEnv
        THEN SubSeq(OriginTraceLog, 1, atoi(IOEnv.MAX_TRACE))
        ELSE OriginTraceLog)

VARIABLES
    l,
    pl

traceVars == <<l, pl, vars>>

TraceInit ==
    /\ l = 1
    /\ pl = 0
    /\ Init

logline == TraceLog[l]
event == logline.event

SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

StepToNextTrace ==
    /\ l' = l + 1
    /\ pl' = l

LoglineIsEvent(name) ==
    /\ l <= Len(TraceLog)
    /\ event.name = name

AfterField(name) ==
    /\ "after" \in DOMAIN event
    /\ name \in DOMAIN event.after

AfterMatches ==
    /\ IF AfterField("status") THEN status' = event.after.status ELSE TRUE
    /\ IF AfterField("baseVisible") THEN baseVisible' = event.after.baseVisible ELSE TRUE
    /\ IF AfterField("staged") THEN staged' = event.after.staged ELSE TRUE
    /\ IF AfterField("intentCount") THEN intentCount' = event.after.intentCount ELSE TRUE
    /\ IF AfterField("savepoint") THEN savepoint' = event.after.savepoint ELSE TRUE
    /\ IF AfterField("visible") THEN visible' = event.after.visible ELSE TRUE
    /\ IF AfterField("identityRows") THEN identityRows' = event.after.identityRows ELSE TRUE
    /\ IF AfterField("conflict") THEN conflict' = event.after.conflict ELSE TRUE
    /\ IF AfterField("stalePending") THEN stalePending' = event.after.stalePending ELSE TRUE
    /\ IF AfterField("rollbackDiscarded") THEN rollbackDiscarded' = event.after.rollbackDiscarded ELSE TRUE
    /\ IF AfterField("cleanedWithUnresolved") THEN cleanedWithUnresolved' = event.after.cleanedWithUnresolved ELSE TRUE
    /\ IF AfterField("txnId") THEN txnId' = event.after.txnId ELSE TRUE
    /\ IF AfterField("writeApplications") THEN writeApplications' = event.after.writeApplications ELSE TRUE
    /\ IF AfterField("transformApplications") THEN transformApplications' = event.after.transformApplications ELSE TRUE
    /\ IF AfterField("coordinatorGroupId") THEN coordinatorGroup' = event.after.coordinatorGroupId ELSE TRUE
    /\ IF AfterField("coordinatorTableName") THEN coordinatorTable' = event.after.coordinatorTableName ELSE TRUE
    /\ IF AfterField("terminalOutcome") THEN terminalOutcome' = event.after.terminalOutcome ELSE TRUE
    /\ IF AfterField("propagationPending") THEN propagationPending' = event.after.propagationPending ELSE TRUE
    /\ IF AfterField("visibilityPending") THEN visibilityPending' = event.after.visibilityPending ELSE TRUE
    /\ IF AfterField("terminalRetained") THEN terminalRetained' = event.after.terminalRetained ELSE TRUE
    /\ IF AfterField("terminalTxnId") THEN terminalTxnId' = event.after.terminalTxnId ELSE TRUE
    /\ IF AfterField("terminalWriteApplications") THEN terminalWriteApplications' = event.after.terminalWriteApplications ELSE TRUE
    /\ IF AfterField("terminalTransformApplications") THEN terminalTransformApplications' = event.after.terminalTransformApplications ELSE TRUE
    /\ IF AfterField("retryCount") THEN retryCount' = event.after.retryCount ELSE TRUE
    /\ IF AfterField("prepared")
       THEN {p \in Participants : participantPrepared'[p]} = SeqToSet(event.after.prepared)
       ELSE TRUE
    /\ IF AfterField("resolved")
       THEN {p \in Participants : participantResolved'[p]} = SeqToSet(event.after.resolved)
       ELSE TRUE

SessionActionFromTrace ==
    \/ /\ LoglineIsEvent("BeginSession")
       /\ BeginSession
    \/ /\ LoglineIsEvent("StageWrite")
       /\ StageWrite
    \/ /\ LoglineIsEvent("CreateSavepoint")
       /\ CreateSavepoint
    \/ /\ LoglineIsEvent("RollbackToSavepoint")
       /\ RollbackToSavepoint
    \/ /\ LoglineIsEvent("DetectConflict")
       /\ DetectConflict
    \/ /\ LoglineIsEvent("Commit")
       /\ Commit
    \/ /\ LoglineIsEvent("Abort")
       /\ Abort
    \/ /\ LoglineIsEvent("MarkStalePending")
       /\ MarkStalePending
    \/ /\ LoglineIsEvent("RecoverStalePending")
       /\ RecoverStalePending
    \/ /\ LoglineIsEvent("CrashFinalizeCommittedOrphan")
       /\ CrashFinalizeCommittedOrphan
    \/ /\ LoglineIsEvent("CrashFinalizeAbortedOrphan")
       /\ CrashFinalizeAbortedOrphan
    \/ /\ LoglineIsEvent("RecoverFinalizedIntents")
       /\ RecoverFinalizedIntents
    \/ /\ LoglineIsEvent("ReachVisibility")
       /\ ReachVisibility
    \/ /\ LoglineIsEvent("StableRetry")
       /\ StableRetry
    \/ /\ LoglineIsEvent("Cleanup")
       /\ Cleanup
    \/ \E p \in Participants:
        \/ /\ LoglineIsEvent("PrepareParticipant")
           /\ "participant" \in DOMAIN event
           /\ event.participant = p
           /\ PrepareParticipant(p)
        \/ /\ LoglineIsEvent("ResolveParticipant")
           /\ "participant" \in DOMAIN event
           /\ event.participant = p
           /\ ResolveParticipant(p)

TraceNext ==
    /\ l <= Len(TraceLog)
    /\ SessionActionFromTrace
    /\ AfterMatches
    /\ StepToNextTrace

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

TraceView == <<vars, l>>

TraceSafety ==
    Safety

\* Violated if TLC cannot consume every line of the fixture.
TraceMatched ==
    [](l <= Len(TraceLog) => [](TLCGet("queue") = 1 \/ l > Len(TraceLog)))

=============================================================================
