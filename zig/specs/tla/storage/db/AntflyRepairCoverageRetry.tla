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

------------------------- MODULE AntflyRepairCoverageRetry -----------------------
(*
  Bounded model of dense repair while a recreated generation's source
  artifacts are still catching up.

  The strict coverage check remains a safety fence: an incomplete shadow
  generation cannot activate. Once the replacement incarnation's durable
  outcome tuple proves complete coverage, activation must use that tuple
  rather than a stale name-scoped artifact counter left by delete/recreate.
  The attempt also carries the current durable repair revision: coverage and
  activation are revalidated under the same fence used by
  index_repair_state.zig transitions and enrichment_runtime.zig terminal
  coverage publication.

  BuggyNameScopedCounter records terminal RepairSourceCoverageIncomplete after
  current-generation coverage catches up. BuggyIgnoreRepairFence allows a
  stale attempt to activate after a newer durable wake/revision.
*)

EXTENDS Naturals, TLC

CONSTANTS BuggyNameScopedCounter, BuggyIgnoreRepairFence

MaxRepairRevision == 2

VARIABLES coverageComplete, repairPhase, repairRevision, attemptRevision

vars == <<coverageComplete, repairPhase, repairRevision, attemptRevision>>

Init ==
    /\ coverageComplete = FALSE
    /\ repairPhase = "building"
    /\ repairRevision = 0
    /\ attemptRevision = 0

ObserveIncomplete ==
    /\ repairPhase = "building"
    /\ ~coverageComplete
    /\ repairRevision < MaxRepairRevision
    /\ repairPhase' = "retry_wait"
    /\ repairRevision' = repairRevision + 1
    /\ UNCHANGED <<coverageComplete, attemptRevision>>

CatchUpCoverage ==
    /\ ~coverageComplete
    /\ coverageComplete' = TRUE
    /\ UNCHANGED <<repairPhase, repairRevision, attemptRevision>>

RetryRepair ==
    /\ repairPhase = "retry_wait"
    /\ repairPhase' = "building"
    /\ attemptRevision' = repairRevision
    /\ UNCHANGED <<coverageComplete, repairRevision>>

\* A durable exact wake can revise the intent while an older attempt is still
\* building. The worker must refresh that fence before activation.
AdvanceRepairFence ==
    /\ repairPhase = "building"
    /\ repairRevision < MaxRepairRevision
    /\ repairRevision' = repairRevision + 1
    /\ UNCHANGED <<coverageComplete, repairPhase, attemptRevision>>

RefreshRepairFence ==
    /\ repairPhase = "building"
    /\ attemptRevision # repairRevision
    /\ attemptRevision' = repairRevision
    /\ UNCHANGED <<coverageComplete, repairPhase, repairRevision>>

ActivateComplete ==
    /\ repairPhase = "building"
    /\ coverageComplete
    /\ (BuggyIgnoreRepairFence \/ attemptRevision = repairRevision)
    /\ repairPhase' =
        IF BuggyNameScopedCounter THEN "terminal" ELSE "ready"
    /\ UNCHANGED <<coverageComplete, repairRevision, attemptRevision>>

Next ==
    \/ ObserveIncomplete
    \/ CatchUpCoverage
    \/ RetryRepair
    \/ AdvanceRepairFence
    \/ RefreshRepairFence
    \/ ActivateComplete

Spec == Init /\ [][Next]_vars

FairSpec ==
    Spec
    /\ WF_vars(CatchUpCoverage)
    /\ WF_vars(RetryRepair)
    /\ WF_vars(RefreshRepairFence)
    /\ WF_vars(ActivateComplete)

TypeOK ==
    /\ coverageComplete \in BOOLEAN
    /\ repairPhase \in {"building", "retry_wait", "ready", "terminal"}
    /\ repairRevision \in 0..MaxRepairRevision
    /\ attemptRevision \in 0..MaxRepairRevision
    /\ BuggyNameScopedCounter \in BOOLEAN
    /\ BuggyIgnoreRepairFence \in BOOLEAN

IncompleteGenerationNeverActivates ==
    repairPhase = "ready" => coverageComplete

ReadyGenerationUsesCurrentRepairFence ==
    repairPhase = "ready" => attemptRevision = repairRevision

CoverageCatchUpEventuallyActivates ==
    coverageComplete ~> repairPhase = "ready"

=============================================================================
