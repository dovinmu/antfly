<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyDistributedTransactionRecovery — structural diagrams

Generated from [`AntflyDistributedTransactionRecovery.tla`](../../../storage/db/AntflyDistributedTransactionRecovery.tla). 22 state variables, 16 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `phase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    none --> begun : BeginParticipant, BeginParticipantAmbiguous, LoadLegacyPending
    begun --> prepared : PrepareParticipant, PrepareParticipantAmbiguous
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_begun fill:#eb683430,stroke:#eb6834
    class begun c_begun
    classDef c_prepared fill:#1baf7a30,stroke:#1baf7a
    class prepared c_prepared
    classDef c_committed fill:#eda10030,stroke:#eda100
    class committed c_committed
    classDef c_aborted fill:#e87ba430,stroke:#e87ba4
    class aborted c_aborted
```

Writes whose source state is not statically determined:

- `RecoverStalePending` sets `phase` to `"aborted"`

### `ambiguousPhase`

Domain: `none`, `begin`, `prepare`, `resolve`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `BeginParticipantAmbiguous` sets `ambiguousPhase` to `"begin"`
- `DeliverRecoveryDecision` sets `ambiguousPhase` to `"resolve"`
- `PrepareParticipantAmbiguous` sets `ambiguousPhase` to `"prepare"`

### `decision`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> none
    none --> committed : DurableCommitDecision
    classDef c_none fill:#2a78d630,stroke:#2a78d6
    class none c_none
    classDef c_committed fill:#eb683430,stroke:#eb6834
    class committed c_committed
    classDef c_aborted fill:#1baf7a30,stroke:#1baf7a
    class aborted c_aborted
```

Writes whose source state is not statically determined:

- `DurableAbortDecision` sets `decision` to `"aborted"`

### `response`

Domain: `none`, `committed`, `committed_visibility_pending`, `committed_recovery_pending`. No statically extractable guard/update transitions.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `DurableCommitDecision` | `topologyEpoch`, `attemptEpoch`, `phase`, `decision` | `decision`, `everCommitted`, `decisionEpochValid`, `propagationPending` |
| `DurableAbortDecision` | `decision` | `decision` |
| `AdvanceTopology` | `topologyEpoch` | `topologyEpoch` |
| `ReachVisibility` | `decision` | `visibilityReached` |
| `ReportCommitOutcome` | `decision`, `propagationPending`, `visibilityReached` | `response` |
| `StableRetry` | `txnGeneration`, `transformApplications`, `retryCount`, `decision`, `retainTerminal`, `propagationPending`, `visibilityReached` | `txnGeneration`, `transformApplications`, `retryCount`, `response` |
| `CleanupCoordinator` | `acknowledged`, `decision`, `propagationPending` | `cleaned` |
| `BeginParticipant` | `phase` | `phase` |
| `BeginParticipantAmbiguous` | `phase`, `ambiguousPhase` | `phase`, `ambiguousPhase` |
| `PrepareParticipant` | `phase`, `everPrepared`, `protectedPending` | `phase`, `everPrepared`, `protectedPending` |
| `PrepareParticipantAmbiguous` | `phase`, `everPrepared`, `protectedPending`, `ambiguousPhase` | `phase`, `everPrepared`, `protectedPending`, `ambiguousPhase` |
| `LoadLegacyPending` | `phase`, `preparedKnown`, `coordinatorKnown`, `protectedPending` | `phase`, `preparedKnown`, `coordinatorKnown`, `protectedPending` |
| `DeliverInitialDecision` | `topologyEpoch`, `attemptEpoch`, `phase`, `delivered`, `decision` | `phase`, `delivered` |
| `DeliverRecoveryDecision` | `phase`, `delivered`, `ambiguousPhase`, `decision` | `phase`, `delivered`, `ambiguousPhase` |
| `RecoverStalePending` | `phase`, `preparedKnown`, `coordinatorKnown`, `presumedAborted`, `decision` | `phase`, `presumedAborted` |
| `AcknowledgeParticipant` | `delivered`, `acknowledged` | `acknowledged`, `propagationPending` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[DurableCommitDecision]
        a1[DurableAbortDecision]
        a2[AdvanceTopology]
        a3[ReachVisibility]
        a4[ReportCommitOutcome]
        a5[StableRetry]
        a6[CleanupCoordinator]
        a7[BeginParticipant]
        a8[BeginParticipantAmbiguous]
        a9[PrepareParticipant]
        a10[PrepareParticipantAmbiguous]
        a11[LoadLegacyPending]
        a12[DeliverInitialDecision]
        a13[DeliverRecoveryDecision]
        a14[RecoverStalePending]
        a15[AcknowledgeParticipant]
    end
    subgraph state["State variables"]
        v0([topologyEpoch])
        v1([attemptEpoch])
        v2([phase])
        v3([decision])
        v4([everCommitted])
        v5([decisionEpochValid])
        v6([propagationPending])
        v7([visibilityReached])
        v8([response])
        v9([txnGeneration])
        v10([transformApplications])
        v11([retryCount])
        v12([retainTerminal])
        v13([acknowledged])
        v14([cleaned])
        v15([ambiguousPhase])
        v16([everPrepared])
        v17([protectedPending])
        v18([preparedKnown])
        v19([coordinatorKnown])
        v20([delivered])
        v21([presumedAborted])
    end
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    v0 -.-> a0
    v1 -.-> a0
    v2 -.-> a0
    a1 --> v3
    a2 --> v0
    a3 --> v7
    v3 -.-> a3
    a4 --> v8
    v3 -.-> a4
    v6 -.-> a4
    v7 -.-> a4
    a5 --> v9
    a5 --> v10
    a5 --> v11
    a5 --> v8
    v3 -.-> a5
    v12 -.-> a5
    a6 --> v14
    v13 -.-> a6
    v3 -.-> a6
    v6 -.-> a6
    a7 --> v2
    a8 --> v2
    a8 --> v15
    a9 --> v2
    a9 --> v16
    a9 --> v17
    a10 --> v2
    a10 --> v16
    a10 --> v17
    a10 --> v15
    a11 --> v2
    a11 --> v18
    a11 --> v19
    a11 --> v17
    a12 --> v2
    a12 --> v20
    v0 -.-> a12
    v1 -.-> a12
    v3 -.-> a12
    a13 --> v2
    a13 --> v20
    a13 --> v15
    v3 -.-> a13
    a14 --> v2
    a14 --> v21
    v18 -.-> a14
    v19 -.-> a14
    v3 -.-> a14
    a15 --> v13
    a15 --> v6
    v20 -.-> a15
```
