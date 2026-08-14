<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyTransactionSession — structural diagrams

Generated from [`AntflyTransactionSession.tla`](../../../storage/db/AntflyTransactionSession.tla). 26 state variables, 17 actions in `Next`. 3 expected-failure mutant action(s) (gated by `Buggy*` constants) omitted.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `status`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> idle
    idle --> open : BeginSession
    open --> aborted : DetectConflict, Abort, RecoverStalePending, CrashFinalizeAbortedOrphan
    prepared --> committed : Commit, CrashFinalizeCommittedOrphan
    prepared --> aborted : Abort, CrashFinalizeAbortedOrphan
    aborted --> idle : Cleanup
    committed --> idle : Cleanup
    open --> prepared : PrepareParticipant
    classDef c_idle fill:#2a78d630,stroke:#2a78d6
    class idle c_idle
    classDef c_open fill:#eb683430,stroke:#eb6834
    class open c_open
    classDef c_prepared fill:#1baf7a30,stroke:#1baf7a
    class prepared c_prepared
    classDef c_committed fill:#eda10030,stroke:#eda100
    class committed c_committed
    classDef c_aborted fill:#e87ba430,stroke:#e87ba4
    class aborted c_aborted
```

### `terminalOutcome`

Domain: `none`, `committed`, `committed_visibility_pending`, `committed_recovery_pending`. No statically extractable guard/update transitions.

Writes whose source state is not statically determined:

- `BeginSession` sets `terminalOutcome` to `"none"`
- `Cleanup` sets `terminalOutcome` to `"none"`
- `Commit` sets `terminalOutcome` to `"committed_recovery_pending"`
- `CrashFinalizeCommittedOrphan` sets `terminalOutcome` to `"committed_recovery_pending"`
- `ReachVisibility` sets `terminalOutcome` to `"committed"`
- `ResolveParticipant` sets `terminalOutcome` to `"committed"`

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `BeginSession` | `status`, `visible`, `txnId` | `status`, `baseVisible`, `staged`, `intentCount`, `savepoint`, `participantPrepared`, `participantResolved`, `conflict`, `stalePending`, `rollbackDiscarded`, `txnId`, `writeApplications`, `transformApplications`, `coordinatorGroup`, `coordinatorTable`, `terminalOutcome`, `propagationPending`, `visibilityPending`, `terminalRetained`, `terminalTxnId`, `terminalWriteApplications`, `terminalTransformApplications`, `retryCount` |
| `StageWrite` | `status`, `baseVisible`, `staged`, `intentCount`, `writeApplications`, `transformApplications` | `staged`, `intentCount`, `writeApplications`, `transformApplications` |
| `CreateSavepoint` | `status`, `staged` | `savepoint` |
| `RollbackToSavepoint` | `status`, `staged`, `savepoint` | `staged`, `intentCount`, `rollbackDiscarded` |
| `DetectConflict` | `status` | `status`, `intentCount`, `conflict` |
| `Commit` | `status`, `baseVisible`, `intentCount`, `txnId`, `writeApplications`, `transformApplications` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending`, `coordinatorGroup`, `coordinatorTable`, `terminalOutcome`, `propagationPending`, `visibilityPending`, `terminalRetained`, `terminalTxnId`, `terminalWriteApplications`, `terminalTransformApplications`, `retryCount` |
| `Abort` | `status`, `baseVisible` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending` |
| `MarkStalePending` | `status` | `stalePending` |
| `RecoverStalePending` | `status`, `baseVisible`, `stalePending` | `status`, `intentCount`, `visible`, `identityRows`, `stalePending` |
| `CrashFinalizeCommittedOrphan` | `status`, `intentCount`, `txnId`, `writeApplications`, `transformApplications` | `status`, `coordinatorGroup`, `coordinatorTable`, `terminalOutcome`, `propagationPending`, `visibilityPending`, `terminalRetained`, `terminalTxnId`, `terminalWriteApplications`, `terminalTransformApplications`, `retryCount` |
| `CrashFinalizeAbortedOrphan` | `status`, `intentCount` | `status`, `stalePending` |
| `RecoverFinalizedIntents` | `status`, `baseVisible`, `intentCount` | `intentCount`, `visible`, `identityRows` |
| `ReachVisibility` | `status`, `propagationPending`, `visibilityPending`, `terminalRetained` | `terminalOutcome`, `visibilityPending` |
| `StableRetry` | `status`, `propagationPending`, `visibilityPending`, `terminalRetained`, `retryCount` | `terminalOutcome`, `retryCount` |
| `Cleanup` | `status`, `intentCount`, `participantPrepared`, `participantResolved`, `visible`, `propagationPending` | `status`, `baseVisible`, `staged`, `savepoint`, `participantPrepared`, `participantResolved`, `conflict`, `stalePending`, `rollbackDiscarded`, `coordinatorGroup`, `coordinatorTable`, `terminalOutcome`, `propagationPending`, `visibilityPending`, `terminalRetained`, `terminalTxnId`, `terminalWriteApplications`, `terminalTransformApplications`, `retryCount` |
| `PrepareParticipant` | `status`, `intentCount`, `participantPrepared`, `conflict` | `status`, `participantPrepared` |
| `ResolveParticipant` | `status`, `participantPrepared`, `participantResolved`, `terminalOutcome`, `visibilityPending` | `participantResolved`, `terminalOutcome`, `propagationPending` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[BeginSession]
        a1[StageWrite]
        a2[CreateSavepoint]
        a3[RollbackToSavepoint]
        a4[DetectConflict]
        a5[Commit]
        a6[Abort]
        a7[MarkStalePending]
        a8[RecoverStalePending]
        a9[CrashFinalizeCommittedOrphan]
        a10[CrashFinalizeAbortedOrphan]
        a11[RecoverFinalizedIntents]
        a12[ReachVisibility]
        a13[StableRetry]
        a14[Cleanup]
        a15[PrepareParticipant]
        a16[ResolveParticipant]
    end
    subgraph state["State variables"]
        v0([status])
        v1([baseVisible])
        v2([staged])
        v3([intentCount])
        v4([savepoint])
        v5([participantPrepared])
        v6([participantResolved])
        v7([visible])
        v8([conflict])
        v9([stalePending])
        v10([rollbackDiscarded])
        v11([txnId])
        v12([writeApplications])
        v13([transformApplications])
        v14([coordinatorGroup])
        v15([coordinatorTable])
        v16([terminalOutcome])
        v17([propagationPending])
        v18([visibilityPending])
        v19([terminalRetained])
        v20([terminalTxnId])
        v21([terminalWriteApplications])
        v22([terminalTransformApplications])
        v23([retryCount])
        v24([identityRows])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a0 --> v3
    a0 --> v4
    a0 --> v5
    a0 --> v6
    a0 --> v8
    a0 --> v9
    a0 --> v10
    a0 --> v11
    a0 --> v12
    a0 --> v13
    a0 --> v14
    a0 --> v15
    a0 --> v16
    a0 --> v17
    a0 --> v18
    a0 --> v19
    a0 --> v20
    a0 --> v21
    a0 --> v22
    a0 --> v23
    v7 -.-> a0
    a1 --> v2
    a1 --> v3
    a1 --> v12
    a1 --> v13
    v0 -.-> a1
    v1 -.-> a1
    a2 --> v4
    v0 -.-> a2
    v2 -.-> a2
    a3 --> v2
    a3 --> v3
    a3 --> v10
    v0 -.-> a3
    v4 -.-> a3
    a4 --> v0
    a4 --> v3
    a4 --> v8
    a5 --> v0
    a5 --> v3
    a5 --> v7
    a5 --> v24
    a5 --> v9
    a5 --> v14
    a5 --> v15
    a5 --> v16
    a5 --> v17
    a5 --> v18
    a5 --> v19
    a5 --> v20
    a5 --> v21
    a5 --> v22
    a5 --> v23
    v1 -.-> a5
    v11 -.-> a5
    v12 -.-> a5
    v13 -.-> a5
    a6 --> v0
    a6 --> v3
    a6 --> v7
    a6 --> v24
    a6 --> v9
    v1 -.-> a6
    a7 --> v9
    v0 -.-> a7
    a8 --> v0
    a8 --> v3
    a8 --> v7
    a8 --> v24
    a8 --> v9
    v1 -.-> a8
    a9 --> v0
    a9 --> v14
    a9 --> v15
    a9 --> v16
    a9 --> v17
    a9 --> v18
    a9 --> v19
    a9 --> v20
    a9 --> v21
    a9 --> v22
    a9 --> v23
    v3 -.-> a9
    v11 -.-> a9
    v12 -.-> a9
    v13 -.-> a9
    a10 --> v0
    a10 --> v9
    v3 -.-> a10
    a11 --> v3
    a11 --> v7
    a11 --> v24
    v0 -.-> a11
    v1 -.-> a11
    a12 --> v16
    a12 --> v18
    v0 -.-> a12
    v17 -.-> a12
    v19 -.-> a12
    a13 --> v16
    a13 --> v23
    v0 -.-> a13
    v17 -.-> a13
    v18 -.-> a13
    v19 -.-> a13
    a14 --> v0
    a14 --> v1
    a14 --> v2
    a14 --> v4
    a14 --> v5
    a14 --> v6
    a14 --> v8
    a14 --> v9
    a14 --> v10
    a14 --> v14
    a14 --> v15
    a14 --> v16
    a14 --> v17
    a14 --> v18
    a14 --> v19
    a14 --> v20
    a14 --> v21
    a14 --> v22
    a14 --> v23
    v3 -.-> a14
    v7 -.-> a14
    a15 --> v0
    a15 --> v5
    v3 -.-> a15
    v8 -.-> a15
    a16 --> v6
    a16 --> v16
    a16 --> v17
    v0 -.-> a16
    v5 -.-> a16
    v18 -.-> a16
```
