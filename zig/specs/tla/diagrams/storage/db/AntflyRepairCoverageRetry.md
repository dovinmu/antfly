<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyRepairCoverageRetry — structural diagrams

Generated from [`AntflyRepairCoverageRetry.tla`](../../../storage/db/AntflyRepairCoverageRetry.tla). 4 state variables, 6 actions in `Next`.

## Phase state machines

Transitions are extracted from action guards and primed updates; edge labels are the actions that perform the transition.

### `repairPhase`

```mermaid
stateDiagram-v2
    direction LR
    [*] --> building
    building --> retry_wait : ObserveIncomplete
    retry_wait --> building : RetryRepair
    building --> ready : ActivateComplete
    building --> terminal : ActivateComplete
    classDef c_building fill:#2a78d630,stroke:#2a78d6
    class building c_building
    classDef c_retry_wait fill:#eb683430,stroke:#eb6834
    class retry_wait c_retry_wait
    classDef c_ready fill:#1baf7a30,stroke:#1baf7a
    class ready c_ready
    classDef c_terminal fill:#eda10030,stroke:#eda100
    class terminal c_terminal
```

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `ObserveIncomplete` | `coverageComplete`, `repairPhase`, `repairRevision` | `repairPhase`, `repairRevision` |
| `CatchUpCoverage` | `coverageComplete` | `coverageComplete` |
| `RetryRepair` | `repairPhase`, `repairRevision` | `repairPhase`, `attemptRevision` |
| `AdvanceRepairFence` | `repairPhase`, `repairRevision` | `repairRevision` |
| `RefreshRepairFence` | `repairPhase`, `repairRevision`, `attemptRevision` | `attemptRevision` |
| `ActivateComplete` | `coverageComplete`, `repairPhase`, `repairRevision`, `attemptRevision` | `repairPhase` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[ObserveIncomplete]
        a1[CatchUpCoverage]
        a2[RetryRepair]
        a3[AdvanceRepairFence]
        a4[RefreshRepairFence]
        a5[ActivateComplete]
    end
    subgraph state["State variables"]
        v0([coverageComplete])
        v1([repairPhase])
        v2([repairRevision])
        v3([attemptRevision])
    end
    a0 --> v1
    a0 --> v2
    v0 -.-> a0
    a1 --> v0
    a2 --> v1
    a2 --> v3
    v2 -.-> a2
    a3 --> v2
    v1 -.-> a3
    a4 --> v3
    v1 -.-> a4
    v2 -.-> a4
    a5 --> v1
    v0 -.-> a5
    v2 -.-> a5
    v3 -.-> a5
```
