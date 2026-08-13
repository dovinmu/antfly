<!-- GENERATED FILE: do not edit. Regenerate with `make -C zig tla-viz` (scripts/tla-viz/gen_structural.py). -->

# AntflyReplayEnrichmentBridge — structural diagrams

Generated from [`AntflyReplayEnrichmentBridge.tla`](../../../storage/db/AntflyReplayEnrichmentBridge.tla). 14 state variables, 13 actions in `Next`.

## Actions and the state they touch

| Action | Reads (incl. helper operators) | Writes |
| --- | --- | --- |
| `AppendGeneratedSource` | `sourceSeq`, `journal`, `coverageDebt` | `sourceSeq`, `journal`, `coverageDebt` |
| `FastConsumerAdvance` | `sourceSeq`, `fastApplied` | `fastApplied` |
| `ProviderFails` | `providerUp` | `providerUp` |
| `ProviderRecovers` | `providerUp` | `providerUp`, `retryAttemptsSinceBoundary`, `retryAttemptsTotal` |
| `TransientProviderRetry` | `coverageDebt`, `providerUp`, `workerArmed`, `retryAttemptsSinceBoundary`, `retryAttemptsTotal` | `retryAttemptsSinceBoundary`, `retryAttemptsTotal` |
| `RetrySchedulerBoundary` | `retryAttemptsSinceBoundary` | `retryAttemptsSinceBoundary` |
| `ExhaustProviderRetry` | `coverageDebt`, `completed`, `volatileCollected`, `providerUp`, `workerArmed`, `retryAttemptsTotal`, `exhausted`, `repairDebt` | `coverageDebt`, `completed`, `volatileCollected`, `retryAttemptsSinceBoundary`, `exhausted`, `repairDebt` |
| `AdvanceEnrichment` | `sourceSeq`, `journal`, `enrichmentApplied`, `coverageDebt`, `volatileCollected` | `enrichmentApplied` |
| `Restart` | `processEpoch` | `volatileCollected`, `workerArmed`, `retryAttemptsSinceBoundary`, `processEpoch` |
| `ArmStartupEnrichment` | `workerArmed`, `processEpoch` | `workerArmed` |
| `CollectPending` | `journal`, `coverageDebt`, `volatileCollected` | `volatileCollected` |
| `CompleteEnrichment` | `journal`, `coverageDebt`, `completed`, `volatileCollected`, `providerUp`, `workerArmed` | `coverageDebt`, `completed`, `volatileCollected`, `retryAttemptsSinceBoundary`, `retryAttemptsTotal` |
| `TruncateReplay` | `journal`, `fastApplied`, `enrichmentApplied` | `journal` |

## Write graph

Solid edges: the action updates the variable. Dotted edges: the action's own definition reads the variable (reads via helper operators appear in the table above, not here).

```mermaid
flowchart LR
    subgraph actions["Actions"]
        a0[AppendGeneratedSource]
        a1[FastConsumerAdvance]
        a2[ProviderFails]
        a3[ProviderRecovers]
        a4[TransientProviderRetry]
        a5[RetrySchedulerBoundary]
        a6[ExhaustProviderRetry]
        a7[AdvanceEnrichment]
        a8[Restart]
        a9[ArmStartupEnrichment]
        a10[CollectPending]
        a11[CompleteEnrichment]
        a12[TruncateReplay]
    end
    subgraph state["State variables"]
        v0([sourceSeq])
        v1([journal])
        v2([coverageDebt])
        v3([fastApplied])
        v4([providerUp])
        v5([retryAttemptsSinceBoundary])
        v6([retryAttemptsTotal])
        v7([workerArmed])
        v8([completed])
        v9([volatileCollected])
        v10([exhausted])
        v11([repairDebt])
        v12([enrichmentApplied])
        v13([processEpoch])
    end
    a0 --> v0
    a0 --> v1
    a0 --> v2
    a1 --> v3
    v0 -.-> a1
    a2 --> v4
    a3 --> v4
    a3 --> v5
    a3 --> v6
    a4 --> v5
    a4 --> v6
    v2 -.-> a4
    v4 -.-> a4
    v7 -.-> a4
    a5 --> v5
    a6 --> v2
    a6 --> v8
    a6 --> v9
    a6 --> v5
    a6 --> v10
    a6 --> v11
    v4 -.-> a6
    v7 -.-> a6
    v6 -.-> a6
    a7 --> v12
    v0 -.-> a7
    v1 -.-> a7
    v2 -.-> a7
    v9 -.-> a7
    a8 --> v9
    a8 --> v7
    a8 --> v5
    a8 --> v13
    a9 --> v7
    v13 -.-> a9
    a10 --> v9
    v1 -.-> a10
    v2 -.-> a10
    a11 --> v2
    a11 --> v8
    a11 --> v9
    a11 --> v5
    a11 --> v6
    v1 -.-> a11
    v4 -.-> a11
    v7 -.-> a11
    a12 --> v1
    v3 -.-> a12
    v12 -.-> a12
```
