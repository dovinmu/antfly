# Semantic Notes — Lean Proving Pilot

## Decision: Separate Reducible Fields from Diagnostic Fields

The `MergeIndexStats` function in Go is *almost* a commutative monoid but has
two categories of order-sensitive fields that must be handled separately.

### 1. Reducible Fields (Commutative Monoid)

These form a commutative monoid under `combine`:

| Field                | Operation     | Identity | Notes                                  |
|----------------------|---------------|----------|----------------------------------------|
| TotalIndexed         | Nat +         | 0        |                                        |
| DiskUsage            | Nat +         | 0        |                                        |
| WalBacklog           | Nat +         | 0        |                                        |
| TotalNodes           | Nat +         | 0        |                                        |
| TotalTerms           | Nat +         | 0        |                                        |
| TotalEdges           | Nat +         | 0        |                                        |
| BackfillItemsProcessed | Nat +       | 0        |                                        |
| ParseErrorCount      | Nat +         | 0        |                                        |
| PlannerSelected      | Nat +         | 0        |                                        |
| PlannerFallbackCount | Nat +       | 0        |                                        |
| AdaptiveProgressCount | Nat +       | 0        |                                        |
| RecommendationCount  | Nat +         | 0        |                                        |
| AdaptiveBackfillingCount | Nat + | 0        |                                        |
| AdaptiveReadyCount   | Nat +         | 0        |                                        |
| AdaptiveStaleCount   | Nat +         | 0        |                                        |
| AdaptiveCleanupRecommendedCount | Nat + | 0        |                                        |
| ActiveProgressRowsProcessed | Nat + | 0        |                                        |
| ActiveProgressTargetRows  | Nat +     | 0        |                                        |
| Healthy              | Bool AND      | true     |                                        |
| SchemaVersion        | Nat max       | 0        |                                        |
| EdgeTypes            | Pointwise +   | empty    | map[String]Nat, only present in Graph index |

**Properties proven in Lean:**
- Associativity: `(x + y) + z = x + (y + z)`
- Commutativity: `x + y = y + x`
- Identity: `x + 0 = 0 + x = x`

### 2. Diagnostic Fields (Ordered "Last Wins")

These are merged with "last non-empty wins" semantics and are NOT commutative:

| Field                        | Type     | Rule                        | Notes                              |
|------------------------------|----------|-----------------------------|------------------------------------|
| Error                        | String   | concat "; "               | order-sensitive                     |
| CapabilityLifecycleStatus    | String   | last non-empty              | order-sensitive                     |
| PlannerLastDecision          | String   | last non-empty              | order-sensitive                     |
| PlannerLastFallbackReason    | String   | last non-empty              | order-sensitive                     |
| PlannerLastEstimatedScanRows | Nat      | last non-zero               | order-sensitive                     |
| PlannerLastEstimatedResultBuckets | Nat  | last non-zero               | order-sensitive                     |
| PlannerLifecycleBlockingReason | String   | last non-empty              | order-sensitive                     |
| ActiveProgressLifecycle      | String   | last non-empty              | order-sensitive                     |
| LastErrorReason              | String   | last non-empty              | order-sensitive                     |

### 3. Backfill Progress — The Identity Ambiguity

**Problem:** `BackfillProgress` is modeled as `float64` with a separate `Rebuilding`
boolean. The identity for the "min among rebuilding" operation is ambiguous:

- When `Rebuilding = false`, the `BackfillProgress` value is meaningless but defaults to `0.0`
- When merging `Rebuilding: false, Progress: 0.0` with `Rebuilding: true, Progress: 0.5`, the
  result should be `Rebuilding: true, Progress: 0.5` (min over rebuilding shards)
- But `0.0` as a float is indistinguishable from "not rebuilding" in the current model

**Lean model:** `Option Float` where:
- `none` = not rebuilding
- `some f` = rebuilding at progress `f`

This makes the invariant explicit: progress only matters when `isRebuilding` is true.

**Recommendation:** Consider adding a `BackfillProgress` struct with explicit fields
in the Go API to make the semantics unambiguous.

### 4. PlannerLifecycleReady — Mixed Model

`PlannerLifecycleReady` uses AND semantics (like Healthy), but it's a diagnostic
field. In the Lean model, this is treated as part of `StatsAccumulator` with AND
semantics, consistent with the Go implementation.

### 5. Implementation Decision Needed

**Question:** Are the order-sensitive diagnostic fields acceptable as-is, or should
we make them explicitly deterministic?

- **Option A (Keep as-is):** The diagnostic fields are intentionally order-dependent
  for debugging convenience. They are NOT part of the distributed aggregate and do
  not affect correctness. The Go implementation uses them only for display/alerting.
  **Verdict: Acceptable.** The Lean model should capture this distinction.

- **Option B (Add timestamps):** Add timestamps/epochs to diagnostic fields so that
  "latest" is well-defined regardless of order. **Verdict: Over-engineering for now.**
  The current approach works because diagnostics are consumed by humans/operators
  who understand that order may vary.

**Decision: Option A.** The Lean model captures the two-tier split:
- `combine` for reducible fields (commutative monoid, order-independent)
- `mergeDiagnostics` for diagnostic fields (ordered "last wins", order-dependent)

## 6. Semantic Mismatches Found

### Mismatch 1: Empty Source Handling

**Go:** `MergeIndexStats` with empty `src` (len=0) returns early without merging.
**Lean:** Not applicable (model doesn't have union encoding).
**Verdict:** No mismatch — this is an implementation detail of the JSON union encoding.

### Mismatch 2: SchemaVersion Comparison Direction

**Go:** `if srcAlg.SchemaVersion > dstAlg.SchemaVersion { dstAlg.SchemaVersion = srcAlg.SchemaVersion }`
**Lean:** `max x.schemaVersion y.schemaVersion`
Both use "max wins", which matches OR semantics for version progression.
**Verdict:** No mismatch.

### Mismatch 3: Status Field "Latest Wins" vs "First Wins"

**Go:** Uses `if src != "" { dst = src }` — last non-empty wins.
**Lean:** Uses `orElse` with right-biased preference — last non-empty wins.
**Verdict:** Matches.

### Mismatch 4: Healthy Field Semantics

**Go:** `dstAlg.Healthy = dstAlg.Healthy && srcAlg.Healthy` — AND.
**Lean:** `x.healthy && y.healthy` — AND.
**Verdict:** Matches.

### Mismatch 5: EdgeTypes nil Handling

**Go:** If `srcGraph.EdgeTypes` is nil, the field is not merged (skip).
**Lean:** Pointwise addition always defined (empty map = all zeros).
**Verdict:** Potential semantic difference. In Go, a nil map is treated as
"no edge types"; in Lean, an empty map is "zero edge types". For the reducible
merge, nil should be treated as identity (empty map). This is a translation detail.

## 7. Summary of Findings

No critical semantic bugs found in `MergeIndexStats`. The existing implementation
is correct for both commutative aggregate fields and ordered diagnostic fields.

The key improvement from the Lean model is making the two-tier distinction
explicit:
1. **Reducible fields** form a commutative monoid → order-independent → safe for distributed aggregation
2. **Diagnostic fields** use ordered merge → order-dependent → must be applied last, with deterministic ordering

This split provides a clear contract for future implementation changes and
enables property-based testing of the commutative core.
