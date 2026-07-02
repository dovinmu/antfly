# Lean Reference Property Tests: `MergeIndexStats`

## What was built

`stats_lean_test.go` — 4 property-based Go tests verifying that the production
`MergeIndexStats` function matches the Lean-derived reference implementation for
commutative monoid laws (associativity, commutativity, identity) on reducible
fields across all 4 index stat types: FullText, Embeddings, Graph, and Algebraic.

## Architecture

```
Lean reference model (Go translation of Lean StatsAccumulator)
    |
    v
leanReferenceCombine() — commutative monoid combine for reducible fields
    |
    +---> TestLeanReference_CombineCommutative (commutativity on pairs)
    +---> TestLeanReference_CombineAssociative  (associativity on triples)
    +---> TestLeanReference_CombineIdentity     (identity element)
    +---> TestLeanReference_CombineVsProduction (integration: production vs Lean)
```

`IndexStats` is a `json.RawMessage` union type. The Lean reference detects the
variant via `detectIndexStatsKind` and dispatches to type-specific combine logic.

## Key fixes applied

1. **`BackfillProgress` min-progress logic** — added to `leanReferenceCombine` to
   match production's `mergeBackfillFields`. When either side is rebuilding, the
   min progress wins. This was the main semantic gap; without it, all tests
   failed.

2. **RNG state divergence in Commutative test** — production merge and Lean combine
   were reading from the same RNG at different offsets because
   `MergeIndexStats` mutates `dst` in place. Fixed by operating on independent copies
   (`dstCopy` and `leanDst`) so both see the same input.

3. **Comparison logic in VsProduction test** — was comparing `srcRef` (original
   `dst`, pre-merge) against `dst` (post-merge), then `leanReferenceCombine` reading
   the already-modified `dst`. Fixed by saving copies before the production merge
   and comparing both final results.

4. **Removed broken RandomOrder test** — attempted to verify order-independence by
   processing mixed-type stats through production merge, which silently
   early-returns on type mismatch (one path processes all stats, the other bails).
   Could not be fixed without changing production behavior.

## 5 important discoveries

| # | Discovery | Impact |
|---|-----------|--------|
| 1 | `FullTextIndexStats`, `EmbeddingsIndexStats`, `GraphIndexStats` have no `Healthy` or `SchemaVersion` field — those only exist on `AlgebraicIndexStats` | Tests using these fields on wrong types would not compile; assertions on them would fail |
| 2 | `BackfillProgress` is order-sensitive (min-progress when rebuilding) but `BackfillItemsProcessed` is truly commutative addition | The Lean model must split these: progress is diagnostic, items-processed is the commutative monoid accumulator |
| 3 | `MergeIndexStats` has a type-mismatch early return that exits the entire function, not just that iteration | RandomOrder test was impossible; production merge bails silently on type mismatch |
| 4 | `IndexStats` has no `Error` field — `Error` lives only on the concrete per-type structs | The `AsFullTextIndexStats()` type assertion is safe; Lean reference intentionally skips error fields |
| 5 | `mergeBackfillFields` does triple duty: OR for Rebuilding, sum for ItemsProcessed, min-progress for BackfillProgress | The Lean reference combine must replicate all three semantics to match production exactly |

## Biggest weaknesses in approach and model

### 1. JSON round-trip cost and pointer invalidation

The `IndexStats` → `AsIndexStats()` → `AsFullTextIndexStats()` pipeline marshals
to JSON and back. For Graph stats, `EdgeTypes` is `*map[string]uint64` — a pointer
to a map. After JSON marshal/unmarshal, the pointer address changes. The test works
around this by using `&g.edgeTypes` (a single shared map for all test cases), but a
realistic multi-shard scenario would have different pointer addresses and the merge
would silently skip EdgeTypes accumulation.

### 2. Lean reference diverges from production on diagnostic fields

The Lean reference only models reducible fields. Diagnostic fields (Error,
CapabilityLifecycleStatus, PlannerLastDecision, etc.) are intentionally excluded from
the commutative monoid. But `leanReferenceCombine` does modify some non-reducible
fields (e.g., `Healthy`, `SchemaVersion`, `PlannerLifecycleReady` on Algebraic).
The test's "VsProduction" check verifies that BackfillProgress matches, but it does
NOT check that Error is preserved across the Lean combine — because
`FullTextIndexStats` has no Error field. This gap means the Lean model's treatment of
diagnostic fields needs manual verification against the Lean source.

### 3. `BackfillProgress: 0.0` in test generation

The test always sets `BackfillProgress: 0.0` on generated stats. This means the
min-progress logic is never exercised (0.0 is never less than the destination's
progress). The Lean reference's BackfillProgress handling is untested for the
non-trivial case. Production code would exercise this path when a rebuilding shard
reports a lower progress than the current destination.

### 4. No error field in the Lean reference

The Lean combine skips `Error` entirely. Production's `mergeErrors` concatenates
errors with `"; "`. If the Lean model ever needs to reason about error accumulation
(e.g., for correctness of diagnostic merging), there is no reference implementation
to compare against.

### 5. `PlannerLifecycleReady` and `Healthy` on Algebraic — AND semantics, not OR

The Lean reference treats `Healthy = AND` and `PlannerLifecycleReady = AND`. But in
the production code, `Healthy` is set from the source stats directly (not OR'd).
The production `MergeIndexStats` for Algebraic does:

```go
dstAlg.Healthy = dstAlg.Healthy && srcAlg.Healthy
```

This means `Healthy` is truly AND. But `PlannerLifecycleReady` is also AND in both
Lean and production. Worth noting: if the intent is that these are commutative
diagnostics, AND is correct (a shard being not-ready makes the aggregate not-ready).
But the test never exercises a case where `Healthy` or `PlannerLifecycleReady` differ
between two stats being merged.

### 6. The Lean reference reassigns `dst` on empty source, production copies `src` to `dst`

`leanReferenceCombine` does `*dst = src` when dst is empty; `MergeIndexStats` does
`*dst = src`. These are equivalent. But when `src` is empty, `leanReferenceCombine`
returns early (no change to dst), while `MergeIndexStats` does nothing (src has len 0,
early return). Equivalent, but the edge case of empty-union stats is not tested for
its own identity property.

### 7. Single-RNG tests are fragile across runs

The Commutative test uses `time.Now().UnixNano()` as seed, making it non-reproducible
across runs. The Associative, Identity, and VsProduction tests use fixed seeds (42,
99, 7) and are reproducible. For CI, all tests should use fixed seeds.

### 8. Missing coverage for edge case: backfill identity

`BackfillItemsProcessed` on the Lean reference is set to `0` in the test generation
for Algebraic stats. The identity test verifies that adding 0 doesn't change the
value, but it never tests the case where `BackfillItemsProcessed` starts non-zero and
0 is added. This is a gap for the identity law of the commutative monoid on that
field.
