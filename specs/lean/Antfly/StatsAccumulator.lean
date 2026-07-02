import Mathlib

/-!
# StatsAccumulator — Commutative Monoid for Index Stats Aggregation

This file models the *reducible* part of `MergeIndexStats`: counters combine by
addition, health-like booleans combine by AND/OR, schema versions combine by
max, and edge-type maps combine by pointwise addition.  Diagnostic fields are
modeled separately because their production semantics are intentionally
order-sensitive ("last non-empty wins").
-/

namespace Antfly

/-! ## 1. Reducible Counters (Nat with addition) -/

section Counters

variable (a b c : Nat)

theorem combine_nat_add_assoc : (a + b) + c = a + (b + c) := by omega

theorem combine_nat_add_comm : a + b = b + a := by omega

theorem combine_nat_add_identity : a + 0 = a := by omega

theorem combine_nat_add_identity_right : 0 + a = a := by omega

end Counters

/-! ## 2. Reducible Booleans (AND and OR monoids) -/

section Booleans

theorem combine_bool_and_assoc (a b c : Bool) : ((a && b) && c) = (a && (b && c)) := by
  cases a <;> cases b <;> cases c <;> rfl

theorem combine_bool_and_comm (a b : Bool) : (a && b) = (b && a) := by
  cases a <;> cases b <;> rfl

theorem combine_bool_and_identity (a : Bool) : (a && true) = a := by
  cases a <;> rfl

theorem combine_bool_and_identity_right (a : Bool) : (true && a) = a := by
  cases a <;> rfl

theorem combine_bool_or_assoc (a b c : Bool) : ((a || b) || c) = (a || (b || c)) := by
  cases a <;> cases b <;> cases c <;> rfl

theorem combine_bool_or_comm (a b : Bool) : (a || b) = (b || a) := by
  cases a <;> cases b <;> rfl

theorem combine_bool_or_identity (a : Bool) : (a || false) = a := by
  cases a <;> rfl

theorem combine_bool_or_identity_right (a : Bool) : (false || a) = a := by
  cases a <;> rfl

end Booleans

/-! ## 3. Schema Version (Nat with max) -/

section SchemaVersion

theorem combine_schema_max_assoc (a b c : Nat) : max (max a b) c = max a (max b c) := by
  exact Nat.max_assoc a b c

theorem combine_schema_max_comm (a b : Nat) : max a b = max b a := Nat.max_comm _ _

theorem combine_schema_max_identity (a : Nat) : max a 0 = a := by omega

theorem combine_schema_max_identity_right (a : Nat) : max 0 a = a := by omega

end SchemaVersion

/-! ## 4. Edge Types (String → Nat with pointwise addition) -/

section EdgeTypes

theorem combine_edge_types_add_assoc (m n p : String → Nat) (k : String) :
    (m k + n k) + p k = m k + (n k + p k) := by omega

theorem combine_edge_types_add_comm (m n : String → Nat) (k : String) :
    m k + n k = n k + m k := by omega

theorem combine_edge_types_add_identity (m : String → Nat) (k : String) :
    m k + 0 = m k := by omega

end EdgeTypes

/-! ## 5. Backfill Progress

The production value is a float paired with a `Rebuilding` flag.  For the proof
we use a discrete progress rank (`Nat`, e.g. basis points) inside an option-like
sum type.  This captures the algebraic point: not rebuilding is the identity,
and rebuilding shards combine by minimum progress.
-/

section BackfillProgress

inductive BackfillProgress : Type where
  | notRebuilding : BackfillProgress
  | rebuildingAt : Nat → BackfillProgress
  deriving Repr, DecidableEq

open BackfillProgress

def BackfillProgress.combine : BackfillProgress → BackfillProgress → BackfillProgress
  | notRebuilding, p => p
  | p, notRebuilding => p
  | rebuildingAt f, rebuildingAt g => rebuildingAt (min f g)

theorem backfill_combine_assoc (p q r : BackfillProgress) :
    (p.combine q).combine r = p.combine (q.combine r) := by
  cases p <;> cases q <;> cases r <;> simp [BackfillProgress.combine, Nat.min_assoc]

theorem backfill_combine_comm (p q : BackfillProgress) : p.combine q = q.combine p := by
  cases p <;> cases q <;> simp [BackfillProgress.combine, Nat.min_comm]

theorem backfill_combine_identity (p : BackfillProgress) : p.combine notRebuilding = p := by
  cases p <;> rfl

theorem backfill_combine_identity_right (p : BackfillProgress) : notRebuilding.combine p = p := by
  cases p <;> rfl

end BackfillProgress

/-! ## 6. StatsAccumulator -/

section StatsAccumulator

@[ext]
structure StatsAccumulator where
  totalIndexed : Nat
  diskUsage : Nat
  walBacklog : Nat
  totalNodes : Nat
  totalTerms : Nat
  totalEdges : Nat
  backfillItemsProcessed : Nat
  parseErrorCount : Nat
  plannerSelected : Nat
  plannerFallbackCount : Nat
  adaptiveProgressCount : Nat
  recommendationCount : Nat
  adaptiveBackfillingCount : Nat
  adaptiveReadyCount : Nat
  adaptiveStaleCount : Nat
  adaptiveCleanupRecommendedCount : Nat
  activeProgressRowsProcessed : Nat
  activeProgressTargetRows : Nat
  healthy : Bool
  schemaVersion : Nat
  edgeTypes : String → Nat

/-- Empty accumulator with all fields at identity values. -/
def StatsAccumulator.zero : StatsAccumulator :=
  { totalIndexed := 0
    diskUsage := 0
    walBacklog := 0
    totalNodes := 0
    totalTerms := 0
    totalEdges := 0
    backfillItemsProcessed := 0
    parseErrorCount := 0
    plannerSelected := 0
    plannerFallbackCount := 0
    adaptiveProgressCount := 0
    recommendationCount := 0
    adaptiveBackfillingCount := 0
    adaptiveReadyCount := 0
    adaptiveStaleCount := 0
    adaptiveCleanupRecommendedCount := 0
    activeProgressRowsProcessed := 0
    activeProgressTargetRows := 0
    healthy := true
    schemaVersion := 0
    edgeTypes := fun _ => 0 }

/-- Combine two accumulators pointwise using the reducible-field monoids. -/
def StatsAccumulator.combine (x y : StatsAccumulator) : StatsAccumulator :=
  { totalIndexed := x.totalIndexed + y.totalIndexed
    diskUsage := x.diskUsage + y.diskUsage
    walBacklog := x.walBacklog + y.walBacklog
    totalNodes := x.totalNodes + y.totalNodes
    totalTerms := x.totalTerms + y.totalTerms
    totalEdges := x.totalEdges + y.totalEdges
    backfillItemsProcessed := x.backfillItemsProcessed + y.backfillItemsProcessed
    parseErrorCount := x.parseErrorCount + y.parseErrorCount
    plannerSelected := x.plannerSelected + y.plannerSelected
    plannerFallbackCount := x.plannerFallbackCount + y.plannerFallbackCount
    adaptiveProgressCount := x.adaptiveProgressCount + y.adaptiveProgressCount
    recommendationCount := x.recommendationCount + y.recommendationCount
    adaptiveBackfillingCount := x.adaptiveBackfillingCount + y.adaptiveBackfillingCount
    adaptiveReadyCount := x.adaptiveReadyCount + y.adaptiveReadyCount
    adaptiveStaleCount := x.adaptiveStaleCount + y.adaptiveStaleCount
    adaptiveCleanupRecommendedCount := x.adaptiveCleanupRecommendedCount + y.adaptiveCleanupRecommendedCount
    activeProgressRowsProcessed := x.activeProgressRowsProcessed + y.activeProgressRowsProcessed
    activeProgressTargetRows := x.activeProgressTargetRows + y.activeProgressTargetRows
    healthy := x.healthy && y.healthy
    schemaVersion := max x.schemaVersion y.schemaVersion
    edgeTypes := fun k => x.edgeTypes k + y.edgeTypes k }

theorem stats_accumulator_combine_assoc (x y z : StatsAccumulator) :
    (x.combine y).combine z = x.combine (y.combine z) := by
  ext k <;> simp [StatsAccumulator.combine, Nat.add_assoc, Nat.max_assoc]
  · cases x.healthy <;> cases y.healthy <;> cases z.healthy <;> rfl

theorem stats_accumulator_combine_comm (x y : StatsAccumulator) :
    x.combine y = y.combine x := by
  ext k <;> simp [StatsAccumulator.combine, Nat.add_comm, Nat.max_comm]
  · cases x.healthy <;> cases y.healthy <;> rfl

theorem stats_accumulator_combine_identity (x : StatsAccumulator) :
    x.combine StatsAccumulator.zero = x := by
  ext k <;> simp [StatsAccumulator.combine, StatsAccumulator.zero]

theorem stats_accumulator_combine_identity_right (x : StatsAccumulator) :
    StatsAccumulator.zero.combine x = x := by
  ext k <;> simp [StatsAccumulator.combine, StatsAccumulator.zero]

end StatsAccumulator

/-! ## 7. Diagnostic Merge — Ordered "Last Wins" -/

section Diagnostics

structure Diagnostics where
  error : Option String
  capabilityLifecycleStatus : Option String
  plannerLastDecision : Option String
  plannerLastFallbackReason : Option String
  plannerLastEstimatedScanRows : Option Nat
  plannerLastEstimatedResultBuckets : Option Nat
  plannerLifecycleBlockingReason : Option String
  activeProgressLifecycle : Option String
  lastErrorReason : Option String
  deriving Repr, DecidableEq

def Diagnostics.zero : Diagnostics :=
  { error := none
    capabilityLifecycleStatus := none
    plannerLastDecision := none
    plannerLastFallbackReason := none
    plannerLastEstimatedScanRows := none
    plannerLastEstimatedResultBuckets := none
    plannerLifecycleBlockingReason := none
    activeProgressLifecycle := none
    lastErrorReason := none }

/-- Right-biased option merge: "last non-empty wins". -/
def Diagnostics.merge (d₁ d₂ : Diagnostics) : Diagnostics :=
  { error := d₂.error.orElse (fun _ => d₁.error)
    capabilityLifecycleStatus := d₂.capabilityLifecycleStatus.orElse (fun _ => d₁.capabilityLifecycleStatus)
    plannerLastDecision := d₂.plannerLastDecision.orElse (fun _ => d₁.plannerLastDecision)
    plannerLastFallbackReason := d₂.plannerLastFallbackReason.orElse (fun _ => d₁.plannerLastFallbackReason)
    plannerLastEstimatedScanRows := d₂.plannerLastEstimatedScanRows.orElse (fun _ => d₁.plannerLastEstimatedScanRows)
    plannerLastEstimatedResultBuckets := d₂.plannerLastEstimatedResultBuckets.orElse (fun _ => d₁.plannerLastEstimatedResultBuckets)
    plannerLifecycleBlockingReason := d₂.plannerLifecycleBlockingReason.orElse (fun _ => d₁.plannerLifecycleBlockingReason)
    activeProgressLifecycle := d₂.activeProgressLifecycle.orElse (fun _ => d₁.activeProgressLifecycle)
    lastErrorReason := d₂.lastErrorReason.orElse (fun _ => d₁.lastErrorReason) }

theorem diagnostics_merge_not_commutative :
    ∃ (d₁ d₂ : Diagnostics), d₁.merge d₂ ≠ d₂.merge d₁ := by
  let d₁ : Diagnostics :=
    { Diagnostics.zero with error := some "err1" }
  let d₂ : Diagnostics :=
    { Diagnostics.zero with error := some "err2" }
  refine ⟨d₁, d₂, ?_⟩
  decide

end Diagnostics

end Antfly
