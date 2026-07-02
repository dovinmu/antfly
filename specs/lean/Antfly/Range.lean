import Mathlib

/-!
# Range Algebra — Key/Range Partition Model

This file models half-open routing ranges `[start, end)`.  To keep this pilot
focused on the split algebra rather than byte-string ordering machinery,
`ByteSlice` is represented by its order rank (`Nat`).  The production byte-slice
lexicographic order can be connected to this model by a monotone/rank
abstraction over the valid (non-sentinel) keyspace.
-/

namespace Antfly

/-- A byte-slice key represented by its rank in the modeled key order. -/
abbrev ByteSlice := Nat

namespace ByteSlice

/-- Empty byte slice / lowest key rank. -/
def empty : ByteSlice := 0

@[simp] theorem empty_le (a : ByteSlice) : empty ≤ a := by
  simp [empty]

end ByteSlice

/-! ## Range -/

/-- A half-open range `[start, end_)`.

`start = none` means unbounded below.  `end_ = none` means unbounded above.
-/
structure Range where
  start : Option ByteSlice
  end_ : Option ByteSlice
  deriving Repr, DecidableEq

def Range.universe : Range := { start := none, end_ := none }

def Range.lowerBound (s : ByteSlice) : Range := { start := some s, end_ := none }

def Range.upperBound (e : ByteSlice) : Range := { start := none, end_ := some e }

def Range.closed (s e : ByteSlice) : Range := { start := some s, end_ := some e }

def Range.lowerContains (start : Option ByteSlice) (k : ByteSlice) : Prop :=
  match start with
  | none => True
  | some s => s ≤ k

def Range.upperContains (end_ : Option ByteSlice) (k : ByteSlice) : Prop :=
  match end_ with
  | none => True
  | some e => k < e

/-- Key containment for half-open ranges. -/
def Range.contains (r : Range) (k : ByteSlice) : Prop :=
  Range.lowerContains r.start k ∧ Range.upperContains r.end_ k

instance (r : Range) (k : ByteSlice) : Decidable (r.contains k) := by
  unfold Range.contains Range.lowerContains Range.upperContains
  cases r.start <;> cases r.end_ <;> infer_instance

/-- Boolean view for executable tests and extraction-oriented code. -/
def Range.containsBool (r : Range) (k : ByteSlice) : Bool :=
  decide (r.contains k)

/-- Two ranges are adjacent when the left end equals the right start. -/
def Range.adjacent (r₁ r₂ : Range) : Prop :=
  ∃ b : ByteSlice, r₁.end_ = some b ∧ r₂.start = some b

/-- Split `[a, c)` at `b`, yielding `[a, b)` and `[b, c)`. -/
def Range.split (r : Range) (b : ByteSlice) : Range × Range :=
  let left : Range := { start := r.start, end_ := some b }
  let right : Range := { start := some b, end_ := r.end_ }
  (left, right)

@[simp] theorem Range.contains_universe (k : ByteSlice) :
    Range.universe.contains k := by
  simp [Range.universe, Range.contains, Range.lowerContains, Range.upperContains]

@[simp] theorem Range.contains_closed_iff (s e k : ByteSlice) :
    (Range.closed s e).contains k ↔ s ≤ k ∧ k < e := by
  simp [Range.closed, Range.contains, Range.lowerContains, Range.upperContains]

/-- The left side of a split contains exactly the original lower-bound side and `k < b`. -/
theorem Range.split_left_contains_iff (r : Range) (b k : ByteSlice) :
    (Range.split r b).1.contains k ↔ Range.lowerContains r.start k ∧ k < b := by
  cases r with
  | mk start end_ =>
      cases start <;> simp [Range.split, Range.contains, Range.lowerContains, Range.upperContains]

/-- The right side of a split contains exactly `b ≤ k` and the original upper-bound side. -/
theorem Range.split_right_contains_iff (r : Range) (b k : ByteSlice) :
    (Range.split r b).2.contains k ↔ b ≤ k ∧ Range.upperContains r.end_ k := by
  cases r with
  | mk start end_ =>
      cases end_ <;> simp [Range.split, Range.contains, Range.lowerContains, Range.upperContains]

/-- The two halves of a split are disjoint. -/
theorem Range.split_halves_disjoint (r : Range) (b k : ByteSlice) :
    ¬ ((Range.split r b).1.contains k ∧ (Range.split r b).2.contains k) := by
  intro h
  have hlt : k < b := (Range.split_left_contains_iff r b k).mp h.1 |>.2
  have hle : b ≤ k := (Range.split_right_contains_iff r b k).mp h.2 |>.1
  exact (Nat.not_lt_of_ge hle) hlt

/-- Splitting preserves coverage in the forward direction: every original key lands in one half. -/
theorem Range.split_covers_original (r : Range) (b k : ByteSlice) :
    r.contains k → (Range.split r b).1.contains k ∨ (Range.split r b).2.contains k := by
  intro h
  by_cases hb : k < b
  · left
    exact (Range.split_left_contains_iff r b k).mpr ⟨h.1, hb⟩
  · right
    have hle : b ≤ k := Nat.le_of_not_gt hb
    exact (Range.split_right_contains_iff r b k).mpr ⟨hle, h.2⟩

/-- If the split point is below the original upper bound, the left half is inside the original range. -/
theorem Range.split_left_subset_original (r : Range) (b k : ByteSlice)
    (hhi : Range.upperContains r.end_ b) :
    (Range.split r b).1.contains k → r.contains k := by
  intro h
  have hl := (Range.split_left_contains_iff r b k).mp h
  exact ⟨hl.1, by
    cases he : r.end_ with
    | none => simp [Range.upperContains]
    | some e =>
        have hb : k < b := hl.2
        have hbe : b < e := by simpa [he, Range.upperContains] using hhi
        exact Nat.lt_trans hb hbe⟩

/-- If the split point is above the original lower bound, the right half is inside the original range. -/
theorem Range.split_right_subset_original (r : Range) (b k : ByteSlice)
    (hlo : Range.lowerContains r.start b) :
    (Range.split r b).2.contains k → r.contains k := by
  intro h
  have hr := (Range.split_right_contains_iff r b k).mp h
  exact ⟨by
    cases hs : r.start with
    | none => simp [Range.lowerContains]
    | some s =>
        have hsb : s ≤ b := by simpa [hs, Range.lowerContains] using hlo
        exact Nat.le_trans hsb hr.1, hr.2⟩

/-- Splitting at a point inside the original range preserves exactly the original coverage. -/
theorem Range.split_preserves_coverage (r : Range) (b k : ByteSlice)
    (hlo : Range.lowerContains r.start b) (hhi : Range.upperContains r.end_ b) :
    r.contains k ↔ (Range.split r b).1.contains k ∨ (Range.split r b).2.contains k := by
  constructor
  · exact Range.split_covers_original r b k
  · intro h
    rcases h with hleft | hright
    · exact Range.split_left_subset_original r b k hhi hleft
    · exact Range.split_right_subset_original r b k hlo hright

/-- A small partition record for route tables.  The invariant says no key is in two listed ranges. -/
structure Partition where
  ranges : List Range
  nonempty : ranges ≠ []
  pairwiseDisjoint : ∀ ⦃r₁ r₂ : Range⦄,
    r₁ ∈ ranges → r₂ ∈ ranges → r₁ ≠ r₂ → ∀ k, ¬ (r₁.contains k ∧ r₂.contains k)

end Antfly
