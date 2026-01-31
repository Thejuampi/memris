# Concurrency Audit Report

**Date:** 2026-01-31  
**Scope:** Full concurrency model audit - documentation alignment, code correctness, and hazard identification  
**Status:** COMPLETED - Issues Identified, Recommendations Provided

---

## Executive Summary

The concurrency implementation has **significant documentation contradictions** and **potential correctness issues**. While the core seqlock + publish protocol is correctly implemented, the documentation presents conflicting guidance about thread safety guarantees. Additionally, several components that appear thread-safe from the documentation are actually not.

**Critical Finding:** The documentation claims "multi-writer supported" but concurrently states "single-writer required" - these cannot both be true.

---

## 1. Documentation Contradictions (CRITICAL)

### The Problem

Different documentation files present **incompatible concurrency models**:

| Document | Claim | Location |
|----------|-------|----------|
| **CONCURRENCY.md** | "Multi-writer supported" with seqlock + CAS | Line 29, Line 150-152 |
| **DEVELOPMENT.md** | "Single-writer: External synchronization required" | Line 328 |
| **README.md** | "Multi-Reader, Multi-Writer" | Line 260 |
| **storage.md** | "Column writes: Require external coordination" | Line 44-45, Line 75 |

### Impact

Users cannot determine the actual thread safety guarantees without reading source code. This leads to:
- Potential data corruption if users rely on docs claiming multi-writer safety
- Unnecessary synchronization if users follow the single-writer guidance
- Confusion about whether concurrent saves are safe

### Root Cause

Documents were updated at different times during the evolution of the concurrency model:
- CONCURRENCY.md and README were updated to reflect the intended multi-writer design
- DEVELOPMENT.md and storage.md still contain older, more conservative guidance

### Recommendation

**Standardize on ONE model** and update all documents consistently:

**Option A: Multi-writer with seqlock (Current Implementation)**
- Update DEVELOPMENT.md and storage.md to match CONCURRENCY.md
- Document that concurrent saves are thread-safe via seqlock
- Keep the caveat about eventual index consistency

**Option B: Single-writer with external sync (Conservative)**
- Update CONCURRENCY.md and README to state single-writer requirement
- Remove claims of multi-writer support
- Keep seqlock for row-level atomicity but document it as internal detail

**RECOMMENDED: Option A** - The code implements multi-writer support via seqlock. The documentation should reflect reality.

---

## 2. MutableRowIdSet Thread Safety Issue (HIGH SEVERITY)

### The Problem

Both `RowIdArraySet` and `RowIdBitSet` implementations are **NOT thread-safe**, but are accessed concurrently by multiple threads:

**File:** `memris-core/src/main/java/io/memris/kernel/RowIdArraySet.java`
- `add()` and `remove()` modify `values[]` and `size` without synchronization
- `size` field is not volatile - readers may see stale values

**File:** `memris-core/src/main/java/io/memris/kernel/RowIdBitSet.java`
- `add()` and `remove()` modify `bitSet` and `size` without synchronization  
- `size` field is not volatile - readers may see stale values

### How This Manifests

In `HashIndex.java` (lines 31-36):
```java
index.compute(key, (ignored, existing) -> {
    MutableRowIdSet set = existing == null ? setFactory.create(1) : existing;
    set.add(rowId);  // <-- Race condition here!
    return setFactory.maybeUpgrade(set);
});
```

While `ConcurrentHashMap.compute()` serializes writers for the same key, the `MutableRowIdSet` instance is returned and can be accessed by readers while writers mutate it.

### Scenarios Where This Fails

1. **Concurrent add/remove on same key:** Two threads add/remove RowIds for the same index key - internal array corruption possible
2. **Reader sees partially modified set:** A query iterating over the set while it's being modified may see inconsistent data
3. **Upgrade race:** `maybeUpgrade()` creates a new set and copies data - concurrent modifications during copy lead to lost updates

### Impact

- **Data corruption:** Index may return incorrect row IDs
- **Phantom reads:** Queries may return rows that don't match the criteria
- **Missing rows:** Valid rows may not appear in query results

### Recommendation

**Option 1: Lock-free Copy-on-Write (Recommended)**
Implement `MutableRowIdSet` with an `AtomicReference` to an immutable snapshot (array or bitset). `add/remove` create a new snapshot and CAS it into place. Reads use the current snapshot. This avoids in-place mutation and stays lock-free.

**Option 2: Lock-free Atomic Bitset**
For dense sets, use a CAS-based bitset backed by `AtomicLongArray` or segmented `long[]` snapshots. This gives O(1) add/remove with eventual consistency for enumeration.

**Option 3: Document Limitation**
If performance is critical and races are acceptable, document that index queries may return stale/inconsistent results and require external coordination for strict correctness.

---

## 3. Column Visibility and Happens-Before (MEDIUM SEVERITY)

### The Problem

In `PageColumnInt.java`, `PageColumnLong.java`, and `PageColumnString.java`:

```java
public void set(int offset, int value) {
    data[offset] = value;      // Non-volatile write
    present[offset] = 1;       // Non-volatile write
}

public void publish(int newPublished) {
    this.published = newPublished;  // Volatile write
}
```

The `publish()` method writes to a volatile field, but **there is no happens-before relationship** between the `set()` calls and the `publish()` call. While `publish()` itself is safe (monotonic increase), a reader may see the updated `published` count but read stale values from `data[]` or `present[]`.

### Why This Might Be OK

In practice, this may work correctly on x86-64 due to strong memory ordering guarantees. However, on ARM or other weakly-ordered architectures, this is a real bug.

### Recommendation

**Fix the ordering in MethodHandleImplementation.java:**

Current code (lines 777-775, 907):
```java
table.beginSeqLock(rowIndex);
try {
    // ... write columns ...
} finally {
    table.endSeqLock(rowIndex);
}
// ... publish each column ...
```

The seqlock provides visibility for reads that check the seqlock, but scans don't use seqlock - they use the `published` watermark.

**Fix:** Ensure all column writes complete before publish:
```java
// After all columns are written but before publish:
VarHandle.storeStoreFence();  // Ensure writes are visible before volatile publish
// Then publish
```

Or use `AtomicIntegerArray` for the `present` array to ensure visibility.

---

## 4. Index Eventual Consistency Window (LOW SEVERITY)

### The Problem

In `MethodHandleImplementation.java` InsertInterceptor (lines 907-919):

```java
// Publish row to make data visible to scans
for (int i = 0; i < columnFields.size(); i++) {
    // ... publish each column ...
}

// Update ID index
// ... index update happens AFTER publish ...
```

There is a window where:
1. Row is published and visible to scans
2. But ID index hasn't been updated yet

If a concurrent thread calls `findById()` during this window, it won't find the row even though it exists.

### Impact

- Race window is microseconds at most
- Only affects ID lookups immediately after save
- Query will return empty, but retry will succeed
- Not a correctness issue for most use cases

### Recommendation

**Option 1: Accept as documented behavior**
The eventual consistency model already documents that indexes may lag. This is consistent with that model.

**Option 2: Reorder operations**
Update the ID index BEFORE publishing the row. This ensures ID lookups work immediately, though scans may briefly not see the row.

---

## 5. Row Reuse and Generation Validation (VERIFIED CORRECT)

### The Good News

The row reuse mechanism is **correctly implemented**:

1. **Free-list is lock-free:** `LockFreeFreeList` uses Treiber stack algorithm with CAS - correct
2. **Generation tracking works:** Each row allocation updates generation via `AtomicLong`
3. **Stale reference detection:** `isLive()` checks both tombstone and generation (MethodHandleImplementation.java:1002-1018)
4. **Tombstone CAS loop:** `AbstractTable.tombstone()` uses proper CAS to ensure exactly one decrement (lines 255-274)

---

## 6. SeqLock Implementation (VERIFIED CORRECT)

### The Good News

The seqlock implementation is **correctly implemented**:

1. **Writer protocol:** beginSeqLock (even→odd), write, endSeqLock (odd→even) - correct
2. **Reader protocol:** read version, check odd, read data, re-read version, compare - correct
3. **Backoff strategy:** SpinWait → Yield → Park - appropriate progression
4. **Used correctly:** `readWithSeqLock()` wraps all column reads in MethodHandleImplementation (lines 216-230)

---

## Summary Table

| Component | Status | Severity | Action Required |
|-----------|--------|----------|-----------------|
| Documentation alignment | **CONTRADICTORY** | CRITICAL | Standardize on one model |
| MutableRowIdSet thread safety | **NOT THREAD-SAFE** | HIGH | Add synchronization |
| Column visibility (happens-before) | **POTENTIAL ISSUE** | MEDIUM | Add memory fence |
| Index update ordering | **EVENTUAL CONSISTENCY** | LOW | Document or reorder |
| Free-list (LockFreeFreeList) | **CORRECT** | - | None |
| SeqLock | **CORRECT** | - | None |
| Tombstone handling | **CORRECT** | - | None |
| Generation tracking | **CORRECT** | - | None |
| Query-time validation | **CORRECT** | - | None |

---

## Recommended Actions (Priority Order)

### Priority 1: Fix Documentation (1-2 hours)
1. Update DEVELOPMENT.md to remove "single-writer required" claim
2. Update storage.md to remove "column writes require external coordination"
3. Ensure all documents consistently state: "Concurrent saves are thread-safe via seqlock"

### Priority 2: Fix MutableRowIdSet Thread Safety (2-4 hours)
1. Add synchronization to `RowIdArraySet` and `RowIdBitSet`
2. Run concurrent index tests to verify fix
3. Measure performance impact (should be minimal for typical use)

### Priority 3: Add Memory Fence (1 hour)
1. Add `VarHandle.storeStoreFence()` before column publish in `MethodHandleImplementation.java`
2. Or document that this is only safe on x86-64

### Priority 4: Optional Improvements
1. Add concurrent index stress tests
2. Document the eventual consistency window for ID lookups
3. Consider reordering ID index update before publish

---

## Test Recommendations

Add these tests to verify the fixes:

1. **ConcurrentIndexModificationTest:** Multiple threads adding/removing from same index key concurrently
2. **ColumnVisibilityTest:** Verify published writes are visible to concurrent readers
3. **SeqLockStressTest:** Heavy concurrent read/write on same row
4. **RowReuseTest:** Concurrent delete/insert cycles to verify generation handling

---

## Conclusion

The concurrency model has a **solid foundation** (seqlock, free-list, generation tracking) but **documentation and MutableRowIdSet need fixes**. The core storage layer is thread-safe for multi-writer scenarios as documented in CONCURRENCY.md, but the index layer has a thread-safety gap that could cause data corruption under concurrent modification.

**Bottom line:** Fix the MutableRowIdSet synchronization and align the documentation, and the concurrency model will be solid.
