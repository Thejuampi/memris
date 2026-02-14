# Concurrency Model

## Current Implementation

### Concurrency Characteristics

**Thread-Safe Operations:**

| Operation | Thread-Safety | Mechanism | Location |
|-----------|--------------|------------|----------|
| ID generation | Lock-free | `AtomicLong.getAndIncrement()` | `MemrisRepositoryFactory.java` |
| ID index lookups | Lock-free | `ConcurrentHashMap.get()` | `LongIdIndex.java`, `StringIdIndex.java` |
| HashIndex lookups | Lock-free | `ConcurrentHashMap.get()` | `HashIndex.java` |
| RangeIndex lookups | Lock-free | `ConcurrentSkipListMap.get()` | `RangeIndex.java` |
| Query scans | Safe reads | `volatile published` watermark | `PageColumn*.java` |
| Index add/remove | Thread-safe | `compute()` / `computeIfAbsent()` | `HashIndex.java`, `RangeIndex.java` |
| Column writes | Coordinated | Row seqlock (CAS) + publish ordering | `AbstractTable.java` |

**Concurrency Caveats (Eventual Consistency):**

| Operation | Behavior | Impact | Location |
|-----------|----------|--------|----------|
| Index updates | Eventually consistent with row writes | Stale index entries are filtered at query time | `RepositoryRuntime.java` |

### Concurrency Model Summary

**Current Design:**
- **Reads**: Thread-safe, lock-free for most operations
- **Writes**: Thread-safe with row seqlock + CAS (multi-writer supported)
- **Read-Write**: SeqLock provides coordination for row updates and typed reads
- **Isolation**: Best-effort (seqlock for rows, no MVCC for transactions)

## Row-Level SeqLock Implementation

### Overview

Each row has an associated version counter managed via `AtomicLongArray`. The seqlock protocol enables:
- Wait-free reads (with retry on concurrent write)
- Exclusive writes via CAS acquisition
- Detection of concurrent modifications

### Data Structure

```java
// In AbstractTable / GeneratedTable
private final AtomicLongArray rowSeqLocks;  // Per-row version counters
```

### Writer Protocol

```java
// 1. Acquire write lock (CAS even -> odd)
boolean acquired = beginSeqLock(rowIndex);
if (!acquired) {
    // Retry or fail - another writer is active
}

try {
    // 2. Write columns (protected by seqlock)
    columnInts[columnIndex].set(rowIndex, value);
    columnLongs[columnIndex].set(rowIndex, value);
    columnStrings[columnIndex].set(rowIndex, value);
    
    // 3. End write lock (increment to even)
    endSeqLock(rowIndex);
} finally {
    // Ensure lock is released on exception
    if (isSeqLocked(rowIndex)) {
        endSeqLock(rowIndex);
    }
}

// 4. Publish for visibility
publish(rowIndex);
```

### Reader Protocol (Optimistic)

```java
// 1. Capture initial version
long v1 = rowSeqLocks.get(rowIndex);

// 2. Check if write in progress (odd = writing)
if ((v1 & 1) == 1) {
    // Writing in progress, retry
    return readWithRetry(rowIndex);
}

// 3. Read columns
int value = columnInts[columnIndex].get(rowIndex);

// 4. Verify version unchanged
long v2 = rowSeqLocks.get(rowIndex);
if (v1 != v2) {
    // Changed during read, retry
    return readWithRetry(rowIndex);
}

return value;
```

### SeqLock Benefits

| Property | Description |
|----------|-------------|
| **Wait-free reads** | No blocking, only retry on conflict |
| **Single-writer per row** | Only one writer per row at a time via CAS |
| **Version tracking** | Detect concurrent modifications |
| **No read locks** | Readers never block writers or other readers |
| **Backoff strategy** | Thread.onSpinWait -> Thread.yield -> LockSupport.parkNanos |

### SeqLock Limitations

| Limitation | Impact |
|------------|--------|
| **Starvation** | Writer can starve readers under heavy write load |
| **Retry overhead** | High contention causes read retries |
| **No read ordering** | Reads may see partial updates during retry |

## Hot Path vs Write Path

**Hot Path (Read-Only, Thread-Safe, Zero Reflection):**
- Query execution uses TypeCode switches and direct array access (no reflection)
- Scans respect the volatile `published` watermark for safe visibility
- Index lookups use lock-free concurrent maps

**Write Path (Thread-Safe, Eventual Index Consistency):**
- Row allocation uses LockFreeFreeList (CAS-based) - Thread-safe
- Column writes are coordinated by row seqlock - Thread-safe
- Tombstone operations use AtomicIntegerArray (CAS-based) - Thread-safe
- Index updates can race with row writes - queries validate row liveness

## Fixed Critical Issues

### 1. Free-List Race Condition (`AbstractTable.java`) - FIXED

**Problem**: Multiple threads could pop the same row ID from free-list:
```java
// OLD PROBLEM: Two threads could get same index
if (freeListSize > 0) {
    int reusedRowId = freeList[--freeListSize];  // Race condition!
}
```

**Solution**: Replaced `int[]` with `LockFreeFreeList` using CAS-based operations:
```java
// NEW SOLUTION: Lock-free CAS-based pop
LockFreeFreeList freeList = new LockFreeFreeList(capacity);
int reusedRowId = freeList.pop();  // Thread-safe CAS
```

**Impact**: Data corruption eliminated - concurrent saves are now thread-safe.

### 2. Tombstone BitSet Not Thread-Safe (`AbstractTable.java`) - FIXED

**Problem**: Multiple threads could decrement `liveRowCount` for the same row:
```java
// OLD PROBLEM: Check-then-act without synchronization
if (!tombstones.get(index)) {
    tombstones.set(index);
    liveRowCount.decrementAndGet();  // Race condition!
}
```

**Solution**: Replaced `BitSet` with `AtomicIntegerArray` using CAS loops:
```java
// NEW SOLUTION: CAS-based tombstone set
while (true) {
    int current = tombstones.get(index);
    if (current == 1) break;  // Already tombstoned
    if (tombstones.compareAndSet(index, current, 1)) {
        liveRowCount.decrementAndGet();
        break;
    }
}
```

**Impact**: Correct row count - concurrent deletes are now thread-safe.

### 3. Column Writes via SeqLock (`AbstractTable.java`) - IMPLEMENTED

Writes are guarded by a CAS-based row seqlock and published after completion:
```java
beginSeqLock(rowIndex);
// write columns
endSeqLock(rowIndex);
publish(rowIndex);
```

**Impact**: Readers retry on concurrent writes; scans only see published rows.

### 4. SeqLock Implementation (`GeneratedTable.java`) - FIXED

**Problem**: Interface promised seqlock semantics but no version field existed.

**Solution**: Added `AtomicLongArray` for per-row version tracking:
```java
private final AtomicLongArray rowSeqLocks = new AtomicLongArray(capacity);

// Writer: CAS even -> odd (acquire), write, increment (release)
// Reader: Read version, read data, verify version unchanged
```

**Impact**: Readers can detect concurrent writes and retry for consistency.

### 5. RepositoryRuntime ID Counter (`RepositoryRuntime.java`) - FIXED

**Problem**: Plain counter not thread-safe:
```java
private static long idCounter = 1L;
private Long generateNextId() {
    return idCounter++; // Not thread-safe
}
```

**Solution**: Changed to `AtomicLong`:
```java
private static final AtomicLong idCounter = new AtomicLong(1L);
private Long generateNextId() {
    return idCounter.getAndIncrement();
}
```

**Impact**: Duplicate IDs eliminated - ID generation is now thread-safe.

## Current Concurrency Guidance

Writes are lock-free and safe with row seqlock + CAS. Indexes are eventually consistent and validated at query time.

**Note**: External synchronization is only required if you need strict index/row atomicity. Otherwise, concurrent saves/deletes are safe.

## Practical Usage Patterns

**Concurrent Writes (Supported):**
- ID generation, free-list, tombstones are thread-safe
- SeqLock provides coordination for row updates
- Index queries validate row liveness (eventual consistency)

**Single-Threaded Writes + Concurrent Reads (Optimization):**
- Single writer can reduce contention under heavy write workloads
- Readers can safely scan and use indexes concurrently

**Read-Only Multi-Threaded Workloads:**
- Safe out of the box (no reflection, direct arrays, lock-free lookups)
- Scales with CPU cores for scan-heavy queries

---

## Concurrency Improvement Roadmap

### Priority 1: Completed

- ~~Fix free-list race condition~~ - LockFreeFreeList with CAS
- ~~Fix tombstone concurrency~~ - AtomicIntegerArray with CAS
- ~~Implement row seqlock~~ - AtomicLongArray per-row versioning

### Priority 2: Performance Improvements

**2.1 Stripe-Based Index Updates**
- **Algorithm**: Partition `MutableRowIdSet` by hash stripes
- **Complexity**: LOW
- **Expected Improvement**: 4-8x index update throughput
- **Implementation**: Add striped locking to `MutableRowIdSet` implementations

**2.2 Optimistic Locking for Updates**
- **Algorithm**: CAS-based versioning with retry loops
- **Complexity**: LOW
- **Expected Improvement**: 2x throughput for low-contention updates
- **Implementation**: Add version CAS to update operations

### Priority 3: Advanced Features

**3.1 MVCC for Snapshot Isolation**
- **Algorithm**: Per-row version chains with timestamp-based snapshots
- **Complexity**: HIGH
- **Expected Improvement**: 3-5x read throughput, snapshot consistency
- **Implementation**: Extend row storage with version tracking and GC

**3.2 Pessimistic Locking API**
- **Algorithm**: Striped ReadWriteLock with timeout
- **Complexity**: MEDIUM
- **Expected Improvement**: Predictable high-contention behavior
- **Implementation**: Add `@Lock` annotation support and lock manager

**3.3 Concurrent Hash Join**
- **Algorithm**: Radix-partitioned parallel join
- **Complexity**: VERY HIGH
- **Expected Improvement**: Near-linear scaling with CPU cores
- **Implementation**: New join executor for @OneToMany/@ManyToMany support

---

## Well-Known Concurrency Algorithms

### SeqLock (Sequence Lock)

**Description**: Version-based read optimization where readers check version before/after reads and retry if changed.

**How It Works:**
- Writer: Increment version (odd), write, increment version (even)
- Reader: Read version, read data, read version again
- If versions differ or version is odd, retry

**Best Use Case**: Read-mostly workloads with infrequent writes.

**Pros**: Wait-free reads, simple to implement.
**Cons**: Writer can starve readers, read amplification.

**Expected Improvement**: 2-3x read throughput under concurrent single-row updates.

### Optimistic Locking

**Description**: Writers assume no conflicts, detect via CAS, retry on failure.

**How It Works:**
- Each row has a version number
- Read: Capture current version
- Write: CAS version with value increment
- If CAS fails, retry entire operation

**Best Use Case**: Low-contention workloads where conflicts are rare.

**Pros**: No locking overhead in happy path, no deadlocks.
**Cons**: Retry storms at high contention, not suitable for write-heavy workloads.

**Expected Improvement**: 2x throughput for low-contention updates.

### Striped Locking

**Description**: Partition data into independent stripes, each with its own lock.

**How It Works:**
- Hash key to determine stripe
- Each stripe has independent lock
- Operations only lock relevant stripe

**Best Use Case**: Hash-based structures with high-frequency operations.

**Pros**: Reduces contention by factor of stripes, simple to understand.
**Cons**: Can have hot stripes, not suitable for range queries.

**Expected Improvement**: 4-8x throughput for concurrent index updates.

### MVCC (Multi-Version Concurrency Control)

**Description**: Maintain multiple versions of each row to allow concurrent readers to see consistent snapshots without blocking writers.

**How It Works:**
- Each write creates a new row version with incremented timestamp/generation
- Readers track their snapshot timestamp
- Old versions are garbage-collected after all readers complete
- Writes never block reads

**Best Use Case**: Read-heavy workloads requiring snapshot isolation.

**Pros**: Readers never block, snapshot isolation, no deadlocks.
**Cons**: Memory overhead for old versions, complex garbage collection, write amplification.

**Expected Improvement**: 3-5x read throughput, snapshot consistency.

### Lock-Free Data Structures

**Description**: Use atomic operations (CAS) for lock-free concurrent access.

**How It Works:**
- Use `AtomicReference`, `AtomicInteger`, `AtomicLong`
- CAS loops for updates
- Wait-free reads or lock-free updates

**Best Use Case**: Hot-path operations, high-frequency updates.

**Pros**: No lock overhead, high throughput, no deadlocks.
**Cons**: Very complex to implement correctly, ABA problem, limited operations supported.

**Expected Improvement**: 5-10x throughput for hot-path operations.

---

## Performance Impact

| Improvement | Complexity | Expected Throughput Gain | Status |
|-------------|-----------|-------------------------|--------|
| Fix free-list race | HIGH | Correctness only | COMPLETED |
| Fix tombstone concurrency | LOW | Correctness only | COMPLETED |
| SeqLock rows | MEDIUM | 2-3x | COMPLETED |
| Striped indexes | LOW | 4-8x | PLANNED |
| Optimistic locking | LOW | 2x | PLANNED |
| Pessimistic API | MEDIUM | Predictable | PLANNED |
| MVCC | HIGH | 3-5x | PLANNED |
| Concurrent join | VERY HIGH | Near-linear scaling | PLANNED |

---

### Performance Characteristics

| Operation | Complexity | Hot Path Notes |
|-----------|------------|----------------|
| HashIndex lookup | O(1) | Lock-free `ConcurrentHashMap.get()` |
| RangeIndex lookup | O(log n) | `ConcurrentSkipListMap.get()` |
| Query scan | O(n) | Direct array scan, `published` watermark |
| Typed read | O(1) | Direct array access, seqlock retry |
| Save / delete | O(1) | Thread-safe via row seqlock + CAS |

---

## See Also

- [ROADMAP.md](ROADMAP.md) - Future feature roadmap including concurrency improvements
- [ARCHITECTURE.md](ARCHITECTURE.md) - Overall architecture
- [SPRING_DATA.md](SPRING_DATA.md) - Spring Data integration
