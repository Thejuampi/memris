# Concurrency Model

## Current Implementation

### Concurrency Characteristics

**Thread-Safe Operations:**

| Operation | Thread-Safety | Mechanism | Location |
|-----------|--------------|------------|----------|
| ID generation | ✅ Lock-free | `AtomicLong.getAndIncrement()` | `MemrisRepositoryFactory.java:287` |
| ID index lookups | ✅ Lock-free | `ConcurrentHashMap.get()` | `LongIdIndex.java:38`, `StringIdIndex.java:38` |
| HashIndex lookups | ✅ Lock-free | `ConcurrentHashMap.get()` | `HashIndex.java:67` |
| RangeIndex lookups | ✅ Lock-free | `ConcurrentSkipListMap.get()` | `RangeIndex.java:51` |
| Query scans | ✅ Safe reads | `volatile published` watermark | `PageColumn*.java:20` |
| Index add/remove | ✅ Thread-safe | `compute()` / `computeIfAbsent()` | `HashIndex.java:31`, `RangeIndex.java:34` |

**NOT Thread-Safe (External Synchronization Required):**

| Operation | Issue | Impact | Location |
|-----------|--------|---------|----------|
| Entity saves | Free-list race condition | Data corruption | `AbstractTable.java:114-136` |
| RepositoryRuntime ID generation | Unsynchronized static counter | Duplicate IDs | `RepositoryRuntime.java:37, 225-227` |
| Row allocation | Unsynchronized free-list | Duplicate row IDs | `AbstractTable.java:36-37` |
| Column writes | No atomicity between writes | Torn reads | `PageColumn*.java:79-84` |
| Entity deletes | Unsynchronized tombstones | Double decrement | `AbstractTable.java:175-186` |
| Index updates | Race with column writes | Inconsistent state | `RepositoryRuntime.java:203-220` |

### Concurrency Model Summary

**Current Design:**
- **Reads**: Thread-safe, lock-free for most operations
- **Writes**: Single-threaded (assumes external coordination)
- **Read-Write**: No coordination, potential inconsistency
- **Isolation**: Best-effort (no MVCC, no transactions)

### Hot Path vs Write Path

**Hot Path (Read-Only, Thread-Safe, Zero Reflection):**
- Query execution uses TypeCode switches and direct array access (no reflection)
- Scans respect the volatile `published` watermark for safe visibility
- Index lookups use lock-free concurrent maps

**Write Path (Requires External Synchronization):**
- Row allocation and free-list reuse are not synchronized
- Column writes are multi-step and not atomic
- Tombstone operations are not synchronized
- Index updates can race with row writes

### Critical Issues

**1. Free-List Race Condition** (`AbstractTable.java:116-117`)
Multiple threads can pop the same row ID from free-list:
```java
// PROBLEM: Two threads can get same index
if (freeListSize > 0) {
    int reusedRowId = freeList[--freeListSize];  // Race condition!
    // Both threads write to same offset
}
```

**Impact**: Data corruption when concurrent saves reuse deleted rows.

**2. Tombstone BitSet Not Thread-Safe** (`AbstractTable.java:175-186`)
Multiple threads can decrement `liveRowCount` for the same row:
```java
// PROBLEM: Check-then-act without synchronization
if (!tombstones.get(index)) {
    tombstones.set(index);
    liveRowCount.decrementAndGet();  // Race condition!
}
```

**Impact**: Incorrect row count with concurrent deletes.

**3. Column Writes Not Atomic** (`PageColumn*.java:79-84`)
No atomicity between `data[]` and `present[]` writes:
```java
// PROBLEM: Two-step write without coordination
data[offset] = value;
present[offset] = 1;  // Reader sees null or value + present=false
```

**Impact**: Readers can see torn or partially-written state.

**4. SeqLock Not Actually Implemented** (`GeneratedTable.java:51-53`, `GeneratedTable.java:119-124`)
Interface promises seqlock semantics for writes/reads but no version field exists in tables.

**Impact**: Design gap, documented but not implemented.

**5. RepositoryRuntime ID Counter Is Not Atomic** (`RepositoryRuntime.java:37, 225-227`)
ID generation in RepositoryRuntime is a plain static counter:
```java
private static long idCounter = 1L;
...
private Long generateNextId() {
    return idCounter++; // Not thread-safe
}
```

**Impact**: Duplicate IDs under concurrent saves in RepositoryRuntime paths.

### Current Concurrency Workarounds

For applications requiring concurrent saves/deletes:

```java
// Option 1: Synchronize on repository
synchronized (repository) {
    repository.save(entity);
}

// Option 2: Use single writer thread
ExecutorService singleWriter = Executors.newSingleThreadExecutor();
singleWriter.submit(() -> repository.save(entity));

// Option 3: Partition repositories by entity type
Map<Class<?>, Repository<?>> repositories = new ConcurrentHashMap<>();
repositories.computeIfAbsent(entity.getClass(), k -> new Repository<T>());
```

### Practical Usage Patterns

**Single-Threaded Writes + Concurrent Reads (Recommended Default):**
- Use a single writer thread (or external lock) for save/delete
- Readers can safely scan and use indexes concurrently

**Read-Only Multi-Threaded Workloads:**
- Safe out of the box (no reflection, direct arrays, lock-free lookups)
- Scales with CPU cores for scan-heavy queries

**Concurrent Writes:**
- Requires external synchronization today
- Use one of the workarounds above, or partition repositories per entity type

---

## Concurrency Improvement Roadmap

### Priority 1: Fix Critical Issues (Correctness)

**1.1 Fix Free-List Race Condition**
**Algorithm**: Lock-free stack or striped locking
**Complexity**: HIGH (lock-free) or MEDIUM (striped)
**Expected Improvement**: Eliminates data corruption
**Implementation**: Replace `int[] freeList` with `ConcurrentLinkedQueue` or striped locks

**1.2 Fix Tombstone BitSet Concurrency**
**Algorithm**: Striped locking or AtomicLongArray
**Complexity**: LOW
**Expected Improvement**: Correct concurrent deletes
**Implementation**: Replace `BitSet tombstones` with striped `AtomicBooleanArray`

**1.3 Fix Column Write Atomicity**
**Algorithm**: SeqLock (Sequence Lock) per row
**Complexity**: MEDIUM
**Expected Improvement**: 2-3x read throughput under concurrent updates
**Implementation**: Add `AtomicLong[] rowSeqLocks` and follow seqlock protocol

### Priority 2: Performance Improvements

**2.1 Stripe-Based Index Updates**
**Algorithm**: Partition `MutableRowIdSet` by hash stripes
**Complexity**: LOW
**Expected Improvement**: 4-8x index update throughput
**Implementation**: Add striped locking to `MutableRowIdSet` implementations

**2.2 Optimistic Locking for Updates**
**Algorithm**: CAS-based versioning with retry loops
**Complexity**: LOW
**Expected Improvement**: 2x throughput for low-contention updates
**Implementation**: Add version CAS to update operations

**2.3 Lock-Free Free-List**
**Algorithm**: CAS-based lock-free stack
**Complexity**: VERY HIGH
**Expected Improvement**: 5-10x row allocation throughput
**Implementation**: Replace with `AtomicReferenceArray` or Michael-Scott queue

### Priority 3: Advanced Features

**3.1 MVCC for Snapshot Isolation**
**Algorithm**: Per-row version chains with timestamp-based snapshots
**Complexity**: HIGH
**Expected Improvement**: 3-5x read throughput, snapshot consistency
**Implementation**: Extend row storage with version tracking and GC

**3.2 Pessimistic Locking API**
**Algorithm**: Striped ReadWriteLock with timeout
**Complexity**: MEDIUM
**Expected Improvement**: Predictable high-contention behavior
**Implementation**: Add `@Lock` annotation support and lock manager

**3.3 Concurrent Hash Join**
**Algorithm**: Radix-partitioned parallel join
**Complexity**: VERY HIGH
**Expected Improvement**: Near-linear scaling with CPU cores
**Implementation**: New join executor for @OneToMany/@ManyToMany support

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

## Implementation Examples

### Example 1: Striped Locking for Free-List

```java
class StripedFreeList {
    private final ReadWriteLock[] locks;
    private final int[] freeList;
    private final AtomicInteger freeListTop = new AtomicInteger(-1);
    private final int stripes;

    public StripedFreeList(int capacity, int stripes) {
        this.stripes = stripes;
        this.locks = new ReadWriteLock[stripes];
        this.freeList = new int[capacity];
        for (int i = 0; i < stripes; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    private int stripeFor(int rowIndex) {
        return rowIndex % stripes;
    }

    public void push(int rowId) {
        int stripe = stripeFor(rowId);
        locks[stripe].writeLock().lock();
        try {
            int top = freeListTop.getAndIncrement();
            freeList[top + 1] = rowId;
        } finally {
            locks[stripe].writeLock().unlock();
        }
    }

    public int pop() {
        int stripe = stripeFor(freeListTop.get());
        locks[stripe].writeLock().lock();
        try {
            int top = freeListTop.get();
            if (top < 0) return -1;  // Empty
            freeListTop.decrementAndGet();
            return freeList[top + 1];
        } finally {
            locks[stripe].writeLock().unlock();
        }
    }
}
```

### Example 2: SeqLock for Row Updates

```java
class SeqLockRow {
    private final AtomicLong version = new AtomicLong(0);
    private final Object[] columns;

    public void write(Object[] newValues) {
        version.getAndIncrement();  // Make odd
        try {
            // Write all columns
            System.arraycopy(newValues, 0, columns, 0, columns.length);
        } finally {
            version.incrementAndGet();  // Make even
        }
    }

    public Object[] read() {
        long v1 = version.get();
        if ((v1 & 1) == 1) return read();  // Writing, retry

        Object[] copy = new Object[columns.length];
        System.arraycopy(columns, 0, copy, 0, columns.length);

        long v2 = version.get();
        if (v1 != v2) return read();  // Changed, retry
        return copy;
    }
}
```

### Example 3: Optimistic Update

```java
class OptimisticRow {
    private final AtomicLong version = new AtomicLong(0);
    private final Object[] columns;

    public boolean update(int columnIndex, Object newValue, long expectedVersion) {
        long oldVersion = version.get();
        if (oldVersion != expectedVersion) return false;

        columns[columnIndex] = newValue;
        return version.compareAndSet(oldVersion, oldVersion + 1);
    }

    public long readVersion() {
        return version.get();
    }
}
```

---

## Performance Impact

| Improvement | Complexity | Expected Throughput Gain | Priority |
|-------------|-----------|-------------------------|----------|
| **Fix free-list race** | HIGH | Correctness only | **CRITICAL** |
| **Striped tombstones** | LOW | Correctness only | **CRITICAL** |
| **SeqLock rows** | MEDIUM | 2-3x | **HIGH** |
| **Striped indexes** | LOW | 4-8x | **HIGH** |
| **Optimistic locking** | LOW | 2x | **MEDIUM** |
| **Pessimistic API** | MEDIUM | Predictable | **MEDIUM** |
| **MVCC** | HIGH | 3-5x | **LOW** |
| **Lock-free free-list** | VERY HIGH | 5-10x | **LOW** |
| **Concurrent join** | VERY HIGH | Near-linear scaling | **LOW** |

---

### Performance Characteristics (Unbenchmarked, Design-Level)

| Operation | Complexity | Hot Path Notes |
|-----------|------------|----------------|
| HashIndex lookup | O(1) | Lock-free `ConcurrentHashMap.get()` |
| RangeIndex lookup | O(log n) | `ConcurrentSkipListMap.get()` |
| Query scan | O(n) | Direct array scan, `published` watermark |
| Typed read | O(1) | Direct array access, no reflection |
| Save / delete | O(1) | Requires external sync for correctness |

---

## See Also

- [ROADMAP.md](ROADMAP.md) - Future feature roadmap including concurrency improvements
- [ARCHITECTURE.md](ARCHITECTURE.md) - Overall architecture
- [SPRING_DATA.md](SPRING_DATA.md) - Spring Data integration
