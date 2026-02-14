# Storage Layer Design

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Core Data Structures

### RowId (64-bit Composite Key)
- **Layout**: `[pageId (48 bits)][offset (16 bits)]`
- **Rationale**: Enables 2^48 pages x 65K rows per page = 2^64 total rows
- **Operations**:
  - `page()`: Extract page ID (shift right 16)
  - `offset()`: Extract offset (bitmask with 0xFFFF)
  - `value()`: Raw 64-bit value for bitmap operations

### PageColumn Types

Memris uses paged columnar storage with three specialized column types:

#### PageColumnInt
Primitive `int` column storage with SIMD-capable scan operations.

**Structure:**
```
AtomicReferenceArray<ColumnPage> pages
  - ColumnPage: int[] values + byte[] present
volatile int published  // watermark for safe reads
```

**Scan Operations:**

| Method | Description | Complexity |
|--------|-------------|------------|
| `scanEquals(target, limit)` | Values equal to target | O(n) |
| `scanGreaterThan(target, limit)` | Values > target | O(n) |
| `scanLessThan(target, limit)` | Values < target | O(n) |
| `scanGreaterThanOrEqual(target, limit)` | Values >= target | O(n) |
| `scanLessThanOrEqual(target, limit)` | Values <= target | O(n) |
| `scanBetween(lower, upper, limit)` | Values in [lower, upper] | O(n) |
| `scanIn(targets[], limit)` | Values in target set | O(n) with O(1) lookup |

**Thread-safety:**
- Writers: single-writer per offset (or external synchronization)
- Readers: many concurrent readers via volatile published semantics
- Publish: monotonic increment (never decreases)

#### PageColumnLong
Primitive `long` column storage - identical API to PageColumnInt but for 64-bit values.

**Key differences from Int:**
- Uses `long[]` instead of `int[]` for values
- Default value is `0L` for unpublished slots
- Same scan operations with `long` parameters

#### PageColumnString
String column storage with case-insensitive scan support.

**Operations:**

| Method | Description | Complexity |
|--------|-------------|------------|
| `scanEquals(target, limit)` | Values equal to target | O(n) |
| `scanEqualsIgnoreCase(target, limit)` | Case-insensitive equality | O(n) |
| `scanIn(targets[], limit)` | Values in target set | O(n) with O(1) lookup |
| `scanNull(limit)` | Null/unpublished slots | O(n) |

**Null Handling:**
- `scanEquals(null, limit)` delegates to `scanNull(limit)`
- Present bitmap tracks null vs. unpublished distinction

### Paged Array Benefits

| Feature | Benefit |
|---------|---------|
| **Cache locality** | Sequential access within pages |
| **SIMD-friendly** | Primitive arrays enable JIT vectorization |
| **Lazy allocation** | Pages created on demand via CAS |
| **No boxing** | Direct primitive array access (5-10x memory savings) |
| **Growth** | 2x capacity when exceeded (amortized O(1)) |

## Index Integration

### HashIndex
- **Structure**: `ConcurrentHashMap<K, MutableRowIdSet>`
- **Write Path**: `ConcurrentHashMap.compute()` for atomic updates
- **Read Path**: Lock-free via `ConcurrentHashMap.get()`
- **Complexity**: O(1) average lookup

### RangeIndex
- **Structure**: `ConcurrentSkipListMap<K, MutableRowIdSet>`
- **Operations**: between, greaterThan, lessThan, greaterThanOrEqual, lessThanOrEqual
- **Complexity**: O(log n) lookup, O(m) for range scans

### GeneratedTable (ByteBuddy)
Tables are generated at runtime via ByteBuddy with:
- Pre-compiled MethodHandles for column access
- TypeCode-based type dispatch (no reflection)
- Dense arrays: column index -> PageColumn* access

## Performance Optimizations

### JVM Optimization Strategies
1. **Primitive arrays** instead of Object[] - no boxing
2. **Direct field access** - no getters in hot paths
3. **Paged growth** - no capacity tuning needed
4. **Lock-free reads** - volatile published watermark

## Current Test Coverage

| Test | Description | Status |
|------|-------------|--------|
| RowIdTest | 64-bit key operations | PASS |
| PageColumnIntTest | Int column scan operations | PASS |
| PageColumnLongTest | Long column scan operations | PASS |
| PageColumnStringTest | String column scan operations | PASS |
| HashIndexTest | Concurrent hash index | PASS |

## Trade-offs

| Decision | Chosen | Alternative | Reason |
|----------|--------|-------------|--------|
| Row storage | Columnar | Row-major | Cache locality, direct array access |
| Column paging | AtomicReferenceArray | Single array | Lazy allocation, growth |
| Index type | MutableRowIdSet | TreeSet | Efficient set ops |
| Write lock | ConcurrentHashMap.compute() | synchronized | Readers don't block |
| Capacity | Dynamic | Fixed | Simplicity, no tuning |

## Current Limitations

- **Column write atomicity**: Coordinated by row seqlock; index visibility is eventual
- **MVCC**: No snapshot isolation

## Decisions Log

### 2026-01-20
- Target runtime: Java 21
- Storage uses Java heap (int[], long[], String[] columns)
- Selection pipeline: primitive array access -> SelectionVector (bitset or int list)

### 2026-01-21
- **Design Principle**: O(1) operations preferred, O(log n) second, O(n) forbidden
- All iteration uses primitive enumerators (IntEnumerator, LongEnumerator) - no boxing
- SelectionVector interface with O(1) `contains()` and O(1) upgrade from sparse to dense
- HeapRuntimeKernel with direct array access for scans
- Primitive-only APIs: no Iterator, no Iterable, no boxed types in hot paths

### 2026-02-14
- **PageColumn types**: Documented PageColumnInt, PageColumnLong, PageColumnString with scan operations
- **Paged storage**: AtomicReferenceArray with CAS-based page allocation
- **SIMD potential**: Primitive arrays enable JIT vectorization
- **Scan operations**: All scan methods return int[] of matching offsets with O(n) complexity

### Design Principles
All core operations follow the O(1) first, O(log n) second, O(n) forbidden principle.
For details on SelectionVector interface and primitive enumerators,
see [Selection Pipeline Design](selection.md).
