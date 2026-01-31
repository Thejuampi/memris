# Storage Layer Design

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Core Data Structures

### RowId (64-bit Composite Key)
- **Layout**: `[pageId (48 bits)][offset (16 bits)]`
- **Rationale**: Enables 2^48 pages × 65K rows per page = 2^64 total rows
- **Operations**:
  - `page()`: Extract page ID (shift right 16)
  - `offset()`: Extract offset (bitmask with 0xFFFF)
  - `value()`: Raw 64-bit value for bitmap operations

### ColumnarBatch (Unbounded Columnar Storage)
- **Layout**: Separate primitive arrays per data type
  - `int[]` for 32-bit integers
  - `long[]` for 64-bit integers  
  - `double[]` for 64-bit floats
  - `String[]` for variable-length strings
- **Growth Strategy**: 2x capacity when exceeded (amortized O(1))
- **Rationale**:
  - No boxing overhead (5-10x memory savings)
  - Cache-friendly sequential access
  - Direct array access for O(1) operations
- **Note**: Not thread-safe for concurrent writes (use synchronization in Phase 2)

### HashIndex (Concurrent Hash + RoaringBitmap)
- **Structure**: `ConcurrentHashMap<K, RoaringBitmap>`
- **Write Path**: `StampedLock.writeLock()` + copy-on-write bitmap
- **Read Path**: Lock-free via `ConcurrentHashMap.get()`
- **Rationale**:
  - RoaringBitmap: 10x smaller than ArrayList<Long>
  - Lock-free readers: readers never block writers
  - Bitmap operations (AND, OR, NOT) for joins

### MemrisStore (Main Engine)
- **Storage**: Single unbounded ColumnarBatch
- **Index Registry**: ConcurrentHashMap<String, Index<?>>
- **No max rows limit**: Grows dynamically
- **Thread Safety**: 
  - Reads: Thread-safe via atomic rowCounter
  - Writes: Single-threaded in v1.0 (Phase 2: concurrent writes)

## Performance Optimizations

### JVM Optimization Strategies
1. **Primitive arrays** instead of Object[] - no boxing
2. **Direct field access** - no getters in hot paths
3. **Unbounded growth** - no capacity tuning needed
4. **Lock-free reads** - StampedLock for readers

## Current Test Coverage

| Test | Description | Status |
|------|-------------|--------|
| RowIdTest | 64-bit key operations | PASS |
| ColumnarBatchTest | Primitive array storage | PASS |
| HashIndexTest | Concurrent hash index | PASS |
| MemrisStoreTest | Store operations + 10M rows | PASS |

## Trade-offs

| Decision | Chosen | Alternative | Reason |
|----------|--------|-------------|-------|
| Row storage | Columnar | Row-major | Cache locality, direct array access |
| Index type | RoaringBitmap | TreeSet | 10x memory, set ops |
| Write lock | StampedLock | synchronized | Readers don't block |
| Capacity | Dynamic | Fixed | Simplicity, no tuning |
| Concurrent writes | Single-threaded | Fine-grained locking | v1.0 simplicity |

## Current Limitations

- **Concurrent writes**: ColumnarBatch is single-threaded for inserts
- **MVCC**: No snapshot isolation
- **@OneToMany and @ManyToMany**: Only @OneToOne and @ManyToOne relationships implemented
- **DISTINCT query modifier**: Tokenized but execution not complete

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

### Design Principles
All core operations follow the O(1) first, O(log n) second, O(n) forbidden principle.
For details on SelectionVector interface and primitive enumerators,
see [Selection Pipeline Design](selection.md).
