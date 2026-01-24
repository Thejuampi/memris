# Memris Decisions Log

## 2026-01-20
- Target runtime: Java 21
- SIMD uses the incubating Vector API (`jdk.incubator.vector`)
- Storage prototype: FFM `MemorySegment` columns
- Selection pipeline: vector masks to SelectionVector (bitset or int list)

## 2026-01-21
- **Design Principle**: O(1) operations preferred, O(log n) second, O(n) forbidden
- All iteration uses primitive enumerators (IntEnumerator, LongEnumerator) - no boxing
- SelectionVector interface with O(1) `contains()` and O(1) upgrade from sparse to dense
- FfmTable with FfmIntColumn and FfmLongColumn using SIMD vector scans
- Simple benchmark runner for performance validation
- Primitive-only APIs: no Iterator, no Iterable, no boxed types in hot paths

## 2026-01-21 (Later)
- Added HashJoin implementation for probe/build join algorithm
- Added SimpleExecutor for plan execution (Scan/Filter)
- Added Spring Data integration stub (MemrisTemplate)
- Added TDD tests for scanBetween and scanIn predicates
- Vector API: use `compare(VectorOperators.GE/LT)` for range comparisons
- All classes marked `final` for JVM optimization
- Added FfmStringColumn for String support
- Added `MemrisRepositoryFactory` for Spring Data integration

## Performance Results (10M rows, 228MB)

### Full Scan
- Time: 15-16ms
- Throughput: **14.3 GB/s**

### Point Filter (50% selectivity)
- Time: 46ms
- Result: 5M rows

### Range Query (10% selectivity)
- Time: 9-23ms (after JIT warmup)
- Result: ~1M rows

### SelectionVector Operations
- Create (10M rows): 16-23ms
- toIntArray(): 31-34ms
- Enumeration: 24-30ms

### Design Principles Applied
- All classes `final` for JVM inlining
- No virtual dispatch in hot paths
- Primitive-only APIs (IntEnumerator, LongEnumerator)
- O(1) contains() via BitSet for dense sets
- O(1) add() amortized for IntSelection
- SIMD vectorization for int/long columns

## 2026-01-24
- Query methods MUST be type-safe derived methods (e.g., `findByProcessor(String name)`)
- Generic string-based queries (`findBy(String field, Object value)`) are explicitly FORBIDDEN
- Removed obsolete `MemrisTemplate.findBy(String, Object)` stub method
- All queries use QueryMethodParser for compile-time type safety
