# Spring Data JPA Support Roadmap

## Ultimate Goal

**Complete Spring Data JPA repository support** with emphasis on **joins and updates**.

### Scope
- ✅ **IN SCOPE**: All Spring Data JPA repository features
- ❌ **OUT OF SCOPE**: ACID transactions
  - We prefer **eventual consistency** over transactional guarantees
  - No `@Transactional` support
  - No rollback mechanisms
  - Optimistic locking (`@Version`) for concurrency control instead

### Design Philosophy
- **Blazing fast**: O(1) operations, no O(n) allowed
- **Eventually consistent**: Changes propagate asynchronously
- **Lock-free**: Atomic operations only, no blocking
- **Memory-first**: In-memory storage with FFM, off-heap

## Current Status

### ✅ Implemented
- Basic CRUD: `save()`, `update()`, `findAll()`, `count()`
- Derived query methods (findByXxx with AND/OR logic)
- ID generation (IDENTITY, UUID, AUTO, CUSTOM)
- TypeConverter system (all primitives + common types)
- Indexes (HashIndex, RangeIndex)
- Basic lifecycle callbacks: `@PrePersist`, `@PreUpdate`, `@PostLoad`

### ⚠️ Partial/Broken
- **Joins**: `@OneToMany`, `@ManyToMany` - Only work with numeric IDs (int/long)
- **Updates**: `updateExistingRow()` - Uses reflection, slow, needs O(1) optimization
- **Query ordering**: OrderBy parsed but not executed

### ❌ Missing (Critical for Joins/Updates)

#### High Priority (Blockers)
1. **Single Entity Operations**
   - `findById(ID id)` - Returns `Optional<T>`
   - `existsById(ID id)` - Returns `boolean`
   - `deleteById(ID id)` - Single delete
   - `delete(T entity)` - Delete by entity reference

2. **Batch Operations**
   - `saveAll(Iterable<T>)` - Batch insert (O(1) required)
   - `deleteAll(Iterable<T>)` - Batch delete
   - `deleteAllById(Iterable<ID>)` - Batch delete by IDs
   - `findAllById(Iterable<ID>)` - Batch lookup

3. **Join Table Fix**
   - Support UUID/String IDs in join tables (currently only int/long work)
   - Store UUID as 2 long columns (128 bits) - NOT as String
   - O(1) join table lookups for relationship management

4. **Update Optimization**
   - Replace reflection-based cell updates with O(1) direct access
   - Use Java 21 switches for type dispatch
   - Batch update support

#### Medium Priority (Important)
5. **Delete Operations**
   - Cascade delete (`@OneToMany` cascade, `orphanRemoval`)
   - Delete query methods (`deleteByXxx`)

6. **Paging & Sorting**
   - `Pageable` support
   - `Page<T>` return type
   - Execute OrderBy clauses (parsed but not executed)
   - `Sort` parameter

7. **Flush & Clear**
   - `flush()` - Force write
   - `clear()` - Clear persistence context
   - Memory management for large datasets

#### Low Priority (Nice to Have)
8. **Query Annotations**
   - `@Query` for custom queries
   - `@Param` named parameters
   - `@Modifying` for update/delete queries

9. **Optimistic Locking** (Concurrency without Transactions)
   - `@Version` annotation for version field
   - Version checking on updates
   - Automatic version increment

10. **Projections**
    - Interface-based projections
    - DTO projections
    - Partial field loading

#### Out of Scope ❌
11. **Transactions** (NOT SUPPORTED)
    - `@Transactional` - Use eventual consistency instead
    - Transaction boundaries
    - Rollback mechanisms
    - ACID guarantees
    - Flush modes tied to transactions

---

## Implementation Plan (TDD - Red → Green → Refactor)

### Phase 1: Foundation for Joins & Updates (Week 1)

#### Feature 1.1: Single Entity Operations
**Test file**: `EntityLookupTest.java`

Tests to write (RED):
```java
void findById_existingEntity_returnsOptional()
void findById_notFound_returnsEmpty()
void existsById_existing_returnsTrue()
void existsById_notFound_returnsFalse()
void deleteById_existing_removesEntity()
void deleteById_notFound_doesNothing()
```

Implementation (GREEN):
- Add `findById(ID)` to `MemrisRepository` interface
- Build hash index on ID column for O(1) lookup
- Return `Optional<T>` from materialized entity

Refactor:
- Use Java 21 switches for ID type dispatch
- Ensure O(1) lookup (no linear scan)
- Benchmark: 10M lookups < 100ms

---

#### Feature 1.2: Batch Operations
**Test file**: `BatchOperationsTest.java`

Tests to write (RED):
```java
void saveAll_multipleEntities_savesAll()
void saveAll_emptyList_returnsEmpty()
void deleteAllById_multipleIds_deletesAll()
void findAllById_multipleIds_returnsMatching()
void deleteAll_multipleEntities_deletesAll()
```

Implementation (GREEN):
- Add batch methods to `MemrisRepository`
- Use bulk insert for `saveAll()` - single `FfmTable.insert()` call
- Use index-based batch delete for O(1) per entity

Refactor:
- Pre-allocate arrays for batch operations (no ArrayList growth)
- Use SIMD for batch comparisons
- Benchmark: 1M batch save < 50ms

---

#### Feature 1.3: UUID Join Tables (Fix)
**Test file**: `UuidJoinTableTest.java`

Tests to write (RED):
```java
void manyToMany_withUuidIds_correctlyStoresRelationships()
void oneToMany_withUuidIds_correctlyStoresRelationships()
void joinTable_uuidStoredAsTwoLongs_notAsString()
```

Implementation (GREEN):
- Rewrite `buildJoinTables()` to store UUID as 2 long columns
- Use `UUID.getMostSignificantBits()` and `getLeastSignificantBits()`
- Update `insertIntoJoinTable()` to handle 2-column UUID storage

Refactor:
- Eliminate String storage for UUID (use 2 longs)
- Add utility methods for UUID ↔ 2 longs conversion
- Benchmark: 1M join inserts < 100ms

---

#### Feature 1.4: Update Optimization
**Test file**: `OptimizedUpdateTest.java`

Tests to write (RED):
```java
void update_existingEntity_updatesFields()
void update_multipleFields_updatesAll()
void update_performance_noReflection()
```

Implementation (GREEN):
- Replace reflection with direct column access
- Use Java 21 pattern matching on column types
- Cache column references per entity class

Refactor:
- O(1) per field update (no reflection overhead)
- Use `MethodHandle` for field access (faster than reflection)
- Benchmark: 10M updates < 200ms

---

### Phase 2: Advanced Features (Week 2)

#### Feature 2.1: Execute OrderBy
- Parse OrderBy (already done)
- Sort SelectionVector using SIMD
- Return sorted results

#### Feature 2.2: Paging & Sorting
- Implement `Pageable` parameter
- Create `Page<T>` wrapper
- Add offset/limit to queries

#### Feature 2.3: Cascade Delete
- Implement cascade types
- Handle orphanRemoval
- Delete dependent entities

#### Feature 2.4: Flush & Clear
- Implement batch flush
- Clear entity cache
- Memory management

---

## Design Principles (From README)

1. **O(1) first, O(log n) second, O(n) forbidden**
2. **Primitive-only APIs** - No boxing, no Iterator, no Iterable
3. **SIMD vectorization** - Panama Vector API
4. **FFM MemorySegment** - Off-heap storage
5. **All classes `final`** - JVM inlining
6. **Java 21 type switches** - Pattern matching
7. **Type conversion extensibility** - TypeConverterRegistry

---

## Performance Benchmarks

### Target Performance (10M rows)

| Operation | Target | Notes |
|-----------|--------|-------|
| findById | < 50ms | Hash index lookup |
| saveAll (1K) | < 10ms | Bulk insert |
| deleteAll (1K) | < 20ms | Index + delete |
| update single field | < 100ns | Direct access |
| Join insert | < 100ns | 2 long columns |
| Query with OrderBy | < 200ms | SIMD sort |

---

## Current Limitations (From README)

### Join Tables with Non-Numeric IDs

**CRITICAL**: Join tables (`@OneToMany`, `@ManyToMany`) currently only support numeric ID types.

Entities with UUID, String, or other non-numeric IDs **cannot use join table relationships**.

**Fix in progress**: Store UUID as 2 long columns (128 bits total) for performance.

---

## Dependencies

None - pure Java 21 with Panama FFM and Vector API.

---

## References

- [Spring Data JPA Documentation](https://docs.spring.io/spring-data/jpa/docs/current/reference/html/)
- [JPA Specification](https://jakarta.ee/specifications/persistence/)
- [Panama Foreign Function & Memory API](https://openjdk.org/projects/panama/)
