# FFM Storage Test Audit - On-Heap Migration Guide

**Generated:** 2026-01-26
**Purpose:** Identify all tests related to FFM storage for consolidation and migration to on-heap storage
**Repository:** memris

---

## Executive Summary

This document catalogs **all 28 test files** in the Memris codebase that relate to storage operations. The analysis identifies FFM-dependent tests that need migration and heap storage tests that are already in place.

### Test Inventory

| Category | Test Files | Direct FFM | Indirect FFM | Heap Only |
|----------|------------|------------|--------------|-----------|
| **Direct FFM Storage** | 2 | 2 | 0 | 0 |
| **Integration Tests** | 9 | 2 | 7 | 0 |
| **Runtime/Execution** | 2 | 1 | 1 | 0 |
| **Spring Data Layer** | 3 | 0 | 0 | 3 |
| **Heap Storage Tests** | 6 | 0 | 0 | 6 |
| **Benchmarks** | 4 | 3 | 0 | 1 |
| **TOTAL** | **26** | **8** | **8** | **10** |

### Key Findings

1. **FFM Import Analysis:** Only 4 files directly import `io.memris.storage.ffm`
2. **New Heap Infrastructure:** 6 heap storage tests added (PersonTableTest, TableGeneratorTest, PageColumnLongTest, PageColumnIntTest, PageColumnStringTest, LongIdIndexTest)
3. **High Duplication:** ECommerceRealWorldTest covers ~80% of other integration test scenarios
4. **Missing Coverage:** No dedicated tests for FfmLongColumn, FfmStringColumn, basic column operations
5. **Missing Tests:** ColumnConstraintsTest, BatchOperationsTest, NestedEntityTest, MemrisRepositoryIntegrationTest were documented but don't exist

---

## Table of Contents

1. [Direct FFM Storage Tests](#1-direct-ffm-storage-tests)
2. [Integration Tests with Direct FFM Imports](#2-integration-tests-with-direct-ffm-imports)
3. [Integration Tests (Indirect FFM)](#3-integration-tests-indirect-ffm)
4. [Runtime and Execution Tests](#4-runtime-and-execution-tests)
5. [Heap Storage Tests (New)](#5-heap-storage-tests-new)
6. [Spring Data Layer Tests](#6-spring-data-layer-tests)
7. [Benchmarks](#7-benchmarks)
8. [Consolidation Recommendations](#8-consolidation-recommendations)
9. [Migration Checklist](#9-migration-checklist)

---

## 1. Direct FFM Storage Tests

Tests that directly test FFM storage classes.

### 1.1 FfmTableScanInTest
**Location:** `memris-core/src/test/java/io/memris/storage/ffm/FfmTableScanInTest.java`

**FFM Imports:**
```java
import io.memris.storage.ffm.FfmTable;
import io.memris.storage.ffm.Predicate;
```

**Description:** Tests `FfmTable.scan()` with `Predicate.In` predicates

**Test Methods (3):**
| Method | Purpose | Status |
|--------|---------|--------|
| `scanIn_should_return_rows_with_values_in_collection()` | IN clause with collection [0, 5, 9] | Active |
| `scanIn_with_empty_collection_should_return_empty()` | Empty collection edge case | Active |
| `scanIn_with_single_value_should_match_equal_rows()` | Single value = equality | Active |

**Migration Action:** ✓ Port to HeapTableScanInTest

---

### 1.2 FfmIntColumnScanBetweenTest
**Location:** `memris-core/src/test/java/io/memris/storage/ffm/FfmIntColumnScanBetweenTest.java`

**FFM Imports:**
```java
import io.memris.storage.ffm.FfmIntColumn;
import io.memris.storage.ffm.SelectionVector;
```

**Description:** Tests SIMD-based BETWEEN operations on `FfmIntColumn`

**Test Methods (4):**
| Method | Purpose | Status |
|--------|---------|--------|
| `scanBetween_should_return_rows_in_range()` | Range scan [25, 75] = 51 rows | Active |
| `scanBetween_with_no_matches_should_return_empty()` | No matches edge case | Active |
| `scanBetween_with_all_matching_should_return_all_rows()` | 100% selectivity | Active |
| `scanBetween_with_partial_row_count_should_only_scan_up_to_rowCount()` | rowCount limit | Active |

**Migration Action:** ✓ Port to HeapIntColumnScanBetweenTest

---

### Coverage Gaps

**Missing FFM Unit Tests:**
- ❌ No tests for `FfmLongColumn`
- ❌ No tests for `FfmStringColumn` / `FfmStringColumnImpl`
- ❌ No tests for basic column operations (get/set at index)
- ❌ No tests for `FfmColumn` interface methods
- ❌ No tests for Arena lifecycle (allocation/deallocation)
- ❌ No tests for column creation and initialization

---

## 2. Integration Tests with Direct FFM Imports

Tests that directly import and use FFM classes.

### 2.1 TableManagerTest
**Location:** `memris-core/src/test/java/io/memris/spring/TableManagerTest.java`

**FFM Imports:**
```java
import io.memris.storage.ffm.FfmTable;
import java.lang.foreign.Arena;
```

**Description:** TableManager's table creation, caching, enum caching, join table creation

**Test Methods (8):**
| Method | Purpose | FFM Dependency |
|--------|---------|----------------|
| `shouldCreateTableForEntity()` | Table creation | ✅ FfmTable + Arena |
| `shouldCacheCreatedTables()` | Table caching | ✅ FfmTable |
| `shouldCreateMultipleEntityTables()` | Multiple tables | ✅ FfmTable |
| `shouldCacheEnumValues()` | Enum caching | ✅ FfmTable |
| `shouldGetOrCreateTableForEntity()` | Get-or-create pattern | ✅ FfmTable |
| `shouldGetTable()` | Table retrieval | ✅ FfmTable |
| `shouldReturnArena()` | Arena access | ✅ Arena |
| `shouldCreateJoinTables()` | Join table creation | ✅ FfmTable |

**Migration Action:** 🟡 **PORT TO HEAP** - Replace FfmTable with HeapTable

---

### 2.2 RepositoryRuntimeIntegrationTest ⚠️ DISABLED
**Location:** `memris-core/src/test/java/io/memris/spring/runtime/RepositoryRuntimeIntegrationTest.java`

**FFM Imports:**
```java
import io.memris.storage.ffm.FfmTable;
import java.lang.foreign.Arena;
```

**Status:** `@Disabled("TODO: Implement proper FfmTable initialization with ColumnSpec and Arena")`

**Test Methods (3 - NOT IMPLEMENTED):**
| Method | Purpose | Status |
|--------|---------|--------|
| `findByIdShouldReturnCorrectPerson()` | findById execution | 🔴 TODO |
| `findAllShouldReturnAllPersons()` | findAll execution | 🔴 TODO |
| `findByAgeGreaterThanShouldFilterCorrectly()` | GT predicate execution | 🔴 TODO |

**Migration Action:** 🟡 **RE-ENABLE FOR HEAP** - Implement with HeapTable

---

## 3. Integration Tests (Indirect FFM)

Tests that use FFM storage through MemrisRepositoryFactory (no direct FFM imports).

### 3.1 ECommerceRealWorldTest ⭐ MOST COMPREHENSIVE
**Location:** `memris-core/src/test/java/io/memris/spring/ECommerceRealWorldTest.java`

**Description:** 646-line comprehensive e-commerce domain with ALL relationship types, indexes, enums, embedded types, lifecycle callbacks

**Test Methods (2):**
| Method | Coverage | Duplicates |
|--------|----------|------------|
| `testEcommerceComplexScenario()` | 20 sub-tests | **YES - 80% of other tests** |
| `testIndexPerformance()` | 100 products indexed | Unique |

**Relationship Types Tested:**
- ✅ One-to-One (Customer ↔ Account)
- ✅ One-to-Many (Customer → Orders, Category → Products)
- ✅ Many-to-One (Order → Customer, OrderItem → Product)
- ✅ Many-to-Many (Product ↔ Category, Order ↔ Coupon)
- ✅ Self-referential (Category parent/children)

**Features Tested:**
- ✅ @Index fields (email, sku, barcode, phone, slug, postalCode, code)
- ✅ @Enumerated (STRING and ORDINAL)
- ✅ @Embeddable inline components (Dimensions)
- ✅ @PrePersist, @PostLoad callbacks
- ✅ @Transient computed fields
- ✅ BETWEEN, IN, LIKE, multi-condition queries

**Consolidation Action:** 🔴 **KEEP AS PRIMARY E2E TEST** - Remove duplicated tests

---

### 3.2 OneToManyTest
**Location:** `memris-core/src/test/java/io/memris/spring/OneToManyTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `onetomany_cascade_save_children()` | Cascade save | ✅ ECommerceRealWorldTest |
| `onetomany_find_children_by_parent()` | Find by parent | ✅ ECommerceRealWorldTest |
| `onetomany_empty_list_handled()` | Empty collections | ✅ ECommerceRealWorldTest |
| `onetomany_multiple_departments()` | Multiple parents | ✅ ECommerceRealWorldTest |

**Consolidation Action:** 🔴 **REMOVE** - Fully covered by ECommerceRealWorldTest

---

### 3.3 ManyToManyTest
**Location:** `memris-core/src/test/java/io/memris/spring/ManyToManyTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `manytomany_set_add_remove()` | Student ↔ Course | ✅ ECommerceRealWorldTest |
| `manytomany_join_table_populated()` | Member ↔ Group | ✅ ECommerceRealWorldTest |
| `manytomany_bidirectional_sync()` | Author ↔ Book | ✅ ECommerceRealWorldTest |
| `manytomany_empty_collections()` | Empty collections | ✅ ECommerceRealWorldTest |

**Consolidation Action:** 🔴 **REMOVE** - Fully covered by ECommerceRealWorldTest

---

### 3.4 DynamicRepositoryTest
**Location:** `memris-core/src/test/java/io/memris/spring/DynamicRepositoryTest.java`

**Test Methods (6):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `findByProcessor()` | Equality | ✅ ECommerceRealWorldTest |
| `findByRamGreaterThan()` | GREATER_THAN | ✅ ECommerceRealWorldTest |
| `findByRamLessThanEqual()` | LESS_THAN_EQUAL | ✅ ECommerceRealWorldTest |
| `findByProcessorNotEqual()` | NOT_EQUAL | ✅ ECommerceRealWorldTest |
| `findByBrand()` | Simple property | ✅ ECommerceRealWorldTest |
| `findAll()` | Find all | ✅ ECommerceRealWorldTest |

**Consolidation Action:** 🔴 **REMOVE** - All operators covered by ECommerceRealWorldTest

---

### 3.5 IdGenerationTest
**Location:** `memris-core/src/test/java/io/memris/spring/IdGenerationTest.java`

**Test Methods (7):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `auto_numeric_int_should_generate_incremental_ids()` | INT IDENTITY | ✅ UNIQUE |
| `auto_numeric_long_should_generate_incremental_ids()` | LONG IDENTITY | ✅ UNIQUE |
| `auto_uuid_should_generate_random_uuids()` | UUID generation | ✅ UNIQUE |
| `custom_generator_should_use_provided_implementation()` | Custom generator | ✅ UNIQUE |
| `explicit_id_should_not_be_overwritten()` | Manual IDs | ✅ UNIQUE |
| `save_with_existing_id_should_update()` | UPSERT behavior | Partial: RED tests |
| `save_with_new_id_should_insert()` | Insert behavior | Partial: RED tests |

**Consolidation Action:** 🟢 **KEEP** - Unique ID generation coverage

---

### 3.6 LifecycleCallbacksTest
**Location:** `memris-core/src/test/java/io/memris/spring/LifecycleCallbacksTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `prepersist_callback_invoked()` | Before insert | Partial: ECommerceRealWorldTest |
| `postload_callback_invoked()` | After materialization | Partial: ECommerceRealWorldTest |
| `preupdate_callback_invoked()` | Before update | ✅ UNIQUE |
| `multiple_callbacks_invoked()` | Multiple callbacks | Partial: ECommerceRealWorldTest |

**Consolidation Action:** 🟡 **MERGE** - Keep @PreUpdate test, merge others

---

### 3.7 EnumeratedTest
**Location:** `memris-core/src/test/java/io/memris/spring/EnumeratedTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `enumerated_string_value()` | STRING enum | ✅ ECommerceRealWorldTest |
| `enumerated_ordinal_value()` | ORDINAL enum | ✅ ECommerceRealWorldTest |
| `enumerated_null_enum()` | Null enum | ✅ UNIQUE |
| `enumerated_find_by_ordinal()` | Query by ordinal | ✅ ECommerceRealWorldTest |

**Consolidation Action:** 🟡 **MERGE** - Keep null enum test, merge others

---

### 3.8 TransientTest
**Location:** `memris-core/src/test/java/io/memris/spring/TransientTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `transient_field_not_in_schema()` | Not persisted | Partial: ECommerceRealWorldTest |
| `transient_field_not_materialized()` | Default behavior | ✅ UNIQUE |
| `transient_computed_on_postload()` | @PostLoad computation | Partial: ECommerceRealWorldTest |
| `all_non_transient_fields_saved()` | Non-transient persisted | ✅ UNIQUE |

**Consolidation Action:** 🟡 **MERGE** - Merge unique tests into ECommerceRealWorldTest

---

### 3.9 EntityLookupTest
**Location:** `memris-core/src/test/java/io/memris/spring/EntityLookupTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `findById_existingEntity_returnsOptional()` | findById | ✅ ECommerceRealWorldTest |
| `existsById_existingEntity_returnsTrue()` | existsById | ✅ UNIQUE |
| `deleteById_existingEntity_removesEntity()` | deleteById | Partial: RED tests |
| `deleteById_deletesFromHashIndex()` | Hash index cleanup | ✅ UNIQUE |

**Consolidation Action:** 🟡 **MERGE** - Keep unique tests, merge others

---

### 3.10 UuidStorageOptimizationTest
**Location:** `memris-core/src/test/java/io/memris/spring/UuidStorageOptimizationTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `uuid_id_stored_and_retrieved_correctly()` | UUID as ID | ✅ UNIQUE |
| `uuid_field_stored_and_retrieved_correctly()` | UUID as field | ✅ UNIQUE |
| `multiple_entities_with_uuid_ids()` | Multiple UUIDs | ✅ UNIQUE |
| `findAllById_with_uuid_ids()` | Batch UUID lookup | ✅ UNIQUE |

**Consolidation Action:** 🟢 **KEEP** - Unique UUID → 2×long optimization

---

### 3.11 UuidJoinTableTest
**Location:** `memris-core/src/test/java/io/memris/spring/UuidJoinTableTest.java`

**Test Methods (2):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `uuid_many_to_many_join_table_works()` | UUID @ManyToMany | ✅ UNIQUE |
| `uuid_join_table_columns_are_2_longs()` | Column verification | ✅ UNIQUE |

**Consolidation Action:** 🟢 **KEEP** - Unique UUID join table coverage

---

### 3.12 QueryMethodCompilationTest
**Location:** `memris-core/src/test/java/io/memris/spring/QueryMethodCompilationTest.java`

**Test Methods (4):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `query_method_compiles_once_on_first_call()` | Compilation caching | ✅ UNIQUE |
| `compiled_query_handles_different_parameters()` | Parameter variance | ✅ UNIQUE |
| `compiled_query_with_multiple_conditions()` | Complex queries | ✅ UNIQUE |
| `compiled_count_query_is_fast()` | Count compilation | ✅ UNIQUE |

**Consolidation Action:** 🟢 **KEEP** - Unique compilation coverage

---

### 3.13 MemrisRepositoryFactoryTest
**Location:** `memris-core/src/test/java/io/memris/spring/MemrisRepositoryFactoryTest.java`

**FFM Imports:**
```java
import io.memris.storage.ffm.FfmTable;
import io.memris.storage.ffm.FfmTable.ColumnSpec;
import java.lang.foreign.Arena;
```

**Description:** Basic factory operations (note: FfmTable imported but factory creates tables internally)

**Test Methods (3):**
| Method | Purpose | Duplicates |
|--------|---------|------------|
| `save_and_findBy_should_work()` | Basic CRUD | ✅ Covered by ECommerceRealWorldTest |
| `findByAgeIn_should_return_matching_rows()` | IN clause | ✅ ECommerceRealWorldTest |
| `findByAgeBetween_should_return_rows_in_range()` | BETWEEN | ✅ ECommerceRealWorldTest |

**Migration Action:** 🔴 **REMOVE** - Fully covered by ECommerceRealWorldTest

---

## 4. Runtime and Execution Tests

Tests for RepositoryRuntime and query execution.

### 4.1 RepositoryRuntimeTest
**Location:** `memris-core/src/test/java/io/memris/spring/runtime/RepositoryRuntimeTest.java`

**Description:** Unit tests for RepositoryRuntime structure (NOT actual execution)

**Test Methods (12):**
| Method | Purpose | FFM Dependency |
|--------|---------|----------------|
| `runtimeShouldStoreCompiledQueries()` | Compiled query storage | ❌ None (null table) |
| `runtimeShouldHaveTypedEntrypoints()` | Typed entry methods | ❌ None |
| `materializationShouldUseDenseArrays()` | Dense array structure | ❌ None |
| `queryIdDispatchShouldSelectCorrectQuery()` | queryId routing | ❌ None |
| `saveOne_shouldInsertNewEntity_whenIdNotExists()` | UPSERT insert | ❌ None |
| `saveOne_shouldUpdateExistingEntity_whenIdExists()` | UPSERT update | ❌ None |
| `saveOne_multipleSaves_sameId_updates()` | UPSERT idempotency | ❌ None |
| `deleteOne_existingEntity_marksRowAsDeleted()` | Soft delete | ❌ None |
| `deleteOne_nonExistingEntity_doesNothing()` | Delete missing | ❌ None |
| `deleteById_existingId_marksRowAsDeleted()` | ID-based delete | ❌ None |
| `deleteById_nonExistingId_doesNothing()` | ID delete missing | ❌ None |
| `delete_filtersRowFromQueryResults()` | Delete filtering | ❌ None |

**Migration Action:** 🟢 **KEEP** - Isolated from FFM, tests core runtime structure

---

## 5. Heap Storage Tests (New)

New heap storage tests added (no FFM dependency).

### 5.1 PersonTableTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/PersonTableTest.java`

**Description:** TDD tests for PersonTable - example of generated table with typed columns and ID index

**Test Methods (7+):**
| Method | Purpose |
|--------|---------|
| `createPersonTableWithName()` | Table creation |
| `insertReturnsRowId()` | Insert returns RowId(page, offset) |
| `insertIncrementsRowCount()` | Row count tracking |
| `findByIdReturnsPerson()` | ID-based lookup |
| `findByIdReturnsNullForMissing()` | Missing ID returns null |
| `findByNameScansColumn()` | Column scan operation |
| `scanWithNoMatchesReturnsEmpty()` | Empty scan results |

**Status:** ✅ Active - Heap storage implementation

---

### 5.2 TableGeneratorTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/TableGeneratorTest.java`

**Description:** TDD tests for TableGenerator - ByteBuddy table generation from entity metadata

**Test Methods (5+):**
| Method | Purpose |
|--------|---------|
| `generatePersonTableCreatesValidClass()` | Class generation |
| `generatedPersonTableHasCorrectColumns()` | Column verification |
| `generatedPersonTableSupportsInsert()` | Insert via reflection |
| `generatedPersonTableSupportsFindById()` | Find by ID via reflection |
| `generatedTableHasCorrectMetadata()` | Metadata preservation |

**Status:** ✅ Active - ByteBuddy generation tests

---

### 5.3 LongIdIndexTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/LongIdIndexTest.java`

**Description:** TDD tests for LongIdIndex - Long key to RowId index for O(1) lookups

**Test Methods (11+):**
| Method | Purpose |
|--------|---------|
| `newIndexHasZeroSize()` | Initial state |
| `putAndGet()` | Basic put/get |
| `getReturnsNullForMissingKey()` | Missing key |
| `putUpdatesExistingKey()` | Update semantics |
| `sizeTracksNumberOfKeys()` | Size tracking |
| `removeDeletesKey()` | Remove operation |
| `removeMissingKeyDoesNothing()` | Remove missing |
| `handlesZeroKey()` | Zero value edge case |
| `handlesNegativeKey()` | Negative values |
| `handlesCollisionWithDifferentKeys()` | Hash collision |
| `handlesManyInsertsBeyondInitialCapacity()` | Growth behavior |

**Status:** ✅ Active - Heap index tests

---

### 5.4 PageColumnLongTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/PageColumnLongTest.java`

**Description:** TDD tests for PageColumnLong - primitive long column with scan operations

**Test Methods (6+):**
| Method | Purpose |
|--------|---------|
| `newColumnHasZeroPublished()` | Initial state |
| `setAndGet()` | Basic get/set |
| `getReturnsDefaultForUnpublished()` | Unpublished behavior |
| `setMultipleValues()` | Multiple values |
| `publishedCountMonotonic()` | Monotonic publish |
| `scanEqualsReturnsMatchingOffsets()` | Equality scan |

**Status:** ✅ Active - Heap long column tests

---

### 5.5 PageColumnIntTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/PageColumnIntTest.java`

**Description:** TDD tests for PageColumnInt - primitive int column with scan operations

**Status:** ✅ Active - Heap int column tests

---

### 5.6 PageColumnStringTest ⭐ NEW
**Location:** `memris-core/src/test/java/io/memris/storage/heap/PageColumnStringTest.java`

**Description:** TDD tests for PageColumnString - String column with scan operations

**Status:** ✅ Active - Heap String column tests

---

## 6. Spring Data Layer Tests

Tests for planning, compilation (no FFM dependency).

### 6.1 QueryPlannerIntegrationTest
**Location:** `memris-core/src/test/java/io/memris/spring/plan/entities/QueryPlannerIntegrationTest.java`

**Description:** Query method parsing, operator mapping, condition building (60+ test methods)

**FFM Dependency:** ❌ NONE - Pure unit tests, no storage interaction

**Migration Action:** 🟢 **KEEP** - No changes needed

---

### 6.2 BuiltInResolverTest
**Location:** `memris-core/src/test/java/io/memris/spring/plan/BuiltInResolverTest.java`

**Description:** Built-in operation resolution with tie-breaking

**FFM Dependency:** ❌ NONE - Pure unit tests

**Migration Action:** 🟢 **KEEP** - No changes needed

---

### 6.3 ContextAwareLexerTest
**Location:** `memris-core/src/test/java/io/memris/spring/plan/entities/ContextAwareLexerTest.java`

**Description:** Query method tokenization with entity context

**FFM Dependency:** ❌ NONE - Pure unit tests

**Migration Action:** 🟢 **KEEP** - No changes needed

---

### 6.4 ComplexNestingTest
**Location:** `memris-core/src/test/java/io/memris/spring/plan/entities/ComplexNestingTest.java`

**Description:** Complex nesting scenarios in QueryMethodLexer

**FFM Dependency:** ❌ NONE - Pure unit tests

**Migration Action:** 🟢 **KEEP** - No changes needed

---

## 7. Benchmarks

Performance benchmarks.

### 7.1 FfmScanBenchmark
**Location:** `memris-core/src/jmh/java/io/memris/benchmarks/FfmScanBenchmark.java`

**Description:** JMH benchmark for FFM scan operations

**Configuration:**
- Row counts: 100,000 / 1,000,000 / 10,000,000
- Operations: scanAll(), scanComparison(EQ, 42)

**Migration Action:** 🟡 **CREATE HEAP VERSION** - Establish heap performance baseline

---

### 7.2 SelectionVectorBenchmark
**Location:** `memris-core/src/main/java/io/memris/benchmarks/SelectionVectorBenchmark.java`

**Description:** SelectionVector performance (10M rows)

**Migration Action:** 🟢 **KEEP** - SelectionVector used by both FFM and heap

---

### 7.3 ThroughputBenchmark
**Location:** `memris-core/src/main/java/io/memris/benchmarks/ThroughputBenchmark.java`

**Description:** End-to-end throughput (10M rows)

**Migration Action:** 🟡 **CREATE HEAP VERSION** - Compare heap vs FFM throughput

---

### 7.4 FullBenchmark
**Location:** `memris-core/src/main/java/io/memris/benchmarks/FullBenchmark.java`

**Description:** Comprehensive benchmark

**Migration Action:** 🟢 **KEEP** - May already be heap-compatible

---

## 8. Consolidation Recommendations

### 8.1 Tests to REMOVE (100% Duplication)

| Test Class | Lines | Reason | Coverage Moved To |
|------------|-------|--------|-------------------|
| `OneToManyTest` | ~80 | 100% covered by ECommerceRealWorldTest | ECommerceRealWorldTest |
| `ManyToManyTest` | ~80 | 100% covered by ECommerceRealWorldTest | ECommerceRealWorldTest |
| `MemrisRepositoryFactoryTest` | ~60 | 100% covered by ECommerceRealWorldTest | ECommerceRealWorldTest |
| `DynamicRepositoryTest` | ~70 | 100% covered by ECommerceRealWorldTest | ECommerceRealWorldTest |

**Estimated Reduction:** 4 test classes, ~290 lines removed

---

### 8.2 Tests to MERGE (Partial Duplication)

| Test Class | Lines | Merge Action | Unique Tests to Keep |
|------------|-------|--------------|---------------------|
| `EnumeratedTest` | ~60 | Keep null enum test only | `enumerated_null_enum()` |
| `LifecycleCallbacksTest` | ~70 | Keep @PreUpdate test only | `preupdate_callback_invoked()` |
| `TransientTest` | ~60 | Merge into ECommerceRealWorldTest | None (merge all unique tests) |
| `EntityLookupTest` | ~70 | Keep existsById, hash index tests | `existsById_*()`, `deleteById_deletesFromHashIndex()` |

**Estimated Reduction:** 4 test classes consolidated, ~100 lines removed

---

### 8.3 Tests to KEEP (Unique Coverage)

| Test Class | Reason for Keeping |
|------------|-------------------|
| `ECommerceRealWorldTest` | **PRIMARY E2E TEST** - Most comprehensive |
| `IdGenerationTest` | All ID generation strategies |
| `UuidStorageOptimizationTest` | UUID → 2×long optimization |
| `UuidJoinTableTest` | UUID join tables |
| `QueryMethodCompilationTest` | Compilation caching |
| `RepositoryRuntimeTest` | Zero-reflection hot path structure |
| `PersonTableTest` | Heap table implementation |
| `TableGeneratorTest` | ByteBuddy generation |
| `LongIdIndexTest` | Heap index implementation |
| `PageColumnLongTest` | Heap long column |
| `PageColumnIntTest` | Heap int column |
| `PageColumnStringTest` | Heap String column |

---

### 8.4 New Tests Needed

| Missing Coverage | Proposed Test Class | Priority |
|------------------|---------------------|----------|
| FfmLongColumn operations | `FfmLongColumnTest` | Before migration |
| FfmStringColumn operations | `FfmStringColumnTest` | Before migration |
| Basic column get/set | `FfmColumnBasicOperationsTest` | Before migration |
| Arena lifecycle | `ArenaLifecycleTest` | Before migration |
| HeapIntColumn BETWEEN | `HeapIntColumnScanBetweenTest` | After migration |
| HeapTable IN clause | `HeapTableScanInTest` | After migration |
| HeapTable join operations | `HeapTableJoinTest` | After migration |
| Heap performance baseline | `HeapScanBenchmark` | After migration |

### 8.5 Missing Tests (Documented but Don't Exist)

| Test Class | Notes |
|------------|-------|
| `ColumnConstraintsTest` | **DOES NOT EXIST** - Should be created for column-level constraints |
| `BatchOperationsTest` | **DOES NOT EXIST** - Should be created for batch operations |
| `NestedEntityTest` | **DOES NOT EXIST** - Covered by ECommerceRealWorldTest |
| `MemrisRepositoryIntegrationTest` | **DOES NOT EXIST** - Should be created for full CRUD + joins |

---

## 9. Migration Checklist

### Phase 1: Pre-Migration (FFM Baseline)

- [ ] **Add missing FFM unit tests:**
  - [ ] `FfmLongColumnTest` - Test all FfmLongColumn operations
  - [ ] `FfmStringColumnTest` - Test all FfmStringColumn operations
  - [ ] `FfmColumnBasicOperationsTest` - Test get/set at index
  - [ ] `ArenaLifecycleTest` - Test Arena allocation/deallocation
  - [ ] `FfmColumnCreationTest` - Test column initialization

- [ ] **Run FFM benchmarks to establish baseline:**
  - [ ] `FfmScanBenchmark` - Record scan throughput
  - [ ] `ThroughputBenchmark` - Record end-to-end throughput
  - [ ] `SelectionVectorBenchmark` - Record selection overhead

- [ ] **Verify all FFM tests pass:**
  - [ ] `FfmTableScanInTest` (3 tests)
  - [ ] `FfmIntColumnScanBetweenTest` (4 tests)
  - [ ] All integration tests with FFM backend

---

### Phase 2: FFM → Heap Migration

- [ ] **Update direct FFM tests:**
  - [ ] Port `FfmTableScanInTest` → `HeapTableScanInTest`
  - [ ] Port `FfmIntColumnScanBetweenTest` → `HeapIntColumnScanBetweenTest`

- [ ] **Update integration tests with direct FFM imports:**
  - [ ] `TableManagerTest` - Replace FfmTable with HeapTable
  - [ ] Re-enable `RepositoryRuntimeIntegrationTest` with HeapTable
  - [ ] Remove `MemrisRepositoryFactoryTest` (covered by ECommerceRealWorldTest)

- [ ] **Verify heap storage tests pass:**
  - [ ] `PersonTableTest` (7 tests)
  - [ ] `TableGeneratorTest` (5 tests)
  - [ ] `LongIdIndexTest` (11 tests)
  - [ ] `PageColumnLongTest` (6 tests)
  - [ ] `PageColumnIntTest` (? tests)
  - [ ] `PageColumnStringTest` (? tests)

---

### Phase 3: Integration Test Consolidation

- [ ] **Verify all integration tests pass with heap:**
  - [ ] `ECommerceRealWorldTest`
  - [ ] All keep-list tests

- [ ] **Remove redundant tests:**
  - [ ] Delete `OneToManyTest.java`
  - [ ] Delete `ManyToManyTest.java`
  - [ ] Delete `MemrisRepositoryFactoryTest.java`
  - [ ] Delete `DynamicRepositoryTest.java`

- [ ] **Merge partial duplicates:**
  - [ ] Merge `EnumeratedTest` → keep null test only
  - [ ] Merge `LifecycleCallbacksTest` → keep @PreUpdate only
  - [ ] Merge `TransientTest` → into ECommerceRealWorldTest
  - [ ] Merge `EntityLookupTest` → keep unique tests

---

### Phase 4: Benchmark Comparison

- [ ] **Create heap benchmarks:**
  - [ ] `HeapScanBenchmark` (equivalent to FfmScanBenchmark)
  - [ ] `HeapThroughputBenchmark` (equivalent to ThroughputBenchmark)

- [ ] **Compare performance:**
  - [ ] Run FFM vs Heap scan benchmarks
  - [ ] Run FFM vs Heap throughput benchmarks
  - [ ] Document performance differences

---

### Phase 5: FFM Removal

- [ ] **Remove FFM dependencies:**
  - [ ] Remove `jdk.incubator.vector` module requirement
  - [ ] Remove `java.base` FFM module requirement
  - [ ] Remove `--enable-native-access=ALL-UNNAMED` flag
  - [ ] Remove `--enable-preview` flag (if no other preview features)

- [ ] **Remove FFM source files:**
  - [ ] Remove `io.memris.storage.ffm` package
  - [ ] Remove FFM-specific tests (FfmTableScanInTest, FfmIntColumnScanBetweenTest)

- [ ] **Update documentation:**
  - [ ] Update ARCHITECTURE.md (remove FFM references)
  - [ ] Update DEVELOPMENT.md (remove FFM build requirements)
  - [ ] Update README.md (remove FFM from requirements)

---

### Phase 6: Validation

- [ ] **Final test suite validation:**
  - [ ] All heap storage tests pass
  - [ ] All integration tests pass
  - [ ] ECommerceRealWorldTest passes (comprehensive E2E)
  - [ ] Benchmarks document performance characteristics

- [ ] **Coverage verification:**
  - [ ] Verify no loss of test coverage from consolidation
  - [ ] Verify all edge cases still tested
  - [ ] Verify all relationship types still tested

---

## Appendix A: Complete Test File List

### FFM Storage Tests (2 files)
```
memris-core/src/test/java/io/memris/storage/ffm/FfmTableScanInTest.java
memris-core/src/test/java/io/memris/storage/ffm/FfmIntColumnScanBetweenTest.java
```

### Integration Tests with Direct FFM Imports (2 files)
```
memris-core/src/test/java/io/memris/spring/TableManagerTest.java
memris-core/src/test/java/io/memris/spring/runtime/RepositoryRuntimeIntegrationTest.java (DISABLED)
```

### Integration Tests (Indirect FFM) (11 files)
```
memris-core/src/test/java/io/memris/spring/ECommerceRealWorldTest.java
memris-core/src/test/java/io/memris/spring/OneToManyTest.java
memris-core/src/test/java/io/memris/spring/ManyToManyTest.java
memris-core/src/test/java/io/memris/spring/DynamicRepositoryTest.java
memris-core/src/test/java/io/memris/spring/IdGenerationTest.java
memris-core/src/test/java/io/memris/spring/LifecycleCallbacksTest.java
memris-core/src/test/java/io/memris/spring/EnumeratedTest.java
memris-core/src/test/java/io/memris/spring/TransientTest.java
memris-core/src/test/java/io/memris/spring/EntityLookupTest.java
memris-core/src/test/java/io/memris/spring/UuidStorageOptimizationTest.java
memris-core/src/test/java/io/memris/spring/UuidJoinTableTest.java
memris-core/src/test/java/io/memris/spring/QueryMethodCompilationTest.java
memris-core/src/test/java/io/memris/spring/MemrisRepositoryFactoryTest.java
```

### Runtime Tests (2 files)
```
memris-core/src/test/java/io/memris/spring/runtime/RepositoryRuntimeTest.java
memris-core/src/test/java/io/memris/spring/runtime/RepositoryRuntimeIntegrationTest.java (DISABLED)
```

### Heap Storage Tests (6 files)
```
memris-core/src/test/java/io/memris/storage/heap/PersonTableTest.java
memris-core/src/test/java/io/memris/storage/heap/TableGeneratorTest.java
memris-core/src/test/java/io/memris/storage/heap/LongIdIndexTest.java
memris-core/src/test/java/io/memris/storage/heap/PageColumnLongTest.java
memris-core/src/test/java/io/memris/storage/heap/PageColumnIntTest.java
memris-core/src/test/java/io/memris/storage/heap/PageColumnStringTest.java
```

### Spring Data Layer Tests (4 files)
```
memris-core/src/test/java/io/memris/spring/plan/QueryPlannerIntegrationTest.java
memris-core/src/test/java/io/memris/spring/plan/BuiltInResolverTest.java
memris-core/src/test/java/io/memris/spring/plan/entities/ContextAwareLexerTest.java
memris-core/src/test/java/io/memris/spring/plan/entities/ComplexNestingTest.java
```

### Benchmarks (4 files)
```
memris-core/src/jmh/java/io/memris/benchmarks/FfmScanBenchmark.java
memris-core/src/main/java/io/memris/benchmarks/SelectionVectorBenchmark.java
memris-core/src/main/java/io/memris/benchmarks/ThroughputBenchmark.java
memris-core/src/main/java/io/memris/benchmarks/FullBenchmark.java
```

---

## Appendix B: FFM Import Summary

**Files with direct `io.memris.storage.ffm` imports:**

| File | Imports |
|------|---------|
| `TableManagerTest.java` | `FfmTable` |
| `RepositoryRuntimeIntegrationTest.java` | `FfmTable` |
| `MemrisRepositoryFactoryTest.java` | `FfmTable`, `FfmTable.ColumnSpec` |
| `FfmTableScanInTest.java` | `FfmTable`, `Predicate` |
| `FfmIntColumnScanBetweenTest.java` | `FfmIntColumn`, `SelectionVector` |

**Total: 5 files require direct modification to remove FFM imports**

---

**End of Document**
