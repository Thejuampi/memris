# Memris Architecture

Heap-based, zero-reflection, columnar in-memory storage engine for Java 21.

## Overview

Memris is a high-performance in-memory storage engine with the following principles:

- **Heap-based storage**: 100% Java heap using primitive arrays and object pools
- **Zero reflection hot paths**: Compile-time metadata extraction, runtime queryId dispatch
- **ByteBuddy table generation**: Generates optimized table classes at build time
- **Plan-driven property paths**: Embedded/dotted access compiled once into `ColumnAccessPlan`
- **Generated entity accessors**: Saver/materializer bytecode for flat and embedded columns
- **Primitive-only APIs**: Direct array access, no boxing in hot paths
- **O(1) design**: Direct index access, hash-based lookups

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Query Pipeline                               │
│                                                                      │
│  Repository Method → QueryMethodLexer → QueryPlanner → CompiledQuery │
│       │                                                      │       │
│       │                                                      │       │
│       ▼                                                      ▼       │
│  [QueryMethod]                                  [HeapRuntimeKernel]  │
│                                          │                           │
│                                          │ Zero-reflection dispatch  │
│                                          ▼                           │
│                                    [GeneratedTable]                  │
│                                          │                           │
│                                          │ Column-indexed access     │
│                                          ▼                           │
│                            [PageColumnInt/Long/String]               │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       Table Generation                               │
│                                                                      │
│  Entity Class + Annotations → TableMetadata                          │
│         │                                                            │
│         │ Build-time (once per entity)                               │
│         ▼                                                            │
│  TableGenerator.generate() → PersonTable.class (ByteBuddy)           │
│         │                                                            │
│         │ Implements GeneratedTable interface                        │
│         ▼                                                            │
│  Generated columns: PageColumnLong, PageColumnInt, PageColumnString  │
│  ID index: LongIdIndex or StringIdIndex                              │
└─────────────────────────────────────────────────────────────────────┘
```

## Package Structure

| Package | Purpose | Key Classes |
|---------|---------|-------------|
| `io.memris.core` | Annotations, types, configuration | `@Entity`, `@Id`, `@Index`, `@GeneratedValue`, `TypeCodes`, `MemrisArena`, `MemrisConfiguration` |
| `io.memris.query` | Query parsing & planning | `QueryMethodLexer`, `QueryPlanner`, `CompiledQuery`, `OpCode`, `BuiltInResolver` |
| `io.memris.runtime` | Query execution | `HeapRuntimeKernel`, `EntityMaterializer`, `EntityExtractor`, `RepositoryRuntime` |
| `io.memris.runtime.codegen` | Runtime code generation | `RuntimeExecutorGenerator` |
| `io.memris.storage` | Storage interfaces | `GeneratedTable`, `Selection` |
| `io.memris.storage.heap` | Heap-based implementation | `TableGenerator`, `AbstractTable`, `PageColumnInt`, `PageColumnLong`, `PageColumnString`, `LongIdIndex`, `StringIdIndex`, `LockFreeFreeList` |
| `io.memris.index` | Index implementations | `HashIndex`, `RangeIndex`, `CompositeHashIndex`, `CompositeRangeIndex`, `StringPrefixIndex`, `StringSuffixIndex` |
| `io.memris.kernel` | Core execution primitives | `Predicate`, `RowId`, `RowIdSet`, `LongEnumerator` |
| `io.memris.repository` | Repository + saver generation | `RepositoryEmitter`, `EntitySaverGenerator`, `MemrisRepositoryFactory`, `RepositoryMethodIntrospector` |

## Layer Responsibilities

### Layer 1: Domain Layer

**Package:** `io.memris.core`

Custom annotations for entity marking:
- `@Entity` - Marks a class as an entity
- `@Id` - Marks primary key field
- `@Index` - Marks field for indexing (HASH/BTREE/PREFIX/SUFFIX)
- `@GeneratedValue` - Auto ID generation with strategy
- `@ManyToOne`, `@OneToMany`, `@OneToOne`, `@ManyToMany` - Relationships
- `@JoinColumn`, `@JoinTable` - Relationship configuration

**Note:** These are NOT Jakarta/JPA annotations. Memris uses its own annotation system.

### Layer 2: Parser Layer

**Package:** `io.memris.query`

**QueryMethodLexer**
- Tokenizes query method names (findByLastname → tokens)
- Handles operators: GreaterThan, Between, IgnoreCase, Like, In, etc.
- Built-in detection: findAll, count, deleteAll
- Supports nested property paths (customer.email)

**Supported Operators:**

| Category | Operators |
|----------|-----------|
| Comparison | EQ, NE (Not), GT (GreaterThan), GTE (GreaterThanEqual), LT (LessThan), LTE (LessThanEqual), BETWEEN |
| String | LIKE, NOT_LIKE, STARTING_WITH, ENDING_WITH, CONTAINING, IGNORE_CASE |
| Boolean | IS_TRUE, IS_FALSE |
| Null | IS_NULL, IS_NOT_NULL |
| Collection | IN, NOT_IN |
| Date/Time | AFTER, BEFORE |
| Logical | AND, OR |
| Modifiers | DISTINCT, ORDER BY (Asc/Desc), TOP/FIRST/LIMIT |

### Layer 3: Planner Layer

**Package:** `io.memris.query`

**QueryPlanner**
- Creates `LogicalQuery` from tokens
- Resolves built-in operations via `BuiltInResolver`
- Validates properties against entity metadata

**CompiledQuery**
- Pre-compiled query with resolved column indices
- Ready for execution by HeapRuntimeKernel

**BuiltInResolver**
- Signature-based built-in method resolution
- Handles: save, findById, delete, deleteById, existsById, count, findAll, saveAll, deleteAll, findAllById, deleteAllById

### Layer 4: Runtime Layer

**Package:** `io.memris.runtime`

**HeapRuntimeKernel**
- Zero-reflection query execution
- TypeCode switch dispatch
- Delegates to GeneratedTable methods

**RuntimeExecutorGenerator**
- Generates type-specialized executors at runtime using ByteBuddy
- Eliminates runtime type switches on hot paths
- Generates: FieldValueReader, FkReader, TargetRowResolver, ConditionExecutor, OrderKeyBuilder
- Feature toggle: `-Dmemris.codegen.enabled=false` to disable

**EntityMaterializerGenerator**
- Generates materializer classes for flat and embedded columns
- Uses direct field bytecode for flat public fields
- Uses `ColumnAccessPlan` for dotted/embedded paths
- Caches generated artifacts by entity shape

**EntitySaverGenerator**
- Generates saver classes for insert/update operations
- Uses direct field bytecode for flat public fields
- Uses `ColumnAccessPlan` for dotted/embedded paths
- Caches generated artifacts by entity shape

### Layer 5: Storage Layer

**Package:** `io.memris.storage.heap`

**TableGenerator**
- ByteBuddy bytecode generation
- Creates table classes extending `AbstractTable`
- Generates typed columns and ID indexes

**AbstractTable**
- Base class for all generated tables
- Manages row allocation with `LockFreeFreeList` for O(1) reuse
- Provides seqlock infrastructure via `rowSeqLocks` (AtomicLongArray)
- Tracks tombstones, row counts, and generations

**GeneratedTable Interface**
- Low-level table interface
- Typed scans: `scanEqualsLong()`, `scanBetweenInt()`, etc.
- Typed reads: `readLong()`, `readInt()`, `readString()`
- Primary key index: `lookupById()`, `removeById()`

**PageColumn Implementations**
- `PageColumnInt` - int[] column with direct array scans
- `PageColumnLong` - long[] column with direct array scans
- `PageColumnString` - String[] column with direct array scans

**ID Indexes**
- `LongIdIndex` - Long-based primary key index
- `StringIdIndex` - String-based primary key index

**Concurrency Primitives**
- `LockFreeFreeList` - Lock-free Treiber stack for row ID reuse with CAS-based push/pop

### Layer 6: Index Layer

**Package:** `io.memris.index`

| Index | Complexity | Use Case |
|-------|------------|----------|
| `HashIndex` | O(1) | Equality lookups |
| `RangeIndex` | O(log n) | Range queries (ConcurrentSkipListMap) |
| `CompositeHashIndex` | O(1) | Multi-field equality lookups |
| `StringPrefixIndex` | O(k) | STARTING_WITH queries |
| `StringSuffixIndex` | O(k) | ENDING_WITH queries |

## Critical Design Principles

### 1. O(1) First, O(log n) Second, O(n) Forbidden

All hot path operations are O(1):
- Direct array access via column indices
- Hash-based ID lookups
- BitSet for dense row sets
- TypeCode switch dispatch (tableswitch bytecode)
- Lock-free row allocation via `LockFreeFreeList`

### 2. Primitive-Only APIs

Never use boxed types in hot paths:
- `int[]` instead of `List<Integer>`
- `IntEnumerator` instead of `Iterator<Integer>`
- `long` row references instead of objects

### 3. TypeCodes (Static Constants)

Type switching uses static byte constants:

```java
public final class TypeCodes {
    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;
    public static final byte TYPE_FLOAT = 2;
    public static final byte TYPE_DOUBLE = 3;
    public static final byte TYPE_BOOLEAN = 4;
    public static final byte TYPE_STRING = 8;
    // ...
}
```

JVM inlines these constants and compiles switch to tableswitch (direct jump).

### 4. Zero Reflection Hot Path

- MethodHandles extracted at build time
- Column indices resolved at compile time
- queryId-based dispatch (int lookup, not string)
- No `Class.forName()`, no `Method.invoke()` in hot path
- Embedded property chains compiled once in `ColumnAccessPlan` and reused

### 5. Lock-Free Concurrency Primitives

- `LockFreeFreeList`: Treiber stack with CAS-based push/pop for O(1) row reuse
- `rowSeqLocks`: AtomicLongArray seqlock per row for atomic read-write transactions

## Build-Time vs Runtime

### Build-Time (Once Per Entity Type)

```
@Entity class Person { ... }  
    ↓
TableMetadata (name, fields, ID type)
    ↓
TableGenerator.generate(metadata)
    ↓
PersonTable.class (ByteBuddy generated)
    ↓
Loads with: PersonTable table = new PersonTable(pageSize, maxPages)
```

**What Gets Generated:**
- Class extending `AbstractTable`
- Fields: `idColumn`, `nameColumn`, `ageColumn`, etc.
- ID index field: `idIndex` (LongIdIndex or StringIdIndex)
- Methods implementing `GeneratedTable` interface

### Runtime (Hot Path)

```
repository.findByAgeGreaterThan(18)
    ↓
QueryMethodLexer.tokenize("findByAgeGreaterThan")
    ↓
[OPERATION: FIND_BY, PROPERTY: age, OPERATOR: GREATER_THAN, VALUE: 18]
    ↓
QueryPlanner.plan() → CompiledQuery
    ↓
HeapRuntimeKernel.executeCondition(compiledQuery, table)
    ↓
table.scanGreaterThanLong(columnIndex, 18, limit)
    ↓
return int[] rowIndices
    ↓
EntityMaterializer.materialize(rows) → List<Person>
```

## Important Files Reference

| File | Purpose |
|------|---------|
| `TableGenerator.java` | ByteBuddy table generation |
| `RuntimeExecutorGenerator.java` | Runtime codegen for type-specialized executors |
| `ColumnAccessPlan.java` | Compiled path metadata for embedded/dotted properties |
| `EntitySaverGenerator.java` | Generated entity saver for flat + embedded paths |
| `EntityMaterializerGenerator.java` | Generated entity materializer for flat + embedded paths |
| `HeapRuntimeKernel.java` | Query execution engine |
| `RepositoryRuntime.java` | Repository runtime with join materialization |
| `TypeCodes.java` | Type code constants |
| `BuiltInResolver.java` | Built-in method resolution |
| `QueryMethodLexer.java` | Method name tokenization |
| `QueryPlanner.java` | Query planning |
| `CompiledQuery.java` | Compiled query structure |
| `GeneratedTable.java` | Table interface |
| `AbstractTable.java` | Base table class with seqlocks |
| `PageColumn*.java` | Column implementations (Int, Long, String) |
| `LockFreeFreeList.java` | Lock-free Treiber stack for row reuse |
| `RangeIndex.java` | Range index (O(log n)) |
| `HashIndex.java` | Hash index (O(1)) |

## Notes

- **Current Storage**: 100% heap-based using primitive arrays
- **SIMD Not Implemented**: Plain loops used; JIT may auto-vectorize
- **Custom annotations**: Uses `@Entity`, `@Index`, etc. (not Jakarta/JPA)
- **Repository generation**: `RepositoryEmitter` generates repository implementations at runtime
- **Relationship Support**: All relationship types fully implemented
- **Lock-Free Data Structures**: `LockFreeFreeList` uses Treiber stack algorithm with CAS
- **Seqlock Support**: `rowSeqLocks` in AbstractTable provides per-row atomicity
