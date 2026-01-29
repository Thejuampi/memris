# Memris Architecture

Heap-based, zero-reflection, columnar in-memory storage engine for Java 21.

## Overview

Memris is a high-performance in-memory storage engine with the following principles:

- **Heap-based storage**: 100% Java heap using primitive arrays and object pools
- **Zero reflection hot paths**: Compile-time metadata extraction, runtime queryId dispatch
- **ByteBuddy table generation**: Generates optimized table classes at build time
- **Primitive-only APIs**: Direct array access, no boxing in hot paths
- **O(1) design**: Direct index access, hash-based lookups

**Key Design Decisions:**
- Tables are generated via ByteBuddy, not repositories
- TypeCodes are static constants (byte), not enum ordinals
- HeapRuntimeKernel executes queries, not RepositoryRuntime
- Custom annotations (not Jakarta/JPA) mark entities and indexes

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
| `io.memris.kernel` | Core execution primitives | `SelectionVector`, `Predicate`, `PlanNode`, `Executor`, `HashJoin` |
| `io.memris.storage` | Storage interfaces | `GeneratedTable`, `Table`, `Selection` |
| `io.memris.storage.heap` | Heap-based implementation | `TableGenerator`, `AbstractTable`, `PageColumn*`, `*IdIndex` |
| `io.memris.index` | Index implementations | `HashIndex`, `RangeIndex`, `LongIdIndex`, `StringIdIndex` |
| `io.memris.spring` | Custom annotations & types | `@Entity`, `@Index`, `@GeneratedValue`, `TypeCodes` |
| `io.memris.spring.plan` | Query parsing & planning | `QueryMethodLexer`, `QueryPlanner`, `CompiledQuery`, `OpCode` |
| `io.memris.spring.runtime` | Query execution | `HeapRuntimeKernel`, `EntityMaterializer`, `EntityExtractor` |
| `io.memris.spring.scaffold` | Repository scaffolding | `RepositoryMethodIntrospector` |

## Layer Responsibilities

### Layer 1: Domain Layer
**Package:** `io.memris.spring`

Custom annotations for entity marking:
- `@Entity` - Marks a class as an entity
- `@Index` - Marks field for indexing (HASH/BTREE)
- `@GeneratedValue` - Auto ID generation with strategy
- `@OneToOne` - Relationship marker

**Note:** These are NOT Jakarta/JPA annotations. Memris uses its own annotation system.

### Layer 2: Parser Layer
**Package:** `io.memris.spring.plan`

**QueryMethodLexer** (`QueryMethodLexer.java:213`)
- Tokenizes query method names (findByLastname → tokens)
- Handles operators: GreaterThan, Between, IgnoreCase, etc.
- Built-in detection: findAll, count, deleteAll

**Key Files:**
- `QueryMethodLexer.java` - Main tokenizer
- `QueryMethodToken.java` - Token type
- `QueryMethodTokenType.java` - Token type enum

### Layer 3: Compiler/Planner Layer
**Package:** `io.memris.spring.plan`

**QueryPlanner** (`QueryPlanner.java`)
- Creates `LogicalQuery` from tokens
- Resolves built-in operations via `BuiltInResolver`
- Validates properties against entity metadata

**CompiledQuery** (`CompiledQuery.java`)
- Pre-compiled query with resolved column indices
- Ready for execution by HeapRuntimeKernel

**BuiltInResolver** (`BuiltInResolver.java:57`)
- Signature-based built-in method resolution
- Deterministic tie-breaking for ambiguous matches
- Handles save, findById, delete, etc.

**Key Files:**
- `QueryPlanner.java` - Main planner
- `CompiledQuery.java` - Compiled query representation
- `LogicalQuery.java` - Intermediate query representation
- `OpCode.java` - Operation codes (FIND, SAVE, DELETE, etc.)
- `BuiltInResolver.java` - Built-in method resolution

### Layer 4: Runtime Layer
**Package:** `io.memris.spring.runtime`

**HeapRuntimeKernel** (`HeapRuntimeKernel.java:9`)
- Zero-reflection query execution
- TypeCode switch dispatch
- Delegates to GeneratedTable methods

**EntityMaterializer** (`EntityMaterializer.java`)
- Converts table rows to entity objects
- Uses MethodHandles for field access

**EntityExtractor** (`EntityExtractor.java`)
- Extracts field values from entities
- Used for insert/update operations

**Key Files:**
- `HeapRuntimeKernel.java` - Main execution engine
- `EntityMaterializer.java` - Entity creation
- `EntityExtractor.java` - Entity decomposition

### Layer 5: Storage Layer
**Package:** `io.memris.storage.heap`

**TableGenerator** (`TableGenerator.java:31`)
- ByteBuddy bytecode generation
- Creates table classes extending `AbstractTable`
- Generates typed columns and ID indexes

**GeneratedTable Interface** (`GeneratedTable.java:15`)
- Low-level table interface
- Typed scans: `scanEqualsLong()`, `scanBetweenInt()`
- Typed reads: `readLong()`, `readInt()`, `readString()`
- Primary key index: `lookupById()`, `removeById()`

**PageColumn Implementations**
- `PageColumnInt` - int[] column with SIMD-capable scans
- `PageColumnLong` - long[] column
- `PageColumnString` - String[] column

**ID Indexes**
- `LongIdIndex` - Long-based primary key index
- `StringIdIndex` - String-based primary key index

**Key Files:**
- `TableGenerator.java` - ByteBuddy generation
- `AbstractTable.java` - Base class for tables
- `GeneratedTable.java` - Table interface
- `PageColumn*.java` - Column implementations
- `*IdIndex.java` - ID indexes

### Layer 6: Index Layer
**Package:** `io.memris.index`

**HashIndex** - Hash-based equality lookups (O(1))
**RangeIndex** - ConcurrentSkipListMap for ranges (O(log n))

**Key Files:**
- `HashIndex.java` - Hash index
- `RangeIndex.java` - Range index

## Critical Design Principles

### 1. O(1) First, O(log n) Second, O(n) Forbidden
All hot path operations are O(1):
- Direct array access via column indices
- Hash-based ID lookups
- BitSet for dense row sets
- TypeCode switch dispatch (tableswitch bytecode)

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

## ByteBuddy Table Generation

Unlike typical Spring Data implementations that generate repository classes, Memris generates **storage table classes**:

**Generated Class Structure:**
```java
public final class PersonTable extends AbstractTable implements GeneratedTable {
    // Column fields
    public final PageColumnLong idColumn;
    public final PageColumnString nameColumn;
    public final PageColumnInt ageColumn;
    
    // ID index
    public LongIdIndex idIndex;
    
    // Constructor
    public PersonTable(int pageSize, int maxPages) {
        super("Person", pageSize, maxPages);
        // Initialize columns and index
    }
    
    // GeneratedTable interface methods
    @Override
    public int[] scanEqualsLong(int columnIndex, long value) { ... }
    
    @Override
    public long lookupById(long id) { ... }
    
    @Override
    public long insertFrom(Object[] values) { ... }
}
```

**Why Tables Instead of Repositories?**
- Repositories = high-level interface (Spring Data concern)
- Tables = low-level storage (Memris concern)
- Clean separation: Memris handles storage, caller handles repository pattern
- Better testability: Can test storage layer independently

## Important Files Reference

| File | Line | Purpose |
|------|------|---------|
| `TableGenerator.java` | 31 | ByteBuddy table generation |
| `HeapRuntimeKernel.java` | 9 | Query execution engine |
| `TypeCodes.java` | 16 | Type code constants |
| `BuiltInResolver.java` | 57 | Built-in method resolution |
| `QueryMethodLexer.java` | 213 | Method name tokenization |
| `QueryPlanner.java` | 1 | Query planning |
| `GeneratedTable.java` | 15 | Table interface |
| `AbstractTable.java` | 26 | Base table class |
| `PageColumnInt.java` | 16 | int column storage |
| `PageColumnLong.java` | 16 | long column storage |
| `PageColumnString.java` | 16 | String column storage |

## Notes

- **No FFM/Foreign Memory**: All storage uses Java heap (int[], long[], String[])
- **No SIMD yet**: Flags configured but not actively used (plain loops)
- **Custom annotations**: Uses `@Entity`, `@Index`, etc. (not Jakarta/JPA)
- **No Repository generation**: Generates tables, caller implements repository pattern
