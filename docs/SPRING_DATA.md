# Spring Data Integration

## Overview

Memris provides a lightweight, high-performance Spring Data-compatible API for in-memory storage with custom annotation support. The system focuses on O(1) operations, zero-reflection hot paths, and compile-once query generation for maximum performance.

**Key Design Philosophy:**
- **Zero Reflection Hot Path**: Query planning happens at compile-time, runtime uses direct array access
- **Dense Arrays Over Maps**: Column indices, type codes, and method handles stored in indexed arrays
- **TypeCode Switch**: Java 21 pattern matching for zero-allocation type dispatch
- **Heap-Based Storage**: 100% Java heap using primitive arrays

**Compatibility Target:**
- Spring Data-like query method patterns (`findBy…And…`, `In`, `Between`, comparisons)
- Custom annotations (`@Entity`, `@Index`, `@GeneratedValue`, `@OneToOne`)
- **Note:** Uses custom annotations, NOT Jakarta/JPA annotations

**Architecture Note:**
- Generates **TABLE** classes via ByteBuddy, not repository classes
- Callers implement their own repository layer on top of generated tables
- Storage is 100% heap-based (no FFM, no MemorySegment)

## Current Implementation Status

| Category | Feature | Status | Test Coverage |
|----------|---------|--------|---------------|
| **Core** | @Entity detection | ✅ Done | ✅ Via tests |
| **Core** | Auto-generated ID (@GeneratedValue) | ✅ Done | ✅ Integrated |
| **Core** | Basic CRUD (save, findAll, findBy) | ✅ Done | ✅ Tests |
| **Core** | Field caching (O(1) lookup) | ✅ Done | ✅ Verified |
| **Relationships** | @OneToOne | ✅ Done | ✅ Tests |
| **Queries** | Query predicates (EQ, IN, BETWEEN, GT, LT, etc.) | ✅ Done | ✅ 50+ tests |
| **Queries** | String operators (Like, Contains, StartsWith, etc.) | ✅ Done | ✅ Tests |
| **Queries** | Boolean operators (IsTrue, IsFalse) | ✅ Done | ✅ Tests |
| **Queries** | Null operators (IsNull, IsNotNull) | ✅ Done | ✅ Tests |
| **Queries** | Logical operators (And, Or) | ✅ Done | ✅ Tests |
| **Type Mapping** | Primitive types (int, long, String) | ✅ Done | ✅ Tests |
| **Advanced** | Hash join | ✅ Done | ✅ Available |
| **Advanced** | MemrisException | ✅ Done | ✅ Integrated |

**Total Test Count:** 148 tests, 147 passing ✅

## Annotations

Memris uses **custom annotations** (not Jakarta/JPA):

### Core Annotations

**@Entity** (`io.memris.spring.Entity`)
- Marks a class as a persistable entity
- Used by TableGenerator to create table metadata

**@Index** (`io.memris.spring.Index`)
- Marks a field for indexing
- Supports HASH or BTREE index types
- Improves query performance for indexed fields

**@GeneratedValue** (`io.memris.spring.GeneratedValue`)
- Marks an ID field for automatic generation
- Strategy: AUTO, IDENTITY, UUID, CUSTOM
- Used with `@Id` (Jakarta) in tests, but custom `@Entity` in main code

**@OneToOne** (`io.memris.spring.OneToOne`)
- Marks a one-to-one relationship
- Supports cascade operations

**GenerationType** (`io.memris.spring.GenerationType`)
- `AUTO` - Automatic strategy selection
- `IDENTITY` - Database identity (auto-increment)
- `UUID` - UUID generation
- `CUSTOM` - Custom IdGenerator implementation

### Usage Example

```java
import io.memris.spring.Entity;
import io.memris.spring.Index;
import io.memris.spring.GeneratedValue;
import io.memris.spring.GenerationType;

@Entity
public class User {
    @Index(type = Index.Type.HASH)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Index(type = Index.Type.HASH)
    private String email;
    
    private String name;
    private int age;
}
```

## Query Method Support

### Supported Query Patterns

**Basic Queries:**
```java
// Equality
List<User> findByName(String name);
User findByEmail(String email);

// Multiple conditions (AND)
List<User> findByNameAndAge(String name, int age);

// Comparison operators
List<User> findByAgeGreaterThan(int age);
List<User> findByAgeGreaterThanEqual(int age);
List<User> findByAgeLessThan(int age);
List<User> findByAgeLessThanEqual(int age);
List<User> findByAgeBetween(int min, int max);

// String operators
List<User> findByNameLike(String pattern);
List<User> findByNameStartingWith(String prefix);
List<User> findByNameEndingWith(String suffix);
List<User> findByNameContaining(String substring);
List<User> findByNameIgnoreCase(String name);

// Null checks
List<User> findByNameIsNull();
List<User> findByNameIsNotNull();

// Boolean checks
List<User> findByActiveTrue();
List<User> findByActiveFalse();

// Collection operators
List<User> findByStatusIn(List<String> statuses);

// Count and exists
long countByActiveTrue();
boolean existsByEmail(String email);
```

**Logical Operators:**
```java
// AND (implicit)
List<User> findByNameAndAge(String name, int age);

// OR
List<User> findByNameOrEmail(String name, String email);
```

**Return Types:**
- `List<T>` - Multiple results
- `Optional<T>` - Single optional result
- `long` - Count
- `boolean` - Existence check

## Architecture & Implementation

### Query Planning Pipeline

```
QueryMethodLexer.tokenize(methodName) → List<QueryMethodToken>
        ↓
QueryPlanner.plan(tokens, entityClass) → LogicalQuery
        ↓
CompiledQuery (pre-compiled with column indices)
        ↓
HeapRuntimeKernel.execute(compiledQuery, table)
        ↓
GeneratedTable.scan*() methods
```

**Key Components:**

**QueryMethodLexer** (`io.memris.spring.plan.QueryMethodLexer`)
- Tokenizes query method names
- Extracts prefix (find/count/exists/delete)
- Identifies operators (GreaterThan, Between, Like, etc.)
- Handles combinators (And, Or)
- Detects built-ins (findAll, count, deleteAll)

**QueryPlanner** (`io.memris.spring.plan.QueryPlanner`)
- Creates LogicalQuery from tokens
- Validates property paths against entity metadata
- Resolves operators to Predicate types
- Uses BuiltInResolver for built-in methods

**BuiltInResolver** (`io.memris.spring.plan.BuiltInResolver`)
- Signature-based built-in method resolution
- Handles findById, save, delete, findAll, count, existsById
- Deterministic tie-breaking for ambiguous matches

**HeapRuntimeKernel** (`io.memris.spring.runtime.HeapRuntimeKernel`)
- Zero-reflection query execution
- TypeCode switch dispatch
- Delegates to GeneratedTable scan methods

**TableGenerator** (`io.memris.storage.heap.TableGenerator`)
- ByteBuddy bytecode generation
- Creates table classes extending AbstractTable
- Generates typed columns (PageColumnInt, PageColumnLong, PageColumnString)
- Creates ID indexes (LongIdIndex, StringIdIndex)

### Generated Table Structure

```java
public final class UserTable extends AbstractTable implements GeneratedTable {
    // Column fields
    public final PageColumnLong idColumn;
    public final PageColumnString emailColumn;
    public final PageColumnString nameColumn;
    public final PageColumnInt ageColumn;
    
    // ID index
    public LongIdIndex idIndex;
    
    // GeneratedTable interface methods
    @Override
    public int[] scanEqualsLong(int columnIndex, long value) { ... }
    
    @Override
    public long lookupById(long id) { ... }
    
    @Override
    public long insertFrom(Object[] values) { ... }
}
```

**Design Principles:**
- **Zero Reflection Hot Path**: Compile-time parsing, runtime array access
- **Dense Arrays Over Maps**: O(1) access via column indices
- **TypeCode Switch**: Java 21 pattern matching for type dispatch
- **Table Generation**: Generate storage tables, not repository classes

*For detailed architecture diagrams and package structure, see [ARCHITECTURE.md](ARCHITECTURE.md)*

## Roadmap

### Phase 1 — Core Storage ✅ COMPLETE
- Table + row layout with primitive arrays
- Hash index + range index
- Filter execution with index selection
- Heap-based columnar storage

### Phase 2 — Joins ✅ COMPLETE
- Hash join (HashJoin.java)
- Foreign key support
- Relationship handling

### Phase 3 — Query Planning ✅ COMPLETE
- QueryMethodLexer tokenization ✅
- QueryPlanner logical query creation ✅
- BuiltInResolver for built-ins ✅
- HeapRuntimeKernel execution ✅
- All operators implemented ✅

### Phase 4 — Advanced Query Features ⏳ FUTURE
- Sorting support (OrderBy)
- Paging/limit support (Top/First)
- Query optimization with cost model

### Phase 5 — Complex Entity Features ⏳ FUTURE
- Inheritance hierarchies
- Composite keys
- Cascade delete / orphan removal
- Optimistic locking

### Phase 6 — Enterprise Features ⏳ FUTURE
- Transaction support
- Schema evolution
- Named queries

## Performance Optimizations

### Implemented Optimizations

**1. Field Caching (O(1) lookup)** ✅
- Direct field access via MethodHandles
- Compile-time extraction
- Impact: ~2-5x faster materialization

**2. Zero-Reflection Hot Path** ✅
- Compile-time query planning
- Direct array access (columnNames[], typeCodes[])
- No Class.forName() or Method.invoke()

**3. Dense Arrays Over Maps** ✅
- `String[] columnNames` vs `Map<String, Integer>`
- `byte[] typeCodes` vs `Map<String, Byte>`
- `MethodHandle[] setters` vs `Map<String, MethodHandle>`

**4. TypeCode Switch** ✅
- Java 21 pattern matching
- Zero-allocation type dispatch
- `switch (typeCode) { case TYPE_INT → ... }`

**5. ByteBuddy Table Generation** ✅
- Generates optimized table classes at runtime
- Pre-compiled MethodHandles for column access
- Two strategies: MethodHandle (~5ns) and Bytecode (~1ns)

## Design Principles

### HickoryCP Philosophy (Followed)

1. **No object instantiation in hot paths** - No lambdas, no streams, no allocations
2. **Static code paths** - Switch expressions, for-loops, direct field access
3. **Primitive-only APIs** - Avoid boxing in hot paths
4. **O(1) operations** - Hash maps, direct indexing
5. **Final classes** - Enable JVM inlining
6. **Package-private fields** - Direct access, no virtual dispatch
7. **Specialized exceptions** - `MemrisException` instead of `RuntimeException`

### Code Patterns

**GOOD - static, no allocation:**
```java
for (int i = 0; i < size; i++) {
    int value = column.get(i);
    // process value
}
```

**BAD - creates objects:**
```java
return list.stream().filter(e -> condition).toList();
```

### Memris Extensions

1. **Annotation-driven** - Custom annotations for entities
2. **Zero-config** - Auto-schema inference from annotations
3. **In-memory first** - No SQL dialect complexity
4. **Composable** - Factory pattern for table generation

## References

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture documentation
- [DEVELOPMENT.md](DEVELOPMENT.md) - Build commands and development guidelines
- [QUERY.md](QUERY.md) - Query method reference
