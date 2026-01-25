# Query Method Full Specification

This document provides the complete specification for Memris query methods, including architecture details, operator implementation status, and migration paths for advanced queries.

## Architecture Overview

Memris uses a zero-reflection query architecture with build-time planning and hot-path execution:

### Build-Time (Once per Entity Type)

```
MetadataExtractor.extractEntityMetadata(entityClass)
    ↓
    EntityMetadata (field mappings, type codes, converters, MethodHandles)
    ↓
QueryPlanner.parse(method) → LogicalQuery
    ↓
    LogicalQuery (ReturnKind, Condition[], Operator, Arity)
    ↓
QueryCompiler.compile(logicalQuery) → CompiledQuery
    ↓
    CompiledQuery (resolved column indices, compiled conditions)
    ↓
RepositoryRuntime.build(table, metadata, compiledQueries[])
    ↓
    RepositoryRuntime (typed entrypoints, dense arrays)
```

### Hot-Path (Reuse Forever, Zero Reflection)

```
Generated Repository Method
    ↓
    RepositoryRuntime.listN(queryId, args)
        ↓
        CompiledQuery[] lookup by queryId (O(1) array access)
        ↓
        executeQuery(conditions, args)
            ↓
            Execute each condition with pre-resolved column index
            ↓
            intersect(results) (AND logic)
        ↓
        materialize(rows)
            ↓
            getTableValue(columnIndex, row) via typeCode switch
            ↓
            entityConstructor.invoke() and MethodHandle[] setters
```

### Key Design Principles

1. **Constant queryId**: Generated methods embed queryId as bytecode constant
2. **Typed Entrypoints**: `list0(0)`, `list1(1, arg)`, `optional1(1, arg)`, etc.
3. **Dense Arrays**: `String[] columnNames`, `byte[] typeCodes`, `MethodHandle[] setters` (no maps)
4. **TypeCode Dispatch**: Switch on `byte typeCode` for zero-allocation type switching
5. **Column-Indexed Access**: FfmTable API: `getInt(columnId, row)`, `getString(columnId, row)`, etc.

## Detailed Operator Reference

### Operators by Category

**Note**: Implementation status is based on code analysis. Maven build is blocked, so operator behavior cannot be verified through automated testing.

#### Comparison Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| EQ | `findByXxx`<br>`findByXxxIs`<br>`findByXxxEquals` | Parsed by QueryPlanner |
| NE | `findByXxxNot` | Parsed by QueryPlanner |
| GT | `findByXxxGreaterThan` | Parsed by QueryPlanner |
| GTE | `findByXxxGreaterThanEqual` | Parsed by QueryPlanner |
| LT | `findByXxxLessThan` | Parsed by QueryPlanner |
| LTE | `findByXxxLessThanEqual` | Parsed by QueryPlanner |
| BETWEEN | `findByXxxBetween` | Parsed by QueryPlanner |

#### String Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| LIKE | `findByXxxLike` | Parsed by QueryMethodParser (not in QueryPlanner) |
| NOT_LIKE | `findByXxxNotLike` | Parsed by QueryMethodParser (not in QueryPlanner) |
| STARTING_WITH | `findByXxxStartingWith`<br>`findByXxxStartsWith` | Parsed by QueryMethodParser (not in QueryPlanner) |
| ENDING_WITH | `findByXxxEndingWith`<br>`findByXxxEndsWith` | Parsed by QueryMethodParser (not in QueryPlanner) |
| CONTAINING | `findByXxxContaining`<br>`findByXxxContains` | Parsed by QueryMethodParser (not in QueryPlanner) |
| IGNORE_CASE | `findByXxxIgnoreCase`<br>`findByXxxAndYyyAllIgnoreCase` | Parsed by QueryPlanner |

#### Boolean Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| IS_TRUE | `findByXxxTrue`<br>`findByXxxIsTrue` | Parsed by QueryPlanner |
| IS_FALSE | `findByXxxFalse`<br>`findByXxxIsFalse` | Parsed by QueryPlanner |

#### Null Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| IS_NULL | `findByXxxIsNull`<br>`findByXxxNull` | Parsed by QueryPlanner |
| IS_NOT_NULL | `findByXxxIsNotNull`<br>`findByXxxNotNull` | Parsed by QueryPlanner |

#### Collection Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| IN | `findByXxxIn` | Parsed by QueryMethodParser (not in QueryPlanner) |
| NOT_IN | `findByXxxNotIn` | Parsed by QueryMethodParser (not in QueryPlanner) |

#### Date/Time Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| AFTER | `findByXxxAfter` | Parsed by QueryMethodParser (not in QueryPlanner) |
| BEFORE | `findByXxxBefore` | Parsed by QueryMethodParser (not in QueryPlanner) |

#### Logical Operators

| Operator | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| AND | `findByXxxAndYyy` | Parsed by QueryPlanner |
| OR | `findByXxxOrYyy` | Parsed by QueryMethodParser (not in QueryPlanner) |

#### Query Modifiers

| Modifier | JPA Pattern | Implementation Notes |
|----------|--------------|---------------------|
| DISTINCT | `findDistinctByXxx` | Parsed by QueryMethodParser (not in QueryPlanner) |
| ORDER BY | `findByXxxOrderByYxxAsc`<br>`findByXxxOrderByYyyDesc` | Parsed by QueryMethodParser (not in QueryPlanner) |
| LIMIT/TOP/FIRST | `findFirstByXxx`<br>`findTopByXxx`<br>`findTop10ByXxx` | Parsed by QueryMethodParser (not in QueryPlanner) |

#### Return Types

| Return Type | Pattern | Implementation Notes |
|------------|----------|---------------------|
| List\<T\> | `findByXxx` | Handled by ReturnKind.MANY_LIST |
| Optional\<T\> | `findById` | Handled by ReturnKind.ONE_OPTIONAL |
| long | `countByXxx` | Handled by ReturnKind.COUNT_LONG |
| boolean | `existsByXxx` | Handled by ReturnKind.EXISTS_BOOL |

### Operator Status Labels

- **Parsed by QueryPlanner** - Operator is recognized and parsed by the zero-reflection query planner
- **Parsed by QueryMethodParser** - Operator is recognized by the older query method parser
- **Not Parsed** - Operator is not yet recognized by any parser

**Note**: Due to Maven build blocking (see [troubleshooting.md](../troubleshooting.md)), operator execution behavior cannot be verified through automated testing.

## Query Method Parsing

### QueryPlanner.parse() Method

The `QueryPlanner.parse(method, idColumnName)` method analyzes Spring Data JPA query method names and converts them to `LogicalQuery` objects.

**Input Parameters:**
- `Method method` - The repository method to parse
- `String idColumnName` - Name of the ID column (e.g., "id")

**Output:**
- `LogicalQuery` object containing:
  - `String methodName` - Original method name
  - `ReturnKind returnKind` - List, Optional, long, boolean
  - `Condition[] conditions` - Parsed conditions (property, operator, arg index)
  - `OrderBy orderBy` - Order clause (⚠️ TODO - returns null)
  - `int arity` - Number of parameters

### ReturnKind Enumeration

```java
enum ReturnKind {
    MANY_LIST,      // List<T>
    ONE_OPTIONAL,   // Optional<T>
    COUNT_LONG,      // long
    EXISTS_BOOL      // boolean
}
```

### Condition Structure

```java
record Condition {
    String propertyPath;    // e.g., "age"
    Operator operator;      // e.g., GT
    int argumentIndex;      // e.g., 0
    boolean ignoreCase;     // case-insensitive flag
}
```

### Operator Enumeration

**Implemented Operators:**
```java
enum Operator {
    EQ, NE, GT, GTE, LTE, LT,      // Comparison
    BETWEEN,                           // Range
    IGNORE_CASE_EQ                     // String matching
}
```

**Planned Operators (TODO):**
```java
// Not yet implemented - placeholder design
enum Operator {
    IN, NOT_IN,                         // Collection
    LIKE, NOT_LIKE,                     // Pattern matching
    STARTING_WITH, ENDING_WITH,          // String matching
    CONTAINING,                         // String matching
    IS_TRUE, IS_FALSE,                  // Boolean
    IS_NULL, IS_NOT_NULL,               // Null checks
    AFTER, BEFORE,                       // Date/time
    OR                                  // Logical
}
```

## Query Execution

### RepositoryRuntime Entrypoints

The `RepositoryRuntime` class provides typed entrypoints for zero-allocation query execution:

| Entrypoint | Signature | Use Case |
|-------------|-----------|-----------|
| `list0(queryId)` | `List<T> list0(int queryId)` | `findAll()`, `count()` |
| `list1(queryId, arg0)` | `List<T> list1(int queryId, Object arg0)` | `findById`, `findByName`, `countByXxx` |
| `list2(queryId, arg0, arg1)` | `List<T> list2(int queryId, Object arg0, Object arg1)` | `findByXxxAndYyy`, `countByXxxAndYyy` |
| `optional1(queryId, arg0)` | `Optional<T> optional1(int queryId, Object arg0)` | `findById` |
| `exists1(queryId, arg0)` | `boolean exists1(int queryId, Object arg0)` | `existsById`, `existsByXxx` |
| `count0(queryId)` | `long count0(int queryId)` | `count()` |
| `count1(queryId, arg0)` | `long count1(int queryId, Object arg0)` | `countByXxx` |
| `count2(queryId, arg0, arg1)` | `long count2(int queryId, Object arg0, Object arg1)` | `countByXxxAndYyy` |

### Query Execution Flow

```java
// 1. Generated repository method calls runtime with constant queryId
public List<User> findByAgeGreaterThan(int age) {
    return rt.list1(queryId=5, age);  // queryId is compile-time constant!
}

// 2. Runtime selects CompiledQuery by array lookup
CompiledQuery cq = compiledQueries[queryId];  // O(1) array access

// 3. Execute query with pre-resolved conditions
int[] rows = executeQuery(cq, new Object[]{age});

// 4. For each condition, execute with column index
for (CompiledCondition condition : cq.conditions()) {
    int[] conditionRows = executeCondition(condition, args);
    rows = intersect(rows, conditionRows);  // AND logic
}

// 5. Materialize entities using dense arrays
return materialize(rows);
```

### TypeCode Dispatch

Materialization uses typeCode-based switch for zero-allocation type switching:

```java
private Object getTableValue(int columnIndex, int row) {
    byte typeCode = typeCodes[columnIndex];

    return switch (typeCode) {
        case TYPE_INT -> table.getInt(columnIndex, row);
        case TYPE_LONG -> table.getLong(columnIndex, row);
        case TYPE_STRING -> table.getString(columnIndex, row);
        case TYPE_BOOLEAN -> table.getBoolean(columnIndex, row);
        // ... all primitive types
        default -> throw new IllegalArgumentException("Unknown typeCode");
    };
}
```

### Dense Array Metadata

RepositoryRuntime stores all metadata in dense arrays for O(1) access:

```java
private final String[] columnNames;           // e.g., ["id", "name", "age"]
private final byte[] typeCodes;               // e.g., [TYPE_LONG, TYPE_STRING, TYPE_INT]
private final MethodHandle[] setters;         // Pre-compiled setters
private final TypeConverter<?, ?>[] converters; // Nullable converters
```

**Contrast with Map-based approach:**

```java
// OLD: Map-based (slow - hash lookup)
TypeConverter converter = converters.get(fieldName);  // O(1) but with hash overhead

// NEW: Array-based (fast - direct access)
TypeConverter converter = converters[columnIndex];  // O(1) direct access
```

## Alternative Approaches for Advanced Queries

The following patterns show alternative approaches when using advanced operators that may not be fully parsed or executed.

### Collection Membership (IN Pattern)

**Goal:** Find users with status in ["gold", "platinum"]

**Alternative:** Multiple queries + manual union
```java
List<User> gold = repo.findByStatus("gold");
List<User> platinum = repo.findByStatus("platinum");
List<User> vips = new ArrayList<>(gold);
vips.addAll(platinum);
```

### Pattern Matching (LIKE Pattern)

**Goal:** Find users with name containing "oh"

**Alternative:** Retrieve all and filter
```java
List<User> allUsers = repo.findAll();
List<User> matches = allUsers.stream()
    .filter(u -> u.getName().contains("oh"))
    .toList();
```

### OR Conditions

**Goal:** Find users with age < 25 OR active = true

**Alternative:** Two queries + union
```java
List<User> young = repo.findByAgeLessThan(25);
List<User> active = repo.findByActiveTrue();
Set<User> result = new LinkedHashSet<>(young);
result.addAll(active);
List<User> youngOrActive = new ArrayList<>(result);
```

### Sorting (ORDER BY Pattern)

**Goal:** Find adults sorted by name descending

**Alternative:** Retrieve all + sort in memory
```java
List<User> adults = repo.findByAgeGreaterThan(18);
adults.sort((a, b) -> b.getLastname().compareTo(a.getLastname()));
```

### Limiting (TOP/FIRST Pattern)

**Goal:** Find top 10 active users

**Alternative:** Retrieve all + sublist
```java
List<User> active = repo.findByActiveTrue();
List<User> top10 = active.size() > 10 ? active.subList(0, 10) : active;
```

### Distinct

**Goal:** Find distinct email addresses

**Alternative:** Stream distinct
```java
List<User> all = repo.findAll();
List<User> unique = all.stream()
    .distinct()
    .toList();
```

### LIKE Operator Workaround

**Goal:** Find users with name containing "oh"

```java
// ⚠️ TODO: Not yet implemented
// List<User> matches = repo.findByNameContaining("oh");

// WORKAROUND: Retrieve all and filter
List<User> allUsers = repo.findAll();
List<User> matches = allUsers.stream()
    .filter(u -> u.getName().contains("oh"))
    .toList();
```

### OR Conditions Workaround

**Goal:** Find users with age < 25 OR active = true

```java
// ⚠️ TODO: Not yet implemented
// List<User> youngOrActive = repo.findByAgeLessThanOrActiveTrue(25, true);

// WORKAROUND: Two queries + union
List<User> young = repo.findByAgeLessThan(25);
List<User> active = repo.findByActiveTrue();
Set<User> result = new LinkedHashSet<>(young);
result.addAll(active);
List<User> youngOrActive = new ArrayList<>(result);
```

### ORDER BY Workaround

**Goal:** Find adults sorted by name descending

```java
// ⚠️ TODO: Not yet implemented
// List<User> sorted = repo.findByAgeGreaterThanOrderByLastnameDesc(18);

// WORKAROUND: Retrieve all + sort in memory
List<User> adults = repo.findByAgeGreaterThan(18);
adults.sort((a, b) -> b.getLastname().compareTo(a.getLastname()));
```

### LIMIT/TOP/FIRST Workaround

**Goal:** Find top 10 active users

```java
// ⚠️ TODO: Not yet implemented
// List<User> top10 = repo.findTop10ByActiveTrue();

// WORKAROUND: Retrieve all + sublist
List<User> active = repo.findByActiveTrue();
List<User> top10 = active.size() > 10 ? active.subList(0, 10) : active;
```

### DISTINCT Workaround

**Goal:** Find distinct email addresses

```java
// ⚠️ TODO: Not yet implemented
// List<User> unique = repo.findDistinctByEmail();

// WORKAROUND: Retrieve all + stream distinct
List<User> all = repo.findAll();
List<User> unique = all.stream()
    .distinct()
    .toList();
```

## Test Coverage

### QueryPlanner Tests

**QueryPlannerTest.java** - Tests QueryPlanner parsing (8 tests)
- findById, existsById, findAll
- findByName, findByAgeGreaterThan
- findByNameAndAge, count, countByAge

### QueryMethodParser Tests

**QueryMethodParserTest.java** - Comprehensive JPA specification tests (640 lines)
- Boolean operators (IsTrue, IsFalse)
- Null operators (IsNull, IsNotNull)
- IN/NOT_IN operators
- Date/Time operators (After, Before)
- LIKE operators (Like, NotLike)
- String matching (StartingWith, EndingWith, Containing)
- Is/Equals operators
- IgnoreCase modifier
- OrderBy clause
- Distinct modifier
- Top/First limiting
- Count/Exists methods
- Not operator
- Delete/Remove methods

**Note:** `QueryMethodParserTest` tests for older QueryMethodParser implementation, not the new QueryPlanner. Many operators tested are not in QueryPlanner operator set.

### Integration Tests

**ECommerceRealWorldTest.java** - Real-world e-commerce domain tests (15 tests)
- Indexed queries (SKU, barcode, email)
- Range queries (price, stock)
- Name pattern matching
- Enum queries (status, payment type)
- Combined conditions
- Lifecycle callbacks (@PostLoad)

**Test Execution Status**: Tests cannot run due to Maven build blocker (see [troubleshooting.md](../troubleshooting.md#maven-build-suppression-issue)).

## Performance Characteristics

### O(1) Operations

| Operation | Complexity | Implementation |
|-----------|------------|----------------|
| `listN(queryId, ...)` | O(1) | Array lookup of CompiledQuery |
| `getTableValue(colIdx, row)` | O(1) | TypeCode switch + direct table access |
| `materializeOne(row)` | O(n) | n = field count, all array access |
| `intersect(a, b)` | O(n) | n = size of larger array, uses HashSet |

### O(log n) Operations

| Operation | Complexity | Implementation |
|-----------|------------|----------------|
| Index lookup | O(log n) | ConcurrentSkipListMap (not yet used) |

### O(n) Operations

| Operation | Complexity | Notes |
|-----------|------------|---------|
| Table scan | O(n) | n = total rows, optimized by SIMD when implemented |
| `findAll()` | O(n) | Returns all rows |
| `materialize(rows)` | O(n * m) | n = row count, m = field count |

## Known Considerations

### Query System Architecture

The codebase contains two parallel query parsing systems:

1. **Zero-Reflection Runtime** (new architecture): QueryPlanner + QueryCompiler + RepositoryRuntime
   - Parses: EQ, NE, GT, LT, GTE, LTE, BETWEEN, IGNORE_CASE
   - Does NOT parse: IN, LIKE, STARTING_WITH, ENDING_WITH, CONTAINING, OR, ORDER_BY, DISTINCT

2. **QueryMethodParser** (older implementation): Full JPA specification coverage
   - Parses all 24+ JPA operators including IN, LIKE, ORDER BY, etc.
   - Used by MemrisRepositoryFactory main path

### Maven Build Blocker

**Status:** ❌ BLOCKED

**Impact:** Cannot run automated tests, cannot verify which operators actually work in practice

**For detailed workarounds:** See [troubleshooting.md](../troubleshooting.md)

## See Also

- [queries.md](queries.md) - Quick reference guide
- [troubleshooting.md](troubleshooting.md) - Common issues and workarounds
- [CLAUDE.md](../CLAUDE.md) - Architecture details
- [AGENTS.md](../AGENTS.md) - Development guidelines
- [SPRING_DATA_ROADMAP.md](archive/SPRING_DATA_ROADMAP.md) - Feature roadmap (archived)
