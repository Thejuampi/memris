# Query Reference

This document provides a complete query operator reference for Memris.

## Quick Reference: Query Operators

**Status Legend:**
- ✅ Implemented - Full support via QueryPlanner (zero-reflection runtime)
- ⚠️ Partial - Tokenized but execution not complete
- ❌ Not Implemented

### Comparison Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| EQ | `findByXxx`<br>`findByXxxIs`<br>`findByXxxEquals` | ✅ |
| NE | `findByXxxNot` | ✅ |
| GT | `findByXxxGreaterThan` | ✅ |
| GTE | `findByXxxGreaterThanEqual` | ✅ |
| LT | `findByXxxLessThan` | ✅ |
| LTE | `findByXxxLessThanEqual` | ✅ |
| BETWEEN | `findByXxxBetween` | ✅ |

### String Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| LIKE | `findByXxxLike` | ✅ |
| NOT_LIKE | `findByXxxNotLike` | ✅ |
| STARTING_WITH | `findByXxxStartingWith`<br>`findByXxxStartsWith` | ✅ |
| ENDING_WITH | `findByXxxEndingWith`<br>`findByXxxEndsWith` | ✅ |
| CONTAINING | `findByXxxContaining`<br>`findByXxxContains` | ✅ |
| IGNORE_CASE | `findByXxxIgnoreCase`<br>`findByXxxAndYyyAllIgnoreCase` | ✅ |

### Boolean Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| IS_TRUE | `findByXxxTrue`<br>`findByXxxIsTrue` | ✅ |
| IS_FALSE | `findByXxxFalse`<br>`findByXxxIsFalse` | ✅ |

### Null Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| IS_NULL | `findByXxxIsNull`<br>`findByXxxNull` | ✅ |
| IS_NOT_NULL | `findByXxxIsNotNull`<br>`findByXxxNotNull` | ✅ |

### Collection Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| IN | `findByXxxIn` | ✅ |
| NOT_IN | `findByXxxNotIn` | ✅ |

### Date/Time Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| AFTER | `findByXxxAfter` | ✅ |
| BEFORE | `findByXxxBefore` | ✅ |

### Logical Operators
| Operator | JPA Pattern | Status |
|----------|-------------|--------|
| AND | `findByXxxAndYyy` | ✅ |
| OR | `findByXxxOrYyy` | ✅ |

### Query Modifiers
| Modifier | JPA Pattern | Status |
|----------|-------------|--------|
| DISTINCT | `findDistinctByXxx` | ⚠️ |
| ORDER BY | `findByXxxOrderByYxxAsc`<br>`findByXxxOrderByYyyDesc` | ✅ |
| LIMIT/TOP/FIRST | `findFirstByXxx`<br>`findTopByXxx`<br>`findTop10ByXxx` | ✅ |

### Return Types
| Return Type | Pattern | Status |
|------------|----------|--------|
| List\<T\> | `findByXxx` | ✅ |
| Optional\<T\> | `findById` | ✅ |
| Set\<T\> | `findByXxx` | ✅ |
| long | `countByXxx` | ✅ |
| boolean | `existsByXxx` | ✅ |

## @Query (JPQL-like)

Memris supports a JPQL-like subset for annotated queries:

```java
@Query("select p from Product p where p.sku = :sku")
Optional<Product> findBySku(@Param("sku") String sku);

@Query("select p from Product p where p.name ilike :name and p.price between :min and :max")
List<Product> findByNameAndPrice(@Param("name") String name, @Param("min") long min, @Param("max") long max);
```

**Supported clauses:**
- `SELECT` / `FROM` / `WHERE` / `GROUP BY` / `HAVING` / `ORDER BY`
- `JOIN` / `LEFT JOIN` (aliases supported)

**Supported predicates:**
- Comparisons: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`
- String matching: `LIKE`, `ILIKE` (ILIKE = case-insensitive LIKE)
- Sets: `IN` / `NOT IN`
- Ranges: `BETWEEN`
- Null checks: `IS NULL` / `IS NOT NULL`
- Boolean literals: `true` / `false`
- Parentheses + `AND` / `OR` (AND binds tighter)

**Parameter binding:**
- Named: `:name` with `@Param("name")`
- Positional: `?1`, `?2`
- If compiled with `-parameters`, unannotated names are also matched

**Return types:**
- `List<T>`, `Optional<T>`, `Set<T>`, `boolean`, `long`, `Map<K, V>`
- `select count(x)` must return `long` or `Map<K, Long>` when grouped

**Literal handling:**
- Integer literals are stored as `Long`
- Decimal literals are stored as `BigDecimal`

**Limitations (current):**
- No `DISTINCT`
- No projections (`select new ...`), no aggregates beyond `count`
- `GROUP BY` supports root-entity selects or `count`, with record-key Map return types
- `HAVING` supports `count(...)` comparisons only
- No subqueries
- No `UPDATE` / `DELETE` queries
- Single-column `ORDER BY` only

## Field Type Support

**Primitive + String types:**
- `int`, `long`, `boolean`, `byte`, `short`, `float`, `double`, `char`, `String`

**Common Java types:**
- `Instant`, `LocalDate`, `LocalDateTime`, `java.util.Date`
- `BigDecimal`, `BigInteger`

**Operator notes:**
- Time types support EQ/NE/GT/GTE/LT/LTE/BETWEEN/IN/NOT_IN
- BigDecimal/BigInteger support EQ/NE/IN/NOT_IN only

## Query Method Naming Quick Reference

Query methods follow Spring Data JPA naming conventions:

```
[find/query/get/stream][Distinct][By][First/Top][Result][OrderBy]*
[count/exists][Distinct][By]*
[delete/remove][Distinct][By]*
```

**Examples:**
```java
List<User> findByEmail(String email);
List<User> findByAgeGreaterThan(int age);
List<User> findByNameContainingIgnoreCase(String name);
long countByActiveTrue();
boolean existsByEmail(String email);
```

### Method Naming Patterns

**Prefix:** `find`, `query`, `get`, `stream`, `count`, `exists`, `delete`, `remove`

**Conditions:**
- `findByXxx` - Equality
- `findByXxxAndYyy` - Multiple conditions (AND)
- `findByXxxGreaterThan` - Comparison
- `findByXxxBetween` - Range

**Return Types:**
- `List<T>` - Multiple results
- `Optional<T>` - Single result
- `Set<T>` - Unique results
- `long` - Count
- `boolean` - Existence check

## Alternative Query Patterns

The following patterns show alternative approaches when using advanced operators that may not be fully parsed or executed.

### Collection Membership (IN Pattern)

**Goal:** Find users with status in ["gold", "platinum"]

**Workaround:** Multiple queries + manual union
```java
List<User> gold = repo.findByStatus("gold");
List<User> platinum = repo.findByStatus("platinum");
List<User> vips = new ArrayList<>(gold);
vips.addAll(platinum);
```

### Pattern Matching (LIKE Pattern)

**Goal:** Find users with name containing "oh"

**Workaround:** Retrieve all and filter
```java
List<User> allUsers = repo.findAll();
List<User> matches = allUsers.stream()
    .filter(u -> u.getName().contains("oh"))
    .toList();
```

### OR Conditions

**Goal:** Find users with age < 25 OR active = true

**Workaround:** Two queries + union
```java
List<User> young = repo.findByAgeLessThan(25);
List<User> active = repo.findByActiveTrue();
Set<User> result = new LinkedHashSet<>(young);
result.addAll(active);
List<User> youngOrActive = new ArrayList<>(result);
```

### Sorting (ORDER BY Pattern)

Sorting is supported via `OrderBy` in method names.

### Limiting (TOP/FIRST Pattern)

Limiting is supported via `Top`/`First` prefixes (with optional numeric limit).

### Distinct

**Goal:** Find distinct email addresses

**Workaround:** Stream distinct
```java
List<User> all = repo.findAll();
List<User> unique = all.stream()
    .distinct()
    .toList();
```

## Query Parsing Flow

Query methods are processed through a pipeline of components:

```
Repository Method Name
    │
    ▼
QueryMethodLexer.tokenize()
    │
    ├─── Prefix extraction (find/query/get/count/exists/delete)
    ├─── Built-in detection (findAll, count, deleteAll)
    ├─── Property path resolution
    ├─── Operator parsing (GreaterThan, Between, Like, etc.)
    ├─── Combinator handling (And, Or)
    └─── OrderBy parsing
    │
    ▼
List<QueryMethodToken>
    │
    ▼
QueryPlanner.plan()
    │
    ├─── Check BuiltInResolver for built-ins (findById, save, etc.)
    ├─── Build LogicalQuery structure
    ├─── Resolve property paths to column indices
    └─── Validate against entity metadata
    │
    ▼
CompiledQuery
    │
    ▼
HeapRuntimeKernel.execute()
    │
    ▼
GeneratedTable.scan*()
```

### Key Components

**QueryMethodLexer** (`io.memris.query.QueryMethodLexer`)
- Tokenizes method names into structured tokens
- Handles 20+ operators and combinators
- Supports nested properties (account.email)
- Built-in detection for parameterless methods (findAll, count, deleteAll)

**QueryPlanner** (`io.memris.query.QueryPlanner`)
- Converts tokens into executable LogicalQuery
- Validates property existence against entity class
- Resolves operators to Predicate types

**BuiltInResolver** (`io.memris.query.BuiltInResolver`)
- Signature-based built-in method resolution
- Matches methods like `findById(ID)`, `save(T)`, `delete(T)`
- Deterministic tie-breaking using inheritance distance
- Handles wildcard matching for entity types

**Key Files:**
- `QueryMethodLexer.java:213` - Main tokenization entry point
- `QueryPlanner.java:1` - Query planning logic
- `BuiltInResolver.java:57` - Built-in resolution logic
- `CompiledQuery.java` - Compiled query structure

## Architecture

For detailed information about query execution architecture, zero-reflection runtime, and performance characteristics, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

*For development guidelines, see [../CLAUDE.md](../CLAUDE.md)*
