# Query Method Quick Reference

This document provides a quick reference for Memris query methods, including currently supported operators and planned features.

## Query Operators

The following operators are designed to be supported by Memris's Spring Data JPA query method parser:

| Operator | Pattern | Example |
|-----------|----------|----------|
| EQ | `findByXxx`<br>`findByXxxIs`<br>`findByXxxEquals` | `findByEmail("a@b.com")` |
| NE | `findByXxxNot` | `findByEmailNot("a@b.com")` |
| GT | `findByXxxGreaterThan` | `findByAgeGreaterThan(18)` |
| LT | `findByXxxLessThan` | `findByAgeLessThan(65)` |
| GTE | `findByXxxGreaterThanEqual` | `findByAgeGreaterThanEqual(18)` |
| LTE | `findByXxxLessThanEqual` | `findByAgeLessThanEqual(65)` |
| BETWEEN | `findByXxxBetween` | `findByAgeBetween(18, 65)` |
| IGNORE_CASE | `findByXxxIgnoreCase`<br>`findByXxxAndYyyAllIgnoreCase` | `findByNameIgnoreCase("john")` |
| IS_TRUE | `findByXxxTrue`<br>`findByXxxIsTrue` | `findByActiveTrue()` |
| IS_FALSE | `findByXxxFalse`<br>`findByXxxIsFalse` | `findByActiveFalse()` |
| IS_NULL | `findByXxxIsNull`<br>`findByXxxNull` | `findByEmailIsNull()` |
| IS_NOT_NULL | `findByXxxIsNotNull`<br>`findByXxxNotNull` | `findByEmailIsNotNull()` |
| IN | `findByXxxIn`<br>`findByXxxNotIn` | `findByStatusIn(List.of("A","B"))` |
| LIKE | `findByXxxLike`<br>`findByXxxNotLike` | `findByNameLike("%john%")` |
| STARTING_WITH | `findByXxxStartingWith`<br>`findByXxxStartsWith` | `findByNameStartingWith("John")` |
| ENDING_WITH | `findByXxxEndingWith`<br>`findByXxxEndsWith` | `findByNameEndingWith("son")` |
| CONTAINING | `findByXxxContaining`<br>`findByXxxContains` | `findByNameContaining("oh")` |
| OR conditions | `findByXxxOrYyy` | `findByEmailOrPhone("a@b.com", "555-1234")` |
| ORDER BY | `findByXxxOrderByYxxAsc`<br>`findByXxxOrderByYxxDesc` | `findByAgeOrderByLastnameDesc(25)` |
| DISTINCT | `findDistinctByXxx` | `findDistinctByEmail("a@b.com")` |
| TOP/FIRST | `findFirstByXxx`<br>`findTopByXxx`<br>`findTop10ByXxx` | `findFirstByOrderByAgeDesc()` |
| AFTER | `findByXxxAfter` | `findByCreatedAtAfter(lastWeek)` |
| BEFORE | `findByXxxBefore` | `findByCreatedAtBefore(lastWeek)` |

**Note**: Operator implementation status varies. Check [queries-spec.md](queries-spec.md) for detailed information about which operators are parsed, compiled, and executed.

## Code Examples

### Basic Query Examples

```java
// Equality queries
List<User> user = repo.findByEmail("john@example.com");
List<User> byName = repo.findByName("John Doe");

// Comparison operators
List<User> adults = repo.findByAgeGreaterThan(18);
List<User> seniors = repo.findByAgeGreaterThanEqual(65);
List<User> children = repo.findByAgeLessThan(18);

// Range queries
List<User> adults = repo.findByAgeBetween(18, 65);

// Case-insensitive search
List<User> john = repo.findByNameIgnoreCase("john");

// Boolean fields
List<User> activeUsers = repo.findByActiveTrue();
List<User> inactiveUsers = repo.findByActiveFalse();
boolean hasActive = repo.existsByActiveTrue();

// Null checks
List<User> noEmail = repo.findByEmailIsNull();
List<User> hasEmail = repo.findByEmailIsNotNull();

// Count queries
long total = repo.count();
long adultCount = repo.countByAgeGreaterThan(18);

// Exists queries
boolean emailExists = repo.existsByEmail("john@example.com");
boolean idExists = repo.existsById(123);
```

### Advanced Query Patterns

```java
// Collection membership (IN)
List<User> vips = repo.findByStatusIn(List.of("gold", "platinum"));
List<User> notDeleted = repo.findByStatusNotIn(List.of("DELETED", "CANCELLED"));

// Pattern matching (LIKE)
List<User> johns = repo.findByNameLike("%john%");
List<User> notSpam = repo.findByEmailNotLike("%spam.com");

// String matching
List<User> startsWithJohn = repo.findByNameStartingWith("John");
List<User> endsWithSon = repo.findByNameEndingWith("son");
List<User> containsOh = repo.findByNameContaining("oh");

// OR conditions
List<User> byEmailOrPhone = repo.findByEmailOrPhone("john@example.com", "555-1234");
List<User> youngOrActive = repo.findByAgeLessThanOrActiveTrue(25, true);

// Sorting
List<User> sorted = repo.findByAgeOrderByLastnameDesc(25);
List<User> recentFirst = repo.findByCreatedAtOrderByCreatedAtDesc();

// Distinct
List<User> uniqueEmails = repo.findDistinctByEmail("john@example.com");

// Limiting
User oldest = repo.findFirstByOrderByAgeDesc();
List<User> top10 = repo.findTop10ByStatus("ACTIVE");
User youngest = repo.findTopByOrderByAgeAsc();

// Date/time queries
List<User> recent = repo.findByCreatedAtAfter(lastWeek);
List<User> old = repo.findByCreatedAtBefore(lastYear);
```

## Query Method Naming Pattern

```
[returnType][distinct?][first/top?n?][findBy][conditions][orderBy?][ignoreCase?]

Where:
- returnType: find, query, get, read, stream, count, exists, delete, remove
- distinct: "Distinct" modifier (⚠️ TODO)
- first/top: "First" or "Top{n}" limiting (⚠️ TODO)
- findBy: "By" prefix required for query methods
- conditions: Property names with operator keywords (And, Or)
- orderBy: "OrderBy{Property}{Asc|Desc}" (⚠️ TODO)
- ignoreCase: "IgnoreCase" or "AllIgnoreCase" modifier
```

### Multiple Conditions (AND Only)

Currently only AND conditions are supported. OR conditions are ⚠️ TODO.

```java
// Supported: AND conditions
List<User> adults = repo.findByAgeAndStatus(25, "ACTIVE");
List<User> named = repo.findByFirstnameAndLastname("John", "Doe");

// ⚠️ NOT SUPPORTED YET: OR conditions
// List<User> youngOrActive = repo.findByAgeLessThanOrActiveTrue(25, true);
```

## Common Query Patterns

### Single Property Queries

```java
// Equality
List<User> users = repo.findByEmail("john@example.com");

// Comparison
List<User> adults = repo.findByAgeGreaterThan(18);
List<User> recent = repo.findByCreatedAtAfter(date);

// Case-insensitive
List<User> john = repo.findByNameIgnoreCase("JOHN");
```

### Multiple Property Queries (AND)

```java
// Two conditions
List<User> adultVips = repo.findByAgeAndStatus(25, "VIP");

// Three conditions
List<User> specific = repo.findByFirstnameAndLastnameAndAge("John", "Doe", 30);
```

### Count and Exists Queries

```java
// Count matching entities
long count = repo.count();
long adultCount = repo.countByAgeGreaterThan(18);

// Check existence
boolean exists = repo.existsByEmail("john@example.com");
boolean idExists = repo.existsById(123);
```

## Operator Implementation Status

Implementation status varies across operators. Some operators are parsed and compiled in the zero-reflection architecture (QueryPlanner + QueryCompiler), while others use the older QueryMethodParser implementation.

**For detailed implementation status**: See [queries-spec.md](queries-spec.md)

**Common scenarios**:
- **Equality queries** (`findByXxx`) use index lookups when available
- **Range queries** (`findByXxxGreaterThan`) use table scans or range indexes
- **Boolean queries** (`findByActiveTrue`) scan boolean columns
- **Null checks** (`findByXxxIsNull`) filter for null values

**Maven build status**: The build environment is currently blocked (see [troubleshooting.md](troubleshooting.md#maven-build-suppression-issue)), so operator implementation cannot be verified through automated testing.

## See Also

- [queries-spec.md](queries-spec.md) - Full query method specification
- [troubleshooting.md](troubleshooting.md) - Common issues and workarounds
- [CLAUDE.md](../CLAUDE.md) - Architecture details
- [AGENTS.md](../AGENTS.md) - Development guidelines
