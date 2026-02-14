# JPA Query Method Parser Design

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Overview
This document captures the design decisions made while implementing JPA Query Method parsing for Memris, ensuring compliance with Spring Data JPA specifications.

## Query Parsing Methods

Memris supports two query definition methods:

### 1. Method Name Derivation (Spring Data standard)

Method names derive queries without explicit JPQL:

```java
// These ARE JPA standard - method names derive queries
List<User> findByLastname(String lastname);                    // EQ
List<User> findByAgeGreaterThan(int age);                      // GT
List<User> findByStatusIn(Collection<String> statuses);        // IN
List<User> findByNameContaining(String name);                  // CONTAINING
List<User> findByActiveTrue();                                 // IS_TRUE
List<User> findByLastnameNot(String lastname);                 // NEQ
List<User> findByAgeOrderByLastnameDesc(int age);              // with ordering
List<User> findFirst10ByActiveTrue();                          // with limit
```

**Parser**: `QueryMethodLexer.java` + `QueryPlanner.java`

### 2. JPQL Queries (@Query annotation)

Full JPQL support via `@Query` annotation:

```java
@Query("SELECT u FROM User u WHERE u.status = :status")
List<User> findActiveUsers(@Param("status") String status);

@Query("SELECT u FROM User u WHERE u.age BETWEEN :min AND :max")
List<User> findByAgeRange(@Param("min") int min, @Param("max") int max);

@Query("SELECT COUNT(u) FROM User u WHERE u.active = true")
long countActiveUsers();

@Query("UPDATE User u SET u.lastLogin = :date WHERE u.id = :id")
@Modifying
int updateLastLogin(@Param("id") Long id, @Param("date") LocalDateTime date);

@Query("DELETE FROM User u WHERE u.status = :status")
@Modifying
int deleteByStatus(@Param("status") String status);
```

**Parser**: `JpqlQueryParser.java`

## JPQL Statement Types

### SELECT Queries

```java
// Basic select
@Query("SELECT u FROM User u WHERE u.email = :email")
Optional<User> findByEmail(@Param("email") String email);

// Distinct
@Query("SELECT DISTINCT u FROM User u WHERE u.active = true")
List<User> findDistinctActive();

// Count
@Query("SELECT COUNT(u) FROM User u WHERE u.status = :status")
long countByStatus(@Param("status") String status);

// Projections (record-based)
record UserName(String firstname, String lastname) {}
@Query("SELECT u.firstname AS firstname, u.lastname AS lastname FROM User u")
List<UserName> findAllNames();
```

### UPDATE Queries

```java
@Modifying
@Query("UPDATE User u SET u.status = :status WHERE u.id = :id")
int updateStatus(@Param("id") Long id, @Param("status") String status);
```

**Requirements:**
- Must have `@Modifying` annotation
- Return type must be `void`, `int`, or `long`
- Cannot modify ID column

### DELETE Queries

```java
@Modifying
@Query("DELETE FROM User u WHERE u.lastLogin < :cutoff")
int deleteInactive(@Param("cutoff") LocalDateTime cutoff);
```

## JPQL Expression Handling

### WHERE Clause

Supports full boolean expression trees:

```java
// AND/OR combinations
@Query("SELECT u FROM User u WHERE u.active = true AND (u.age > 18 OR u.parent IS NOT NULL)")
List<User> findEligibleUsers();

// NOT operator
@Query("SELECT u FROM User u WHERE NOT u.status = 'BLOCKED'")
List<User> findNonBlockedUsers();

// Parenthesized expressions
@Query("SELECT u FROM User u WHERE (u.a = 1 AND u.b = 2) OR (u.a = 2 AND u.b = 1)")
List<User> findByConditions();
```

### DNF Normalization

WHERE clauses are normalized to Disjunctive Normal Form:

```
(A AND B) OR (C AND D) -> [[A, B], [C, D]]
```

This enables efficient query execution via:
1. Evaluate each AND-group independently
2. Union results across OR-groups

### Comparison Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `=` | `u.status = :status` | Equality |
| `<>` | `u.status <> 'ACTIVE'` | Inequality |
| `>` | `u.age > 18` | Greater than |
| `>=` | `u.age >= 18` | Greater than or equal |
| `<` | `u.age < 65` | Less than |
| `<=` | `u.age <= 65` | Less than or equal |
| `LIKE` | `u.name LIKE :pattern` | Pattern match |
| `NOT LIKE` | `u.name NOT LIKE :pattern` | Negated pattern |
| `ILIKE` | `u.name ILIKE :pattern` | Case-insensitive LIKE |

### Special Predicates

```java
// IS NULL / IS NOT NULL
@Query("SELECT u FROM User u WHERE u.deletedAt IS NULL")
List<User> findActive();

@Query("SELECT u FROM User u WHERE u.email IS NOT NULL")
List<User> findWithEmail();

// BETWEEN
@Query("SELECT u FROM User u WHERE u.age BETWEEN :min AND :max")
List<User> findByAgeRange(@Param("min") int min, @Param("max") int max);

// IN
@Query("SELECT u FROM User u WHERE u.status IN :statuses")
List<User> findByStatuses(@Param("statuses") Collection<String> statuses);

// NOT IN
@Query("SELECT u FROM User u WHERE u.status NOT IN :excluded")
List<User> findNotInStatuses(@Param("excluded") Collection<String> excluded);
```

## Parameter Binding

### Named Parameters

```java
@Query("SELECT u FROM User u WHERE u.email = :email AND u.status = :status")
Optional<User> findByEmailAndStatus(
    @Param("email") String email,
    @Param("status") String status
);
```

### Positional Parameters

```java
@Query("SELECT u FROM User u WHERE u.email = ?1 AND u.status = ?2")
Optional<User> findByEmailAndStatus(String email, String status);
```

### Implicit Parameter Names

If `@Param` is omitted and parameter names are available (compile with `-parameters`):

```java
@Query("SELECT u FROM User u WHERE u.email = :email")
Optional<User> findByEmail(String email);  // 'email' bound automatically
```

## JOIN Support

```java
// Inner join
@Query("SELECT u FROM User u JOIN u.orders o WHERE o.total > 1000")
List<User> findBigSpenders();

// Left join
@Query("SELECT u FROM User u LEFT JOIN u.profile p WHERE p.bio IS NULL")
List<User> findWithoutBio();
```

## GROUP BY and HAVING

```java
@Query("SELECT u FROM User u WHERE u.status = :status GROUP BY u.department")
Map<String, List<User>> groupByDepartment(@Param("status") String status);

@Query("SELECT COUNT(u) FROM User u GROUP BY u.department HAVING COUNT(u) > :minCount")
Map<String, Long> countByDepartment(@Param("minCount") long minCount);
```

## ORDER BY

```java
@Query("SELECT u FROM User u WHERE u.active = true ORDER BY u.lastname ASC, u.firstname ASC")
List<User> findActiveOrdered();

@Query("SELECT u FROM User u ORDER BY u.createdAt DESC")
List<User> findRecentFirst();
```

## Projection Queries

Record-based projections:

```java
record UserSummary(Long id, String email, String status) {}

@Query("SELECT u.id AS id, u.email AS email, u.status AS status FROM User u")
List<UserSummary> findAllSummaries();

@Query("SELECT u.id AS id, u.email AS email FROM User u WHERE u.id = :id")
Optional<UserSummary> findSummaryById(@Param("id") Long id);
```

**Requirements:**
- Each select item must have an alias
- Alias must match record component name
- Types must be compatible

## Method Name Operators (Spring Data standard)

### Operator Keywords (from Spring Data JPA)

| Keyword | Example | Operator | Status |
|---------|---------|----------|--------|
| `Distinct` | `findDistinctByLastnameAndFirstname` | DISTINCT | Implemented |
| `And` | `findByLastnameAndFirstname` | AND | Implemented |
| `Or` | `findByLastnameOrFirstname` | OR | Implemented |
| `Is`, `Equals` | `findByFirstname`, `findByFirstnameIs`, `findByFirstnameEquals` | EQ | Implemented |
| `Between` | `findByAgeBetween` | BETWEEN | Implemented |
| `LessThan` | `findByAgeLessThan` | LT | Implemented |
| `LessThanEqual` | `findByAgeLessThanEqual` | LTE | Implemented |
| `GreaterThan` | `findByAgeGreaterThan` | GT | Implemented |
| `GreaterThanEqual` | `findByAgeGreaterThanEqual` | GTE | Implemented |
| `After` | `findByCreatedAtAfter` | AFTER | Implemented |
| `Before` | `findByCreatedAtBefore` | BEFORE | Implemented |
| `IsNull` | `findByEmailIsNull` | IS_NULL | Implemented |
| `IsNotNull`, `NotNull` | `findByEmailIsNotNull`, `findByEmailNotNull` | IS_NOT_NULL | Implemented |
| `Like` | `findByNameLike` | LIKE | Implemented |
| `NotLike` | `findByNameNotLike` | NOT_LIKE | Implemented |
| `StartingWith` | `findByNameStartingWith` | STARTING_WITH | Implemented |
| `EndingWith` | `findByNameEndingWith` | ENDING_WITH | Implemented |
| `Containing` | `findByNameContaining` | CONTAINING | Implemented |
| `OrderBy{Prop}{Asc|Desc}` | `findByAgeOrderByLastnameDesc` | ORDER BY | Implemented |
| `Not` | `findByLastnameNot` | NEQ | Implemented |
| `NotEqual` | `findByLastnameNotEqual` | NEQ | Implemented |
| `In` | `findByAgeIn(Collection)` | IN | Implemented |
| `NotIn` | `findByAgeNotIn(Collection)` | NOT_IN | Implemented |
| `True` | `findByActiveTrue` | IS_TRUE | Implemented |
| `False` | `findByActiveFalse` | IS_FALSE | Implemented |
| `IgnoreCase`, `AllIgnoreCase` | `findByNameIgnoreCase` | IGNORE_CASE | Implemented |
| `First{n}`, `Top{n}` | `findFirst10ByActive` | LIMIT | Implemented |
| `Count` | `countByLastname` | COUNT | Implemented |
| `Exists` | `existsByEmail` | EXISTS | Implemented |
| `Delete` | `deleteByLastname` | DELETE | Implemented |
| `Remove` | `removeByStatus` | DELETE | Implemented |

### JPA Query Prefixes

| Prefix | Purpose | Example |
|--------|---------|---------|
| `find`, `query`, `get`, `read` | Find entities | `findByLastname` |
| `count` | Count entities | `countByLastname` |
| `exists` | Check existence | `existsByEmail` |
| `delete`, `remove` | Delete entities | `deleteByLastname` |

## Key Design Decisions

### 1. Operator Precedence (CRITICAL)

**Compound suffixes take precedence over simple suffixes:**
- `findByStatusNotIn` -> `Status` + `NOT_IN` (compound suffix wins over `In`)
- `findByNameNotLike` -> `Name` + `NOT_LIKE` (compound suffix wins over `Like`)
- `findByAgeNotNull` -> `Age` + `IS_NOT_NULL` (compound suffix wins over `Null`)

**Suffix precedence order:**
1. `GreaterThanEqual`, `LessThanEqual` (11 chars)
2. `GreaterThan`, `LessThan` (10 chars)
3. `Between`, `StartingWith`, `EndingWith` (7-13 chars)
4. `NotLike`, `NotIn`, `Containing`, `IsNotNull` (6-10 chars)
5. `Like`, `NotNull`, `IsNull`, `True`, `False` (3-6 chars)
6. `After`, `Before` (5 chars)
7. `IgnoreCase` (10 chars)
8. `In` (2 chars)
9. `Not` (3 chars) -> NEQ

### 2. Column Name Conversion

Property names are converted from camelCase to snake_case:
- `firstname` -> `firstname`
- `firstName` -> `first_name`
- `createdAt` -> `created_at`
- `orderNumber` -> `order_number`

### 3. Top/First Limiting

- `findFirstByLastname` -> limit 1
- `findTop5ByLastname` -> limit 5
- `findTopByOrderByAgeDesc` -> limit 1, order by age DESC
- `findFirst10ByStatus` -> limit 10

## Query Method Recognition

A method is recognized as a query method if it matches:
```
^(find|query|get|read|stream|count|exists|delete|remove)
  (Distinct|First|Top\d*)?
  (By|All)
  (.+)$
```

Examples:
- `findByLastname` - matches
- `findDistinctByLastname` - matches with Distinct
- `findTop10ByAge` - matches with Top10
- `findFirstByStatus` - matches with First
- `countByLastname` - count query
- `existsByEmail` - exists query
- `save` - doesn't match (built-in)
- `findBy` - no condition after By (invalid)

## Files

| File | Purpose |
|------|---------|
| `QueryMethodLexer.java` | Tokenizes query method names into structured tokens |
| `QueryPlanner.java` | Creates LogicalQuery from method name tokens |
| `JpqlQueryParser.java` | Parses JPQL queries from @Query annotation |
| `LogicalQuery.java` | Query representation with conditions, joins, projections |
| `CompiledQuery.java` | Pre-compiled query with resolved column indices |

## Test Coverage

Tests verify:
- Boolean operators (True, False)
- Null operators (IsNull, IsNotNull, NotNull)
- IN/NOT_IN operators
- Date/Time operators (After, Before)
- LIKE operators (Like, NotLike, Containing, StartingWith, EndingWith)
- IgnoreCase modifier
- OrderBy clauses with Asc/Desc
- Distinct modifier
- Top/First limiting
- Count/Exists query types
- Not suffix for NEQ
- Delete/Remove query types
- JPQL SELECT, UPDATE, DELETE statements
- JPQL projections
- JPQL parameter binding
- GROUP BY / HAVING clauses
