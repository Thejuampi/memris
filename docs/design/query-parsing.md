# JPA Query Method Parser Design

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Overview
This document captures the design decisions made while implementing JPA Query Method parsing for Memris, ensuring compliance with Spring Data JPA specifications.

## Important Distinction: Memris Methods vs JPA Query Methods

### Memris Convenience Methods (Memris-specific)
These are **NOT** JPA standard - they are Memris-specific convenience methods:

```java
// These are Memris-specific, NOT JPA standard
repo.findBy("status", "active");              // EQ comparison
repo.findBy("amount", 1000L, true, false);    // GT comparison
repo.findByNot("status", "inactive");         // NEQ comparison
repo.findByIn("category", List.of("A", "B")); // IN comparison
repo.findByBetween("amount", 100L, 500L);     // BETWEEN
repo.findById(1);                             // Find by primary key
```

### JPA Query Methods (Spring Data standard)
These follow the Spring Data JPA specification - method names derive queries:

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

// JPA also supports alternative keywords (Is, Null, Equals, etc.)
List<User> findByEmailIs(String email);        // Is = EQ (alternative)
List<User> findByEmailEquals(String email);    // Equals = EQ (alternative)
List<User> findByEmail(String email);          // Implicit EQ
List<User> findByEmailNull();                  // Null = IS_NULL (without Is prefix)
```

## JPA Specification Compliance

### Operator Keywords (from Spring Data JPA)

| Keyword | Example | Operator | Status |
|---------|---------|----------|--------|
| `Distinct` | `findDistinctByLastnameAndFirstname` | DISTINCT | ✅ |
| `And` | `findByLastnameAndFirstname` | AND | ✅ |
| `Or` | `findByLastnameOrFirstname` | OR | ✅ |
| `Is`, `Equals` | `findByFirstname`, `findByFirstnameIs`, `findByFirstnameEquals` | EQ | ✅ |
| `Between` | `findByAgeBetween` | BETWEEN | ✅ |
| `LessThan` | `findByAgeLessThan` | LT | ✅ |
| `LessThanEqual` | `findByAgeLessThanEqual` | LTE | ✅ |
| `GreaterThan` | `findByAgeGreaterThan` | GT | ✅ |
| `GreaterThanEqual` | `findByAgeGreaterThanEqual` | GTE | ✅ |
| `After` | `findByCreatedAtAfter` | AFTER | ✅ |
| `Before` | `findByCreatedAtBefore` | BEFORE | ✅ |
| `IsNull`, `Null` | `findByEmailIsNull`, `findByEmailNull` | IS_NULL | ✅ |
| `IsNotNull`, `NotNull` | `findByEmailIsNotNull`, `findByEmailNotNull` | IS_NOT_NULL | ✅ |
| `Like` | `findByNameLike` | LIKE | ✅ |
| `NotLike` | `findByNameNotLike` | NOT_LIKE | ✅ |
| `StartingWith`, `StartsWith` | `findByNameStartingWith` | STARTING_WITH | ✅ |
| `EndingWith`, `EndsWith` | `findByNameEndingWith` | ENDING_WITH | ✅ |
| `Containing`, `Contains` | `findByNameContaining` | CONTAINING | ✅ |
| `OrderBy{Prop}{Asc\|Desc}` | `findByAgeOrderByLastnameDesc` | ORDER BY | ✅ |
| `Not` | `findByLastnameNot` | NEQ | ✅ |
| `NotEqual` | `findByLastnameNotEqual` | NEQ | ✅ |
| `In` | `findByAgeIn(Collection)` | IN | ✅ |
| `NotIn` | `findByAgeNotIn(Collection)` | NOT_IN | ✅ |
| `True` | `findByActiveTrue` | IS_TRUE | ✅ |
| `False` | `findByActiveFalse` | IS_FALSE | ✅ |
| `IgnoreCase`, `AllIgnoreCase` | `findByNameIgnoreCase` | IGNORE_CASE | ✅ |
| `First{n}`, `Top{n}` | `findFirst10ByActive` | LIMIT | ✅ |
| `Count` | `countByLastname` | COUNT | ✅ |
| `Exists` | `existsByEmail` | EXISTS | ✅ |
| `Delete` | `deleteByLastname` | DELETE | ✅ |
| `Remove` | `removeByStatus` | DELETE | ✅ |

### JPA Query Prefixes

| Prefix | Purpose | Example |
|--------|---------|---------|
| `find`, `query`, `get`, `read`, `stream` | Find entities | `findByLastname` |
| `count` | Count entities | `countByLastname` |
| `exists` | Check existence | `existsByEmail` |
| `delete`, `remove` | Delete entities | `deleteByLastname` |

### Operator Keywords (from Spring Data JPA)

| Keyword | Example | JPQL |
|---------|---------|------|
| `Distinct` | `findDistinctByLastname` | `select distinct ...` |
| `And` | `findByLastnameAndFirstname` | `x.lastname = ?1 and x.firstname = ?2` |
| `Or` | `findByLastnameOrFirstname` | `x.lastname = ?1 or x.firstname = ?2` |
| `Is`, `Equals` | `findByFirstname` | `x.firstname = ?1` |
| `Between` | `findByAgeBetween` | `x.age between ?1 and ?2` |
| `LessThan` | `findByAgeLessThan` | `x.age < ?1` |
| `LessThanEqual` | `findByAgeLessThanEqual` | `x.age <= ?1` |
| `GreaterThan` | `findByAgeGreaterThan` | `x.age > ?1` |
| `GreaterThanEqual` | `findByAgeGreaterThanEqual` | `x.age >= ?1` |
| `After` | `findByCreatedAtAfter` | `x.createdAt > ?1` |
| `Before` | `findByCreatedAtBefore` | `x.createdAt < ?1` |
| `IsNull`, `Null` | `findByEmailIsNull` | `x.email is null` |
| `IsNotNull`, `NotNull` | `findByEmailIsNotNull` | `x.email is not null` |
| `Like` | `findByNameLike` | `x.name like ?1` |
| `NotLike` | `findByNameNotLike` | `x.name not like ?1` |
| `StartingWith` | `findByNameStartingWith` | `x.name like ?1%` |
| `EndingWith` | `findByNameEndingWith` | `x.name like %?1` |
| `Containing` | `findByNameContaining` | `x.name like %?1%` |
| `OrderBy` | `findByAgeOrderByLastnameDesc` | `... order by x.lastname desc` |
| **`Not`** | `findByLastnameNot` | `x.lastname <> ?1` |
| `In` | `findByAgeIn` | `x.age in ?1` |
| **`NotIn`** | `findByAgeNotIn` | `x.age not in ?1` |
| **`True`** | `findByActiveTrue` | `x.active = true` |
| **`False`** | `findByActiveFalse` | `x.active = false` |
| `IgnoreCase` | `findByNameIgnoreCase` | `UPPER(x.name) = UPPER(?1)` |

## Key Design Decisions

### 1. Operator Precedence (CRITICAL)

**Compound suffixes take precedence over simple suffixes:**
- `findByStatusNotIn` → `Status` + `NOT_IN` (compound suffix wins over `In`)
- `findByNameNotLike` → `Name` + `NOT_LIKE` (compound suffix wins over `Like`)
- `findByAgeNotNull` → `Age` + `IS_NOT_NULL` (compound suffix wins over `Null`)

**Suffix precedence order:**
1. `GreaterThanEqual`, `LessThanEqual` (11 chars)
2. `GreaterThan`, `LessThan` (10 chars)
3. `Between`, `StartingWith`, `EndingWith` (7-13 chars)
4. `NotLike`, `NotIn`, `Containing`, `IsNotNull` (6-10 chars)
5. `Like`, `NotNull`, `IsNull`, `IsTrue`, `True`, `IsFalse`, `False` (3-6 chars)
6. `After`, `Before` (5 chars)
7. `IgnoreCase` (10 chars)
8. `In` (2 chars)
9. `Not` (3 chars) → NEQ

### 2. Not Prefix vs Not Suffix

**`Not` as PREFIX** (negates the operator that follows):
- `findByStatusNotIn` - "Not" at START → This is actually a suffix case (see below)

Wait, in JPA:
- `findByLastnameNot` → `Lastname` + `Not` suffix → `x.lastname <> ?1`
- `findByStatusNotIn` → `Status` + `NotIn` compound suffix → `x.status not in ?1`

The "Not" in `StatusNotIn` is part of the compound suffix `NotIn`, not a prefix.

### 3. Property Name Extraction

For a condition like `LastnameNot`:
1. Detect operator suffix: `Not` → NEQ
2. Extract property name: `LastnameNot` - `Not` = `Lastname`
3. Convert to column name: `lastname` (camelCase to snake_case)

### 4. True/False Suffixes

For boolean properties:
- `findByActiveTrue` → `Active` + `True` suffix → `x.active = true`
- `findByActiveFalse` → `Active` + `False` suffix → `x.active = false`
- `findByActiveIsTrue` → Same as above (IsTrue = True)
- `findByActiveIsFalse` → Same as above (IsFalse = False)

### 5. OrderBy Parsing

For `findByLastnameOrderByFirstnameDesc`:
1. Split by `OrderBy`: conditions=`Lastname`, orderPart=`FirstnameDesc`
2. Parse conditions: `Lastname` → `lastname`
3. Parse order: `Firstname` + `Desc` → `firstname` + `DESC`

**Important**: The Asc/Desc is part of the ORDER clause, not the property name.

### 6. Top/First Limiting

- `findFirstByLastname` → limit 1
- `findTop5ByLastname` → limit 5
- `findTopByOrderByAgeDesc` → limit 1, order by age DESC
- `findFirst10ByStatus` → limit 10

### 7. Column Name Conversion

Property names are converted from camelCase to snake_case:
- `firstname` → `firstname`
- `firstName` → `first_name`
- `createdAt` → `created_at`
- `orderNumber` → `order_number`

## Implemented Operators

```java
enum Operator {
    EQ, NEQ, GT, GTE, LT, LTE,
    BETWEEN, IN, NOT_IN,
    CONTAINING, STARTING_WITH, ENDING_WITH,
    LIKE, NOT_LIKE,
    IS_TRUE, IS_FALSE, IS_NULL, IS_NOT_NULL,
    AFTER, BEFORE, IGNORE_CASE
}
```

## Query Method Recognition

A method is recognized as a query method if it matches:
```
^(find|query|get|read|stream|count|exists|delete|remove)
  (Distinct|First|Top\d*)?
  (By|And|Or)
  (.+)$
```

Examples:
- ✅ `findByLastname` - matches
- ✅ `findDistinctByLastname` - matches with Distinct
- ✅ `findTop10ByAge` - matches with Top10
- ✅ `findFirstByStatus` - matches with First
- ✅ `countByLastname` - count query
- ✅ `existsByEmail` - exists query
- ❌ `save` - doesn't match
- ❌ `findBy` - no condition after By

## Files Modified

| File | Purpose |
|------|---------|
| `QueryMethodParser.java` | Main parser with JPA-compliant operator detection |
| `MemrisRepository.java` | Generic repository interface with convenience methods |
| `MemrisRepositoryFactory.java` | Factory that handles dynamic proxy invocation |
| `Predicate.java` | Core predicate types (extended with NOT_IN) |

## Test Coverage

Tests verify:
- Boolean operators (True, False, IsTrue, IsFalse)
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
