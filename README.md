# Memris

**Memris** = "Memory" + "Iris" — a heap-based, zero-reflection in-memory storage engine for Java 21. Like iris (the eye), it provides vision/insight into your data. Like iris (a flower), it blooms fast.

> "Iris suggests looking through data/vision. It sounds like an engine that can 'see' through the heap instantly."

## What is Memris?

**Memris** is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with Spring Data-compatible query method patterns.

Built on 100% Java heap storage with ByteBuddy bytecode generation, Memris delivers columnar storage performance with familiar query patterns. Zero reflection in hot paths, O(1) design principles, and primitive-only APIs ensure maximum throughput.

**Key highlights:**
- **100% Heap-Based**: Uses primitive arrays (int[], long[], String[]) — no FFM/MemorySegment required
- **ByteBuddy Table Generation**: Generates optimized table classes at runtime (~1ns overhead)
- **Spring Data-Compatible**: Use familiar JPA query method patterns (findBy, countBy, existsBy)
- **Zero Reflection**: Compile-time query derivation with type-safe dispatch
- **Custom Annotations**: `@Entity`, `@Index`, `@GeneratedValue` (not Jakarta/JPA)
- **Multi-Reader, Multi-Writer**: Lock-free row writes with SeqLock coordination (eventual index consistency)

## Quick Start

### 1. Define Your Entity

```java
import io.memris.core.Entity;
import io.memris.core.Index;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class User {
    @Index(type = Index.IndexType.HASH)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Index(type = Index.IndexType.HASH)
    private String email;
    
    private String name;
    private int age;
    
    public User() {}
    
    public User(String email, String name, int age) {
        this.email = email;
        this.name = name;
        this.age = age;
    }
}
```

### 2. Create Repository Interface

```java
import io.memris.repository.MemrisRepository;
import io.memris.core.Query;
import io.memris.core.Param;
import java.util.List;
import java.util.Optional;

public interface UserRepository extends MemrisRepository<User> {
    // Built-in methods
    Optional<User> findByEmail(String email);
    List<User> findByAgeBetween(int min, int max);
    long countByAgeGreaterThan(int age);
    boolean existsByEmail(String email);
    
    // @Query with JPQL
    @Query("select u from User u where u.name ilike :name")
    List<User> findByNameContaining(@Param("name") String name);
}
```

### 3. Use Repository

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;

public class Main {
    public static void main(String[] args) throws Exception {
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
        MemrisArena arena = factory.createArena();
        
        // Create repository
        UserRepository repo = arena.createRepository(UserRepository.class);
        
        // Save entity
        User user = repo.save(new User("john@example.com", "John Doe", 30));
        
        // Find by email
        Optional<User> found = repo.findByEmail("john@example.com");
        
        // Query methods
        List<User> adults = repo.findByAgeBetween(18, 65);
        long count = repo.countByAgeGreaterThan(25);
        
        // Cleanup
        factory.close();
    }
}
```

## Supported Features

### Entity Annotations

| Annotation | Purpose | Status |
|------------|---------|--------|
| `@Entity` | Marks class as persistable | ✅ Implemented |
| `@Index` | Creates HASH or BTREE index | ✅ Implemented |
| `@GeneratedValue` | Auto ID generation | ✅ Implemented |
| `@OneToOne` | One-to-one relationship | ✅ Implemented |
| `@ManyToOne` | Many-to-one relationship | ✅ Implemented |
| `@OneToMany` | One-to-many relationship | ✅ Implemented |
| `@ManyToMany` | Many-to-many relationship | ✅ Implemented |
| `@JoinColumn` | Specifies foreign key column | ✅ Implemented |
| `@Query` | JPQL-like query string | ✅ Implemented |
| `@Param` | Named parameter binding | ✅ Implemented |

### GenerationType Options

- **AUTO**: Auto-detect based on field type (numeric → IDENTITY, UUID → UUID)
- **IDENTITY**: Numeric atomic increment (uses AtomicLong)
- **UUID**: Random UUID generation
- **CUSTOM**: User-provided IdGenerator implementation

### Supported Field Types

| Type | Storage | Notes |
|------|---------|-------|
| `int`, `Integer` | Primitive int | TYPE_INT |
| `long`, `Long` | Primitive long | TYPE_LONG |
| `boolean`, `Boolean` | Primitive boolean | TYPE_BOOLEAN |
| `byte`, `Byte` | Primitive byte | TYPE_BYTE |
| `short`, `Short` | Primitive short | TYPE_SHORT |
| `float`, `Float` | Primitive float | TYPE_FLOAT |
| `double`, `Double` | Primitive double | TYPE_DOUBLE |
| `char`, `Character` | Primitive char | TYPE_CHAR |
| `String` | String | TYPE_STRING |
| `Instant` | long (epoch millis) | TYPE_INSTANT |
| `LocalDate` | long (epoch day) | TYPE_LOCAL_DATE |
| `LocalDateTime` | long (epoch millis) | TYPE_LOCAL_DATE_TIME |
| `java.util.Date` | long (epoch millis) | TYPE_DATE |
| `BigDecimal` | String | TYPE_BIG_DECIMAL (EQ/NE/IN/NOT_IN only) |
| `BigInteger` | String | TYPE_BIG_INTEGER (EQ/NE/IN/NOT_IN only) |

### Query Method Patterns

**Comparison Operators:**
- `findByAgeEquals(int)` / `findByAge(int)` — Equality
- `findByAgeNotEqual(int)` / `findByAgeNot(int)` — Inequality
- `findByAgeGreaterThan(int)` / `findByAgeAfter(int)` — Greater than
- `findByAgeLessThan(int)` / `findByAgeBefore(int)` — Less than
- `findByAgeBetween(int, int)` — Range query

**String Operators:**
- `findByNameContaining(String)` — Substring match
- `findByNameStartingWith(String)` — Prefix match
- `findByNameEndingWith(String)` — Suffix match
- `findByNameLike(String)` — Pattern match
- `findByNameContainingIgnoreCase(String)` — Case-insensitive

**Null & Boolean:**
- `findByDepartmentIsNull()` / `findByDepartmentIsNotNull()`
- `findByActiveTrue()` / `findByActiveFalse()`

**Collection:**
- `findBySkuIn(List<String>)` — IN query
- `findByStatusNotIn(List<String>)` — NOT IN query

**Logical Operators:**
- `findByCustomerIdAndStatus(Long, String)` — AND
- `findByPriceLessThanOrStockEqual(int, long)` — OR

**Ordering & Limiting:**
- `findByPriceOrderByPriceDesc(long)` — ORDER BY
- `findTopByOrderByPriceDesc()` — Top/First

**Return Types:**
- `List<T>`, `Optional<T>`, `T` — Find methods
- `long` — Count methods
- `boolean` — Exists methods

### @Query (JPQL-like Syntax)

**Basic Query:**
```java
@Query("select u from User u where u.email = :email")
Optional<User> findByEmail(@Param("email") String email);
```

**Range Query:**
```java
@Query("select u from User u where u.age between :min and :max")
List<User> findByAgeRange(@Param("min") int min, @Param("max") int max);
```

**Case-Insensitive ILIKE:**
```java
@Query("select u from User u where u.name ilike :name")
List<User> findByNameContaining(@Param("name") String name);
```

**Joins:**
```java
@Query("select o from Order o join o.customer c where c.email = :email")
List<Order> findByCustomerEmail(@Param("email") String email);
```

**Projections (Records):**
```java
public record UserSummary(String name, int age) {}

@Query("select u.name as name, u.age as age from User u where u.age > :minAge")
List<UserSummary> findSummaries(@Param("minAge") int minAge);
```

### Built-in Repository Methods

| Method | Return Type | Description |
|--------|------------|-------------|
| `save(T entity)` | `T` | Saves entity (generates ID if needed) |
| `findById(ID id)` | `Optional<T>` | Find by ID (O(1) via HashIndex) |
| `findAll()` | `List<T>` | Returns all entities |
| `delete(T entity)` | `void` | Deletes entity |
| `deleteById(ID id)` | `void` | Deletes by ID |
| `deleteAll()` | `void` | Deletes all entities |
| `count()` | `long` | Counts all entities |
| `existsById(ID id)` | `boolean` | Checks if entity exists |

## Architecture

### Build-Time (Runtime, per entity type)
```
Entity Class → EntityMetadata → ByteBuddy → GeneratedTable Class
```

### Runtime (Hot Path)
```
Repository Method → QueryCompiler → HeapRuntimeKernel 
    → GeneratedTable.scan*() / Index lookup → Results
```

### Storage Layer

| Component | Purpose | Complexity |
|-----------|---------|------------|
| `PageColumnInt` | int[] column with scans | O(1) access |
| `PageColumnLong` | long[] column with scans | O(1) access |
| `PageColumnString` | String[] column with scans | O(1) access |
| `HashIndex` | ConcurrentHashMap for equality | O(1) lookup |
| `RangeIndex` | ConcurrentSkipListMap for ranges | O(log n) lookup |
| `LongIdIndex` | Primary key index (long) | O(1) lookup |
| `StringIdIndex` | Primary key index (String) | O(1) lookup |

### Concurrency Model

**Multi-Reader, Single-Writer (with SeqLock coordination)**

| Operation | Thread-Safe | Mechanism |
|-----------|-------------|-----------|
| ID generation | ✅ | AtomicLong |
| ID index lookups | ✅ | ConcurrentHashMap |
| HashIndex lookups | ✅ | ConcurrentHashMap |
| RangeIndex lookups | ✅ | ConcurrentSkipListMap |
| Query scans | ✅ | SeqLock + volatile watermark |
| Row allocation | ✅ | LockFreeFreeList (CAS-based) |
| Column writes | ✅ | Row seqlock (CAS) + publish ordering |
| Index updates | ⚠️ | Eventual consistency + query validation |

**Isolation Level:** Best-effort (no MVCC, no transactions)

**Note:** Concurrent saves/deletes are supported; external synchronization is only needed for strict index/row atomicity.

## Current Limitations

1. **DISTINCT** — Tokenized but execution incomplete
   - Workaround: Use `Stream.distinct()` on results

2. **Query Aggregates** — Only `COUNT` supported
   - No `SUM`, `AVG`, `MIN`, `MAX`

3. **GROUP BY / HAVING** — Not implemented

4. **Subqueries** — Not implemented

5. **Transactions** — Not supported (no persistence context)

### Type Limitations

1. **BigDecimal/BigInteger** — Only EQ/NE/IN/NOT_IN operators
2. **STARTING_WITH / ENDING_WITH** — For String type: NOT YET implemented
3. **BETWEEN for Double** — NOT YET implemented

### Enterprise Features

- ✅ `@Embeddable` components
- ✅ `@Enumerated` types
- ✅ Lifecycle callbacks (@PrePersist, @PostLoad, @PreUpdate)
- No inheritance hierarchies
- No composite keys

## Performance

| Operation | Complexity | Notes |
|-----------|------------|-------|
| HashIndex lookup | O(1) | Via ConcurrentHashMap |
| RangeIndex lookup | O(log n) | Via ConcurrentSkipListMap |
| Table scan | O(n) | With early termination |
| IN operation | O(n) | Optimized via HashSet |

**Design Principles:**
1. **O(1) First, O(log n) Second, O(n) Forbidden** (for hot paths)
2. **Primitive-Only APIs** — No boxing in hot paths
3. **TypeCode Switch** — Java 21 pattern matching with static byte constants
4. **Zero Reflection Hot Path** — Compile-time metadata extraction
5. **Dense Arrays Over Maps** — Column indices, type codes in indexed arrays

## Requirements

- **Java Version**: 21 (required)
- **Maven**: 3.8+
- **Platforms**: Windows/Linux/Mac
- **JVM Flags**: None required for production (100% heap-based)

## Building

```bash
# Full clean build
mvn.cmd clean compile

# Run all tests
mvn.cmd -q -e -pl memris-core test

# Run single test class
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName

# Run single test method
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName#methodName

# Code quality checks
mvn.cmd spotbugs:check
mvn.cmd checkstyle:check
mvn.cmd pmd:check

# Package and install
mvn.cmd clean install
```

## Maven Dependency

Memris is currently in SNAPSHOT version (1.0.0-SNAPSHOT). Build locally to use:

```xml
<dependency>
    <groupId>io.memris</groupId>
    <artifactId>memris-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Transitive Dependencies:**
- ByteBuddy 1.18.4 (dynamic bytecode generation)
- RoaringBitmap 1.0.0 (compressed indexes)
- SparseBitSet 1.2 (sparse indexes)
- Lombok 1.18.36 (compile-time annotation processing)

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — Detailed architecture documentation
- [DEVELOPMENT.md](docs/DEVELOPMENT.md) — Development guidelines and code style
- [QUERY.md](docs/QUERY.md) — Query method reference and operators
- [SPRING_DATA.md](docs/SPRING_DATA.md) — Spring Data integration details
- [CONCURRENCY.md](docs/CONCURRENCY.md) — Concurrency model and guarantees

## Roadmap

- **FFM Off-Heap Storage** — For large datasets (reduces GC pressure)
- **MVCC** — Snapshot isolation for better concurrency
- **SIMD Vectorization** — Via Vector API for scan operations
- **Striped Index Updates** — 4-8x better write throughput
- **Projections from Method Names** — Without @Query annotation
- **Aggregate Functions** — SUM, AVG, MIN, MAX
- **DISTINCT** — Full implementation
- **Persistence** — Disk-based storage with crash recovery

### Future Enhancements

- No inheritance hierarchies
- No composite keys
- Schema evolution (online changes)

## License

[License information]
