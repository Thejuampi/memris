# Memris

[![CI](https://github.com/Thejuampi/memris/actions/workflows/ci.yml/badge.svg)](https://github.com/Thejuampi/memris/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.thejuampi/memris.svg?label=maven%20central)](https://central.sonatype.com/artifact/io.github.thejuampi/memris)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

**Memris** — blazing fast, heap-based, zero-reflection in-memory storage for Java 21.

Memris combines columnar, primitive-backed storage with ByteBuddy-generated tables and a Spring-Data-like query surface. Built for ultra-low-latency reads and concurrent, lock-free writes.

---

## ✨ Highlights

- 🚀 **100% heap-based columns** (primitive arrays) — no off-heap required
- ⚡ **ByteBuddy-generated tables** for zero-reflection hot paths
- 🧠 **Plan-driven embedded paths** via precompiled `ColumnAccessPlan`
- 🔧 **Generated saver/materializer** for flat + embedded fields (no runtime reflection fallback)
- 🏟️ **Arena-scoped codegen caches** (no static mutable runtime registries)
- 🔒 **Thread-safe arena lifecycle** with fail-fast close semantics
- 🎯 **Spring Data-style queries** (`findBy*`, `countBy*`, `existsBy*`)
- 🔍 **Lightweight indexes** — hash (O(1)) and range (O(log n))
- ☕ **Targeted for Java 21** — leverages modern language features

---

## 🚀 Quick Start

Add the library from Maven Central:

### Maven

```xml
<dependency>
  <groupId>io.github.thejuampi</groupId>
  <artifactId>memris</artifactId>
  <version>0.2.0</version>
</dependency>
```

### Gradle (Groovy)

```groovy
implementation 'io.github.thejuampi:memris:0.2.0'
```

### Gradle (Kotlin)

```kotlin
implementation("io.github.thejuampi:memris:0.2.0")
```

---

## 📖 Complete Example

### 1. Define Your Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class User {
    @Id
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
    
    public Long getId() { return id; }
    public String getEmail() { return email; }
    public String getName() { return name; }
    public int getAge() { return age; }
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
    User save(User user);
    
    Optional<User> findByEmail(String email);
    
    List<User> findByAgeBetween(int min, int max);
    
    long countByAgeGreaterThan(int age);
    
    boolean existsByEmail(String email);
    
    @Query("select u from User u where u.name ilike :name")
    List<User> findByNameContaining(@Param("name") String name);
}
```

### 3. Use Repository

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;

public class Main {
    public static void main(String[] args) throws Exception {
        // Option 1: Default configuration
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
        
        // Option 2: Custom configuration
        // MemrisRepositoryFactory factory = new MemrisRepositoryFactory(
        //     MemrisConfiguration.builder()
        //         .pageSize(2048)
        //         .enableParallelSorting(true)
        //         .build()
        // );
        
        MemrisArena arena = factory.createArena();
        UserRepository repo = arena.createRepository(UserRepository.class);
        
        User user = repo.save(new User("alice@example.com", "Alice", 30));
        Optional<User> found = repo.findByEmail("alice@example.com");
        
        List<User> adults = repo.findByAgeBetween(18, 65);
        long count = repo.countByAgeGreaterThan(25);
        
        factory.close();
    }
}
```

---

## 🎯 When to Use Memris

| Scenario | Why Memris? |
|----------|-------------|
| **High-throughput applications** | O(1) hash index lookups and columnar scans |
| **Low-latency requirements** | Generated hot paths with minimal dispatch overhead |
| **In-memory caching** | 100% heap-based, no external dependencies |
| **Spring Data migration** | Familiar query method patterns, easy to learn |
| **Concurrent workloads** | Multi-reader, multi-writer with SeqLock coordination |

---

## 📊 Performance

| Operation | Complexity | Implementation |
|-----------|------------|----------------|
| Hash index lookup | **O(1)** | ConcurrentHashMap |
| Range index lookup | **O(log n)** | ConcurrentSkipListMap |
| Column scan | O(n) | Early termination, dense arrays |
| IN operation | O(n) | Optimized via HashSet |

### Design Principles

1. **O(1) First, O(log n) Second, O(n) Forbidden** — for hot paths
2. **Primitive-Only APIs** — no boxing in critical sections
3. **TypeCode Switch** — Java 21 pattern matching with static byte constants
4. **Zero Reflection Hot Path** — compile-time metadata extraction
5. **Dense Arrays Over Maps** — column indices, type codes in indexed arrays

---

## 🧩 Supported Features

### Entity Annotations

| Annotation | Purpose |
|------------|---------|
| `@Entity` | Marks class as persistable |
| `@Id` | Marks field as primary key |
| `@Index` | Creates index (see types below) |
| `@GeneratedValue` | Auto ID generation |
| `@OneToOne` | One-to-one relationship |
| `@ManyToOne` | Many-to-one relationship |
| `@OneToMany` | One-to-many relationship |
| `@ManyToMany` | Many-to-many relationship |
| `@JoinColumn` | Specifies foreign key column |
| `@JoinTable` | Specifies join table for @ManyToMany |
| `@Query` | JPQL-like query string |
| `@Param` | Named parameter binding |
| `@Modifying` | Marks @Query as UPDATE/DELETE operation |


**Index Types** (`@Index(type = ...)`):
| Type | Complexity | Use For |
|------|------------|---------|
| `HASH` | O(1) | Equality lookups (EQ, NE, IN, NOT_IN) |
| `BTREE` | O(log n) | Range queries (GT, LT, BETWEEN) |
| `PREFIX` | O(k) | Prefix/STARTING_WITH queries |
| `SUFFIX` | O(k) | Suffix/ENDING_WITH queries |

**Composite Indexes:**
Class-level `@Index(fields = {"field1", "field2"})` for multi-field indexes.

### Supported Field Types

| Type | Storage | Notes |
|------|---------|-------|
| `int`, `Integer` | `int[]` | TYPE_INT |
| `long`, `Long` | `long[]` | TYPE_LONG |
| `boolean`, `Boolean` | `boolean[]` | TYPE_BOOLEAN |
| `byte`, `Byte` | `byte[]` | TYPE_BYTE |
| `short`, `Short` | `short[]` | TYPE_SHORT |
| `float`, `Float` | `float[]` | TYPE_FLOAT |
| `double`, `Double` | `double[]` | TYPE_DOUBLE |
| `char`, `Character` | `char[]` | TYPE_CHAR |
| `String` | `String[]` | TYPE_STRING |
| `Instant` | `long[]` (epoch millis) | TYPE_INSTANT |
| `LocalDate` | `long[]` (epoch day) | TYPE_LOCAL_DATE |
| `LocalDateTime` | `long[]` (epoch millis) | TYPE_LOCAL_DATE_TIME |
| `java.util.Date` | `long[]` (epoch millis) | TYPE_DATE |
| `java.time.LocalTime` | `String[]` | TYPE_LOCAL_TIME |
| `java.sql.Date` | `long[]` (epoch millis) | TYPE_SQL_DATE |
| `java.sql.Timestamp` | `long[]` (epoch millis) | TYPE_SQL_TIMESTAMP |
| `java.util.UUID` | `String[]` | EQ/NE/IN/NOT_IN |
| `BigDecimal` | `String[]` | TYPE_BIG_DECIMAL (EQ/NE/IN/NOT_IN) |
| `BigInteger` | `String[]` | TYPE_BIG_INTEGER (EQ/NE/IN/NOT_IN) |

### Query Method Patterns

#### Query Method Prefixes

| Prefix | Purpose | Example |
|--------|---------|---------|
| `find`, `read`, `query`, `get` | Retrieve entities | `findByEmail` |
| `count` | Count matching entities | `countByActiveTrue` |
| `exists` | Check existence | `existsByEmail` |
| `delete`, `remove` | Delete matching entities | `deleteByExpiredTrue` |

#### Built-in CRUD Methods

> **Note:** `MemrisRepository<T>` is a marker interface (empty by design). Built-in CRUD methods are dynamically resolved at runtime.

```java
T save(T entity);
Iterable<T> saveAll(Iterable<T> entities);
Optional<T> findById(ID id);
Iterable<T> findAllById(Iterable<ID> ids);
List<T> findAll();
long count();
boolean existsById(ID id);
void delete(T entity);
void deleteById(ID id);
void deleteAll();
void deleteAllById(Iterable<ID> ids);
```

#### Comparison Operators

```java
findByAgeEquals(int) / findByAge(int)                    // Equality
findByAgeNotEqual(int) / findByAgeNot(int)               // Inequality
findByAgeGreaterThan(int) / findByAgeAfter(int)          // Greater than
findByAgeGreaterThanEqual(int)                           // Greater than or equal
findByAgeLessThan(int) / findByAgeBefore(int)            // Less than
findByAgeLessThanEqual(int)                              // Less than or equal
findByAgeBetween(int, int)                               // Range query
```

#### String Operators

```java
findByNameLike(String) / findByNameIsLike(String)              // Pattern match
findByNameNotLike(String)                                       // Negative pattern match
findByNameStartingWith(String)                                  // Prefix match
findByNameNotStartingWith(String)                               // Negative prefix match
findByNameEndingWith(String)                                    // Suffix match
findByNameNotEndingWith(String)                                 // Negative suffix match
findByNameContaining(String)                                     // Substring match
findByNameNotContaining(String)                                 // Negative substring match
findByNameIgnoreCase(String)                                    // Case-insensitive equality
findByNameStartingWithIgnoreCase(String)                        // Case-insensitive prefix
findByNameContainingIgnoreCase(String)                          // Case-insensitive substring
```

#### Boolean Operators

```java
findByActiveTrue() / findByActiveIsTrue()
findByActiveFalse() / findByActiveIsFalse()
```

#### Null Operators

```java
findByDepartmentIsNull() / findByDepartmentNull() / findByDepartmentIs()
findByDepartmentIsNotNull() / findByDepartmentNotNull()
```

#### Collection Operators

```java
findBySkuIn(List<String>)
findBySkuNotIn(List<String>)
```

#### Date/Time Operators

```java
findByCreatedAfter(Instant) / findByCreatedGreaterThan(Instant)
findByCreatedBefore(Instant) / findByCreatedLessThan(Instant)
findByCreatedBetween(Instant, Instant)
```

#### Logical Operators

```java
findByCustomerIdAndStatus(Long, String)                // AND
findByPriceLessThanOrStockEqual(int, long)             // OR
```

#### Ordering & Limiting

```java
findByPriceOrderByPriceDesc(long)                      // ORDER BY
findTopByOrderByPriceDesc()                             // First result
findFirstByCustomerId(Long)                             // Alias for findTop...By
findTop3ByOrderByPriceDesc()                            // First N results
findDistinctByStatus(String)                            // Unique results (partial support)
```

#### Return Types

| Return Type | Use Case |
|-------------|----------|
| `T` | Single entity (throws if not found) |
| `Optional<T>` | Optional single entity |
| `List<T>` | List of entities |
| `Set<T>` | Set of unique entities |
| `long` | Count of matching entities |
| `boolean` | Existence check |
| `Map<K, List<T>>` | Grouped results |
| `Map<K, Long>` | Count per group |
| `void` | Delete by methods |

#### GroupBy and Having

```java
@Query("select status, count(p) from Product p group by status")
Map<String, Long> countByStatus();

@Query("select category, count(p) from Product p group by category having count(p) > :min")
Map<String, Long> countByCategoryWithMinimum(@Param("min") long min);
```

#### @Query Annotation (JPQL-like)

```java
@Query("select u from User u where u.email = :email and u.age > :minAge")
List<User> findByEmailAndMinAge(@Param("email") String email, @Param("minAge") int age);

@Query("select p from Product p where p.name ilike :pattern")
List<Product> findByNamePattern(@Param("pattern") String pattern);
```

**Supported:** SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LEFT JOIN
**Predicates:** `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, LIKE, ILIKE, IN, NOT IN, BETWEEN, IS NULL, IS NOT NULL
**Parameters:** Named (`:name` with `@Param`) or positional (`?1`, `?2`)

#### Nested Property Resolution

```java
// Resolves account.email (nested property)
List<Account> findByAccountEmail(String email);
```

> Note: nested property paths are supported for persisted field access and query predicates.
> Nested `@Query` update assignments are intentionally unsupported.

---

## 🚀 Advanced Features

### Type Converters

Custom type converters allow you to map domain types to storage types:

```java
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;

public class MoneyConverter implements TypeConverter<Money, Long> {
    public Class<Money> javaType() { return Money.class; }
    public Class<Long> storageType() { return Long.class; }

    public Long toStorage(Money value) {
        return value == null ? null : value.toCents();
    }

    public Money fromStorage(Long value) {
        return value == null ? null : Money.fromCents(value);
    }
}

// Register the converter
TypeConverterRegistry.getInstance().register(new MoneyConverter());
```

### Custom ID Generation

Provide custom ID generation strategies using `@GeneratedValue(generator = "...")`:

```java
import io.memris.core.IdGenerator;
import java.util.concurrent.atomic.AtomicLong;

public class CustomIdGenerator implements IdGenerator<String> {
    private final AtomicLong counter = new AtomicLong();

    public String generate() {
        return "CUSTOM-" + counter.incrementAndGet();
    }
}

// Use in entity with CUSTOM strategy
@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.CUSTOM, generator = "customIdGenerator")
    private String id;
    
    // The generator must be registered via Spring or provided at runtime
}
```

> **Note:** Custom ID generators require external registration (e.g., via Spring Data integration) or manual lookup.

### Configuration Options

`MemrisConfiguration.builder()` supports the following options:

| Option | Default | Description |
|--------|---------|-------------|
| `tableImplementation(BYTECODE/METHOD_HANDLE)` | `BYTECODE` | Table implementation strategy |
| `pageSize(int)` | `1024` | Number of rows per page |
| `maxPages(int)` | `1024` | Maximum number of pages |
| `initialPages(int)` | `1024` | Initial number of pages |
| `enableParallelSorting(boolean)` | `true` | Enable parallel sorting for large results |
| `parallelSortThreshold(int)` | `1000` | Threshold for parallel sorting |
| `auditProvider(AuditProvider)` | `null` | Provider for @CreatedBy/@LastModifiedBy |
| `codegenEnabled(boolean)` | `true` | Enable runtime code generation |
| `enablePrefixIndex(boolean)` | `true` | Enable PREFIX index optimization |
| `enableSuffixIndex(boolean)` | `true` | Enable SUFFIX index optimization |
| `entityMetadataProvider(EntityMetadataProvider)` | `MetadataExtractor::extractEntityMetadata` | Custom metadata extraction |

Example:

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .pageSize(2048)
    .maxPages(2048)
    .enableParallelSorting(true)
    .parallelSortThreshold(500)
    .build();

MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
```

### Audit Provider

Configure audit information for `@CreatedBy` and `@LastModifiedBy` annotations:

```java
import io.memris.core.AuditProvider;
import io.memris.core.MemrisConfiguration;

AuditProvider auditProvider = () -> SecurityContextHolder.getCurrentUser();

MemrisConfiguration.builder()
    .auditProvider(auditProvider)
    .build();
```

> **Note:** Audit support is available via field naming convention (fields named `createdBy`, `lastModifiedBy`) or Spring Data modules.

### Entity Relationships

| Relationship | Annotation | Loading |
|--------------|------------|---------|
| One-to-one | `@OneToOne` | Eager |
| Many-to-one | `@ManyToOne` | Eager |
| One-to-many | `@OneToMany` | Eager |
| Many-to-many | `@ManyToMany` + `@JoinTable` | Eager |

> **Note:** Lazy loading is not supported. All relationships are eagerly fetched.

#### Example: One-to-Many / Many-to-One

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.ManyToOne;
import io.memris.core.OneToMany;
import io.memris.core.JoinColumn;
import java.util.List;

@Entity
public class Customer {
    @Id
    private Long id;
    
    private String email;
    
    @OneToMany(mappedBy = "customer")
    private List<Order> orders;
    
    // constructors, getters...
}

@Entity
public class Order {
    @Id
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
    
    private long total;
    
    // constructors, getters...
}

// Query by nested property
public interface OrderRepository extends MemrisRepository<Order> {
    List<Order> findByCustomerEmail(String email);
}
```

#### Example: Many-to-Many

```java
import io.memris.core.ManyToMany;
import io.memris.core.JoinTable;
import io.memris.core.JoinColumn;
import java.util.Set;

@Entity
public class Student {
    @Id
    private Long id;
    
    private String name;
    
    @ManyToMany
    @JoinTable(
        name = "student_course",
        joinColumn = "student_id",
        inverseJoinColumn = "course_id"
    )
    private Set<Course> courses;
    
    // constructors, getters...
}

@Entity
public class Course {
    @Id
    private Long id;
    
    private String title;
    
    @ManyToMany(mappedBy = "courses")
    private Set<Student> students;
    
    // constructors, getters...
}
```

---

## 🛠️ Requirements

- **Java Version**: 21 (required)
- **Maven**: 3.8+ (for building from source)

---

## 🔧 Build Locally

```bash
mvn -B clean install
```

### Performance Guardrails

- Dedicated perf workflow: `.github/workflows/perf.yml`
- Embedded-path benchmark: `io.memris.benchmarks.EmbeddedPathBenchmark`
- Regression checker script: `scripts/check-jmh-regression.py`
- Baseline file: `memris-core/src/jmh/resources/embedded-path-baseline.json`

### Thread-Safety Guardrails

- Dedicated thread-safety workflow: `.github/workflows/thread-safety.yml`
- Runs arena/codegen concurrency tests with forked JVMs (`forkCount > 1`, `reuseForks=false`)
- Enables JUnit parallel class execution in the dedicated lane

---

## 📚 Documentation

**Online Documentation**: [https://thejuampi.github.io/memris/](https://thejuampi.github.io/memris/)

Local documentation files:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Architecture and design
- [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) — Development and testing guidelines
- [`docs/QUERY.md`](docs/QUERY.md) — Query method reference and operators
- [`docs/SPRING_DATA.md`](docs/SPRING_DATA.md) — Spring Data integration details
- [`docs/CONCURRENCY.md`](docs/CONCURRENCY.md) — Concurrency model and guarantees

---

## 🤝 Contributing

Contributions welcome — open an issue or a PR. See [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) for the development workflow and test guidelines.

---

## 📄 License

This project is licensed under the **MIT License** — see the [`LICENSE`](LICENSE) file for details.

---

## 👤 Author

**Juan Pablo Abelardo Lescano** — [@Thejuampi](https://github.com/Thejuampi)
