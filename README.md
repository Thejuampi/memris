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
  <version>0.1.4</version>
</dependency>
```

### Gradle (Groovy)

```groovy
implementation 'io.github.thejuampi:memris:0.1.4'
```

### Gradle (Kotlin)

```kotlin
implementation("io.github.thejuampi:memris:0.1.4")
```

---

## 📖 Complete Example

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

public class Main {
    static void main(String[] args) throws Exception {
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
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
| **Low-latency requirements** | Zero reflection in hot paths, ~1ns ByteBuddy overhead |
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
| `@Index` | Creates HASH or BTREE index |
| `@GeneratedValue` | Auto ID generation |
| `@OneToOne` | One-to-one relationship |
| `@ManyToOne` | Many-to-one relationship |
| `@OneToMany` | One-to-many relationship |
| `@ManyToMany` | Many-to-many relationship |
| `@JoinColumn` | Specifies foreign key column |
| `@Query` | JPQL-like query string |
| `@Param` | Named parameter binding |

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
| `BigDecimal` | `String[]` | TYPE_BIG_DECIMAL (EQ/NE/IN/NOT_IN) |
| `BigInteger` | `String[]` | TYPE_BIG_INTEGER (EQ/NE/IN/NOT_IN) |

### Query Method Patterns

**Comparison Operators**
- `findByAgeEquals(int)` / `findByAge(int)` — Equality
- `findByAgeNotEqual(int)` / `findByAgeNot(int)` — Inequality
- `findByAgeGreaterThan(int)` / `findByAgeAfter(int)` — Greater than
- `findByAgeLessThan(int)` / `findByAgeBefore(int)` — Less than
- `findByAgeBetween(int, int)` — Range query

**String Operators**
- `findByNameContaining(String)` — Substring match
- `findByNameStartingWith(String)` — Prefix match
- `findByNameEndingWith(String)` — Suffix match
- `findByNameLike(String)` — Pattern match
- `findByNameContainingIgnoreCase(String)` — Case-insensitive

**Null & Boolean**
- `findByDepartmentIsNull()` / `findByDepartmentIsNotNull()`
- `findByActiveTrue()` / `findByActiveFalse()`

**Collection**
- `findBySkuIn(List<String>)` — IN query
- `findByStatusNotIn(List<String>)` — NOT IN query

**Logical Operators**
- `findByCustomerIdAndStatus(Long, String)` — AND
- `findByPriceLessThanOrStockEqual(int, long)` — OR

**Ordering & Limiting**
- `findByPriceOrderByPriceDesc(long)` — ORDER BY
- `findTopByOrderByPriceDesc()` — Top/First

**Return Types**
- `List<T>`, `Optional<T>`, `T` — Find methods
- `Set<T>` — Find methods (unique results)
- `long` — Count methods
- `boolean` — Exists methods
- `Map<K, V>` — Grouped results

---

## 🛠️ Requirements

- **Java Version**: 21 (required)
- **Maven**: 3.8+ (for building from source)

---

## 🔧 Build Locally

```bash
mvn -B clean install
```

---

## 📚 Documentation

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
