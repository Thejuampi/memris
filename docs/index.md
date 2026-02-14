# Getting Started with Memris

Welcome to Memris - a high-performance in-memory data storage for Java with Spring Boot support.

## What is Memris?

Memris is a blazingly fast, concurrency-safe, in-memory storage engine designed for Java applications. It provides:

- **Zero-reflection query execution** - Sub-microsecond query performance via pre-compiled MethodHandles
- **Columnar storage** - Paged primitive arrays (PageColumnInt, PageColumnLong, PageColumnString) for efficient memory usage and SIMD-friendly access
- **Spring Boot integration** - Auto-configuration and Spring Data repository support
- **JPQL queries** - Full @Query annotation support with SELECT, UPDATE, DELETE statements
- **Method name derivation** - Spring Data standard query methods (findBy..., countBy..., existsBy...)
- **Thread-safe concurrency** - Multi-reader, multi-writer with row-level seqlock synchronization
- **Rich indexing** - Hash, Range, Prefix, Suffix, and Composite indexes for O(1) to O(log n) lookups

## Quick Links

- [Installation](getting-started/installation.md) - Get started with Maven or Gradle
- [Quick Start](getting-started/quick-start.md) - Your first Memris application in 5 minutes
- [Configuration](getting-started/configuration.md) - Tune performance and behavior
- [Spring Boot Setup](getting-started/spring-boot-setup.md) - Spring Boot integration guide

## Spring Boot Integration

Memris provides first-class Spring Boot support with auto-configuration:

```java
@SpringBootApplication
@EnableMemrisRepositories
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

## Query Methods

### Method Name Derivation

```java
public interface UserRepository extends MemrisRepository<User, Long> {
    // Equality
    List<User> findByEmail(String email);
    Optional<User> findByEmailAndStatus(String email, String status);
    
    // Comparisons
    List<User> findByAgeGreaterThan(int age);
    List<User> findByAgeBetween(int min, int max);
    
    // Collections
    List<User> findByStatusIn(Collection<String> statuses);
    
    // String matching
    List<User> findByNameContaining(String name);
    List<User> findByNameStartingWith(String prefix);
    List<User> findByNameEndingWith(String suffix);
    
    // Boolean
    List<User> findByActiveTrue();
    
    // Null checks
    List<User> findByDeletedAtIsNull();
    
    // Ordering and limiting
    List<User> findByStatusOrderByCreatedDesc(String status);
    Optional<User> findFirstByActiveTrueOrderByCreatedDesc();
    List<User> findTop10ByStatus(String status);
    
    // Count and exists
    long countByStatus(String status);
    boolean existsByEmail(String email);
    
    // Delete
    int deleteByStatus(String status);
}
```

### JPQL Queries

```java
public interface UserRepository extends MemrisRepository<User, Long> {
    @Query("SELECT u FROM User u WHERE u.email = :email")
    Optional<User> findByEmailQuery(@Param("email") String email);
    
    @Query("SELECT u FROM User u WHERE u.age BETWEEN :min AND :max ORDER BY u.age")
    List<User> findByAgeRange(@Param("min") int min, @Param("max") int max);
    
    @Query("SELECT COUNT(u) FROM User u WHERE u.active = true")
    long countActiveUsers();
    
    @Modifying
    @Query("UPDATE User u SET u.status = :status WHERE u.id = :id")
    int updateStatus(@Param("id") Long id, @Param("status") String status);
    
    // Projections
    record UserSummary(Long id, String email, String status) {}
    
    @Query("SELECT u.id AS id, u.email AS email, u.status AS status FROM User u")
    List<UserSummary> findAllSummaries();
}
```

## Index Types

Memris supports multiple index types for different query patterns:

| Index | Use Case | Complexity |
|-------|----------|------------|
| `HashIndex` | Equality queries (`=`, `IN`) | O(1) lookup |
| `RangeIndex` | Range and comparison queries (`>`, `<`, `BETWEEN`) | O(log n) lookup, O(log n + m) range |
| `StringPrefixIndex` | STARTING_WITH queries | O(k) where k = prefix length |
| `StringSuffixIndex` | ENDING_WITH queries | O(k) where k = suffix length |
| `CompositeHashIndex` | Multi-column equality | O(1) lookup |
| `CompositeRangeIndex` | Multi-column range | O(log n) lookup, O(log n + m) range |

```java
@Entity
public class User {
    @Id
    private Long id;
    
    @Index(IndexType.HASH)
    private String email;
    
    @Index(IndexType.RANGE)
    private Integer age;
    
    @Index(IndexType.PREFIX)
    private String name;
}
```

## Storage Architecture

Memris uses columnar storage with paged primitive arrays:

```
GeneratedTable
    -> PageColumnInt[] intColumns
    -> PageColumnLong[] longColumns
    -> PageColumnString[] stringColumns
    -> volatile int published (watermark for safe reads)
```

Benefits:
- **No boxing**: Direct primitive array access
- **SIMD-friendly**: JIT can vectorize scans
- **Lazy allocation**: Pages created on demand via CAS
- **Cache locality**: Sequential access within pages

## Important Limitations

!!! warning "No Transaction Support"
    Memris does **not** support transactions. Each save/update/delete operation is atomic and immediately visible to other threads.

!!! warning "No Native Queries"
    Native SQL queries are not supported. Use JPQL with @Query annotation instead.

!!! warning "No Lazy Loading"
    All entity relationships are eagerly loaded.

## Documentation Structure

### Getting Started
Learn the basics of Memris with step-by-step guides.

### Spring Boot Integration
Deep dive into Spring Boot modules, auto-configuration, and examples.

### Reference
Complete reference documentation for architecture, queries, concurrency, and more.

- [Architecture](ARCHITECTURE.md) - System design and components
- [Query Reference](QUERY.md) - Query operators and syntax
- [Concurrency Model](CONCURRENCY.md) - Thread safety and seqlock details
- [Design Documents](design/storage.md) - Storage, selection, indexes, query parsing

### Examples
Practical examples from basic CRUD to complex e-commerce scenarios.

## Performance Highlights

- **Query execution**: ~1-5 nanoseconds overhead (pre-compiled)
- **Concurrent operations**: Lock-free ID generation and index lookups
- **Memory efficiency**: Columnar storage with dense primitive arrays
- **Scan performance**: SIMD-friendly paged arrays

See [Benchmark Baselines](BENCHMARK_BASELINES.md) for detailed performance metrics.

## Contributing

Contributions are welcome! See the [Development Guide](DEVELOPMENT.md) for build instructions and coding standards.

## License

Memris is licensed under the MIT License.
