# Memris

## The Vibe

**Memris** = "Memory" + "Iris" — an in-memory storage engine that can *see* through the heap instantly. Like iris (the eye), it provides vision/insight into your data. Like iris (a flower), it blooms fast.

> "Iris suggests looking through data/vision. It sounds like an engine that can 'see' through the heap instantly."

## What is Memris?

**Memris** is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with SIMD vectorized execution and Spring Data integration.

### Performance Results (10M rows, 228MB)

| Operation | Time | Throughput | Selectivity |
|-----------|------|------------|-------------|
| Full table scan | 15-16ms | **14.3 GB/s** | 100% |
| Point filter (status='pending') | 46ms | - | 50% |
| Range query (amount 10k-20k) | 9-23ms | - | 10% |
| SelectionVector create | 16-23ms | - | - |
| SelectionVector enumerate | 24-30ms | - | - |

## Architecture

### Architecture

For detailed architecture information, see [CLAUDE.md](CLAUDE.md). Here's a high-level overview:

**Storage Layer:**
- `FfmTable` - FFM MemorySegment-backed table with type-specific columns
- SIMD vectorized column storage for primitive types
- `FfmStringColumn` - Variable-length string storage

**Spring Data Integration:**
- `MemrisRepositoryFactory` - Creates repositories via ByteBuddy bytecode generation
- Dynamic repository implementation with type-safe query methods
- `QueryMethodParser` - Parses JPA-style query method names

**Selection Pipeline:**
- `SelectionVector` - Row selection result (sparse or dense)
- Optimized for O(1) operations with automatic upgrade logic

**Query Execution:**
- `PlanNode` - Scan/Filter/Join/Sort/Limit operators
- SIMD-accelerated predicate evaluation

For detailed component descriptions, design principles, and development guidelines, see:
- [CLAUDE.md](CLAUDE.md) - Comprehensive architecture and design
- [AGENTS.md](AGENTS.md) - Development guidelines and best practices

### Primitive-Only Design (JVM Optimized)
- `IntEnumerator` / `LongEnumerator` - No boxing
- All classes `final` for inlining
- O(1) operations preferred, O(log n) second, O(n) forbidden

## Java Runtime Requirements

- **Java Version**: 21 (required)
- **Preview Features**: `--enable-preview`
- **Modules**: `jdk.incubator.vector` (for SIMD), `java.base` (FFM)
- **Native Access**: `--enable-native-access=ALL-UNNAMED`

## Quick Start

```java
// Define repository interface with JPA query methods
interface UserRepository extends MemrisRepository<User> {
    List<User> findByLastname(String lastname);
    List<User> findByAgeGreaterThan(int age);
    List<User> findByStatusIn(Collection<String> statuses);
}

// Create repository
try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
    UserRepository repo = factory.createJPARepository(UserRepository.class);
    
    // Save entities
    repo.save(new User(1, "Alice", 30));
    repo.save(new User(2, "Bob", 25));
    
    // Query using JPA query methods
    List<User> alice = repo.findByLastname("Alice");
    List<User> adults = repo.findByAgeGreaterThan(18);
    List<User> vip = repo.findByStatusIn(List.of("gold", "platinum"));
}
```

### Entity Class

```java
final class User {
    int id;
    String name;
    int age;
    
    User() {}
    User(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }
}
```

## Design Principles

Memris is built on these core design principles for maximum performance:

1. **O(1) First** - All hot path operations must be constant time
2. **Primitive-Only APIs** - No boxing, no Iterator, no Iterable in hot paths
3. **SIMD Vectorization** - Panama Vector API for batch processing
4. **Memory Efficiency** - FFM MemorySegment for off-heap storage
5. **JVM Optimization** - Final classes for inlining, type switches for dispatch
6. **Type Safety** - Compile-time type-safe queries with no string-based operations
7. **Extensible** - Custom type converters for unsupported types

For detailed implementation guidelines, see [AGENTS.md](AGENTS.md) and [CLAUDE.md](CLAUDE.md).

## Current Limitations

See [SPRING_DATA_ROADMAP.md](SPRING_DATA_ROADMAP.md) for detailed status and implementation plan.

### Transaction Support
**NOT SUPPORTED**: Memris uses **eventual consistency** instead of ACID transactions.
- No `@Transactional` annotation
- No rollback mechanisms
- Changes are immediately visible to other threads
- Use `@Version` (optimistic locking) for concurrency control

### Join Tables with Non-Numeric IDs

**CRITICAL**: Join tables (`@OneToMany`, `@ManyToMany`) currently only support numeric ID types (`int`, `long`, `Integer`, `Long`).

Entities with UUID, String, or other non-numeric IDs **cannot use join table relationships**. This is a fundamental architectural limitation that will be fixed in a future release.

**Why this limitation exists:**
- Join tables are hardcoded with `int.class` columns for storing entity references
- Converting UUID (128 bits) or String IDs to `int` loses data and is incorrect
- The proper fix requires join tables to dynamically match the ID type of referenced entities

**Workaround:**
- Use numeric IDs (`int` or `long`) for entities that participate in `@OneToMany` or `@ManyToMany` relationships
- For UUID/String ID entities, use manual relationship management or foreign key fields instead of join tables

**Planned fix:**
- Join table columns will match the ID type of the entities they reference
- UUID IDs will be stored as two `long` columns (128 bits total) for performance
- String IDs will be stored in `String` columns with proper indexing

## Type Conversion & Extensibility

Memris supports all Java primitives and common types out-of-the-box:

### Supported Types

| Type | Storage Type | Notes |
|------|---------------|-------|
| `int`, `Integer` | `int` | Direct mapping |
| `long`, `Long` | `long` | Direct mapping |
| `boolean`, `Boolean` | `boolean` | Direct mapping |
| `byte`, `Byte` | `byte` | Direct mapping |
| `short`, `Short` | `short` | Direct mapping |
| `float`, `Float` | `float` | Direct mapping |
| `double`, `Double` | `double` | Direct mapping |
| `char`, `Character` | `char` | Direct mapping |
| `String` | `String` | Variable-length storage |
| `BigDecimal` | `String` | ISO format, no precision loss |
| `BigInteger` | `String` | ISO format, arbitrary precision |
| `LocalDate` | `String` | ISO format |
| `LocalDateTime` | `String` | ISO format |
| `LocalTime` | `String` | ISO format |
| `Instant` | `String` | ISO format |
| `java.sql.Date` | `String` | ISO format |
| `java.sql.Timestamp` | `String` | ISO format |

### Custom Type Support

Clients can register custom TypeConverters for unsupported types:

```java
// Define custom converter
class UUIDConverter implements TypeConverter<UUID, String> {
    @Override public Class<UUID> getJavaType() { return UUID.class; }
    @Override public Class<String> getStorageType() { return String.class; }
    @Override public String toStorage(UUID value) { return value.toString(); }
    @Override public UUID fromStorage(String value) { return UUID.fromString(value); }
}

// Register converter before creating repository
TypeConverterRegistry.getInstance().register(new UUIDConverter());

// Now UUID fields work automatically
class User {
    UUID id;
    String name;
}
```

## Project Structure

```
memris/
├── README.md
├── AGENTS.md
├── pom.xml
├── docs/
│   ├── decisions.md
│   └── design/
│       ├── 001-core-architecture.md
│       ├── 002-selection-pipeline.md
│       └── 003-jpa-query-method-parser.md
└── memris-core/
    ├── pom.xml
    └── src/
        ├── main/java/io/memris/
        │   ├── kernel/
        │   │   ├── RowId.java
        │   │   ├── Predicate.java
        │   │   ├── PlanNode.java
        │   │   ├── LongEnumerator.java
        │   │   ├── IntEnumerator.java
        │   │   └── selection/
        │   │       ├── SelectionVector.java
        │   │       ├── IntSelection.java
        │   │       ├── BitsetSelection.java
        │   │       └── SelectionVectorFactory.java
        │   ├── storage/
        │   │   ├── MemrisStore.java
        │   │   └── ffm/
        │   │       ├── FfmTable.java
        │   │       ├── FfmIntColumn.java
        │   │       ├── FfmLongColumn.java
        │   │       ├── FfmBooleanColumn.java
        │   │       ├── FfmByteColumn.java
        │   │       ├── FfmShortColumn.java
        │   │       ├── FfmFloatColumn.java
        │   │       ├── FfmDoubleColumn.java
        │   │       ├── FfmCharColumn.java
        │   │       └── FfmStringColumn.java
        │   ├── index/
        │   │   ├── HashIndex.java
        │   │   └── RangeIndex.java
        │   ├── query/
        │   │   └── SimpleExecutor.java
        │   ├── spring/
        │   │   ├── MemrisRepository.java
        │   │   ├── MemrisRepositoryFactory.java
        │   │   ├── QueryMethodParser.java
        │   │   └── converter/
        │   │       ├── TypeConverter.java
        │   │       └── TypeConverterRegistry.java
        │   └── benchmarks/
        │       ├── BenchmarkRunner.java
        │       ├── FullBenchmark.java
        │       └── ThroughputBenchmark.java
        └── test/java/io/memris/
            ├── storage/ffm/
            │   ├── FfmIntColumnScanBetweenTest.java
            │   └── FfmTableScanInTest.java
            └── spring/
                ├── MemrisRepositoryFactoryTest.java
                ├── MemrisRepositoryIntegrationTest.java
                ├── QueryMethodParserTest.java
                └── ECommerceRealWorldTest.java
```

## Spring Data Integration

`MemrisRepositoryFactory` creates Spring Data JPA-style repositories with dynamic query derivation:

```java
// Define extended repository interface
interface UserRepository extends MemrisRepository<User> {
    List<User> findByLastname(String lastname);                    // EQ
    List<User> findByAgeGreaterThan(int age);                      // GT
    List<User> findByStatusIn(Collection<String> statuses);       // IN
    List<User> findByNameContaining(String name);                  // CONTAINING
    List<User> findByActiveTrue();                                 // IS_TRUE
    List<User> findByLastnameNot(String lastname);                 // NEQ
    List<User> findByAgeOrderByLastnameDesc(int age);              // with ordering
    List<User> findFirst10ByActiveTrue();                          // with limit
}

// Use with factory
try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
    UserRepository userRepo = factory.createJPARepository(UserRepository.class);
    List<User> vips = userRepo.findByActiveTrue();
}
```

#### Supported JPA Keywords

| Keyword | Description | Example |
|---------|-------------|---------|
| `And`, `Or` | Logical operators | `findByLastnameAndFirstname` |
| `Is`, `Equals` | Equality check | `findByEmail`, `findByEmailIs`, `findByEmailEquals` |
| `Between` | Range query | `findByAgeBetween(18, 65)` |
| `LessThan`, `LessThanEqual` | Comparison | `findByAgeLessThan(18)` |
| `GreaterThan`, `GreaterThanEqual` | Comparison | `findByAgeGreaterThan(18)` |
| `After`, `Before` | Date/Time | `findByCreatedAtAfter(date)` |
| `IsNull`, `Null` | Null check | `findByEmailIsNull()`, `findByEmailNull()` |
| `IsNotNull`, `NotNull` | Not-null check | `findByEmailIsNotNull()` |
| `Like`, `NotLike` | Pattern match | `findByNameLike("%John%")` |
| `StartingWith`, `StartsWith` | String prefix | `findByNameStartingWith("John")` |
| `EndingWith`, `EndsWith` | String suffix | `findByNameEndingWith("son")` |
| `Containing`, `Contains` | String contains | `findByNameContaining("ohn")` |
| `In`, `NotIn` | Collection membership | `findByStatusIn(List.of("A", "B"))` |
| `True`, `False` | Boolean check | `findByActiveTrue()` |
| `Not`, `NotEqual` | Negation | `findByLastnameNot("Doe")` |
| `IgnoreCase`, `AllIgnoreCase` | Case insensitive | `findByNameIgnoreCase("john")` |
| `OrderBy{Prop}{Asc\|Desc}` | Sorting | `findByAgeOrderByLastnameDesc` |
| `Distinct` | Deduplication | `findDistinctByLastname` |
| `First{n}`, `Top{n}` | Limit results | `findFirst10ByActiveTrue` |
| `Count` | Count query | `countByLastname` |
| `Exists` | Exists query | `existsByEmail` |
| `Delete`, `Remove` | Delete query | `deleteByLastname` |

## Running Benchmarks

Memris provides two types of benchmarks:

### Throughput Benchmark
Measures maximum throughput operations:
```bash
mvn compile
java --enable-preview --add-modules jdk.incubator.vector -cp memris-core/target/classes io.memris.benchmarks.ThroughputBenchmark
```

### Latency Benchmark (JMH)
Microbenchmark suite for detailed latency analysis:
```bash
mvn clean compile
java --enable-preview --add-modules jdk.incubator.vector -cp memris-core/target/classes:jmh-benchmarks.jar io.memris.benchmarks.MemrisBenchmarks
```

## Running Tests

```bash
mvn test -X                 # All tests (show warnings only)
mvn test -X -Dtest=ClassName   # Single test class (show warnings only)
mvn test -X -Dtest=ClassName#methodName  # Single test (show warnings only)
```

For testing guidelines and best practices, see [AGENTS.md](AGENTS.md).

## License

MIT
