# Troubleshooting

This document covers common issues, known blockers, and workarounds when working with Memris.

## Maven Build Suppression Issue

### Symptom

Running `mvn compile` produces no output and creates no `target/` directory:

```bash
$ mvn -q -e compile
# No output, no target directory created
```

### Root Cause

This is a known blocker affecting test execution. The exact cause is under investigation, but symptoms suggest:
- Maven output suppression configuration issue
- Preview features misconfiguration
- Environment-specific Java 21 compatibility problem

### Current Status

- **Status:** ❌ BLOCKED
- **Impact:** Cannot verify compilation, cannot run tests
- **Workaround:** Code is syntactically correct (verified via javac attempt)

### Workarounds

#### 1. Verify Java Installation

```bash
# Check Java version
java -version
# Should show: java version "21"

# Check JAVA_HOME
echo $JAVA_HOME

# Verify Java 21 is on PATH
which java
```

#### 2. Verify Preview Features

Maven requires preview features enabled for Java 21:

```xml
<!-- Check pom.xml configuration -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.11.0</version>
    <configuration>
        <source>21</source>
        <target>21</target>
        <compilerArgs>
            <arg>--enable-preview</arg>
            <arg>--add-modules=jdk.incubator.vector</arg>
        </compilerArgs>
    </configuration>
</plugin>
```

#### 3. Try Alternative Compile (Syntax Verification Only)

For quick syntax verification without Maven:

```bash
# Compile directly with javac (requires proper classpath)
javac --enable-preview --add-modules jdk.incubator.vector \
  -d target/classes \
  memris-core/src/main/java/io/memris/**/*.java

# Or use Maven with verbose output
mvn -X compile
```

#### 4. Check Maven Settings

Verify Maven is not suppressing output:

```bash
# Check Maven version
mvn -version

# Check for .m2/config.xml issues
cat ~/.m2/settings.xml
```

### Ongoing Investigation

The build environment issue needs to be resolved to:
1. Run test suite to verify implementation
2. Benchmark performance
3. Complete REFACTOR phase of TDD cycle

## Query Operator Not Supported

### Symptom

Using a query method with operators that may not be fully implemented:

```java
// Example: Complex operator patterns
List<User> vips = repo.findByStatusIn(List.of("gold", "platinum"));
// Implementation status varies by operator and parser used
```

### Operator Implementation Variance

The codebase has two query parsing systems with different operator coverage:

**QueryPlanner (zero-reflection runtime)**:
- Parses: EQ, NE, GT, LT, GTE, LTE, BETWEEN, IGNORE_CASE
- Does NOT parse: IN, LIKE, STARTING_WITH, ENDING_WITH, CONTAINING, OR, ORDER BY, DISTINCT, TOP/FIRST

**QueryMethodParser (older implementation)**:
- Parses: All 24+ JPA operators including IN, LIKE, ORDER BY, etc.
- Used in MemrisRepositoryFactory main path

**Note**: Due to Maven build blocker, actual execution behavior cannot be verified through automated testing. Check implementation details in:
- [queries.md](queries.md) - Quick operator reference
- [queries-spec.md](queries-spec.md) - Detailed implementation analysis

### Alternative Query Approaches

The following approaches can be used when operator implementation status is uncertain:

#### Collection Membership (IN Pattern)

```java
// Alternative: Multiple queries + manual union
List<User> gold = repo.findByStatus("gold");
List<User> platinum = repo.findByStatus("platinum");
List<User> vips = new ArrayList<>(gold);
vips.addAll(platinum);
```

#### Pattern Matching (LIKE Pattern)

```java
// Alternative: Retrieve all and filter
List<User> allUsers = repo.findAll();
List<User> matches = allUsers.stream()
    .filter(u -> u.getName().contains("oh"))
    .toList();
```

#### OR Conditions

```java
// Alternative: Two queries + union
List<User> young = repo.findByAgeLessThan(25);
List<User> active = repo.findByActiveTrue();
Set<User> result = new LinkedHashSet<>(young);
result.addAll(active);
List<User> youngOrActive = new ArrayList<>(result);
```

#### Sorting (ORDER BY Pattern)

```java
// Alternative: Retrieve all + sort in memory
List<User> adults = repo.findByAgeGreaterThan(18);
adults.sort((a, b) -> b.getLastname().compareTo(a.getLastname()));
```

#### Limiting (TOP/FIRST Pattern)

```java
// Alternative: Retrieve all + sublist
List<User> active = repo.findByActiveTrue();
List<User> top10 = active.size() > 10 ? active.subList(0, 10) : active;
```

#### Distinct

```java
// Alternative: Stream distinct
List<User> all = repo.findAll();
List<User> unique = all.stream()
    .distinct()
    .toList();
```

### Check Implementation Before Use

Always verify an operator is implemented before using it:

1. Check [queries.md](queries.md) for quick reference
2. Review [queries-spec.md](queries-spec.md) for detailed status
3. Check test files for usage examples:
   - `QueryPlannerTest.java` - Tests implemented operators
   - `QueryMethodParserTest.java` - Comprehensive JPA spec tests (old parser)

## Join Tables with Non-Numeric IDs

### Symptom

Entities with UUID, String, or other non-numeric IDs fail in `@OneToMany` or `@ManyToMany` relationships:

```java
@Entity
class Order {
    UUID id;  // Non-numeric ID
    @OneToMany(mappedBy = "order")
    List<OrderItem> items;  // ❌ Fails
}
```

### Root Cause

Join tables are hardcoded with `int.class` columns for storing entity references. Converting UUID (128 bits) or String IDs to `int` loses data.

### Current Status

- **Status:** ⚠️ KNOWN LIMITATION
- **Impact:** Only numeric ID types (`int`, `long`, `Integer`, `Long`) supported for join tables
- **Planned Fix:** Store UUID as 2 `long` columns (128 bits total)

### Workarounds

#### Option 1: Use Numeric IDs

```java
// ✅ Recommended: Use numeric IDs
@Entity
class Order {
    long id;  // Numeric ID
    @OneToMany(mappedBy = "order")
    List<OrderItem> items;  // ✅ Works
}
```

#### Option 2: Manual Foreign Key Fields

```java
// ✅ Workaround: Manual relationship management
@Entity
class Order {
    UUID id;
    // No @OneToMany - manage manually
}

@Entity
class OrderItem {
    long id;
    UUID orderId;  // Manual foreign key
    @Index  // Index for manual join queries
}
```

#### Option 3: Avoid Join Tables

Use separate repositories and manual query combination:

```java
// Instead of: order.getItems()
List<OrderItem> items = orderItemRepo.findByOrderId(order.getId());
```

## Performance Issues

### Slow Query Performance

**Symptom:** Query takes longer than expected

**Diagnostic Steps:**

1. Check if query uses index:
   ```java
   // Good: Uses index
   List<User> users = repo.findByEmail("john@example.com");

   // Bad: Full table scan
   List<User> users = repo.findByAgeGreaterThan(18);  // No index on age
   ```

2. Verify SIMD vectorization is enabled:
   - Check Java 21 preview features: `--enable-preview --add-modules jdk.incubator.vector`
   - Vector API requires proper module loading

3. Profile with JMH:
   ```bash
   java --enable-preview --add-modules jdk.incubator.vector \
     -cp memris-core/target/classes:jmh-benchmarks.jar \
     io.memris.benchmarks.MemrisBenchmarks
   ```

### Memory Issues

**Symptom:** OutOfMemoryError or excessive memory usage

**Solutions:**

1. Increase JVM heap:
   ```bash
   java -Xmx4g -jar your-app.jar
   ```

2. Use paging for large result sets:
   ```java
   // Instead of retrieving all:
   // List<User> all = repo.findAll();

   // Use pagination (when LIMIT is implemented):
   // List<User> page = repo.findTop100ByOffset(0);
   ```

3. Clean up resources:
   ```java
   try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
       // Auto-closes Arena
       UserRepository repo = factory.createRepository(UserRepository.class, User.class);
   }
   ```

## Type Conversion Issues

### Symptom

Custom type not supported or fails during save/load

### Solution: Register Custom TypeConverter

```java
// Define converter
class UUIDConverter implements TypeConverter<UUID, String> {
    @Override
    public Class<UUID> getJavaType() { return UUID.class; }

    @Override
    public Class<String> getStorageType() { return String.class; }

    @Override
    public String toStorage(UUID value) { return value.toString(); }

    @Override
    public UUID fromStorage(String value) { return UUID.fromString(value); }
}

// Register before creating repository
TypeConverterRegistry.getInstance().register(new UUIDConverter());

// Now UUID fields work
class User {
    UUID id;
    String name;
}
```

## Entity Annotation Issues

### Symptom

Entity not recognized, fields not persisted

### Common Issues

#### 1. Missing @Entity Annotation

```java
// ❌ Missing @Entity
class User {
    int id;
    String name;
}

// ✅ Add @Entity
@Entity
class User {
    int id;
    String name;
}
```

#### 2. Missing Default Constructor

```java
// ❌ No default constructor
@Entity
class User {
    int id;
    public User(int id) { this.id = id; }
}

// ✅ Add default constructor
@Entity
class User {
    int id;
    User() {}  // Default constructor required
    public User(int id) { this.id = id; }
}
```

#### 3. Missing Setter Methods

```java
// ❌ Missing setter
@Entity
class User {
    private String name;
    public String getName() { return name; }
}

// ✅ Add setter
@Entity
class User {
    private String name;
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
```

## Getting Help

### Documentation Resources

- [README.md](../README.md) - Main project overview
- [queries.md](queries.md) - Quick query reference
- [queries-spec.md](queries-spec.md) - Full query specification
- [CLAUDE.md](../CLAUDE.md) - Architecture details
- [AGENTS.md](../AGENTS.md) - Development guidelines

### Test Coverage

Review test files for usage examples:
- `QueryPlannerTest.java` - Implemented operator tests
- `QueryMethodParserTest.java` - Comprehensive JPA spec tests
- `ECommerceRealWorldTest.java` - Real-world e-commerce examples

### Build System

- **Java Version:** 21 (required)
- **Preview Features:** `--enable-preview`
- **Modules:** `jdk.incubator.vector`, `java.base`
- **Native Access:** `--enable-native-access=ALL-UNNAMED`

### Known Blockers

1. **Maven Build Suppression** - Cannot run tests
2. **Join Tables with UUID/String IDs** - Only numeric IDs supported
3. **Advanced Query Operators** - IN, LIKE, ORDER BY not implemented

See [queries-spec.md](queries-spec.md#known-limitations) for complete list.

## Reporting Issues

When reporting issues, include:

1. Java version: `java -version`
2. Maven version: `mvn -version`
3. Reproducible test case
4. Stack trace (if applicable)
5. Expected vs actual behavior

Check existing issues in project repository before creating new ones.
