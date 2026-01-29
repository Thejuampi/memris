# Troubleshooting Guide

This document provides troubleshooting guidance for common issues when working with Memris.

## Join Tables with Non-Numeric IDs

**Problem:** Entities with UUID, String, or other non-numeric IDs fail in `@OneToMany` or `@ManyToMany` relationships

**Symptoms:**
```java
@Entity
class Order {
    UUID id;  // Non-numeric ID
    @OneToMany(mappedBy = "order")
    List<OrderItem> items;  // ❌ Fails
}
```

**Root Cause:**
Join tables are hardcoded with `int.class` columns for storing entity references. Converting UUID (128 bits) or String IDs to `int` loses data.

**Current Status:**
- **Status:** ⚠️ KNOWN LIMITATION
- **Impact:** Only numeric ID types (`int`, `long`, `Integer`, `Long`) supported for join tables
- **Planned Fix:** Store UUID as 2 `long` columns (128 bits total)

**Workarounds:**

1. **Use Numeric IDs**
   ```java
   // ✅ Recommended: Use numeric IDs
   @Entity
   class Order {
       long id;  // Numeric ID
       @OneToMany(mappedBy = "order")
       List<OrderItem> items;  // ✅ Works
   }
   ```

2. **Manual Foreign Key Fields**
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

3. **Avoid Join Tables**

   Use separate repositories and manual query combination:
   ```java
   // Instead of: order.getItems()
   List<OrderItem> items = orderItemRepo.findByOrderId(order.getId());
   ```

## Performance Issues

**Problem:** Query takes longer than expected

**Diagnostic Steps:**

1. **Check if query uses index:**
   ```java
   // Good: Uses index
   List<User> users = repo.findByEmail("john@example.com");

   // Bad: Full table scan
   List<User> users = repo.findByAgeGreaterThan(18);  // No index on age
   ```

2. **Verify SIMD vectorization is enabled:**
   - Check Java 21 preview features: `--enable-preview --add-modules jdk.incubator.vector`
   - Vector API requires proper module loading

3. **Profile with JMH:**
   ```bash
   java --enable-preview --add-modules jdk.incubator.vector \
     -cp memris-core/target/classes:jmh-benchmarks.jar \
     io.memris.benchmarks.MemrisBenchmarks
   ```

**Problem:** OutOfMemoryError or excessive memory usage

**Solutions:**

1. **Increase JVM heap:**
   ```bash
   java -Xmx4g -jar your-app.jar
   ```

2. **Use paging for large result sets:**
   ```java
   // Instead of retrieving all:
   // List<User> all = repo.findAll();

   // Use pagination (when LIMIT is implemented):
   // List<User> page = repo.findTop100ByOffset(0);
   ```

3. **Clean up resources:**
   ```java
   // Tables are heap-based, no manual resource cleanup needed
   // GeneratedTable instances are garbage collected normally
   UserTable table = new UserTable(pageSize, maxPages);
   // Use table...
   // No explicit close() needed
   ```

## Entity Annotation Issues

**Problem:** Entity not recognized, fields not persisted

**Common Issues:**

1. **Missing @Entity Annotation**
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

2. **Missing Default Constructor**
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

3. **Missing Setter Methods**
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

## Known Limitations

### Query System Architecture

The codebase contains two parallel query parsing systems:

1. **Zero-Reflection Runtime** (new architecture): QueryPlanner + QueryCompiler + RepositoryRuntime
   - Parses: EQ, NE, GT, LT, GTE, LTE, BETWEEN, IGNORE_CASE
   - Does NOT parse: IN, LIKE, STARTING_WITH, ENDING_WITH, CONTAINING, OR, ORDER_BY, DISTINCT

2. **QueryMethodLexer + QueryPlanner**: Full JPA specification coverage
   - Parses all 24+ JPA operators including IN, LIKE, ORDER BY, etc.
   - Used by HeapRuntimeKernel for query execution

### Current Blockers

1. **Join Tables with UUID/String IDs** - Only numeric IDs supported
2. **Advanced Query Operators** - IN, LIKE, ORDER BY not implemented in QueryPlanner

### Test Coverage

When tests can run, verify:
- `QueryPlannerTest.java` - Tests QueryPlanner parsing (8 tests)
- `QueryMethodParserTest.java` - Comprehensive JPA specification tests (640 lines)
- `ECommerceRealWorldTest.java` - Real-world e-commerce domain tests (15 tests)

---

*For development guidelines, see [../CLAUDE.md](../CLAUDE.md)*
