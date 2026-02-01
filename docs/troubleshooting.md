# Troubleshooting Guide

This document provides troubleshooting guidance for common issues when working with Memris.

## Concurrency Issues

### Fixed: Major Race Conditions (Resolved)

**Status:** ✅ All major concurrency race conditions have been fixed

The following concurrency issues have been resolved in recent updates:

1. **Free-List Race Condition** - Fixed with lock-free stack implementation
   - Multiple threads no longer corrupt data when reusing deleted rows
   - Location: `AbstractTable.java` (fixed in commit 0d2e6a8)

2. **RepositoryRuntime ID Counter** - Made atomic
   - Duplicate IDs no longer occur under concurrent saves
   - Location: `RepositoryRuntime.java` (fixed in commit 79963fd)

3. **Tombstone BitSet Concurrency** - Fixed with proper synchronization
   - Concurrent deletes now correctly track row counts
   - Location: `AbstractTable.java` (verified in commit 251917d)

4. **SeqLock for Row-Level Atomicity** - Implemented
   - Prevents torn reads when readers observe concurrent writes
   - Provides version-based read optimization for read-mostly workloads
   - Location: `GeneratedTable.java` (implemented in commit bdfdf29)

### Fixed: Bounds Checking

**Status:** ✅ New bounds checking added

IndexOutOfBoundsException errors are now prevented through bounds checking in the MethodHandle implementation:
- Location: `MethodHandleImplementation` (added in commit 852c9ce)

### Current Concurrency Model

**Thread-Safe Operations:**

| Operation | Thread-Safety | Mechanism |
|-----------|--------------|------------|
| ID generation | ✅ Lock-free | `AtomicLong.getAndIncrement()` |
| ID index lookups | ✅ Lock-free | `ConcurrentHashMap.get()` |
| HashIndex lookups | ✅ Lock-free | `ConcurrentHashMap.get()` |
| RangeIndex lookups | ✅ Lock-free | `ConcurrentSkipListMap.get()` |
| Query scans | ✅ Safe reads | SeqLock + volatile watermark |
| Index add/remove | ✅ Thread-safe | `compute()` / `computeIfAbsent()` |
| Row allocation | ✅ Lock-free | Lock-free stack implementation |
| Column writes | ✅ Atomic | SeqLock prevents torn reads |
| Entity deletes | ✅ Thread-safe | Synchronized tombstones |

**Concurrency Characteristics:**
- **Reads**: Thread-safe, lock-free for most operations
- **Writes**: Thread-safe with SeqLock and lock-free structures
- **Read-Write**: SeqLock provides atomicity for concurrent access
- **Isolation**: Best-effort (no MVCC, no transactions)

**Practical Usage Patterns:**

For **read-mostly workloads with occasional writes**:
- Multi-reader, multi-writer is fully supported without external synchronization
- Readers benefit from lock-free lookups and wait-free seqlock reads

For **write-heavy workloads**:
- SeqLock provides good performance but may cause read amplification during writes
- Consider batching writes if experiencing contention

**See Also:** [CONCURRENCY.md](CONCURRENCY.md) for detailed concurrency model and architecture

---

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

---

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

2. **Check index usage:**
   - Verify indexed fields are used in equality queries for O(1) lookups
   - For range queries, verify RangeIndex is available on the field

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

---

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

---

## Current Limitations

1. **Join Tables with UUID/String IDs** - Only numeric IDs supported
2. **DISTINCT query modifier** - Tokenized but execution not complete
3. **No MVCC or transactions** - Best-effort isolation only (see CONCURRENCY.md)
4. **No pessimistic locking API** - External synchronization required for some write-heavy scenarios

### Test Coverage

When tests can run, verify:
- `QueryPlannerTest.java` - Tests QueryPlanner parsing (8 tests)
- `QueryMethodParserTest.java` - Comprehensive JPA specification tests (640 lines)
- `ECommerceRealWorldTest.java` - Real-world e-commerce domain tests (15 tests)
- `ConcurrencyTestSuite.java` - Validates thread-safety model and concurrent operations

---

*For development guidelines, see [../CLAUDE.md](../CLAUDE.md)*
