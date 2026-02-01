# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reading Strategy: On-Demand, Selective

**DO NOT load all documentation upfront.** Read documents selectively based on the task at hand.

**When to read:**
- **CLAUDE.md** (this file) - First, for quick context
- **docs/DEVELOPMENT.md** - When building, testing, or writing code
- **docs/ARCHITECTURE.md** - When modifying system structure or understanding components
- **docs/QUERY.md** - When working with queries or troubleshooting issues
- **docs/SPRING_DATA.md** - When implementing Spring Data features

**Read only what you need, when you need it.**

---

## Future Roadmap: FFM Off-Heap Storage

**Planned Feature**: Off-heap storage using Java Foreign Function & Memory (FFM) API to replace current heap-based storage.

**Benefits:**
- Reduced GC pressure for large datasets
- Direct memory control for predictable latency
- Potential for SIMD vectorization via Vector API
- Memory-mapped file persistence

**Current Design**: Heap-based storage with primitive arrays is sufficient for most use cases and provides:
- Simpler deployment (no `--enable-native-access` required)
- Better Java integration and debugging
- JIT-optimized primitive arrays
- Zero configuration overhead

**Implementation Complexity**: HIGH
- Requires Arena lifecycle management
- String pooling for off-heap storage
- Coordination across threads for Arena access
- Module system integration

**Bytecode Implementation**: Default table generation strategy with ~1ns overhead
**MethodHandle Implementation**: Fallback strategy with ~5ns overhead

---

## Build Commands

```bash
# Full clean build
mvn.cmd clean compile

# Quick compile (quiet mode)
mvn.cmd -q -e compile

# Run all tests
mvn.cmd -q -e -pl memris-core test

# Run single test class
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName

# Run single test method
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName#methodName
```

## Java Runtime Requirements

- **Java Version**: 21 (required)

- **Native Access**: `--enable-native-access=ALL-UNNAMED`
- **Storage**: 100% heap-based (no FFM/MemorySegment)

---

## Architecture Overview

Memris is a **heap-based**, columnar in-memory storage engine with **zero reflection in hot paths**. The key architectural split is:

### Build-Time (once per entity type)
```
TableMetadata → TableGenerator → GeneratedTable (ByteBuddy)
```
- Scans entity class, creates TableMetadata with field info
- Generates table class extending AbstractTable via ByteBuddy
- Pre-compiles MethodHandles for column access

### Runtime (hot path - zero reflection)
```
HeapRuntimeKernel → GeneratedTable (column-indexed access)
```
- Uses TypeCode switch for type dispatch
- Dense arrays: column indices → PageColumn* access
- Direct scan methods: scanEqualsLong(), scanBetweenInt(), etc.

### Layer Responsibilities

| Layer | Purpose |
|-------|---------|
| **Parser** | Tokenizes query method names (QueryMethodLexer) |
| **Planner** | Creates LogicalQuery from tokens (QueryPlanner) |
| **Runtime** | Query execution (HeapRuntimeKernel) |
| **Storage** | Heap-based table storage (TableGenerator, PageColumn*) |
| **Index** | Hash and range indexes (HashIndex, RangeIndex) |

---

## Critical Design Principles

### 1. O(1) First, O(log n) Second, O(n) Forbidden
All hot path operations must be O(1). Use `BitSet` for dense sets, direct array access.

### 2. Primitive-Only APIs
Never use boxed types in hot paths. Use `IntEnumerator`/`LongEnumerator` instead of `Iterator<Integer>`.

### 3. Java 21 Type Switches (CRITICAL)
Always use pattern matching switch with class literals and TypeCodes:
```java
byte typeCode = switch (type) {
    case int.class, Integer.class -> TypeCodes.TYPE_INT;
    case long.class, Long.class -> TypeCodes.TYPE_LONG;
    case String.class -> TypeCodes.TYPE_STRING;
    default -> throw new IllegalArgumentException("Unsupported type: " + type);
};
```

### 4. Zero Reflection Hot Path
- Pre-compile MethodHandles at build time
- Use index-based array access, never maps or string lookups
- ByteBuddy table generation with TypeCode dispatch

### 5. TypeCodes (NOT TypeCode Enum)
TypeCodes is a **final class with static byte constants** (NOT an enum):
```java
public final class TypeCodes {
    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;
    public static final byte TYPE_STRING = 8;
    // ...
}
```

---

## Package Structure

| Package | Purpose |
|---------|---------|
| `io.memris.storage` | Core storage interfaces (Table, GeneratedTable, Selection) |
| `io.memris.storage.heap` | Heap-based implementation (TableGenerator, AbstractTable, PageColumn*, *IdIndex) |
| `io.memris.kernel` | Selection vectors, predicates, plan nodes, executor |
| `io.memris.kernel.selection` | SelectionVector implementations (IntSelection) |
| `io.memris.index` | Index implementations (HashIndex, RangeIndex, LongIdIndex, StringIdIndex) |
| `io.memris.core` | Custom annotations, TypeCodes, BuiltInResolver |
| `io.memris.query` | Query planning (QueryMethodLexer, QueryPlanner, CompiledQuery, OpCode) |
| `io.memris.runtime` | Query execution (HeapRuntimeKernel, EntityMaterializer, EntityExtractor) |
| `io.memris.repository` | Repository scaffolding (RepositoryMethodIntrospector) |

**Note:** ByteBuddy generates table classes in `io.memris.storage.generated` package (created at runtime).

---

## Important Files

| File | Purpose |
|------|---------|
| `TableGenerator.java` | ByteBuddy table class generation |
| `HeapRuntimeKernel.java` | Zero-reflection query execution engine |
| `QueryMethodLexer.java` | Tokenizes query method names (findByLastname → tokens) |
| `QueryPlanner.java` | Creates LogicalQuery from tokens |
| `CompiledQuery.java` | Pre-compiled query with resolved column indices |
| `BuiltInResolver.java` | Built-in operation resolution (findById, save, delete, etc.) |
| `TypeCodes.java` | Type code constants (final class with static bytes) |
| `GeneratedTable.java` | Low-level table interface with scan methods |
| `AbstractTable.java` | Base class for generated tables |
| `PageColumnInt.java` | int[] column with scan operations |
| `PageColumnLong.java` | long[] column with scan operations |
| `PageColumnString.java` | String[] column with scan operations |

---

## Custom Annotations (NOT Jakarta/JPA)

Memris uses **custom annotations** (not Jakarta/JPA):

- `@Entity` (io.memris.Entity) - Marks entity classes
- `@Index` (io.memris.Index) - Marks fields for indexing
- `@GeneratedValue` (io.memris.GeneratedValue) - Auto ID generation
- `@OneToOne` (io.memris.OneToOne) - Relationship marker
- `GenerationType` - ID generation strategies (AUTO, IDENTITY, UUID, CUSTOM)

**Note:** Test entities may use Jakarta annotations, but main code uses custom annotations.

---

## Relationship Annotations Supported

All relationship annotations are fully implemented:

- **@ManyToOne** - Foreign key relationships with targetEntity and optional attributes
- **@OneToMany** - Bidirectional relationships requiring mappedBy attribute
- **@ManyToMany** - Join table relationships with @JoinTable annotation
- **@OneToOne** - One-to-one relationships with mappedBy support

**Join Table Implementation**:
- **Numeric IDs (int, long)**: direct mapping to int/long columns
- **UUID IDs**: store as two long columns (128 bits total)
- **String IDs**: store in String columns with proper indexing
- **Current limitation**: Only numeric IDs fully supported

All relationships use eager loading since Memris is an in-memory storage engine (no lazy loading).

## Concurrency Model

**Current Implementation:**
- **Multi-reader**: Thread-safe concurrent queries (via HashIndex, RangeIndex)
- **Multi-writer**: Thread-safe with row seqlock + CAS (concurrent saves supported)
- **Read-write**: SeqLock provides coordination for row updates and typed reads
- **Isolation**: Best-effort (no MVCC, no transactions)

**Thread-Safe Operations:**
- ID generation: `AtomicLong` per entity class (lock-free)
- ID indexes: `ConcurrentHashMap` for lock-free lookups
- Query execution: Thread-safe reads on published data
- Index updates: `ConcurrentHashMap.compute()` / `ConcurrentSkipListMap.compute()`
- Entity saves: Coordinated by row seqlock (AbstractTable.java:172-195)
- Entity deletes: AtomicIntegerArray with CAS loops (AbstractTable.java:255-274)
- Row allocation: Lock-free via LockFreeFreeList (CAS-based)
- Column writes: Protected by beginSeqLock/endSeqLock

**See Also:** [CONCURRENCY.md](CONCURRENCY.md) for detailed concurrency model and improvement roadmap.

---

## Query Method Naming

Query methods are parsed from method names:
```
findByLastname              → EQ lastname
findByAgeGreaterThan        → GT age
findByAgeBetween            → BETWEEN age
findByLastnameIgnoreCase    → EQ lastname (case-insensitive)
```

Return types determine execution path:
- `List<T>` / `T` / `Optional<T>` / `long` (count) / `boolean` (exists)

For complete operator reference, see **[docs/QUERY.md](docs/QUERY.md)**.

---

## Testing Guidelines

### Single Assertion Requirement

**IMPORTANT**: All tests must use a single AssertJ assertion. The entity classes (Customer, Product, Order, OrderItem) do not implement `equals()`/`hashCode()`, so tests should use AssertJ's `usingRecursiveComparison()` API.

**Strategy by Assertion Type:**

| Current Pattern | Single Assertion Replacement |
|----------------|------------------------------|
| `assertThat(entity.field).isEqualTo(value)` | `assertThat(actual).usingRecursiveComparison().ignoringFields("created").isEqualTo(expected)` |
| `assertThat(list).hasSize(N) + extracting` | `assertThat(list).usingRecursiveFieldByFieldElementComparator().ignoringFields("created").containsExactly(expected)` |
| `containsExactlyInAnyOrder` | `usingRecursiveFieldByFieldElementComparator().ignoringFields("created").containsExactlyInAnyOrder(expectedElements)` |
| Boolean checks (isTrue, isFalse) | Keep as single assertions (already single) |
| Exception checks (assertThrownBy) | Keep as single assertions (already single) |

**Key Considerations:**

1. **Ignore created field**: Customer entity has timestamp-based field that won't match
2. **Order-sensitive tests**: Use `containsExactly()` when order matters (sorting tests)
3. **Order-agnostic tests**: Use `containsExactlyInAnyOrder()` for most cases
4. **ID fields**: Keep in comparison when testing specific entities
5. **Float/Double precision**: For entities with float/double fields, use `usingComparator()` with delta:
   ```java
   // For floating-point comparisons
   assertThat(actual).usingComparator(
       (d1, d2) -> Math.abs(d1 - d2) < 0.001 ? 0 : Double.compare(d1, d2)
   ).ignoringFields("created").isEqualTo(expected);
   ```

**Test Grouping:**

| Category | Tests Affected | Approach |
|----------|----------------|----------|
| Single entity verification | `shouldCreateAndFindCustomerByEmail`, `shouldCreateAndFindProductBySku`, `shouldHandleUpdateSemantics` | `usingRecursiveComparison()` |
| Collection verification | 15+ tests with list assertions | `usingRecursiveFieldByFieldElementComparator()` |
| Single boolean/count checks | `shouldCheckCustomerExistsByEmail`, `shouldCountOrdersByStatus`, etc. | Already single assertions |
