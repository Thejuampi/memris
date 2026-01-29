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
- **Preview Features**: `--enable-preview`
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
| `io.memris.storage.heap` | Heap-based implementation (TableGenerator, PageColumn*, AbstractTable) |
| `io.memris.kernel` | Selection vectors, predicates, plan nodes, executor |
| `io.memris.kernel.selection` | SelectionVector implementations (IntSelection) |
| `io.memris.index` | HashIndex, RangeIndex, LongIdIndex, StringIdIndex |
| `io.memris.spring` | Custom annotations, TypeCodes, BuiltInResolver |
| `io.memris.spring.plan` | Query planning (QueryMethodLexer, QueryPlanner, CompiledQuery, OpCode) |
| `io.memris.spring.runtime` | Query execution (HeapRuntimeKernel, EntityMaterializer, EntityExtractor) |
| `io.memris.spring.scaffold` | Repository scaffolding (RepositoryMethodIntrospector) |

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

- `@Entity` (io.memris.spring.Entity) - Marks entity classes
- `@Index` (io.memris.spring.Index) - Marks fields for indexing
- `@GeneratedValue` (io.memris.spring.GeneratedValue) - Auto ID generation
- `@OneToOne` (io.memris.spring.OneToOne) - Relationship marker
- `GenerationType` - ID generation strategies (AUTO, IDENTITY, UUID, CUSTOM)

**Note:** Test entities may use Jakarta annotations, but main code uses custom annotations.

---

## Join Table Implementation

When implementing join tables (@OneToMany, @ManyToMany):
- **Numeric IDs (int, long)**: direct mapping to int/long columns
- **UUID IDs**: store as two long columns (128 bits total)
- **String IDs**: store in String columns with proper indexing
- **Current limitation**: Only numeric IDs fully supported

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
