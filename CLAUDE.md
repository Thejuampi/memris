# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Reading Strategy: On-Demand, Selective

**DO NOT load all documentation upfront.** Read documents selectively based on the task at hand.

**When to read:**
- **CLAUDE.md** (this file) - First, for quick context
- **docs/DEVELOPMENT.md** - When building, testing, or writing code
- **docs/ARCHITECTURE.md** - When modifying system structure or understanding components
- **docs/REFERENCE.md** - When working with queries or troubleshooting issues
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
- **Modules**: `jdk.incubator.vector` (SIMD), `java.base` (FFM)
- **Native Access**: `--enable-native-access=ALL-UNNAMED`

---

## Architecture Overview

Memris is a columnar in-memory storage engine with **zero reflection in hot paths**. The key architectural split is:

### Build-Time (once per entity type)
```
MetadataExtractor → EntityMetadata → QueryPlanner → QueryCompiler → RepositoryEmitter
```
- Scans entity class, builds MethodHandles, extracts TypeConverters
- Compiles query methods into `CompiledQuery` objects with resolved column indices
- Generates bytecode via ByteBuddy

### Runtime (hot path - zero reflection)
```
UserRepositoryImpl → RepositoryRuntime (queryId dispatch) → FfmTable (column-indexed access)
```
- Uses constant `queryId` (int) for method dispatch
- Dense arrays: `String[] columnNames`, `byte[] typeCodes`, `MethodHandle[] setters`
- TypeCode switch dispatch, no maps/string lookups

### Layer Responsibilities

| Layer | Purpose |
|-------|---------|
| **Domain** | No dependencies - core interfaces |
| **Parser** | Lexer → Tokens (QueryMethodLexer) |
| **Compiler** | LogicalQuery → CompiledQuery (QueryCompiler) |
| **Metadata** | Entity → Structure (EntityMetadata) |
| **Runtime** | Query Execution (RepositoryRuntime) |
| **Emitter** | ByteBuddy Generation (RepositoryEmitter) |
| **Factory** | Orchestration & CRUD (MemrisRepositoryFactory) |

---

## Critical Design Principles

### 1. O(1) First, O(log n) Second, O(n) Forbidden
All hot path operations must be O(1). Use `BitSet` for dense sets, direct array access.

### 2. Primitive-Only APIs
Never use boxed types in hot paths. Use `IntEnumerator`/`LongEnumerator` instead of `Iterator<Integer>`.

### 3. Java 21 Type Switches (CRITICAL)
Always use pattern matching switch with class literals:
```java
FfmColumn<?> column = switch (type) {
    case int.class, Integer.class -> new FfmIntColumn(...);
    case long.class, Long.class -> new FfmLongColumn(...);
    case String.class -> new FfmStringColumnImpl(...);
    default -> throw new IllegalArgumentException("Unsupported type: " + type);
};
```

### 4. Zero Reflection Hot Path
- Pre-compile MethodHandles at build time
- Use index-based array access, never maps or string lookups
- ByteBuddy bytecode generation with `queryId` dispatch

---

## Package Structure

| Package | Purpose |
|---------|---------|
| `io.memris.storage.ffm` | Storage layer (FfmTable, FfmColumn, MemorySegments) |
| `io.memris.kernel` | Selection pipeline, predicates, plan nodes |
| `io.memris.kernel.selection` | SelectionVector implementations (IntSelection, BitsetSelection) |
| `io.memris.spring` | Spring Data integration (factory, runtime) |
| `io.memris.spring.plan` | Query planning (Lexer, Planner, Compiler) |
| `io.memris.spring.generated` | ByteBuddy-generated repository implementations |

---

## Important Files

| File | Purpose |
|------|---------|
| `MemrisRepositoryFactory.java` | Entry point - manages tables, indexes, repositories |
| `RepositoryBytecodeGenerator.java` | ByteBuddy bytecode generation for repositories |
| `QueryMethodLexer.java` | Tokenizes query method names (findByLastname → tokens) |
| `QueryPlanner.java` | Creates LogicalQuery from tokens |
| `QueryCompiler.java` | Compiles LogicalQuery to CompiledQuery with column indices |
| `RepositoryRuntime.java` | Zero-reflection query execution with queryId dispatch |
| `FfmTable.java` | Columnar storage with SIMD scans |
| `BuiltInResolver.java` | Built-in operator keyword resolution (GreaterThan, Between, etc.) |

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

For complete operator reference, see **[docs/REFERENCE.md](docs/REFERENCE.md)**.
