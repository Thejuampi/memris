# Development Guide

This guide provides comprehensive development guidelines for working on the Memris project.

## Getting Started

### Build Commands

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

### Java Runtime Requirements

- **Java Version**: 21 (required)

- **Native Access**: `--enable-native-access=ALL-UNNAMED`
- **Storage**: 100% heap-based (primitive arrays, no FFM/MemorySegment)

## Project Overview

**Memris** is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with:
- Heap-based columnar storage using primitive arrays (int[], long[], String[])
- ByteBuddy dynamic bytecode generation for table classes
- Zero-reflection hot paths with compile-time MethodHandle extraction
- O(1) design principle: no O(n) operations allowed in hot paths
- Spring Data-compatible query method parsing

**Architecture Notes:**
- Generates **TABLE** classes via ByteBuddy, not repository classes
- Storage is 100% Java heap (no FFM, no MemorySegment, no off-heap)
- SIMD not implemented; plain loops used (JIT may auto-vectorize)
- Custom annotations (`@Entity`, `@Index`, etc.) instead of Jakarta/JPA

## Critical Design Principles

### O(1) Design Principle
**O(1) first, O(log n) second, O(n) forbidden.**

- `contains()` must be O(1) - use `BitSet` for dense sets
- `add()` should be O(1) amortized
- Enumerator creation O(1) - return preallocated cursor
- Index lookups O(1) via hash or direct access
- Range queries use `ConcurrentSkipListMap` (O(log n)) as fallback

### Zero Reflection in Hot Paths
- Use ByteBuddy bytecode generation with pre-compiled MethodHandles
- No reflective field access in generated table implementations
- Compile-time type safety through bytecode generation

## Code Style Guidelines

### Primitive-Only APIs (CRITICAL)
**Never use boxed types in hot paths.**
- Use `int`, `long`, `double` instead of `Integer`, `Long`, `Double`
- Use `IntEnumerator`/`LongEnumerator` instead of `Iterator<Integer>`/`Iterator<Long>`
- Never implement `Iterable<T>` - use `IntEnumerator` interface
- Never use `for (Object o : collection)` - use enumerator pattern

```java
// GOOD
public interface IntEnumerator {
    boolean hasNext();
    int nextInt();
}

// BAD - boxing
public interface IntEnumerator extends Iterator<Integer> {
    @Override
    Integer next();  // BOXING!
}
```

## Java 21 Pattern Matching Switches (CRITICAL)

**Always use type switches with class literals for type dispatch.**

- Use `switch (Class<?> type)` with pattern matching - faster than if-else chains
- Match both primitive and wrapper types explicitly
- Use class literals (`int.class`, `String.class`) instead of strings
- Never use `switch (type.getName())` or string comparisons

```java
// GOOD - Java 21 pattern matching switch with TypeCodes
byte typeCode = switch (type) {
    case int.class, Integer.class -> TypeCodes.TYPE_INT;
    case long.class, Long.class -> TypeCodes.TYPE_LONG;
    case boolean.class, Boolean.class -> TypeCodes.TYPE_BOOLEAN;
    case byte.class, Byte.class -> TypeCodes.TYPE_BYTE;
    case short.class, Short.class -> TypeCodes.TYPE_SHORT;
    case float.class, Float.class -> TypeCodes.TYPE_FLOAT;
    case double.class, Double.class -> TypeCodes.TYPE_DOUBLE;
    case char.class, Character.class -> TypeCodes.TYPE_CHAR;
    case String.class -> TypeCodes.TYPE_STRING;
    default -> throw new IllegalArgumentException("Unsupported type: " + type);
};

// GOOD - Using TypeCodes.forClass()
byte typeCode = TypeCodes.forClass(type);

// BAD - string-based switch (slower, no type safety)
switch (type.getName()) {
    case "int" -> ...
}

// BAD - if-else chain (slower, harder to read)
if (type == int.class) {
    ...
} else if (type == long.class) {
    ...
}
```

## Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `PageColumnInt`, `RowIdSet` |
| Interfaces | PascalCase | `SelectionVector`, `IntEnumerator` |
| Records | PascalCase | `Predicate.Comparison` |
| Methods | camelCase | `scanEquals()`, `enumerator()` |
| Constants | UPPER_SNAKE_CASE | `OFFSET_BITS`, `DEFAULT_CAPACITY` |
| Variables | camelCase | `rowIndex`, `bitSet` |
| Packages | lowercase | `io.memris.kernel.selection` |
| Test classes | ClassNameTest | `PageColumnIntTest` |

## Class Design Guidelines

- Use `final` classes for implementations
- Use `sealed` interfaces for extensible types (Predicate, PlanNode)
- Use `record` for simple data carriers
- Package-private when not part of public API
- Public API must have Javadoc

```java
// Sealed interface for extensibility
public sealed interface Predicate permits Predicate.Comparison, Predicate.In, Predicate.Between { }

// Record for data carrier
public record Comparison(String column, Operator operator, Object value) implements Predicate { }

// Final implementation
public final class IntSelection implements MutableSelectionVector { }
```

## Error Handling

- Use `IllegalArgumentException` for parameter validation
- Use `IndexOutOfBoundsException` for index errors
- Use `NoSuchElementException` for enumerator exhaustion
- Guard clauses with early returns
- Validate null checks on public APIs

```java
public void add(int rowIndex) {
    if (rowIndex < 0) {
        throw new IllegalArgumentException("rowIndex must be non-negative: " + rowIndex);
    }
    // ... rest of implementation
}
```

## Import Organization

```java
// JDK imports first (alphabetical)
import java.util.*;
import java.util.concurrent.*;

// Third-party imports
import net.bytebuddy.*;

// Project imports (absolute)
import io.memris.kernel.*;
import io.memris.kernel.selection.*;
import io.memris.storage.heap.*;
```

## TypeCodes Usage

**TypeCodes** is a final class with static byte constants (NOT an enum):

```java
public final class TypeCodes {
    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;
    public static final byte TYPE_BOOLEAN = 2;
    // ... etc
}
```

**Benefits:**
- Constants inlined by JIT (zero overhead)
- Switch compiles to tableswitch (O(1) jump table)
- No enum allocation overhead

**Usage:**
```java
// Lookup type code
byte typeCode = TypeCodes.forClass(int.class);  // Returns TYPE_INT

// Switch on type code
switch (typeCode) {
    case TypeCodes.TYPE_LONG -> handleLong();
    case TypeCodes.TYPE_INT -> handleInt();
    case TypeCodes.TYPE_STRING -> handleString();
}
```

## Testing Guidelines (CRITICAL)

**NEVER use `System.out.println` for test verification** - Use REAL assertions only
- Follow TDD: RED -> GREEN -> REFACTOR
- Place tests in `memris-core/src/test/java/io/memris/`
- Test class naming: `ClassNameTest`
- **Use AssertJ idiomatic assertions** - 1 test, 1 assertion, fluent chaining
- Test O(1) guarantees explicitly
- **All tests must verify behavior through assertions, not print statements**

```java
// GOOD - AssertJ idiomatic (1 test, 1 assertion)
assertThat(table.scanEqualsInt(0, 42))
    .hasSize(2)
    .containsExactly(5, 10);

assertThat(index.get(123L))
    .isNotNull()
    .extracting("page")
    .isEqualTo(0);

// BAD - Multiple scattered assertions
int[] results = table.scanEqualsInt(0, 42);
assertEquals(2, results.length);
assertEquals(5, results[0]);

// BAD - Print statements don't verify behavior
int[] results = table.scanEqualsInt(0, 42);
System.out.println("Found " + results.length + " matches");  // NOT A TEST!
```

## Documentation Standards

- Public APIs must have Javadoc
- Include complexity guarantees in Javadoc
- Update `docs/QUERY.md` for query specification changes
- Historical design documents are in `docs/archive/`

## Performance Validation

- Target: Table scan of 1M rows < 10ms
- Target: Hash index lookup O(1) < 1μs
- Target: Range index lookup O(log n) < 10μs
- Use JMH for microbenchmarks when needed

## Forbidden Patterns

- Boxing primitives (`Integer`, `Long`, `Boolean`) in hot paths
- `Iterator`/`Iterable` in hot paths
- `Collection.contains()` for lookups (use HashIndex)
- Linear scans without early termination
- O(n) operations without justification
- Throwing generic `Exception`
- Mutable collections in public APIs

## Git Commit Requirements (CRITICAL)

**NEVER include information about AI assistants in commit messages.**
- Commit messages must describe technical changes only
- No "Co-Authored-By: Claude" or similar attribution
- No mention of AI tools in commit history
- Use conventional commit format: `type: description`
- Types: `fix`, `feat`, `refactor`, `test`, `docs`, `perf`, `style`

```bash
# GOOD
git commit -m "fix: handle @ManyToOne foreign key relationships in materializeSingle"

# BAD
git commit -m "fix: handle @ManyToOne foreign keys (Co-Authored-By: Claude)"
```

## Join Table Implementation Notes

When implementing join tables (@OneToMany, @ManyToMany):
- Join tables must support the ID types of referenced entities
- For numeric IDs (int, long): direct mapping to int/long columns
- For UUID IDs: store as two long columns (128 bits total) for performance
- For String IDs: store in String columns with proper indexing
- Ensure join table columns match the ID type of the entities they reference

## Key Implementation Files

| File | Purpose |
|------|---------|
| `TableGenerator.java` | ByteBuddy table class generation |
| `HeapRuntimeKernel.java` | Zero-reflection query execution |
| `QueryMethodLexer.java` | Query method tokenization |
| `QueryPlanner.java` | Logical query creation |
| `BuiltInResolver.java` | Built-in operation resolution |
| `GeneratedTable.java` | Low-level table interface |
| `TypeCodes.java` | Type code constants (final class) |

---

*For detailed architecture and package structure, see [ARCHITECTURE.md](ARCHITECTURE.md)*
*For Spring Data integration details, see [SPRING_DATA.md](SPRING_DATA.md)*
*For query method reference, see [QUERY.md](QUERY.md)*
*For current implementation status, see [IMPLEMENTATION.md](IMPLEMENTATION.md)*

## Concurrency Guidelines

### Current Concurrency Model

- **Multi-reader**: Thread-safe concurrent queries
- **Multi-writer**: Thread-safe row writes coordinated by per-row seqlock
- **Read-write**: Best-effort isolation (seqlock for rows, no MVCC)
- **Isolation**: Best-effort (no transactions)

### When to Use External Synchronization

Use external synchronization only when you need strict index/row atomicity across multiple operations:
1. You require read-your-writes guarantees across index queries
2. You need strict consistency across multiple writes in a batch
3. You want deterministic ordering between concurrent writers

### Best Practices

1. Prefer query-only workloads (thread-safe)
2. Use single-writer thread pattern for writes
3. Partition repositories by entity type for isolation
4. Avoid concurrent saves to same table

### Current Limitations

- Index updates are eventually consistent with row writes
- No MVCC or snapshot isolation
- No optimistic locking support for full transactions

**Recently Fixed (Previously Issues):**
- ~~Free-list race condition~~ → LockFreeFreeList with CAS
- ~~Tombstone BitSet not thread-safe~~ → AtomicIntegerArray with CAS
- ~~SeqLock not implemented~~ → AtomicLongArray for per-row versioning

See [CONCURRENCY.md](CONCURRENCY.md) for detailed concurrency model and improvement roadmap.
