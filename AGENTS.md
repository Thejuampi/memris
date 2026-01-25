# AGENTS.md - Memris Development Guide

This file provides guidelines for AI agents operating in the Memris repository.

## Build Commands

### Core Commands
```bash
# Full build
mvn.cmd clean compile

# Compile with preview features (required)
mvn.cmd -q -e compile

# Run tests
mvn.cmd -q -e -pl memris-core test

# Run single test class
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName

# Run single test method
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName#methodName

# Run throughput benchmark
java --enable-preview --add-modules jdk.incubator.vector -cp memris-core/target/classes io.memris.benchmarks.BenchmarkRunner

# Run with Maven exec plugin
mvn.cmd -q -e -pl memris-core exec:java -Dexec.mainClass=io.memris.benchmarks.BenchmarkRunner

# Run JMH microbenchmarks (latency-focused)
mvn.cmd clean compile
java --enable-preview --add-modules jdk.incubator.vector -cp memris-core/target/classes:jmh-benchmarks.jar io.memris.benchmarks.MemrisBenchmarks
```

### Java Configuration
- **Java Version**: 21 (required)
- **Preview Features**: `--enable-preview`
- **Modules**: `jdk.incubator.vector` (for SIMD), `java.base` (FFM)
- **Native Access**: `--enable-native-access=ALL-UNNAMED`

### Maven Properties
- `java.version`: 21
- `jmh.version`: 1.37
- `junit.version`: 5.10.1

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

### O(1) Design Principle
**O(1) first, O(log n) second, O(n) forbidden.**

- `contains()` must be O(1) - use `BitSet` for dense sets
- `add()` should be O(1) amortized
- Enumerator creation O(1) - return preallocated cursor
- Index lookups O(1) via hash or direct access
- Range queries use `ConcurrentSkipListMap` (O(log n)) as fallback

### Naming Conventions
| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `FfmIntColumn`, `RowIdSet` |
| Interfaces | PascalCase | `SelectionVector`, `IntEnumerator` |
| Records | PascalCase | `Predicate.Comparison` |
| Methods | camelCase | `scanEquals()`, `enumerator()` |
| Constants | UPPER_SNAKE_CASE | `OFFSET_BITS`, `DEFAULT_CAPACITY` |
| Variables | camelCase | `rowIndex`, `bitSet` |
| Packages | lowercase | `io.memris.kernel.selection` |
| Test classes | ClassNameTest | `FfmIntColumnTest` |

### Package Structure
```
io.memris/
├── kernel/          # Core types (RowId, Predicate, PlanNode)
│   └── selection/   # SelectionVector, enumerators
├── storage/         # Storage engines (FfmTable, MemrisStore)
│   └── ffm/         # FFM MemorySegment implementations
├── index/           # HashIndex, RangeIndex
├── query/           # Executor, planner
├── spring/          # Spring Data integration
└── benchmarks/      # JMH benchmarks
```

### Class Design
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

### Error Handling
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

### Import Organization
```java
// JDK imports first (alphabetical)
import java.lang.foreign.*;
import java.util.*;

// Third-party imports
import jdk.incubator.vector.*;

// Project imports (absolute)
import io.memris.kernel.*;
import io.memris.kernel.selection.*;
```

### Vector API Usage
- Use `SPECIES_PREFERRED` for portability
- Use `VectorMask.toLong()` for packed lane extraction
- Handle tail loop with `loopBound()`
- Use `fromMemorySegment()` for FFM integration

```java
IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
VectorMask<Integer> mask = vector.eq(value);
long lanes = mask.toLong();
while (lanes != 0L) {
    int lane = Long.numberOfTrailingZeros(lanes);
    // ...
    lanes &= (lanes - 1);
}
```

### Type Switch Pattern (Java 21 Pattern Matching) (CRITICAL)
**Always use type switches with class literals for type dispatch.**

- Use `switch (Class<?> type)` with pattern matching - faster than if-else chains
- Match both primitive and wrapper types explicitly
- Use class literals (`int.class`, `String.class`) instead of strings
- Never use `switch (type.getName())` or string comparisons

```java
// GOOD - Java 21 pattern matching switch
FfmColumn<?> column = switch (type) {
    case int.class, Integer.class -> new FfmIntColumn(spec.name(), arena, capacity);
    case long.class, Long.class -> new FfmLongColumn(spec.name(), arena, capacity);
    case boolean.class, Boolean.class -> new FfmBooleanColumn(spec.name(), arena, capacity);
    case byte.class, Byte.class -> new FfmByteColumn(spec.name(), arena, capacity);
    case short.class, Short.class -> new FfmShortColumn(spec.name(), arena, capacity);
    case float.class, Float.class -> new FfmFloatColumn(spec.name(), arena, capacity);
    case double.class, Double.class -> new FfmDoubleColumn(spec.name(), arena, capacity);
    case char.class, Character.class -> new FfmCharColumn(spec.name(), arena, capacity);
    case String.class -> new FfmStringColumnImpl(spec.name(), arena, capacity);
    default -> throw new IllegalArgumentException("Unsupported type: " + type);
};

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

### Testing Guidelines (CRITICAL)
- **NEVER use `System.out.println` for test verification** - Use REAL assertions only
- Follow TDD: RED -> GREEN -> REFACTOR
- Place tests in `memris-core/src/test/java/io/memris/`
- Test class naming: `ClassNameTest`
- **Use AssertJ idiomatic assertions** - 1 test, 1 assertion, fluent chaining
- Test O(1) guarantees explicitly
- Test SIMD paths with large datasets
- **All tests must verify behavior through assertions, not print statements**

```java
// GOOD - AssertJ idiomatic (1 test, 1 assertion)
assertThat(productRepo.findByPriceBetween(min, max))
    .hasSize(2)
    .extracting("name")
    .containsExactlyInAnyOrder("SmartPhone X Pro", "ProBook Laptop 15");

assertThat(customerRepo.findById(123))
    .isNotNull()
    .extracting("fullName")
    .isEqualTo("John Doe");

// BAD - Multiple scattered assertions
List<Product> results = productRepo.findByPriceBetween(min, max);
assertEquals(2, results.size());
assertEquals("SmartPhone X Pro", results.get(0).name);

// BAD - Print statements don't verify behavior
List<Product> results = productRepo.findByPriceBetween(min, max);
System.out.println("Found " + results.size() + " products");  // ❌ NOT A TEST!
```

### Documentation
- Public APIs must have Javadoc
- Include complexity guarantees in Javadoc
- Update `docs/queries-spec.md` for query specification changes
- Update `docs/troubleshooting.md` for known issues and workarounds
- Historical design documents are in `docs/archive/`

### Performance Validation
- Run benchmarks after performance-sensitive changes
- Use `BenchmarkRunner` for quick sanity checks
- Target: 10M rows scan < 50ms
- Target: 1% selectivity filter < 10ms

### Forbidden Patterns
- ❌ Boxing primitives (`Integer`, `Long`, `Boolean`)
- ❌ `Iterator`/`Iterable` in hot paths
- ❌ `Collection.contains()` for lookups
- ❌ Linear scans without SIMD vectorization
- ❌ O(n) operations without justification
- ❌ Throwing generic `Exception`
- ❌ Mutable collections in public APIs
- ❌ Generic query methods (`findBy(String field, Object value)`) - Use type-safe derived query methods instead

### Git Commit Requirements (CRITICAL)
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
