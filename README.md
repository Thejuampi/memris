# Memris

**Memris** = "Memory" + "Iris" — a heap-based, zero-reflection in-memory storage engine for Java 21. Like iris (the eye), it provides vision/insight into your data. Like iris (a flower), it blooms fast.

> "Iris suggests looking through data/vision. It sounds like an engine that can 'see' through the heap instantly."

## What is Memris?

**Memris** is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with Spring Data-compatible query methods.

Built on 100% Java heap storage with ByteBuddy bytecode generation, Memris delivers columnar storage performance with familiar Spring Data JPA query patterns. Zero reflection in hot paths, O(1) design principles, and primitive-only APIs ensure maximum throughput.

**Key highlights:**
- **100% Heap-Based**: Uses primitive arrays (int[], long[], String[]) - no FFM/MemorySegment
- **ByteBuddy Table Generation**: Generates optimized table classes at build time
- **Spring Data-Compatible**: Use familiar JPA query method patterns
- **Zero Reflection**: Compile-time query derivation with type-safe dispatch
- **Custom Annotations**: `@Entity`, `@Index`, `@GeneratedValue` (not Jakarta/JPA)
- **Future**: FFM off-heap storage planned for large datasets (see [docs/ROADMAP.md](docs/ROADMAP.md))

## Quick Start

```java
// Define your entity
@Entity
public class User {
    @Index(type = Index.Type.HASH)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Index(type = Index.Type.HASH)
    private String email;
    
    private String name;
    private int age;
}

// Generate table (build-time)
TableMetadata metadata = new TableMetadata("User", "com.example.User", 
    List.of(
        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
        new FieldMetadata("email", TypeCodes.TYPE_STRING, false, false),
        new FieldMetadata("name", TypeCodes.TYPE_STRING, false, false),
        new FieldMetadata("age", TypeCodes.TYPE_INT, false, false)
    ));

GeneratedTable table = TableGenerator.generate(metadata)
    .getConstructor(int.class, int.class)
    .newInstance(1024, 100);

// Usage
table.insertFrom(new Object[]{1L, "john@example.com", "John", 30});
int[] results = table.scanEqualsString(1, "john@example.com");
```

## Why Memris?

- **Blazing Fast**: O(1) operations with primitive arrays
- **Zero Reflection**: Compile-time bytecode generation
- **Type-Safe**: Static type checking with no runtime overhead
- **Spring Data Compatible**: Familiar query method patterns
- **Modern Java**: Built on Java 21 with pattern matching switches

## Architecture

### Build-Time (Once per Entity)
```
Entity Class → TableMetadata → TableGenerator → GeneratedTable Class
```

### Runtime (Hot Path)
```
Query Method → QueryMethodLexer → QueryPlanner → HeapRuntimeKernel 
    → GeneratedTable.scan*() → Results
```

### Storage Layer
- **PageColumnInt**: int[] with direct array scans
- **PageColumnLong**: long[] with hash index lookups
- **RangeIndex**: ConcurrentSkipListMap for O(log n) range queries  
- **PageColumnString**: String[] with range queries
- **AbstractTable**: Base class for generated tables

### Query Pipeline
- **QueryMethodLexer**: Tokenizes method names (findByAgeGreaterThan)
- **QueryPlanner**: Creates LogicalQuery with conditions
- **BuiltInResolver**: Handles built-in methods (findById, save, etc.)
- **HeapRuntimeKernel**: Executes with TypeCode dispatch

## Performance

- **Table Scans**: O(n) with early termination
- **Hash Lookups**: O(1) via LongIdIndex/StringIdIndex
- **Range Queries**: O(log n) via ConcurrentSkipListMap
- **Zero Boxing**: Primitive-only APIs
- **TypeCode Dispatch**: tableswitch bytecode for type routing

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Detailed architecture documentation
- [DEVELOPMENT.md](docs/DEVELOPMENT.md) - Development guidelines and code style
- [QUERY.md](docs/QUERY.md) - Query method reference and operators
- [SPRING_DATA.md](docs/SPRING_DATA.md) - Spring Data integration details

## Building

```bash
# Full clean build
mvn.cmd clean compile

# Run tests
mvn.cmd -q -e -pl memris-core test

# Run code quality checks
mvn.cmd spotbugs:check
mvn.cmd checkstyle:check
mvn.cmd pmd:check
```

## Requirements

- Java 21 (with --enable-preview)
- Maven 3.8+
- Windows/Linux/Mac

## License

[License information]
