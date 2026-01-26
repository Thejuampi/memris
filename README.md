# Memris

**Memris** = "Memory" + "Iris" — an in-memory storage engine that can *see* through the heap instantly. Like iris (the eye), it provides vision/insight into your data. Like iris (a flower), it blooms fast.

> "Iris suggests looking through data/vision. It sounds like an engine that can 'see' through the heap instantly."

## What is Memris?

**Memris** is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with SIMD vectorized execution and Spring Data integration.

Built on modern Java APIs (Panama Vector API and Foreign Function & Memory), Memris delivers columnar storage performance with familiar Spring Data JPA repository patterns. Zero reflection in hot paths, O(1) design principles, and primitive-only APIs ensure maximum throughput.

**Key highlights:**
- Java 21 with SIMD vectorized execution using Panama Vector API
- FFM MemorySegment-based storage for zero-copy operations
- Spring Data JPA repository integration with ByteBuddy bytecode generation
- Zero reflection in hot paths with compile-time query derivation

## Quick Start

```java
// Simple repository interface
public interface UserRepository extends MemrisRepository<User, Long> {
    List<User> findByLastname(String lastname);
}

// Usage
userRepository.save(user);
List<User> users = userRepository.findByLastname("Smith");
```

## Why Memris?

- **Blazing Fast**: 10M rows in 15ms (14.3 GB/s throughput)
- **Spring Data Compatible**: Use familiar JPA repository patterns
- **Type-Safe**: Compile-time query derivation with zero runtime overhead
- **Modern Java**: Built on Java 21 with Panama Vector API and FFM

## Performance

*See [Architecture](docs/ARCHITECTURE.md#performance-characteristics) for detailed benchmarks*

## Learn More

### Documentation
- **[Architecture](docs/ARCHITECTURE.md)** - System design, query pipeline, and layer separation
- **[Development Guide](docs/DEVELOPMENT.md)** - Build commands, coding guidelines, and testing
- **[Spring Data Integration](docs/SPRING_DATA.md)** - Feature requirements and implementation status
- **[Query Reference](docs/REFERENCE.md)** - Complete query method specification and troubleshooting

### Project Info
- **[Build Commands](docs/DEVELOPMENT.md#getting-started)** - Maven build and test commands
- **[Contributing](docs/DEVELOPMENT.md#git-commit-requirements)** - Contribution guidelines

## License

MIT
