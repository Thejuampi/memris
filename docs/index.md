# Getting Started with Memris

Welcome to Memris - a high-performance in-memory data storage for Java with Spring Boot support.

## What is Memris?

Memris is a blazingly fast, concurrency-safe, in-memory storage engine designed for Java applications. It provides:

- **Zero-reflection query execution** - Sub-microsecond query performance
- **Columnar storage** - Efficient memory usage and cache-friendly access patterns
- **Spring Boot integration** - Auto-configuration and Spring Data repository support
- **JPQL queries** - Full @Query annotation support with complex queries
- **Thread-safe concurrency** - Multi-reader, multi-writer with seqlock synchronization

## Quick Links

- [Installation](getting-started/installation.md) - Get started with Maven or Gradle
- [Quick Start](getting-started/quick-start.md) - Your first Memris application in 5 minutes
- [Configuration](getting-started/configuration.md) - Tune performance and behavior
- [Spring Boot Setup](getting-started/spring-boot-setup.md) - Spring Boot integration guide

## Spring Boot Integration

Memris provides first-class Spring Boot support with auto-configuration:

```java
@SpringBootApplication
@EnableMemrisRepositories
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

## Important Limitations

!!! warning "No Transaction Support"
    Memris does **not** support transactions. Each save/update/delete operation is atomic and immediately visible to other threads.

!!! warning "No Native Queries"
    Native SQL queries are not supported. Use JPQL with @Query annotation instead.

!!! warning "No Lazy Loading"
    All entity relationships are eagerly loaded.

## Documentation Structure

### Getting Started
Learn the basics of Memris with step-by-step guides.

### Spring Boot Integration
Deep dive into Spring Boot modules, auto-configuration, and examples.

### Reference
Complete reference documentation for architecture, queries, concurrency, and more.

### Examples
Practical examples from basic CRUD to complex e-commerce scenarios.

## Performance Highlights

- **Query execution**: ~1-5 nanoseconds overhead
- **Concurrent operations**: Lock-free ID generation and index updates
- **Memory efficiency**: Columnar storage with dense primitive arrays

See [Benchmark Baselines](benchmark-baselines.md) for detailed performance metrics.

## Contributing

Contributions are welcome! See the [Development Guide](development.md) for build instructions and coding standards.

## License

Memris is licensed under the MIT License.
