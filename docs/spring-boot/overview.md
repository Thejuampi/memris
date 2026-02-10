# Spring Boot Integration Overview

Memris provides comprehensive Spring Boot integration with auto-configuration and Spring Data repository support.

## Available Modules

Memris includes **4 Spring-related modules** to support different Spring Boot versions:

### Starter Modules (Aggregator POMs)

| Module | Artifact | Spring Boot | Purpose |
|--------|----------|-------------|---------|
| **memris-spring-boot-starter-2** | `memris-spring-boot-starter-2` | 2.7.x | Aggregator for Boot 2 / Spring 5 |
| **memris-spring-boot-starter-3** | `memris-spring-boot-starter-3` | 3.2.x | Aggregator for Boot 3 / Spring 6 |

!!! tip "Which starter to use?"
    - Use `memris-spring-boot-starter-3` for Spring Boot 3.x (Jakarta EE namespace)
    - Use `memris-spring-boot-starter-2` for Spring Boot 2.7.x (Java EE namespace)

### Implementation Modules

| Module | Artifact | Spring Versions | Key Classes |
|--------|----------|-----------------|-------------|
| **memris-spring-data-boot2** | `memris-spring-data-boot2` | Boot 2.7.18 / Spring 5.3.33 / Data 2.7.18 | Auto-configuration, repositories |
| **memris-spring-data-boot3** | `memris-spring-data-boot3` | Boot 3.2.2 / Spring 6.1.3 / Data 3.2.2 | Auto-configuration, repositories |

!!! note "Implementation Details"
    The implementation modules contain the actual Spring Boot integration code. The starters are convenience POMs that aggregate the correct dependencies.

## Choosing the Right Module

### Spring Boot 3.x (Recommended)

For new projects using Spring Boot 3.x:

```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-3</artifactId>
    <version>0.1.10</version>
</dependency>
```

**Key characteristics:**
- Uses `jakarta.persistence.*` namespace
- Spring Framework 6.1.3
- Spring Data 3.2.2
- Java 21 required

### Spring Boot 2.x

For existing projects on Spring Boot 2.7.x:

```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-2</artifactId>
    <version>0.1.10</version>
</dependency>
```

**Key characteristics:**
- Uses `javax.persistence.*` namespace
- Spring Framework 5.3.33
- Spring Data 2.7.18
- Java 21 required

## Key Differences Between Boot 2 and Boot 3

| Aspect | Boot 2 | Boot 3 |
|--------|--------|--------|
| Configuration annotation | `@Configuration` | `@AutoConfiguration` |
| Auto-config registration | `spring.factories` | `META-INF/spring/*.imports` |
| JPA namespace | `javax.persistence.*` | `jakarta.persistence.*` |
| Spring Framework | 5.3.33 | 6.1.3 |
| Spring Data | 2.7.18 | 3.2.2 |
| Minimum Java | 21 | 21 |

## What Gets Auto-Configured?

When you include a Memris starter, the following beans are automatically configured:

| Bean | Type | Description |
|------|------|-------------|
| `memrisConfiguration` | `MemrisConfiguration` | Core configuration from properties |
| `memrisRepositoryFactory` | `MemrisRepositoryFactory` | Factory for creating arenas |
| `memrisArenaProvider` | `MemrisArenaProvider` | Resolves named arenas |
| `memrisConverterRegistrar` | `MemrisConverterRegistrar` | Registers JPA converters (static bean) |
| `memrisArena` | `MemrisArena` | Default arena instance |
| Repository beans | Your interfaces | Scanned from `@EnableMemrisRepositories` |

All beans are created with `@ConditionalOnMissingBean`, allowing you to override any bean by defining your own.

## Features

### Spring Data Repository Support

Full support for Spring Data repository patterns:

```java
public interface CustomerRepository extends MemrisSpringRepository<Customer, Long> {
    List<Customer> findByLastName(String lastName);
    List<Customer> findByAgeGreaterThan(int age);
}
```

### Property-Based Configuration

Configure Memris via `application.yml`:

```yaml
memris:
  arenas:
    default:
      page-size: 2048
      enable-parallel-sorting: true
```

### JPA Attribute Converter Support

Automatic registration of JPA `@Converter` beans:

```java
@Converter(autoApply = true)
public class MoneyConverter implements AttributeConverter<Money, BigDecimal> {
    // Conversion logic
}
```

### Named Arenas

Support for multiple isolated data arenas:

```yaml
memris:
  default-arena: primary
  arenas:
    primary:
      page-size: 1024
    cache:
      page-size: 512
```

## Important Limitations

!!! warning "No Transaction Support"
    Memris does **not** support transactions. The following will have no effect:
    - `@Transactional` annotations
    - Transaction propagation
    - Rollback behavior
    
    Each save/update/delete operation is atomic and immediately visible to other threads.

!!! warning "No Native Queries"
    Native SQL queries are **not supported**:
    ```java
    // This will throw UnsupportedOperationException
    @Query(value = "SELECT * FROM products", nativeQuery = true)
    List<Product> findAllNative();
    ```

!!! warning "No Lazy Loading"
    All entity relationships are **eagerly loaded**. There is no lazy loading support for:
    - `@OneToMany` collections
    - `@ManyToOne` references
    - `@ManyToMany` collections

!!! warning "No Pageable/Sort Parameters"
    Spring Data's `Pageable` and `Sort` parameters are **not supported**:
    ```java
    // NOT supported
    List<Product> findByName(String name, Pageable pageable);
    
    // Use instead
    List<Product> findTop10ByNameOrderByPriceDesc(String name);
    // or
    @Query("SELECT p FROM Product p WHERE p.name = :name ORDER BY p.price DESC")
    List<Product> findByNameOrdered(@Param("name") String name);
    ```

## Next Steps

- [Auto-Configuration](auto-configuration.md) - Deep dive into how auto-configuration works
- [Configuration Properties](configuration-properties.md) - All available configuration options
- [Simple Examples](examples-simple.md) - Basic Spring Boot usage examples
- [Advanced Examples](examples-advanced.md) - Complex scenarios and patterns
