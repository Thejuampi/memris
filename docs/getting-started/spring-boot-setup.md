# Spring Boot Setup

Memris provides first-class Spring Boot integration with auto-configuration and Spring Data repository support.

## Quick Setup

### 1. Add Dependency

Add the appropriate starter to your `pom.xml`:

**Spring Boot 3.x:**
```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-3</artifactId>
    <version>0.1.10</version>
</dependency>
```

**Spring Boot 2.x:**
```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-2</artifactId>
    <version>0.1.10</version>
</dependency>
```

### 2. Enable Repositories

Add `@EnableMemrisRepositories` to your main application class:

```java
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.memris.spring.data.repository.config.EnableMemrisRepositories;

@SpringBootApplication
@EnableMemrisRepositories
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

### 3. Define Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;

@Entity
public class Product {
    @Id
    private Long id;
    private String name;
    private String sku;
    private double price;
    
    // Constructors, getters...
}
```

### 4. Create Repository

```java
import io.memris.spring.data.repository.MemrisSpringRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisSpringRepository<Product, Long> {
    Optional<Product> findById(Long id);
    List<Product> findByName(String name);
    List<Product> findByPriceGreaterThan(double price);
    boolean existsBySku(String sku);
}
```

### 5. Use in Service

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class ProductService {
    
    private final ProductRepository repository;
    
    @Autowired
    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }
    
    public Product createProduct(String name, String sku, double price) {
        Product product = new Product();
        product.setName(name);
        product.setSku(sku);
        product.setPrice(price);
        return repository.save(product);
    }
    
    public List<Product> findExpensiveProducts(double minPrice) {
        return repository.findByPriceGreaterThan(minPrice);
    }
}
```

## What's Auto-Configured?

The starter automatically configures:

| Bean | Type | Description |
|------|------|-------------|
| `memrisConfiguration` | `MemrisConfiguration` | Configuration from properties |
| `memrisRepositoryFactory` | `MemrisRepositoryFactory` | Factory for creating arenas |
| `memrisArenaProvider` | `MemrisArenaProvider` | Resolves named arenas |
| `memrisArena` | `MemrisArena` | Default arena instance |
| Repository beans | Your interfaces | Scanned from `@EnableMemrisRepositories` |

## Repository Interface Options

Memris provides multiple repository base interfaces:

### MemrisSpringRepository (Recommended)

Full-featured repository with Spring Data integration:

```java
public interface ProductRepository extends MemrisSpringRepository<Product, Long> {
    // Custom query methods
}
```

Features:
- All CRUD operations
- Query method derivation
- @Query annotation with JPQL support
- Custom query methods

### MemrisCrudRepository

Standard Spring Data CRUD interface:

```java
public interface ProductRepository extends MemrisCrudRepository<Product, Long> {
    // Custom query methods
}
```

Features:
- Standard Spring Data `CrudRepository` methods
- Query method derivation

### MemrisRepository (Core)

Basic Memris repository without Spring Data dependencies:

```java
public interface ProductRepository extends MemrisRepository<Product> {
    // Custom query methods
}
```

Use when you want Memris without Spring Data dependencies.

## Configuration

### Basic Configuration

```yaml
memris:
  arenas:
    default:
      page-size: 1024
      max-pages: 2048
```

### Advanced Configuration

See [Configuration Properties](../spring-boot/configuration-properties.md) for all options.

## Next Steps

- [Spring Boot Overview](../spring-boot/overview.md) - Learn about the 4 Spring Boot modules
- [Auto-Configuration Details](../spring-boot/auto-configuration.md) - Deep dive into auto-configuration
- [Simple Examples](../spring-boot/examples-simple.md) - Basic Spring Boot examples
- [Advanced Examples](../spring-boot/examples-advanced.md) - Complex scenarios
