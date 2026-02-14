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
| `memrisConverterRegistrar` | `MemrisConverterRegistrar` | Registers JPA converters |
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

## Arena Isolation

Each arena is completely isolated - data saved in one arena is not visible in another. This enables:

- **Multi-tenant applications**: One arena per tenant
- **Test isolation**: Fresh arena per test class
- **Parallel processing**: Different arenas in different threads

### Using Multiple Arenas

Configure multiple arenas in `application.yml`:

```yaml
memris:
  default-arena: primary
  arenas:
    primary:
      page-size: 1024
      max-pages: 1024
    analytics:
      page-size: 4096
      max-pages: 8192
```

Inject arenas using `MemrisArenaProvider`:

```java
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import io.memris.core.MemrisArena;

@Service
public class AnalyticsService {
    private final MemrisArena analyticsArena;
    
    public AnalyticsService(MemrisArenaProvider arenaProvider) {
        this.analyticsArena = arenaProvider.getArena("analytics");
    }
    
    public void processAnalytics() {
        ProductRepository repo = analyticsArena.createRepository(ProductRepository.class);
        // Analytics-specific data operations
    }
}
```

Or inject the default arena directly:

```java
@Service
public class DefaultService {
    private final MemrisArena arena;
    
    public DefaultService(MemrisArena arena) {
        this.arena = arena;
    }
}
```

### Multi-Tenant Pattern

Create tenant-specific arenas programmatically:

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;

@Service
public class TenantService {
    private final MemrisRepositoryFactory factory;
    
    public TenantService(MemrisRepositoryFactory factory) {
        this.factory = factory;
    }
    
    public MemrisArena getTenantArena(String tenantId) {
        return factory.createArena();
    }
}
```

### Test Isolation Pattern

Create a fresh arena for each test to ensure complete isolation:

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

class CustomerRepositoryTest {
    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private CustomerRepository repository;
    
    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
        repository = arena.createRepository(CustomerRepository.class);
    }
    
    @AfterEach
    void tearDown() {
        arena.close();
    }
    
    // Tests run in complete isolation
}
```

## Next Steps

- [Spring Boot Overview](../spring-boot/overview.md) - Learn about the 4 Spring Boot modules
- [Auto-Configuration Details](../spring-boot/auto-configuration.md) - Deep dive into auto-configuration
- [Simple Examples](../spring-boot/examples-simple.md) - Basic Spring Boot examples
- [Advanced Examples](../spring-boot/examples-advanced.md) - Complex scenarios
