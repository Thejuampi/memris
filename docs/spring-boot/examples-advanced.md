# Advanced Spring Boot Examples

Complex examples showcasing advanced Memris features.

## Boot 2 vs Boot 3 Summary

| Feature | Boot 2 | Boot 3 |
|---------|--------|--------|
| JPA AttributeConverter | `javax.persistence.*` | `jakarta.persistence.*` |
| Memris annotations | `io.memris.core.*` | `io.memris.core.*` |
| Repository interfaces | `io.memris.spring.data.repository.*` | `io.memris.spring.data.repository.*` |
| EnableMemrisRepositories | `io.memris.spring.data.repository.config.*` | `io.memris.spring.data.repository.config.*` |

All Memris core annotations (`@Entity`, `@Id`, `@Query`, `@Param`, `@ManyToOne`, etc.) are in `io.memris.core.*` for both versions. Only JPA converters differ.

## Example 1: @Query with JPQL

Memris supports JPQL-like queries via the `@Query` annotation from `io.memris.core`.

### Basic SELECT with Named Parameters

```java
import io.memris.core.Query;
import io.memris.core.Param;
import io.memris.spring.data.repository.MemrisCrudRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisCrudRepository<Product, Long> {
    
    @Query("SELECT p FROM Product p WHERE p.sku = :sku")
    Optional<Product> findBySkuQuery(@Param("sku") String sku);
    
    @Query("SELECT p FROM Product p WHERE p.price BETWEEN :min AND :max")
    List<Product> findByPriceRange(@Param("min") double min, @Param("max") double max);
    
    @Query("SELECT p FROM Product p WHERE p.category IN :categories")
    List<Product> findByCategories(@Param("categories") List<String> categories);
}
```

### UPDATE with @Modifying

```java
import io.memris.core.Query;
import io.memris.core.Modifying;
import io.memris.core.Param;

public interface InventoryRepository extends MemrisCrudRepository<Inventory, Long> {
    
    @Modifying
    @Query("UPDATE Inventory i SET i.stock = :stock WHERE i.sku = :sku")
    int updateStockBySku(@Param("sku") String sku, @Param("stock") int stock);
    
    @Modifying
    @Query("UPDATE Product p SET p.price = p.price * :multiplier WHERE p.category = :category")
    int applyDiscountToCategory(@Param("category") String category, @Param("multiplier") double multiplier);
}
```

### DELETE with @Modifying

```java
public interface CustomerRepository extends MemrisCrudRepository<Customer, Long> {
    
    @Modifying
    @Query("DELETE FROM Customer c WHERE c.lastLogin < :date")
    int deleteInactiveCustomers(@Param("date") LocalDate date);
    
    @Modifying
    @Query("DELETE FROM Order o WHERE o.status = :status")
    long deleteOrdersByStatus(@Param("status") String status);
}
```

### GROUP BY with Map Return Type

```java
public interface OrderRepository extends MemrisCrudRepository<Order, Long> {
    
    @Query("SELECT o FROM Order o GROUP BY o.status")
    Map<String, List<Order>> findAllGroupedByStatus();
    
    @Query("SELECT o FROM Order o GROUP BY o.customerId, o.status")
    Map<CustomerStatusKey, List<Order>> findAllGroupedByCustomerAndStatus();
    
    @Query("SELECT COUNT(o) FROM Order o GROUP BY o.status")
    Map<String, Long> countByStatus();
    
    @Query("SELECT COUNT(o) FROM Order o GROUP BY o.customerId HAVING COUNT(o) > :min")
    Map<Long, Long> findCustomersWithManyOrders(@Param("min") long minOrders);
}

public record CustomerStatusKey(Long customerId, String status) {}
```

### Record Projections with SELECT Aliases

```java
public interface OrderRepository extends MemrisCrudRepository<Order, Long> {
    
    @Query("SELECT o.total as total, o.customer.name as customerName FROM Order o WHERE o.total >= :min")
    List<OrderSummary> findLargeOrders(@Param("min") double minTotal);
    
    @Query("SELECT o.total as total, o.customer.name as customerName FROM Order o ORDER BY o.total DESC")
    List<OrderSummary> findAllOrderSummaries();
}

public record OrderSummary(double total, String customerName) {}
```

### JOIN Queries

```java
public interface OrderRepository extends MemrisCrudRepository<Order, Long> {
    
    @Query("SELECT o FROM Order o JOIN o.customer c WHERE c.email = :email")
    List<Order> findOrdersByCustomerEmail(@Param("email") String email);
    
    @Query("SELECT o FROM Order o LEFT JOIN o.items i WHERE i.product.sku = :sku")
    List<Order> findOrdersContainingProduct(@Param("sku") String sku);
    
    @Query("SELECT o FROM Order o JOIN o.customer c WHERE c.address.city = :city")
    List<Order> findOrdersByCustomerCity(@Param("city") String city);
}
```

## Example 2: Entity Relationships

Memris provides relationship annotations from `io.memris.core` package. These are the same for both Boot 2 and Boot 3.

### @ManyToOne with @JoinColumn

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;

@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "customer_id", nullable = false)
    private Customer customer;
    
    private double total;
    private String status;
    
    // Getters and setters...
}

@Entity
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String name;
    private String email;
    
    // Getters and setters...
}
```

### @OneToMany with mappedBy

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.OneToMany;
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;
import java.util.List;

@Entity
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String name;
    private String email;
    
    @OneToMany(mappedBy = "customer")
    private List<Order> orders;
    
    // Getters and setters...
}

@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
    
    private double total;
    
    // Getters and setters...
}
```

### Query by Nested Properties

```java
public interface OrderRepository extends MemrisCrudRepository<Order, Long> {
    List<Order> findByCustomerEmail(String email);
    List<Order> findByCustomerNameContaining(String name);
    List<Order> findByCustomerStatus(String status);
}
```

### @ManyToMany with @JoinTable

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.ManyToMany;
import io.memris.core.JoinTable;
import java.util.List;

@Entity
public class Student {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String name;
    
    @ManyToMany
    @JoinTable(name = "student_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
    private List<Course> courses;
    
    // Getters and setters...
}

@Entity
public class Course {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String name;
    private String code;
    
    @ManyToMany(mappedBy = "courses")
    private List<Student> students;
    
    // Getters and setters...
}
```

!!! warning "Eager Loading Only"
    All relationships are eagerly loaded. There is no lazy loading support in Memris.

## Example 3: Multiple Arenas

### Configuration

```yaml
memris:
  default-arena: primary
  arenas:
    primary:
      page-size: 1024
      max-pages: 1024
    cache:
      page-size: 512
      max-pages: 512
    analytics:
      page-size: 4096
      max-pages: 8192
      enable-parallel-sorting: true
```

### Repository Configuration

```java
@Configuration
public class ArenaConfiguration {
    
    @Bean
    public MemrisArena primaryArena(MemrisArenaProvider provider) {
        return provider.getArena("primary");
    }
    
    @Bean
    public MemrisArena cacheArena(MemrisArenaProvider provider) {
        return provider.getArena("cache");
    }
    
    @Bean
    public MemrisArena analyticsArena(MemrisArenaProvider provider) {
        return provider.getArena("analytics");
    }
}
```

### Using Multiple Arenas

```java
@Service
public class MultiArenaService {
    private final MemrisArena primaryArena;
    private final MemrisArena cacheArena;
    private final MemrisArena analyticsArena;
    
    public MultiArenaService(
            @Qualifier("primary") MemrisArena primaryArena,
            @Qualifier("cache") MemrisArena cacheArena,
            @Qualifier("analytics") MemrisArena analyticsArena) {
        this.primaryArena = primaryArena;
        this.cacheArena = cacheArena;
        this.analyticsArena = analyticsArena;
    }
    
    public CustomerRepository getPrimaryCustomerRepo() {
        return primaryArena.createRepository(CustomerRepository.class);
    }
    
    public CacheRepository getCacheRepo() {
        return cacheArena.createRepository(CacheRepository.class);
    }
    
    public AnalyticsRepository getAnalyticsRepo() {
        return analyticsArena.createRepository(AnalyticsRepository.class);
    }
}
```

## Example 4: JPA Attribute Converters

Memris automatically registers JPA `AttributeConverter` beans. The JPA namespace differs between Boot versions, but Memris core annotations are the same.

### Namespace Differences

| Boot Version | JPA AttributeConverter | Memris Annotations |
|--------------|------------------------|-------------------|
| Boot 2.x | `javax.persistence.*` | `io.memris.core.*` |
| Boot 3.x | `jakarta.persistence.*` | `io.memris.core.*` |

### Custom Converter (Boot 3)

```java
import jakarta.persistence.Converter;
import jakarta.persistence.AttributeConverter;
import java.math.BigDecimal;

@Converter(autoApply = true)
public class MoneyConverter implements AttributeConverter<Money, BigDecimal> {
    
    @Override
    public BigDecimal convertToDatabaseColumn(Money money) {
        return money != null ? money.getAmount() : null;
    }
    
    @Override
    public Money convertToEntityAttribute(BigDecimal amount) {
        return amount != null ? new Money(amount) : null;
    }
}

public class Money {
    private final BigDecimal amount;
    
    public Money(BigDecimal amount) {
        this.amount = amount;
    }
    
    public BigDecimal getAmount() {
        return amount;
    }
}
```

### Custom Converter (Boot 2)

Same logic, different import:

```java
import javax.persistence.Converter;
import javax.persistence.AttributeConverter;
import java.math.BigDecimal;

@Converter(autoApply = true)
public class MoneyConverter implements AttributeConverter<Money, BigDecimal> {
    
    @Override
    public BigDecimal convertToDatabaseColumn(Money money) {
        return money != null ? money.getAmount() : null;
    }
    
    @Override
    public Money convertToEntityAttribute(BigDecimal amount) {
        return amount != null ? new Money(amount) : null;
    }
}
```

### Entity with Custom Type

```java
@Entity
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String name;
    private Money price; // Uses MoneyConverter automatically
    
    // Getters and setters...
}
```

### Converter Registration

Converters are automatically registered with Spring Boot:

```java
@SpringBootApplication
@EnableMemrisRepositories
public class Application {
    // MoneyConverter is auto-detected and registered
}
```

## Example 5: What's NOT Supported

### Transactions

```java
@Service
public class OrderService {
    
    // @Transactional HAS NO EFFECT in Memris
    @Transactional
    public void createOrderWithItems(Order order, List<Item> items) {
        // Each save is atomic and immediately visible
        // There's no rollback if an exception occurs later
        orderRepository.save(order);
        
        for (Item item : items) {
            itemRepository.save(item); // Committed immediately
        }
        
        throw new RuntimeException("Oops!");
        // Order and items are NOT rolled back
    }
}
```

!!! danger "No Transaction Support"
    `@Transactional` has no effect. Each operation commits immediately.

### Native Queries

```java
public interface ProductRepository extends MemrisCrudRepository<Product, Long> {
    
    // This will throw UnsupportedOperationException
    @Query(value = "SELECT * FROM products WHERE active = 1", nativeQuery = true)
    List<Product> findActiveNative();
}
```

!!! danger "Native Queries Not Supported"
    `nativeQuery = true` throws `UnsupportedOperationException`.

### Pageable and Sort Parameters

```java
public interface ProductRepository extends MemrisCrudRepository<Product, Long> {
    
    // NOT supported
    List<Product> findByCategory(String category, Pageable pageable);
    
    // NOT supported  
    List<Product> findByCategory(String category, Sort sort);
    
    // Use instead:
    List<Product> findTop10ByCategoryOrderByPriceDesc(String category);
    
    // Or use @Query:
    @Query("SELECT p FROM Product p WHERE p.category = :category ORDER BY p.price DESC")
    List<Product> findByCategoryOrdered(@Param("category") String category);
}
```

!!! danger "No Pageable/Sort Support"
    Use `ORDER BY` in @Query or method names instead.

### Specifications

```java
// NOT supported
public interface ProductRepository extends MemrisCrudRepository<Product, Long>,
                                          JpaSpecificationExecutor<Product> {
    // Specifications won't work
}

// Use query methods or @Query instead
```

!!! danger "No Specification Support"
    JPA Specifications are not supported.

### Lazy Loading

```java
@Entity
public class Customer {
    @OneToMany(mappedBy = "customer")
    private List<Order> orders; // Always eagerly loaded
}

// All orders are loaded when customer is loaded
// There's no way to lazy load
```

!!! danger "No Lazy Loading"
    All relationships are eagerly loaded.
