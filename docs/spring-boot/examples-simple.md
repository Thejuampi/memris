# Simple Spring Boot Examples

Basic examples for getting started with Memris and Spring Boot.

## Example 1: Basic Entity and Repository

### Maven Configuration

**pom.xml for Spring Boot 3:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.2</version>
    </parent>
    
    <dependencies>
        <dependency>
            <groupId>io.github.thejuampi</groupId>
            <artifactId>memris-spring-boot-starter-3</artifactId>
            <version>0.1.10</version>
        </dependency>
    </dependencies>
</project>
```

**pom.xml for Spring Boot 2:**
```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-2</artifactId>
    <version>0.1.10</version>
</dependency>
```

### Entity Class

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String firstName;
    private String lastName;
    private String email;
    private int age;
    
    // Constructors
    public Customer() {}
    
    public Customer(String firstName, String lastName, String email, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.age = age;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }
}
```

### Repository Interface

```java
import io.memris.spring.data.repository.MemrisSpringRepository;
import java.util.List;
import java.util.Optional;

public interface CustomerRepository extends MemrisSpringRepository<Customer, Long> {
    Optional<Customer> findById(Long id);
    List<Customer> findAll();
    List<Customer> findByLastName(String lastName);
    List<Customer> findByAgeGreaterThan(int age);
    boolean existsByEmail(String email);
}
```

### Service Class

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Optional;

@Service
public class CustomerService {
    private final CustomerRepository customerRepository;
    
    @Autowired
    public CustomerService(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }
    
    public Customer createCustomer(String firstName, String lastName, String email, int age) {
        Customer customer = new Customer(firstName, lastName, email, age);
        return customerRepository.save(customer);
    }
    
    public Optional<Customer> findById(Long id) {
        return customerRepository.findById(id);
    }
    
    public List<Customer> findByLastName(String lastName) {
        return customerRepository.findByLastName(lastName);
    }
    
    public List<Customer> findAdults() {
        return customerRepository.findByAgeGreaterThan(18);
    }
}
```

### Application Class

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

## Example 2: Query Methods

### Repository with Query Methods

```java
import io.memris.spring.data.repository.MemrisSpringRepository;
import io.memris.core.Index;
import io.memris.core.IndexType;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisSpringRepository<Product, Long> {
    
    // Equality
    Optional<Product> findBySku(String sku);
    
    // Comparison
    List<Product> findByPriceGreaterThan(double price);
    List<Product> findByPriceLessThan(double price);
    List<Product> findByPriceBetween(double min, double max);
    
    // String operations
    List<Product> findByNameContaining(String substring);
    List<Product> findByNameStartingWith(String prefix);
    List<Product> findByNameEndingWith(String suffix);
    List<Product> findByNameIgnoreCase(String name);
    
    // Logical combinations
    List<Product> findByCategoryAndPriceGreaterThan(String category, double price);
    List<Product> findByCategoryOrNameContaining(String category, String name);
    
    // Existence and counting
    boolean existsBySku(String sku);
    long countByCategory(String category);
    
    // Delete by
    long deleteByCategory(String category);
}
```

### Entity with Index

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;
import io.memris.core.IndexType;

@Entity
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Index(IndexType.HASH)
    private String sku;
    
    private String name;
    private String category;
    private double price;
    private int stock;
    
    // Getters and setters...
}
```

### Using the Repository

```java
@Service
public class ProductService {
    private final ProductRepository productRepository;
    
    public ProductService(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }
    
    public Product createProduct(String sku, String name, String category, double price) {
        Product product = new Product();
        product.setSku(sku);
        product.setName(name);
        product.setCategory(category);
        product.setPrice(price);
        product.setStock(0);
        return productRepository.save(product);
    }
    
    public List<Product> findExpensiveProducts(double minPrice) {
        return productRepository.findByPriceGreaterThan(minPrice);
    }
    
    public List<Product> searchProducts(String searchTerm) {
        return productRepository.findByNameContaining(searchTerm);
    }
    
    public List<Product> findElectronicsUnder(double maxPrice) {
        return productRepository.findByCategoryAndPriceGreaterThan("Electronics", 0);
    }
}
```

## Example 3: Application Configuration

### application.yml

```yaml
spring:
  application:
    name: memris-demo

memris:
  arenas:
    default:
      page-size: 1024
      max-pages: 2048
      enable-parallel-sorting: true
      enable-prefix-index: true
      enable-suffix-index: true

logging:
  level:
    io.memris: INFO
```

## Example 4: Complete CRUD Operations

### Repository

```java
public interface OrderRepository extends MemrisSpringRepository<Order, Long> {
    Optional<Order> findById(Long id);
    List<Order> findAll();
    List<Order> findByStatus(String status);
    List<Order> findByCustomerId(Long customerId);
}
```

### Service with Full CRUD

```java
@Service
public class OrderService {
    private final OrderRepository orderRepository;
    
    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }
    
    // CREATE
    public Order createOrder(Long customerId, double total) {
        Order order = new Order();
        order.setCustomerId(customerId);
        order.setTotal(total);
        order.setStatus("PENDING");
        order.setCreatedAt(LocalDateTime.now());
        return orderRepository.save(order);
    }
    
    // READ
    public Optional<Order> findById(Long id) {
        return orderRepository.findById(id);
    }
    
    public List<Order> findAll() {
        return orderRepository.findAll();
    }
    
    public List<Order> findPendingOrders() {
        return orderRepository.findByStatus("PENDING");
    }
    
    // UPDATE
    public Order updateOrderStatus(Long id, String newStatus) {
        Optional<Order> optional = orderRepository.findById(id);
        if (optional.isPresent()) {
            Order order = optional.get();
            order.setStatus(newStatus);
            return orderRepository.save(order);
        }
        throw new OrderNotFoundException(id);
    }
    
    // DELETE
    public void deleteOrder(Long id) {
        orderRepository.deleteById(id);
    }
    
    public void deleteAllOrders() {
        orderRepository.deleteAll();
    }
}
```

## Running the Application

```bash
# Build
mvn clean package

# Run
java -jar target/myapp-1.0.jar

# Or with Spring Boot plugin
mvn spring-boot:run
```

## Testing

```java
@SpringBootTest
class CustomerServiceTest {
    @Autowired
    private CustomerService customerService;
    
    @Test
    void shouldCreateAndFindCustomer() {
        Customer customer = customerService.createCustomer(
            "John", "Doe", "john@example.com", 30);
        
        assertThat(customer.getId()).isNotNull();
        
        Optional<Customer> found = customerService.findById(customer.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getEmail()).isEqualTo("john@example.com");
    }
}
```
