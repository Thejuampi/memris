# E-Commerce Domain Model Example

This document demonstrates a complete e-commerce domain model with entities, relationships, repositories, and query patterns.

## Entity Definitions

### Customer Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;

@Entity
public class Customer {
    @Id
    public Long id;
    
    @Index(type = Index.IndexType.HASH)
    public String email;
    
    public String name;
    public String phone;
    public long created;
    
    public Customer() {
        this.created = System.currentTimeMillis();
    }
    
    public Customer(String email, String name, String phone) {
        this.email = email;
        this.name = name;
        this.phone = phone;
        this.created = System.currentTimeMillis();
    }
}
```

### Product Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;

@Entity
public class Product {
    @Id
    public Long id;
    
    @Index(type = Index.IndexType.HASH)
    public String sku;
    
    public String name;
    public long price;  // Stored in cents (e.g., $19.99 = 1999)
    public int stock;
    
    public Product() {}
    
    public Product(String sku, String name, long price, int stock) {
        this.sku = sku;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }
    
    public double getPriceDollars() {
        return price / 100.0;
    }
}
```

### Order Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;
import io.memris.core.OneToMany;

import java.util.List;

@Entity
public class Order {
    @Id
    public Long id;
    
    public Long customerId;  // FK stored directly for simplicity
    public String status;
    public long total;
    public long date;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    public Customer customer;
    
    @OneToMany(mappedBy = "order")
    public List<OrderItem> items;
    
    public Order() {}
    
    public Order(Long customerId, String status, long total) {
        this.customerId = customerId;
        this.status = status;
        this.total = total;
        this.date = System.currentTimeMillis();
    }
    
    public Order(Customer customer, String status, long total) {
        this.customer = customer;
        this.customerId = customer != null ? customer.id : null;
        this.status = status;
        this.total = total;
        this.date = System.currentTimeMillis();
    }
}
```

### OrderItem Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;

@Entity
public class OrderItem {
    @Id
    public Long id;
    
    public Long orderId;    // FK stored directly
    public Long productId;  // FK stored directly
    public int quantity;
    public long unitPrice;  // Stored in cents
    
    @ManyToOne
    @JoinColumn(name = "order_id")
    public Order order;
    
    public OrderItem() {}
    
    public OrderItem(Long orderId, Long productId, int quantity, long unitPrice) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }
    
    public long getSubtotal() {
        return unitPrice * quantity;
    }
}
```

## Repository Interfaces

### CustomerRepository

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface CustomerRepository extends MemrisRepository<Customer> {
    Customer save(Customer customer);
    Optional<Customer> findById(Long id);
    Optional<Customer> findByEmail(String email);
    List<Customer> findByNameContainingIgnoreCase(String name);
    List<Customer> findAll();
    boolean existsByEmail(String email);
    void deleteById(Long id);
}
```

### ProductRepository

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisRepository<Product> {
    Product save(Product product);
    Optional<Product> findById(Long id);
    Optional<Product> findBySku(String sku);
    List<Product> findByPriceBetween(long min, long max);
    List<Product> findByStockGreaterThan(int stock);
    List<Product> findAll();
    List<Product> findTop3ByOrderByPriceDesc();
}
```

### OrderRepository

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface OrderRepository extends MemrisRepository<Order> {
    Order save(Order order);
    Optional<Order> findById(Long id);
    List<Order> findAll();
    List<Order> findByCustomerId(Long customerId);
    List<Order> findByCustomerIdAndStatus(Long customerId, String status);
    List<Order> findByStatus(String status);
    List<Order> findByStatusOrderByTotalDesc(String status);
    List<Order> findByStatusIn(List<String> statuses);
    List<Order> findByStatusAndTotalGreaterThanEqual(String status, long total);
    List<Order> findTop3ByStatusOrderByIdAsc(String status);
    long countByStatus(String status);
    void deleteById(Long id);
}
```

### OrderItemRepository

```java
import io.memris.repository.MemrisRepository;
import java.util.List;

public interface OrderItemRepository extends MemrisRepository<OrderItem> {
    OrderItem save(OrderItem item);
    List<OrderItem> findAll();
    List<OrderItem> findByOrder(Long orderId);
    List<OrderItem> findByProduct(Long productId);
    List<OrderItem> findByQtyGreaterThan(int minQty);
}
```

## Usage Examples

### Basic CRUD Operations

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;

public class ECommerceDemo {
    public static void main(String[] args) {
        var factory = new MemrisRepositoryFactory();
        var arena = factory.createArena();
        
        var customerRepo = arena.createRepository(CustomerRepository.class);
        var productRepo = arena.createRepository(ProductRepository.class);
        var orderRepo = arena.createRepository(OrderRepository.class);
        var itemRepo = arena.createRepository(OrderItemRepository.class);
        
        Customer customer = customerRepo.save(
            new Customer("john@example.com", "John Doe", "555-1234")
        );
        
        Product laptop = productRepo.save(
            new Product("LAPTOP-001", "Gaming Laptop", 129999, 50)
        );
        Product mouse = productRepo.save(
            new Product("MOUSE-001", "Wireless Mouse", 2999, 100)
        );
        
        Order order = orderRepo.save(new Order(customer.id, "PENDING", 0));
        
        itemRepo.save(new OrderItem(order.id, laptop.id, 1, laptop.price));
        itemRepo.save(new OrderItem(order.id, mouse.id, 2, mouse.price));
        
        order.total = laptop.price + (mouse.price * 2);
        orderRepo.save(order);
        
        factory.close();
    }
}
```

### Query Patterns

```java
Optional<Customer> found = customerRepo.findByEmail("john@example.com");

List<Product> affordable = productRepo.findByPriceBetween(5000, 20000);

List<Product> inStock = productRepo.findByStockGreaterThan(0);

List<Order> pendingOrders = orderRepo.findByStatus("PENDING");

long pendingCount = orderRepo.countByStatus("PENDING");

boolean exists = customerRepo.existsByEmail("john@example.com");

List<Customer> matches = customerRepo.findByNameContainingIgnoreCase("john");

List<Order> top3 = orderRepo.findTop3ByStatusOrderByIdAsc("PENDING");

List<Order> byStatuses = orderRepo.findByStatusIn(List.of("PENDING", "SHIPPED"));
```

### Relationship Queries

Memris supports traversing relationships in queries using property paths:

```java
List<Customer> customers = customerRepo.findByOrdersStatus("PAID");
```

This finds customers who have at least one order with status "PAID".

### Relationship Entity Definitions

For relationship traversal to work, entities must define the relationship annotations:

```java
@Entity
public class Customer {
    @Id
    public Long id;
    public String name;
    
    @OneToMany(mappedBy = "customer")
    public List<Order> orders;
}

@Entity
public class Order {
    @Id
    public Long id;
    public String status;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    public Customer customer;
}

public interface CustomerRepository extends MemrisRepository<Customer> {
    List<Customer> findByOrdersStatus(String status);
}
```

## Composite Indexes

Composite indexes optimize queries that filter on multiple fields. Define them at the class level using `@Indexes`:

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.Indexes;

@Entity
@Indexes({
    @Index(name = "status_total_idx", fields = {"status", "total"}, type = Index.IndexType.BTREE),
    @Index(name = "status_date_idx", fields = {"status", "date"}, type = Index.IndexType.BTREE)
})
public class Order {
    @Id
    public Long id;
    public String status;
    public long total;
    public long date;
}
```

### When to Use Composite Indexes

| Query Pattern | Index Type | Benefit |
|---------------|------------|---------|
| `findByStatusAndTotalGreaterThan("PENDING", 10000)` | BTREE on (status, total) | O(log n) instead of O(n) scan |
| `findByStatusAndDateBetween(...)` | BTREE on (status, date) | Efficient range within status partition |

### Composite Index Best Practices

1. **Order matters**: Put equality filter fields first, range filter fields second
2. **Use BTREE for range queries**: HASH indexes only support equality
3. **Limit composite indexes**: Each index consumes memory and adds write overhead

```java
@Entity
@Indexes({
    @Index(name = "customer_status_idx", fields = {"customerId", "status"}, type = Index.IndexType.HASH)
})
public class Order {
    @Id
    public Long id;
    public Long customerId;
    public String status;
    public long total;
}

public interface OrderRepository extends MemrisRepository<Order> {
    List<Order> findByCustomerIdAndStatus(Long customerId, String status);
}
```

## Index Strategy

| Field | Index Type | Use Case |
|-------|------------|----------|
| `email`, `sku` | HASH | Exact match lookups (O(1)) |
| `status` | HASH | Status filtering |
| `price`, `total` | BTREE | Range queries (O(log n)) |
| `status`, `total` | BTREE (composite) | Combined status + range queries |

## Performance Characteristics

| Query Type | Without Index | With Index |
|------------|---------------|------------|
| `findBySku("SKU-001")` | O(n) scan | O(1) HashIndex |
| `findByPriceBetween(100, 500)` | O(n) scan | O(log n) RangeIndex |
| `findByStatus("PENDING")` | O(n) scan | O(1) HashIndex |
| `findByCustomerIdAndStatus(...)` | O(n) scan | O(log n) CompositeIndex |

## Supported Relationship Types

| Annotation | Description | Use Case |
|------------|-------------|----------|
| `@ManyToOne` | Many orders belong to one customer | Order -> Customer |
| `@OneToMany` | One customer has many orders | Customer -> List&lt;Order&gt; |
| `@OneToOne` | One-to-one relationship | User -> Profile |
| `@ManyToMany` | Many-to-many via join table | Student <-> Course |
