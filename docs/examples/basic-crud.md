# Basic CRUD Example

This example demonstrates basic CRUD operations with Memris.

## Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String sku;
    private String name;
    private double price;
    private int stock;
    
    // Constructors
    public Product() {}
    
    public Product(String sku, String name, double price, int stock) {
        this.sku = sku;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }
    public int getStock() { return stock; }
    public void setStock(int stock) { this.stock = stock; }
}
```

## Repository

```java
import io.memris.core.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisRepository<Product> {
    Optional<Product> findById(Long id);
    List<Product> findAll();
    List<Product> findBySku(String sku);
    List<Product> findByNameContaining(String name);
    boolean existsBySku(String sku);
    long countByNameContaining(String name);
    void deleteBySku(String sku);
}
```

## Service with CRUD Operations

```java
import io.memris.core.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import java.util.List;
import java.util.Optional;

public class ProductService {
    private final ProductRepository productRepository;
    
    public ProductService(MemrisArena arena) {
        this.productRepository = arena.createRepository(ProductRepository.class);
    }
    
    // CREATE
    public Product createProduct(String sku, String name, double price, int stock) {
        Product product = new Product(sku, name, price, stock);
        return productRepository.save(product);
    }
    
    // READ
    public Optional<Product> findById(Long id) {
        return productRepository.findById(id);
    }
    
    public List<Product> findAll() {
        return productRepository.findAll();
    }
    
    public Optional<Product> findBySku(String sku) {
        List<Product> products = productRepository.findBySku(sku);
        return products.isEmpty() ? Optional.empty() : Optional.of(products.get(0));
    }
    
    public List<Product> searchByName(String searchTerm) {
        return productRepository.findByNameContaining(searchTerm);
    }
    
    // UPDATE
    public Product updateProduct(Long id, String newName, Double newPrice) {
        Optional<Product> optional = findById(id);
        if (optional.isPresent()) {
            Product product = optional.get();
            if (newName != null) {
                product.setName(newName);
            }
            if (newPrice != null) {
                product.setPrice(newPrice);
            }
            return productRepository.save(product);
        }
        throw new ProductNotFoundException(id);
    }
    
    public Product updateStock(Long id, int newStock) {
        Optional<Product> optional = findById(id);
        if (optional.isPresent()) {
            Product product = optional.get();
            product.setStock(newStock);
            return productRepository.save(product);
        }
        throw new ProductNotFoundException(id);
    }
    
    // DELETE
    public void deleteProduct(Long id) {
        productRepository.deleteById(id);
    }
    
    public void deleteBySku(String sku) {
        productRepository.deleteBySku(sku);
    }
    
    public void deleteAllProducts() {
        productRepository.deleteAll();
    }
    
    // UTILITY
    public boolean exists(String sku) {
        return productRepository.existsBySku(sku);
    }
    
    public long countProducts() {
        return productRepository.count();
    }
    
    public long countByNameSearch(String searchTerm) {
        return productRepository.countByNameContaining(searchTerm);
    }
}
```

## Main Application

```java
import io.memris.core.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;

public class CrudApplication {
    public static void main(String[] args) {
        // Initialize
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
        MemrisArena arena = factory.createArena();
        ProductService service = new ProductService(arena);
        
        // CREATE
        System.out.println("=== Creating Products ===");
        Product laptop = service.createProduct("LAPTOP-001", "Gaming Laptop", 1299.99, 10);
        Product mouse = service.createProduct("MOUSE-001", "Wireless Mouse", 29.99, 50);
        Product keyboard = service.createProduct("KEYBOARD-001", "Mechanical Keyboard", 149.99, 25);
        
        System.out.println("Created: " + laptop.getName() + " (ID: " + laptop.getId() + ")");
        System.out.println("Created: " + mouse.getName() + " (ID: " + mouse.getId() + ")");
        System.out.println("Created: " + keyboard.getName() + " (ID: " + keyboard.getId() + ")");
        
        // READ
        System.out.println("\n=== Reading Products ===");
        System.out.println("All products:");
        service.findAll().forEach(p -> 
            System.out.println("  - " + p.getName() + " ($" + p.getPrice() + ")"));
        
        System.out.println("\nFind by ID " + laptop.getId() + ":");
        service.findById(laptop.getId()).ifPresent(p ->
            System.out.println("  Found: " + p.getName()));
        
        System.out.println("\nSearch for 'Gaming':");
        service.searchByName("Gaming").forEach(p ->
            System.out.println("  Found: " + p.getName()));
        
        // UPDATE
        System.out.println("\n=== Updating Products ===");
        Product updated = service.updateProduct(laptop.getId(), "Pro Gaming Laptop", 1499.99);
        System.out.println("Updated: " + updated.getName() + " - $" + updated.getPrice());
        
        service.updateStock(mouse.getId(), 45);
        System.out.println("Updated stock for: " + mouse.getName());
        
        // DELETE
        System.out.println("\n=== Deleting Products ===");
        service.deleteProduct(keyboard.getId());
        System.out.println("Deleted keyboard");
        
        // Check count
        System.out.println("\nTotal products: " + service.countProducts());
        
        // Cleanup
        factory.close();
    }
}
```

## Spring Boot Version

### Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String sku;
    private String name;
    private double price;
    private int stock;
    
    // Getters and setters...
}
```

### Repository

```java
import io.memris.spring.data.repository.MemrisSpringRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisSpringRepository<Product, Long> {
    Optional<Product> findById(Long id);
    List<Product> findByNameContaining(String name);
    boolean existsBySku(String sku);
}
```

### Service

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Optional;

@Service
public class ProductService {
    private final ProductRepository repository;
    
    @Autowired
    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }
    
    public Product createProduct(String sku, String name, double price, int stock) {
        Product product = new Product();
        product.setSku(sku);
        product.setName(name);
        product.setPrice(price);
        product.setStock(stock);
        return repository.save(product);
    }
    
    public List<Product> findAll() {
        return repository.findAll();
    }
    
    public Optional<Product> findById(Long id) {
        return repository.findById(id);
    }
    
    public Product updateProduct(Long id, String newName, Double newPrice) {
        Optional<Product> optional = repository.findById(id);
        if (optional.isPresent()) {
            Product product = optional.get();
            if (newName != null) product.setName(newName);
            if (newPrice != null) product.setPrice(newPrice);
            return repository.save(product);
        }
        throw new ProductNotFoundException(id);
    }
    
    public void deleteProduct(Long id) {
        repository.deleteById(id);
    }
}
```

### Application

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

### REST Controller

```java
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    private final ProductService productService;
    
    public ProductController(ProductService productService) {
        this.productService = productService;
    }
    
    @GetMapping
    public List<Product> getAll() {
        return productService.findAll();
    }
    
    @GetMapping("/{id}")
    public Product getById(@PathVariable Long id) {
        return productService.findById(id)
            .orElseThrow(() -> new ProductNotFoundException(id));
    }
    
    @PostMapping
    public Product create(@RequestBody ProductRequest request) {
        return productService.createProduct(
            request.getSku(),
            request.getName(),
            request.getPrice(),
            request.getStock()
        );
    }
    
    @PutMapping("/{id}")
    public Product update(@PathVariable Long id, @RequestBody ProductRequest request) {
        return productService.updateProduct(id, request.getName(), request.getPrice());
    }
    
    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        productService.deleteProduct(id);
    }
}
```

## Key Points

- **Save operation**: Creates or updates (upsert behavior)
- **ID generation**: Automatic with `@GeneratedValue`
- **Null safety**: Always check `Optional` before accessing
- **No transactions**: Each operation commits immediately

See [Spring Boot Examples](../spring-boot/examples-simple.md) for more advanced patterns.
