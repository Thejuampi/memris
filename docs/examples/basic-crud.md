# Basic CRUD Example

This example demonstrates basic CRUD operations with Memris using the repository pattern.

## Entity Definition

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

## Repository Interface

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends MemrisRepository<Product> {
    Product save(Product product);
    Optional<Product> findById(Long id);
    List<Product> findAll();
    Optional<Product> findBySku(String sku);
    List<Product> findByNameContaining(String name);
    List<Product> findByPriceBetween(long min, long max);
    List<Product> findByStockGreaterThan(int stock);
    List<Product> findByPriceLessThanEqual(long maxPrice);
    boolean existsBySku(String sku);
    long countByStockGreaterThan(int stock);
    void deleteById(Long id);
    void deleteAll();
}
```

## Service Layer

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import java.util.List;
import java.util.Optional;

public class ProductService {
    private final ProductRepository repository;
    
    public ProductService(MemrisArena arena) {
        this.repository = arena.createRepository(ProductRepository.class);
    }
    
    public Product create(String sku, String name, long price, int stock) {
        return repository.save(new Product(sku, name, price, stock));
    }
    
    public Optional<Product> findById(Long id) {
        return repository.findById(id);
    }
    
    public Optional<Product> findBySku(String sku) {
        return repository.findBySku(sku);
    }
    
    public List<Product> findAll() {
        return repository.findAll();
    }
    
    public List<Product> searchByName(String term) {
        return repository.findByNameContaining(term);
    }
    
    public List<Product> findByPriceRange(double min, double max) {
        return repository.findByPriceBetween(min, max);
    }
    
    public Product updatePrice(Long id, long newPrice) {
        return repository.findById(id).map(product -> {
            product.price = newPrice;
            return repository.save(product);
        }).orElseThrow(() -> new IllegalArgumentException("Product not found: " + id));
    }
    
    public Product updateStock(Long id, int newStock) {
        return repository.findById(id).map(product -> {
            product.stock = newStock;
            return repository.save(product);
        }).orElseThrow(() -> new IllegalArgumentException("Product not found: " + id));
    }
    
    public void delete(Long id) {
        repository.deleteById(id);
    }
    
    public boolean exists(String sku) {
        return repository.existsBySku(sku);
    }
    
    public long count() {
        return repository.count();
    }
    
    public long countLowStock(int threshold) {
        return repository.countByStockGreaterThan(threshold);
    }
}
```

## Complete Application

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import java.util.List;
import java.util.Optional;

public class CrudApplication {
    public static void main(String[] args) {
        var factory = new MemrisRepositoryFactory();
        var arena = factory.createArena();
        var service = new ProductService(arena);
        
        Product laptop = service.create("LAPTOP-001", "Gaming Laptop", 129999, 10);
        Product mouse = service.create("MOUSE-001", "Wireless Mouse", 2999, 50);
        Product keyboard = service.create("KEYBOARD-001", "Mechanical Keyboard", 14999, 25);
        
        service.updateStock(laptop.id, 8);
        service.updatePrice(mouse.id, 2499);
        
        List<Product> all = service.findAll();
        
        Optional<Product> found = service.findBySku("LAPTOP-001");
        
        List<Product> affordable = service.findByPriceRange(0, 10000);
        
        service.delete(keyboard.id);
        
        factory.close();
    }
}
```

## Key Points

| Feature | Description |
|---------|-------------|
| **Save** | Creates new or updates existing (upsert) |
| **ID Generation** | Automatic (AtomicLong per entity class) |
| **Null Safety** | Use `Optional` for single-result queries |
| **No Transactions** | Each operation commits immediately |
| **Indexing** | Add `@Index` for faster lookups |

## Built-in Repository Methods

All repositories extending `MemrisRepository<T>` provide:

| Method | Description |
|--------|-------------|
| `save(T entity)` | Insert or update entity |
| `saveAll(Iterable<T>)` | Batch insert/update |
| `findById(ID id)` | Find by primary key |
| `findAll()` | Return all entities |
| `findAllById(Iterable<ID>)` | Find multiple by IDs |
| `count()` | Count all entities |
| `existsById(ID id)` | Check existence by ID |
| `deleteById(ID id)` | Delete by ID |
| `delete(T entity)` | Delete entity |
| `deleteAll()` | Delete all entities |
| `deleteAllById(Iterable<ID>)` | Delete multiple by IDs |
