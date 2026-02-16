# Quick Start

Get up and running with Memris in under 5 minutes.

## Complete Example

Here's a minimal, complete example of using Memris:

### 1. Define an Entity

```java
import io.memris.core.Entity;
import io.memris.core.Id;

@Entity
public class Customer {
    @Id
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    private int age;
    
    // Constructors
    public Customer() {}
    
    public Customer(Long id, String firstName, String lastName, String email, int age) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.age = age;
    }
    
    // Getters
    public Long getId() { return id; }
    public String getFirstName() { return firstName; }
    public String getLastName() { return lastName; }
    public String getEmail() { return email; }
    public int getAge() { return age; }
}
```

### 2. Create a Repository Interface

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface CustomerRepository extends MemrisRepository<Customer> {
    Customer save(Customer customer);
    List<Customer> findAll();
    Optional<Customer> findById(Long id);
    List<Customer> findByLastName(String lastName);
    List<Customer> findByAgeGreaterThan(int age);
    boolean existsByEmail(String email);
}
```

### 3. Use the Repository

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;

public class QuickStart {
    public static void main(String[] args) {
        // Create the repository factory
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
        MemrisArena arena = factory.createArena();
        
        // Create repository instance
        CustomerRepository repository = arena.createRepository(CustomerRepository.class);
        
        // Save some customers
        Customer john = repository.save(new Customer(null, "John", "Doe", "john@example.com", 30));
        Customer jane = repository.save(new Customer(null, "Jane", "Smith", "jane@example.com", 25));
        
        // Query by ID
        Customer found = repository.findById(john.getId()).orElse(null);
        System.out.println("Found: " + found.getFirstName() + " " + found.getLastName());
        
        // Query by last name
        List<Customer> does = repository.findByLastName("Doe");
        System.out.println("Found " + does.size() + " Doe(s)");
        
        // Query with condition
        List<Customer> adults = repository.findByAgeGreaterThan(18);
        System.out.println("Found " + adults.size() + " adults");
        
        // Check existence
        boolean exists = repository.existsByEmail("john@example.com");
        System.out.println("Email exists: " + exists);
    }
}
```

## What's Happening?

1. **Entity Definition**: The `@Entity` annotation marks `Customer` as a storable entity. The `@Id` annotation designates the primary key.

2. **Repository Interface**: `MemrisRepository<Customer>` is a marker interface, so you declare the CRUD/query method signatures you use. Memris resolves those signatures at runtime, including derived methods like `findByLastName`.

3. **Arena Isolation**: Each `MemrisArena` is completely isolated - it has its own tables, repositories, and indexes. Data saved in one arena is not visible in another, making arenas ideal for multi-tenant applications and test isolation.

4. **Runtime Generation**: At runtime, Memris uses ByteBuddy to generate optimized storage tables and query implementations based on your entity and repository definitions.

## Query Methods

Memris supports automatic query derivation from method names:

```java
public interface CustomerRepository extends MemrisRepository<Customer> {
    // Explicit CRUD signatures used in this guide
    Customer save(Customer customer);
    List<Customer> findAll();

    // Equality
    List<Customer> findByLastName(String lastName);
    
    // Greater than
    List<Customer> findByAgeGreaterThan(int age);
    
    // Less than
    List<Customer> findByAgeLessThan(int age);
    
    // Between
    List<Customer> findByAgeBetween(int min, int max);
    
    // Like (substring)
    List<Customer> findByEmailContaining(String substring);
    
    // Multiple conditions
    List<Customer> findByLastNameAndAgeGreaterThan(String lastName, int age);
    
    // Exists
    boolean existsByEmail(String email);
    
    // Count
    long countByLastName(String lastName);
}
```

## Arena Isolation Pattern

Each `MemrisArena` is a completely isolated data space with its own tables, repositories, and indexes. Data saved in one arena is not visible in another.

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;

// Factory creates isolated arenas
MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

// Create multiple isolated arenas
MemrisArena tenantA = factory.createArena();
MemrisArena tenantB = factory.createArena();

CustomerRepository repoA = tenantA.createRepository(CustomerRepository.class);
CustomerRepository repoB = tenantB.createRepository(CustomerRepository.class);

// Data is completely isolated between arenas
repoA.save(new Customer(null, "Alice", "TenantA", "alice@a.com", 25));
repoB.save(new Customer(null, "Bob", "TenantB", "bob@b.com", 30));

// repoA only sees Alice, repoB only sees Bob
assert repoA.findAll().size() == 1;  // Alice only
assert repoB.findAll().size() == 1;  // Bob only

// Clean up when done
tenantA.close();
tenantB.close();
```

### Use Cases

| Use Case | Pattern |
|----------|---------|
| **Multi-tenant applications** | One arena per tenant, created on-demand |
| **Test isolation** | Fresh arena per test class/method with `@BeforeEach` |
| **Parallel processing** | Different arenas in different threads without contention |
| **Data snapshots** | Create arena, load data, process without affecting live data |

### Arena Lifecycle

```java
// Create arena (allocates resources)
MemrisArena arena = factory.createArena();

// Use arena
CustomerRepository repo = arena.createRepository(CustomerRepository.class);
repo.save(customer);

// Close arena (releases resources)
arena.close();
```

## Next Steps

Now that you've seen the basics:

- Learn about [Configuration Options](configuration.md)
- Explore [Spring Boot Integration](spring-boot-setup.md)
- See more [Examples](../examples/README.md)
- Read the [Query Reference](../QUERY.md) for all supported operations
