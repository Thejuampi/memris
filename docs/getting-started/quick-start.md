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
    Optional<Customer> findById(Long id);
    List<Customer> findByLastName(String lastName);
    List<Customer> findByAgeGreaterThan(int age);
    boolean existsByEmail(String email);
}
```

### 3. Use the Repository

```java
import io.memris.core.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;

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

2. **Repository Interface**: By extending `MemrisRepository<Customer>`, you get basic CRUD operations. Method names like `findByLastName` are automatically implemented using Memris's query derivation.

3. **Runtime Generation**: At runtime, Memris uses ByteBuddy to generate optimized storage tables and query implementations based on your entity and repository definitions.

## Query Methods

Memris supports automatic query derivation from method names:

```java
public interface CustomerRepository extends MemrisRepository<Customer> {
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

## Next Steps

Now that you've seen the basics:

- Learn about [Configuration Options](configuration.md)
- Explore [Spring Boot Integration](spring-boot-setup.md)
- See more [Examples](../examples/index.md)
- Read the [Query Reference](../query-reference.md) for all supported operations
