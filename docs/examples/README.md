# Examples

This directory contains example domain models and use cases for Memris.

## Available Examples

### [Basic CRUD](basic-crud.md)

Getting started guide covering:
- Entity definition with annotations
- Repository interface creation
- CRUD operations (Create, Read, Update, Delete)
- Query method naming conventions
- Service layer patterns

### [E-Commerce Domain](ecommerce-domain.md)

Complete e-commerce domain model demonstrating:
- Multiple related entities (Customer, Product, Order, OrderItem)
- Relationship annotations (@ManyToOne, @OneToMany, @ManyToMany)
- Index strategies for query optimization
- Composite indexes
- Repository patterns for complex domains
- Query patterns for common e-commerce scenarios

## Quick Start

```java
import io.memris.core.Id;
import io.memris.core.Entity;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import java.util.List;
import java.util.Optional;

@Entity
public class User {
    @Id
    public Long id;
    public String email;
    public String name;
    
    public User() {}
    
    public User(String email, String name) {
        this.email = email;
        this.name = name;
    }
}

public interface UserRepository extends MemrisRepository<User> {
    User save(User user);
    Optional<User> findById(Long id);
    Optional<User> findByEmail(String email);
    List<User> findByNameContaining(String name);
    List<User> findAll();
    void deleteById(Long id);
}

public class App {
    public static void main(String[] args) {
        var factory = new MemrisRepositoryFactory();
        var arena = factory.createArena();
        var repo = arena.createRepository(UserRepository.class);
        
        var user = repo.save(new User("test@example.com", "Test User"));
        var found = repo.findByEmail("test@example.com");
        
        factory.close();
    }
}
```

## Documentation

- [Query Reference](../QUERY.md) - Complete query operator reference
- [Entity Annotations](../entity-annotations.md) - All supported annotations
- [Architecture](../ARCHITECTURE.md) - System design and internals
