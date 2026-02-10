# Entity Annotations Reference

This reference documents all annotations supported by Memris for entity definitions.

## Core Annotations

All annotations are in the `io.memris.core` package.

### @Entity

Marks a class as a Memris entity.

```java
import io.memris.core.Entity;

@Entity
public class Customer {
    // fields...
}
```

### @Id

Designates the primary key field of an entity.

```java
import io.memris.core.Id;

@Entity
public class Customer {
    @Id
    private Long id;
}
```

### @GeneratedValue

Configures automatic ID generation.

```java
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;

@Entity
public class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
}
```

**Generation Types:**

| Strategy | Description |
|----------|-------------|
| `AUTO` | Automatically selects best strategy (default) |
| `IDENTITY` | Uses auto-incrementing long values |
| `UUID` | Generates UUID strings |
| `CUSTOM` | Uses custom IdGenerator |

## Indexing Annotations

### @Index

Creates an index on a field for faster queries.

```java
import io.memris.core.Index;
import io.memris.core.IndexType;

@Entity
public class Customer {
    @Id
    private Long id;
    
    @Index(IndexType.HASH)
    private String email;
    
    @Index(IndexType.BTREE)
    private int age;
}
```

**Index Types:**

| Type | Best For | Use Cases |
|------|----------|-----------|
| `HASH` | Exact match lookups | findByEmail, findBySku |
| `BTREE` | Range queries | findByAgeGreaterThan, findByDateBetween |
| `PREFIX` | Starts with queries | findByNameStartingWith |
| `SUFFIX` | Ends with queries | findByEmailEndingWith |

### @Indexes

Container annotation for multiple @Index declarations on a single field.

```java
@Entity
@Indexes({
    @Index(name = "email_hash", type = IndexType.HASH),
    @Index(name = "email_prefix", type = IndexType.PREFIX)
})
public class Customer {
    private String email;
}
```

## Relationship Annotations

!!! warning "Eager Loading Only"
    All relationships in Memris are eagerly loaded. There is no lazy loading support.

### @ManyToOne

Defines a many-to-one relationship.

```java
@Entity
public class Order {
    @Id
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
}
```

### @OneToMany

Defines a one-to-many relationship with mappedBy.

```java
@Entity
public class Customer {
    @Id
    private Long id;
    
    @OneToMany(mappedBy = "customer")
    private List<Order> orders;
}
```

### @ManyToMany

Defines a many-to-many relationship.

```java
@Entity
public class Student {
    @Id
    private Long id;
    
    @ManyToMany
    @JoinTable(
        name = "student_course",
        joinColumns = @JoinColumn(name = "student_id"),
        inverseJoinColumns = @JoinColumn(name = "course_id")
    )
    private List<Course> courses;
}
```

### @JoinColumn

Specifies the foreign key column for relationships.

```java
@Entity
public class Order {
    @ManyToOne
    @JoinColumn(name = "customer_id", nullable = false)
    private Customer customer;
}
```

### @JoinTable

Configures the join table for many-to-many relationships.

```java
@JoinTable(
    name = "student_course",
    joinColumns = @JoinColumn(name = "student_id"),
    inverseJoinColumns = @JoinColumn(name = "course_id")
)
```

### @OneToOne

Defines a one-to-one relationship.

```java
@Entity
public class User {
    @Id
    private Long id;
    
    @OneToOne
    @JoinColumn(name = "profile_id")
    private Profile profile;
}
```

## Query Annotations

### @Query

Defines a JPQL query for a repository method.

```java
import io.memris.core.Query;
import io.memris.core.Param;

public interface CustomerRepository extends MemrisRepository<Customer> {
    @Query("select c from Customer c where c.email = :email")
    Optional<Customer> findByEmailQuery(@Param("email") String email);
}
```

### @Modifying

Marks a @Query method as modifying (UPDATE or DELETE).

```java
import io.memris.core.Modifying;

public interface CustomerRepository extends MemrisRepository<Customer> {
    @Modifying
    @Query("update Customer c set c.active = false where c.lastLogin < :date")
    int deactivateOldCustomers(@Param("date") LocalDate date);
}
```

### @Param

Binds method parameters to named query parameters.

```java
@Query("select c from Customer c where c.age > :minAge and c.status = :status")
List<Customer> findByAgeAndStatus(@Param("minAge") int age, @Param("status") String status);
```

## JPA Attribute Converter

### @Converter

While not a Memris annotation, JPA @Converter is supported for custom type conversion.

```java
import jakarta.persistence.Converter;
import jakarta.persistence.AttributeConverter;

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

Converters are automatically registered when using Spring Boot integration.

## Unsupported JPA Annotations

The following standard JPA annotations are **not supported** by Memris:

- `@Column` - Use field names directly
- `@Table` - Entity class name is used as table name
- `@Embedded` / `@Embeddable`
- `@Enumerated` - Enums are handled automatically
- `@Temporal` - Date/time types are handled automatically
- `@Lob`
- `@Version` (optimistic locking)
- `@CreatedDate` / `@LastModifiedDate` - Use @CreatedBy/@LastModifiedBy with AuditProvider
- `@EntityListeners`
- `@PrePersist` / `@PreUpdate` / etc.

## Complete Example

```java
import io.memris.core.*;
import java.util.List;

@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Index(IndexType.HASH)
    private String orderNumber;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
    
    @OneToMany(mappedBy = "order")
    private List<OrderItem> items;
    
    private double total;
    private OrderStatus status;
    
    // Getters and setters...
}
```
