# Entity Annotations Reference

This reference documents all annotations supported by Memris for entity definitions.

All annotations are in the `io.memris.core` package.

## Core Annotations

### @Entity

Marks a class as a Memris entity. Required for all entity classes.

```java
import io.memris.core.Entity;

@Entity
public class Customer {
    // fields...
}
```

### @Id

Designates the primary key field of an entity. Required on exactly one field.

```java
import io.memris.core.Id;

@Entity
public class Customer {
    @Id
    private Long id;
}
```

### @GeneratedValue

Configures automatic ID generation. Use with `@Id`.

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

**Generation Strategies:**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `AUTO` | Auto-detect based on field type (default) | General use |
| `IDENTITY` | Atomic increment per entity class | Numeric IDs (long, int) |
| `UUID` | Generates random UUID via `UUID.randomUUID()` | UUID strings |
| `CUSTOM` | Uses custom `IdGenerator<T>` implementation | Custom ID schemes |

**Custom Generator Example:**

```java
import io.memris.core.IdGenerator;

public class CustomIdGenerator implements IdGenerator<String> {
    @Override
    public String generate() {
        return "CUST-" + System.nanoTime();
    }
}

@Entity
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.CUSTOM, generator = "customIdGenerator")
    private String orderId;
}
```

## Index Annotations

### @Index

Creates an index on a field for faster queries. Can be applied to fields or at class level for composite indexes.

**Field-level Index:**

```java
import io.memris.core.Index;

@Entity
public class Customer {
    @Id
    private Long id;
    
    @Index(type = Index.IndexType.HASH)
    private String email;
    
    @Index(type = Index.IndexType.BTREE)
    private int age;
}
```

**Index Types:**

| Type | Best For | Complexity | Use Cases |
|------|----------|------------|-----------|
| `HASH` | Exact match lookups | O(1) | `findByEmail`, `findBySku` |
| `BTREE` | Range queries | O(log n) | `findByAgeGreaterThan`, `findByDateBetween` |
| `PREFIX` | Starts with queries | O(k) | `findByNameStartingWith` |
| `SUFFIX` | Ends with queries | O(k) | `findByEmailEndingWith` |

**Class-level Composite Index:**

```java
import io.memris.core.Index;
import io.memris.core.Indexes;

@Entity
@Indexes({
    @Index(name = "status_date_idx", fields = {"status", "createdAt"}, type = Index.IndexType.BTREE)
})
public class Order {
    @Id
    private Long id;
    private String status;
    private LocalDateTime createdAt;
}
```

### @Indexes

Container annotation for multiple `@Index` declarations. Used when defining multiple composite indexes.

```java
@Entity
@Indexes({
    @Index(name = "idx_status_total", fields = {"status", "total"}, type = Index.IndexType.BTREE),
    @Index(name = "idx_customer_date", fields = {"customerId", "orderDate"}, type = Index.IndexType.BTREE)
})
public class Order {
    // fields...
}
```

## Relationship Annotations

All relationships in Memris are loaded eagerly (no lazy loading).

### @ManyToOne

Defines a many-to-one relationship.

```java
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;

@Entity
public class Order {
    @Id
    private Long id;
    
    @ManyToOne
    @JoinColumn(name = "customer_id")
    private Customer customer;
}
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `targetEntity` | `Class<?>` | Inferred | The entity class this field references |
| `optional` | `boolean` | `true` | Whether the relationship is optional |

### @OneToMany

Defines a one-to-many relationship. Requires `mappedBy` to specify the owning field.

```java
import io.memris.core.OneToMany;

@Entity
public class Customer {
    @Id
    private Long id;
    
    @OneToMany(mappedBy = "customer")
    private List<Order> orders;
}
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `targetEntity` | `Class<?>` | Inferred from collection | The entity class in the collection |
| `mappedBy` | `String` | `""` | The field that owns the relationship |

### @OneToOne

Defines a one-to-one relationship.

```java
import io.memris.core.OneToOne;
import io.memris.core.JoinColumn;

@Entity
public class User {
    @Id
    private Long id;
    
    @OneToOne
    @JoinColumn(name = "profile_id")
    private Profile profile;
}

@Entity
public class Profile {
    @Id
    private Long id;
    
    @OneToOne(mappedBy = "profile")
    private User user;
}
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `mappedBy` | `String` | `""` | The field that owns the relationship on the other side |

**Note:** All relationships in Memris are loaded eagerly since it's an in-memory storage engine. There is no lazy loading support.

### @ManyToMany

Defines a many-to-many relationship using a join table.

```java
import io.memris.core.ManyToMany;
import io.memris.core.JoinTable;

@Entity
public class Student {
    @Id
    private Long id;
    
    @ManyToMany
    @JoinTable(name = "student_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
    private List<Course> courses;
}

@Entity
public class Course {
    @Id
    private Long id;
    
    @ManyToMany(mappedBy = "courses")
    private List<Student> students;
}
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `targetEntity` | `Class<?>` | Inferred | The entity class in the collection |
| `mappedBy` | `String` | `""` | The field that owns the relationship |

### @JoinColumn

Specifies the foreign key column for `@ManyToOne` and `@OneToOne` relationships.

```java
@ManyToOne
@JoinColumn(name = "customer_id", nullable = false)
private Customer customer;
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `String` | `fieldName_id` | Foreign key column name |
| `referencedColumnName` | `String` | `"id"` | Column in the referenced table |
| `nullable` | `boolean` | `true` | Whether the FK column is nullable |
| `unique` | `boolean` | `false` | Whether to add unique constraint |

### @JoinTable

Configures the join table for `@ManyToMany` relationships.

```java
@ManyToMany
@JoinTable(
    name = "student_course",
    joinColumn = "student_id",
    inverseJoinColumn = "course_id"
)
private List<Course> courses;
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `String` | `""` | Join table name |
| `joinColumn` | `String` | `""` | FK column referencing owning entity |
| `inverseJoinColumn` | `String` | `""` | FK column referencing inverse entity |

## Query Annotations

### @Query

Defines a JPQL-like query for a repository method.

```java
import io.memris.core.Query;
import io.memris.core.Param;

public interface CustomerRepository extends MemrisRepository<Customer> {
    @Query("select c from Customer c where c.email = :email")
    Optional<Customer> findByEmailQuery(@Param("email") String email);
    
    @Query("select c from Customer c where c.name ilike :name and c.age between :min and :max")
    List<Customer> searchByNameAndAge(
        @Param("name") String name,
        @Param("min") int min,
        @Param("max") int max
    );
}
```

**Supported JPQL Features:**
- `SELECT` / `FROM` / `WHERE` / `ORDER BY`
- `JOIN` / `LEFT JOIN` (with aliases)
- Comparisons: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`
- String matching: `LIKE`, `ILIKE` (case-insensitive)
- Sets: `IN` / `NOT IN`
- Ranges: `BETWEEN`
- Null checks: `IS NULL` / `IS NOT NULL`
- Boolean literals: `true` / `false`
- `AND` / `OR` with parentheses

### @Param

Binds method parameters to named query parameters.

```java
@Query("select c from Customer c where c.age > :minAge and c.status = :status")
List<Customer> findByAgeAndStatus(
    @Param("minAge") int age,
    @Param("status") String status
);
```

### @Modifying

Marks a `@Query` method as modifying (UPDATE or DELETE).

```java
import io.memris.core.Modifying;

public interface CustomerRepository extends MemrisRepository<Customer> {
    @Modifying
    @Query("update Customer c set c.active = false where c.lastLogin < :date")
    int deactivateOldCustomers(@Param("date") LocalDate date);
    
    @Modifying
    @Query("delete from Customer c where c.active = false")
    int deleteInactiveCustomers();
}
```

## Complete Entity Example

```java
import io.memris.core.*;
import java.util.List;

@Entity
@Indexes({
    @Index(name = "status_total_idx", fields = {"status", "total"}, type = Index.IndexType.BTREE)
})
public class Order {
    @Id
    public Long id;
    
    @Index(type = Index.IndexType.HASH)
    public String orderNumber;
    
    public Long customerId;
    public String status;
    public long total;
    public long date;
    
    @ManyToOne
    @JoinColumn(name = "customer_id", nullable = false)
    public Customer customer;
    
    @OneToMany(mappedBy = "order")
    public List<OrderItem> items;
    
    public Order() {
        this.date = System.currentTimeMillis();
    }
    
    public Order(Long customerId, String status, long total) {
        this.customerId = customerId;
        this.status = status;
        this.total = total;
        this.date = System.currentTimeMillis();
    }
}
```

## Unsupported JPA Annotations

The following standard JPA annotations are **not supported** by Memris:

| Annotation | Reason |
|------------|--------|
| `@Column` | Use field names directly |
| `@Table` | Entity class name is used as table name |
| `@Embedded` / `@Embeddable` | Not yet implemented |
| `@Enumerated` | Enums are handled automatically (stored as strings) |
| `@Temporal` | Date/time types are handled automatically |
| `@Lob` | Not applicable for in-memory storage |
| `@Version` | Optimistic locking not implemented |
| `@CreatedDate` / `@LastModifiedDate` | Not yet implemented |
| `@EntityListeners` | Not yet implemented |
| `@PrePersist` / `@PostLoad` etc. | Not yet implemented |
