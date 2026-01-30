package io.memris.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a many-to-one relationship between entities.
 * 
 * <p>All relationships in Memris are loaded eagerly since it's an in-memory
 * storage engine. There is no lazy loading support.
 * 
 * <p>Example usage:
 * <pre>
 * @Entity
 * public class Order {
 *     @Id
 *     private Long id;
 *     
 *     @ManyToOne
 *     @JoinColumn(name = "customer_id")
 *     private Customer customer;
 *     
 *     private long total;
 * }
 * 
 * @Entity
 * public class Customer {
 *     @Id
 *     private Long id;
 *     
 *     private String email;
 * }
 * 
 * // Query with implicit join:
 * List<Order> orders = orderRepo.findByCustomerEmail("john@example.com");
 * </pre>
 * 
 * <p>The relationship can be queried using dot notation in repository method names.
 * The field name becomes the property path prefix (e.g., "customer" in the example above).
 * 
 * @see JoinColumn
 * @see OneToMany
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToOne {
    
    /**
     * The entity class this field references.
     * If not specified, inferred from the field's type.
     * 
     * @return the target entity class, or void.class to use field type
     */
    Class<?> targetEntity() default void.class;
    
    /**
     * Whether the relationship is optional.
     * If false, the foreign key column will be non-nullable.
     * 
     * @return true if the relationship is optional
     */
    boolean optional() default true;
}
