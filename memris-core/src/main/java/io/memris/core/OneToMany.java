package io.memris.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a one-to-many relationship between entities.
 * 
 * <p>
 * One-to-many relationships require a {@code mappedBy} attribute to specify
 * the inverse field on the target entity that owns the relationship.
 * 
 * <p>
 * Example usage:
 * 
 * <pre>
 * &#64;Entity
 * public class Customer {
 *     &#64;Id
 *     private Long id;
 * 
 *     &#64;OneToMany(mappedBy = "customer")
 *     private List&lt;Order&gt; orders;
 * }
 * 
 * &#64;Entity
 * public class Order {
 *     &#64;Id
 *     private Long id;
 * 
 *     &#64;ManyToOne
 *     &#64;JoinColumn(name = "customer_id")
 *     private Customer customer;
 * }
 * </pre>
 * 
 * @see ManyToOne
 * @see ManyToMany
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {

    /**
     * The entity class that is the target of the relationship.
     * If not specified, inferred from the collection's element type.
     * 
     * @return the target entity class, or void.class to infer from collection
     */
    Class<?> targetEntity() default void.class;

    /**
     * The field that owns the relationship on the target entity.
     * Required for bidirectional one-to-many relationships.
     * 
     * @return the name of the field owning the relationship
     */
    String mappedBy() default "";
}
