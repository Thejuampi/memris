package io.memris.spring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the foreign key column for a relationship.
 * 
 * <p>Used with {@link ManyToOne} to define the join column details.
 * 
 * <p>Example:
 * <pre>
 * @Entity
 * public class Order {
 *     @Id
 *     private Long id;
 *     
 *     @ManyToOne
 *     @JoinColumn(name = "customer_id", referencedColumnName = "id")
 *     private Customer customer;
 * }
 * 
 * @Entity  
 * public class Customer {
 *     @Id
 *     private Long id;
 *     
 *     private String email;
 * }
 * </pre>
 * 
 * @see ManyToOne
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinColumn {
    
    /**
     * Name of the foreign key column.
     * If not specified, defaults to {fieldName}_{referencedColumnName}
     * (e.g., "customer_id" for a field named "customer").
     * 
     * @return the column name
     */
    String name() default "";
    
    /**
     * Name of the column in the referenced table.
     * Defaults to "id" (the primary key).
     * 
     * @return the referenced column name
     */
    String referencedColumnName() default "id";
    
    /**
     * Whether the foreign key column is nullable.
     * If false and the relationship field is null, an error will be thrown on save.
     * 
     * @return true if nullable
     */
    boolean nullable() default true;
    
    /**
     * Whether this column is unique.
     * Typically false for ManyToOne relationships.
     * 
     * @return true if unique constraint should be added
     */
    boolean unique() default false;
}
