package io.memris.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the join table for a many-to-many relationship.
 * 
 * <p>
 * The join table contains foreign key columns referencing both entities
 * in the relationship.
 * 
 * <p>
 * Example usage:
 * 
 * <pre>
 * &#64;ManyToMany
 * &#64;JoinTable(name = "student_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
 * private List&lt;Course&gt; courses;
 * </pre>
 * 
 * @see ManyToMany
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface JoinTable {

    /**
     * The name of the join table.
     * 
     * @return the join table name
     */
    String name() default "";

    /**
     * The foreign key column referencing the owning entity.
     * 
     * @return the join column name
     */
    String joinColumn() default "";

    /**
     * The foreign key column referencing the inverse entity.
     * 
     * @return the inverse join column name
     */
    String inverseJoinColumn() default "";
}
