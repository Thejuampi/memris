package io.memris.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a many-to-many relationship between entities.
 * 
 * <p>
 * Many-to-many relationships use a join table to store the associations.
 * Configure the join table using {@link JoinTable}.
 * 
 * <p>
 * Example usage:
 * 
 * <pre>
 * &#64;Entity
 * public class Student {
 *     &#64;Id
 *     private Long id;
 * 
 *     &#64;ManyToMany
 *     &#64;JoinTable(name = "student_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
 *     private List&lt;Course&gt; courses;
 * }
 * 
 * &#64;Entity
 * public class Course {
 *     &#64;Id
 *     private Long id;
 * 
 *     &#64;ManyToMany(mappedBy = "courses")
 *     private List&lt;Student&gt; students;
 * }
 * </pre>
 * 
 * @see JoinTable
 * @see OneToMany
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToMany {

    /**
     * The entity class that is the target of the relationship.
     * If not specified, inferred from the collection's element type.
     * 
     * @return the target entity class, or void.class to infer from collection
     */
    Class<?> targetEntity() default void.class;

    /**
     * The field that owns the relationship on the target entity.
     * Used for bidirectional many-to-many relationships.
     * 
     * @return the name of the field owning the relationship, or empty for owning
     *         side
     */
    String mappedBy() default "";
}
