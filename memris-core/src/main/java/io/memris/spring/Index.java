package io.memris.spring;

import java.lang.annotation.*;

/**
 * Marks a field to be indexed for faster query lookups.
 * Supports both equality (HashIndex) and range (RangeIndex) queries.
 *
 * <p>Example usage:
 * <pre>
 * {@code @Entity}
 * public class User {
 *     {@code @Id} int id;
 *     {@code @Index} String email;        // HashIndex for O(1) lookups
 *     {@code @Index} int age;            // RangeIndex for GT/LT queries
 * }
 * </pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Index {
    /**
     * Optional index name for reference.
     */
    String name() default "";

    /**
     * Index type - HASH for equality lookups, BTREE for range queries.
     */
    IndexType type() default IndexType.HASH;

    enum IndexType {
        /** Hash-based index for equality lookups (O(1)) */
        HASH,
        /** B-tree/skip-list index for range queries (O(log n)) */
        BTREE
    }
}
