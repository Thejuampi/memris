package io.memris.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
@Target({ ElementType.FIELD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Repeatable(Indexes.class)
public @interface Index {
    /**
     * Optional index name for reference.
     */
    String name() default "";

    /**
     * Fields for composite class-level index declarations.
     * Ignored for field-level usage.
     */
    String[] fields() default {};

    /**
     * Index type - HASH for equality lookups, BTREE for range queries.
     */
    IndexType type() default IndexType.HASH;

    enum IndexType {
        /** Hash-based index for equality lookups (O(1)) */
        HASH,
        /** B-tree/skip-list index for range queries (O(log n)) */
        BTREE,
        /** Prefix tree (trie) index for STARTING_WITH queries (O(k)) */
        PREFIX,
        /** Reverse string index for ENDING_WITH queries (O(k)) */
        SUFFIX
    }
}
