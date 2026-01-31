package io.memris.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ID generation strategy annotation (subset of JPA).
 * Lock-free, thread-safe ID generation for maximum performance.
 * Supports numeric primitives, UUID, and custom generators.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {
    /**
     * ID generation strategy (no locking).
     * AUTO: Auto-detect based on field type (numeric = IDENTITY, UUID = UUID)
     * IDENTITY: Numeric types only - atomic increment per entity class
     * UUID: UUID types only - use UUID.randomUUID()
     * CUSTOM: Use custom IdGenerator<T> implementation
     */
    GenerationType strategy() default GenerationType.AUTO;

    /**
     * Optional custom generator class name or bean name.
     * Generator must implement IdGenerator<T> interface.
     * For CUSTOM strategy, generator() is required.
     */
    String generator() default "";
}
