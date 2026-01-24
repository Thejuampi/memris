package io.memris.spring;

/**
 * Lock-free, thread-safe ID generation strategies.
 * No synchronization - uses atomic operations for maximum performance.
 */
public enum GenerationType {
    /**
     * Auto-detect based on field type:
     * - Numeric types: IDENTITY (atomic increment)
     * - UUID: UUID.randomUUID()
     */
    AUTO,

    /**
     * Numeric types only - atomic increment per entity class.
     * Uses AtomicLong for lock-free thread-safety.
     */
    IDENTITY,

    /**
     * UUID types only - generates random UUID via UUID.randomUUID().
     */
    UUID,

    /**
     * Custom generator - user provides IdGenerator implementation.
     */
    CUSTOM
}
