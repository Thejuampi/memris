package io.memris.core;

/**
 * Lock-free, thread-safe custom ID generator.
 * Implementations must be deterministic for testability.
 *
 * @param <T> ID type (numeric primitives or UUID)
 */
@FunctionalInterface
public interface IdGenerator<T> {
    /**
     * Generate the next unique ID.
     * Must be thread-safe and lock-free for performance.
     *
     * @return The generated ID
     */
    T generate();
}
