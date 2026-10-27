package io.memris.core;

/**
 * Custom ID generator extension declaration.
 * The default engine does not discover, register, or invoke these implementations.
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
