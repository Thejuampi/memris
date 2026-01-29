package io.memris.kernel;

/**
 * Factory for creating selection vectors.
 */
public interface SelectionVectorFactory {
    MutableSelectionVector create(int initialCapacity);
}
