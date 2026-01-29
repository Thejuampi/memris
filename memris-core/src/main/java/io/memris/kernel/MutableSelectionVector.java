package io.memris.kernel;

/**
 * Mutable selection vector for storing row indices.
 */
public interface MutableSelectionVector extends SelectionVector {
    void add(int rowIndex);
}
