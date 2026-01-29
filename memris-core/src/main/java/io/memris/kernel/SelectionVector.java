package io.memris.kernel;

import io.memris.kernel.selection.IntEnumerator;

/**
 * Immutable selection vector for storing row indices.
 */
public interface SelectionVector {
    int size();
    boolean contains(int rowIndex);
    int[] toIntArray();
    IntEnumerator enumerator();
}
