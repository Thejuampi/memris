package io.memris.kernel.selection;

public interface SelectionVector {
    int size();

    boolean contains(int rowIndex);

    int[] toIntArray();

    IntEnumerator enumerator();

    SelectionVector filter(io.memris.kernel.Predicate predicate, SelectionVectorFactory factory);
}
