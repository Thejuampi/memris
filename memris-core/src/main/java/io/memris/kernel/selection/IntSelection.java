package io.memris.kernel.selection;

import io.memris.kernel.MutableSelectionVector;
import io.memris.kernel.SelectionVector;
import io.memris.kernel.SelectionVectorFactory;

import java.util.Arrays;
import java.util.NoSuchElementException;

public final class IntSelection implements MutableSelectionVector {
    private static final int DEFAULT_CAPACITY = 16;

    private int[] values;
    private int size;

    public IntSelection() {
        this.values = new int[DEFAULT_CAPACITY];
    }

    public IntSelection(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative");
        }
        this.values = new int[Math.max(DEFAULT_CAPACITY, initialCapacity)];
    }

    @Override
    public void add(int rowIndex) {
        if (rowIndex < 0) {
            throw new IllegalArgumentException("rowIndex must be non-negative");
        }
        ensureCapacity(size + 1);
        values[size++] = rowIndex;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(int rowIndex) {
        // NOTE: O(n) linear search - acceptable for sparse selections
        // For O(1) contains(), system auto-upgrades to BitsetSelection
        if (rowIndex < 0) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (values[i] == rowIndex) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int[] toIntArray() {
        return Arrays.copyOf(values, size);
    }

    @Override
    public IntEnumerator enumerator() {
        return new IntEnumerator() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public int nextInt() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }

    public SelectionVector filter(io.memris.kernel.Predicate predicate, SelectionVectorFactory factory) {
        throw new UnsupportedOperationException(
            "SelectionVector.filter() is not supported. " +
            "Filtering requires table data access - use Table.scan(Predicate) instead. " +
            "SelectionVector only stores row indices and cannot evaluate predicates without column data.");
    }

    private void ensureCapacity(int desired) {
        if (desired <= values.length) {
            return;
        }
        int newCapacity = Math.max(values.length * 2, desired);
        values = Arrays.copyOf(values, newCapacity);
    }
}
