package io.memris.kernel.selection;

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

    @Override
    public SelectionVector filter(io.memris.kernel.Predicate predicate, SelectionVectorFactory factory) {
        MutableSelectionVector result = factory.create(size);
        IntEnumerator e = enumerator();
        while (e.hasNext()) {
            int idx = e.nextInt();
            if (matches(idx, predicate)) {
                result.add(idx);
            }
        }
        return result;
    }

    private boolean matches(int rowIndex, io.memris.kernel.Predicate predicate) {
        return true;
    }

    private void ensureCapacity(int desired) {
        if (desired <= values.length) {
            return;
        }
        int newCapacity = Math.max(values.length * 2, desired);
        values = Arrays.copyOf(values, newCapacity);
    }
}
