package io.memris.kernel.selection;

import java.util.BitSet;
import java.util.NoSuchElementException;

public final class BitsetSelection implements MutableSelectionVector {
    private final BitSet bitSet = new BitSet();
    private int size;

    @Override
    public void add(int rowIndex) {
        if (rowIndex < 0) {
            throw new IllegalArgumentException("rowIndex must be non-negative");
        }
        if (!bitSet.get(rowIndex)) {
            bitSet.set(rowIndex);
            size++;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(int rowIndex) {
        return rowIndex >= 0 && bitSet.get(rowIndex);
    }

    @Override
    public int[] toIntArray() {
        int[] values = new int[size];
        int index = 0;
        for (int bit = bitSet.nextSetBit(0); bit >= 0; bit = bitSet.nextSetBit(bit + 1)) {
            values[index++] = bit;
        }
        return values;
    }

    @Override
    public IntEnumerator enumerator() {
        return new IntEnumerator() {
            private int current = bitSet.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return current >= 0;
            }

            @Override
            public int nextInt() {
                if (current < 0) {
                    throw new NoSuchElementException();
                }
                int value = current;
                current = bitSet.nextSetBit(current + 1);
                return value;
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
}
