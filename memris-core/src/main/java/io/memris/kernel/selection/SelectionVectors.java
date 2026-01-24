package io.memris.kernel.selection;

import java.util.NoSuchElementException;

public final class SelectionVectors {
    private static final SelectionVector EMPTY = new SelectionVector() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean contains(int rowIndex) {
            return false;
        }

        @Override
        public int[] toIntArray() {
            return new int[0];
        }

        @Override
        public IntEnumerator enumerator() {
            return new IntEnumerator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public int nextInt() {
                    throw new NoSuchElementException();
                }
            };
        }

        @Override
        public SelectionVector filter(io.memris.kernel.Predicate predicate, SelectionVectorFactory factory) {
            return EMPTY;
        }
    };

    private SelectionVectors() {
    }

    public static SelectionVector empty() {
        return EMPTY;
    }
}
