package io.memris.kernel;

import java.util.NoSuchElementException;

public final class RowIdSets {
    private static final RowIdSet EMPTY = new RowIdSet() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean contains(RowId rowId) {
            return false;
        }

        @Override
        public long[] toLongArray() {
            return new long[0];
        }

        @Override
        public LongEnumerator enumerator() {
            return new LongEnumerator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public long nextLong() {
                    throw new NoSuchElementException();
                }
            };
        }
    };

    private RowIdSets() {
    }

    public static RowIdSet empty() {
        return EMPTY;
    }
}
