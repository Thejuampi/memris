package io.memris.kernel;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public final class RowIdBitSet implements MutableRowIdSet {
    private static final int BITS_PER_WORD = 64;
    private static final int DEFAULT_WORDS = 1;

    private final AtomicReference<State> state = new AtomicReference<>(new State(new long[DEFAULT_WORDS], 0));

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        var index = toIndex(rowId);
        var wordIndex = index >>> 6;
        var bitMask = 1L << (index & 63);

        while (true) {
            var current = state.get();
            var words = current.words;
            if (wordIndex >= words.length) {
                var newLength = Math.max(words.length * 2, wordIndex + 1);
                var expanded = new long[newLength];
                System.arraycopy(words, 0, expanded, 0, words.length);
                words = expanded;
            }

            var word = words[wordIndex];
            if ((word & bitMask) != 0L) {
                return;
            }

            var nextWords = words.clone();
            nextWords[wordIndex] = word | bitMask;
            var next = new State(nextWords, current.size + 1);
            if (state.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        var index = toIndex(rowId);
        var wordIndex = index >>> 6;
        var bitMask = 1L << (index & 63);

        while (true) {
            var current = state.get();
            if (wordIndex >= current.words.length) {
                return;
            }
            var word = current.words[wordIndex];
            if ((word & bitMask) == 0L) {
                return;
            }
            var nextWords = current.words.clone();
            nextWords[wordIndex] = word & ~bitMask;
            var next = new State(nextWords, current.size - 1);
            if (state.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public int size() {
        return state.get().size;
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        var index = toIndex(rowId);
        var wordIndex = index >>> 6;
        var bitMask = 1L << (index & 63);
        var current = state.get();
        if (wordIndex >= current.words.length) {
            return false;
        }
        return (current.words[wordIndex] & bitMask) != 0L;
    }

    @Override
    public long[] toLongArray() {
        var snapshot = state.get();
        var values = new long[snapshot.size];
        var index = 0;
        var words = snapshot.words;
        for (int wordIndex = 0; wordIndex < words.length; wordIndex++) {
            var word = words[wordIndex];
            while (word != 0L) {
                var bit = Long.numberOfTrailingZeros(word);
                values[index] = ((long) wordIndex * BITS_PER_WORD) + bit;
                index++;
                word &= word - 1L;
            }
        }
        return values;
    }

    @Override
    public LongEnumerator enumerator() {
        var snapshot = state.get();
        return new LongEnumerator() {
            private int wordIndex;
            private long word = snapshot.words.length == 0 ? 0L : snapshot.words[0];
            private long nextValue = findNextValue();

            @Override
            public boolean hasNext() {
                return nextValue >= 0L;
            }

            @Override
            public long nextLong() {
                if (nextValue < 0L) {
                    throw new NoSuchElementException();
                }
                var value = nextValue;
                nextValue = findNextValue();
                return value;
            }

            private long findNextValue() {
                while (true) {
                    if (word != 0L) {
                        var bit = Long.numberOfTrailingZeros(word);
                        word &= word - 1L;
                        return ((long) wordIndex * BITS_PER_WORD) + bit;
                    }
                    wordIndex++;
                    if (wordIndex >= snapshot.words.length) {
                        return -1L;
                    }
                    word = snapshot.words[wordIndex];
                }
            }
        };
    }

    private static int toIndex(RowId rowId) {
        var value = rowId.value();
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("rowId too large for bitset: " + value);
        }
        if (value < 0) {
            throw new IllegalArgumentException("rowId negative for bitset: " + value);
        }
        return (int) value;
    }

    private record State(long[] words, int size) {
    }
}
