package io.memris.kernel;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Lock-free chunked bitset for large row ID sets (≥4096 entries).
 * <p>
 * Partitions the bit space into fixed-size chunks of 64 longs (4096 bits each).
 * Mutations CAS only the affected chunk — O(512 bytes) fixed cost regardless
 * of total set size. Reads are fully lock-free: single volatile read to get
 * the chunk reference, then plain array access.
 * <p>
 * <b>Contract:</b>
 * <ul>
 *   <li>This is a <b>set</b> - no duplicates allowed. add() is idempotent.</li>
 *   <li>remove() is idempotent - no-op when value absent.</li>
 *   <li>size() returns the cardinality (unique count).</li>
 *   <li>enumerator() is snapshot-at-creation and never fails due to concurrent mutation.</li>
 *   <li>Iteration order is unspecified.</li>
 * </ul>
 * <p>
 * <b>Write path:</b> CAS loop on the single affected chunk. Contention is
 * per-chunk (1/N of total), and each retry copies only 512 bytes.
 * <p>
 * <b>Read path:</b> Completely lock-free. No CAS, no locks, no retries.
 * Snapshot-consistent per chunk.
 */
public final class RowIdBitSet implements MutableRowIdSet {
    private static final int BITS_PER_WORD = 64;
    private static final int CHUNK_WORDS = 64;          // 64 longs = 4096 bits per chunk
    private static final int BITS_PER_CHUNK = CHUNK_WORDS * BITS_PER_WORD; // 4096
    private static final int INITIAL_CHUNKS = 1;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<RowIdBitSet, AtomicReferenceArray> CHUNKS_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(RowIdBitSet.class, AtomicReferenceArray.class, "chunks");
    private static final AtomicIntegerFieldUpdater<RowIdBitSet> SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RowIdBitSet.class, "size");

    private volatile AtomicReferenceArray<long[]> chunks;
    private volatile int size;

    public RowIdBitSet() {
        var initial = new AtomicReferenceArray<long[]>(INITIAL_CHUNKS);
        initial.set(0, new long[CHUNK_WORDS]);
        this.chunks = initial;
    }

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        var index = toIndex(rowId);
        var chunkIndex = index / BITS_PER_CHUNK;
        var wordInChunk = (index / BITS_PER_WORD) % CHUNK_WORDS;
        var bitMask = 1L << (index & 63);

        ensureChunks(chunkIndex);

        while (true) {
            var currentChunks = chunks;
            var chunk = currentChunks.get(chunkIndex);

            if ((chunk[wordInChunk] & bitMask) != 0L) {
                return; // Already present
            }

            var newChunk = chunk.clone(); // Fixed 512 bytes, always
            newChunk[wordInChunk] = chunk[wordInChunk] | bitMask;

            if (currentChunks.compareAndSet(chunkIndex, chunk, newChunk)) {
                SIZE_UPDATER.incrementAndGet(this);
                return;
            }
            // CAS failed — another writer touched THIS chunk. Retry.
            // Cost: re-clone 512 bytes. Contention is 1/numChunks.
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        var index = toIndex(rowId);
        var chunkIndex = index / BITS_PER_CHUNK;
        var wordInChunk = (index / BITS_PER_WORD) % CHUNK_WORDS;
        var bitMask = 1L << (index & 63);

        var currentChunks = chunks;
        if (chunkIndex >= currentChunks.length()) {
            return;
        }
        var chunk = currentChunks.get(chunkIndex);
        if (chunk == null) {
            return;
        }

        while (true) {
            if ((chunk[wordInChunk] & bitMask) == 0L) {
                return; // Not present
            }

            var newChunk = chunk.clone();
            newChunk[wordInChunk] = chunk[wordInChunk] & ~bitMask;

            if (currentChunks.compareAndSet(chunkIndex, chunk, newChunk)) {
                SIZE_UPDATER.decrementAndGet(this);
                return;
            }
            // Reload chunk after failed CAS
            chunk = currentChunks.get(chunkIndex);
            if (chunk == null) {
                return;
            }
        }
    }

    @Override
    public int size() {
        return size; // Volatile read, ~1ns
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        var index = toIndex(rowId);
        var chunkIndex = index / BITS_PER_CHUNK;
        var wordInChunk = (index / BITS_PER_WORD) % CHUNK_WORDS;
        var bitMask = 1L << (index & 63);

        var currentChunks = chunks;
        if (chunkIndex >= currentChunks.length()) {
            return false;
        }
        var chunk = currentChunks.get(chunkIndex);
        if (chunk == null) {
            return false;
        }
        return (chunk[wordInChunk] & bitMask) != 0L;
    }

    @Override
    public long[] toLongArray() {
        // Snapshot: read size first, then iterate chunks
        var currentSize = size;
        if (currentSize == 0) {
            return new long[0];
        }
        var result = new long[currentSize];
        var resultIndex = 0;
        var currentChunks = chunks;
        var numChunks = currentChunks.length();

        for (var ci = 0; ci < numChunks && resultIndex < currentSize; ci++) {
            var chunk = currentChunks.get(ci);
            if (chunk == null) {
                continue;
            }
            for (var wi = 0; wi < CHUNK_WORDS && resultIndex < currentSize; wi++) {
                var word = chunk[wi];
                while (word != 0L && resultIndex < currentSize) {
                    var bit = Long.numberOfTrailingZeros(word);
                    result[resultIndex++] = ((long) ci * BITS_PER_CHUNK)
                            + ((long) wi * BITS_PER_WORD) + bit;
                    word &= word - 1L;
                }
            }
        }

        if (resultIndex < currentSize) {
            return Arrays.copyOf(result, resultIndex);
        }
        return result;
    }

    @Override
    public LongEnumerator enumerator() {
        // Snapshot the chunks array reference and iterate lock-free
        var currentChunks = chunks;
        var numChunks = currentChunks.length();
        return new LongEnumerator() {
            private int chunkIdx;
            private int wordIdx;
            private long word = loadFirstWord();
            private long nextValue = findNext();

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
                nextValue = findNext();
                return value;
            }

            private long loadFirstWord() {
                while (chunkIdx < numChunks) {
                    var chunk = currentChunks.get(chunkIdx);
                    if (chunk != null && chunk[0] != 0L) {
                        return chunk[0];
                    }
                    if (chunk != null) {
                        // Scan for first non-zero word in chunk
                        for (wordIdx = 1; wordIdx < CHUNK_WORDS; wordIdx++) {
                            if (chunk[wordIdx] != 0L) {
                                return chunk[wordIdx];
                            }
                        }
                    }
                    chunkIdx++;
                    wordIdx = 0;
                }
                return 0L;
            }

            private long findNext() {
                while (true) {
                    if (word != 0L) {
                        var bit = Long.numberOfTrailingZeros(word);
                        word &= word - 1L;
                        return ((long) chunkIdx * BITS_PER_CHUNK)
                                + ((long) wordIdx * BITS_PER_WORD) + bit;
                    }
                    // Advance to next non-zero word
                    wordIdx++;
                    while (true) {
                        if (wordIdx >= CHUNK_WORDS) {
                            chunkIdx++;
                            wordIdx = 0;
                            if (chunkIdx >= numChunks) {
                                return -1L;
                            }
                        }
                        var chunk = currentChunks.get(chunkIdx);
                        if (chunk == null) {
                            chunkIdx++;
                            wordIdx = 0;
                            if (chunkIdx >= numChunks) {
                                return -1L;
                            }
                            continue;
                        }
                        if (chunk[wordIdx] != 0L) {
                            word = chunk[wordIdx];
                            break;
                        }
                        wordIdx++;
                    }
                }
            }
        };
    }

    /**
     * Ensure the chunks array is large enough for the given chunk index.
     * Uses CAS to grow atomically — only one thread's expansion wins,
     * others retry and see the expanded array.
     */
    private void ensureChunks(int requiredChunkIndex) {
        while (true) {
            var currentChunks = chunks;
            if (requiredChunkIndex < currentChunks.length()) {
                // Chunk slot exists — re-read chunks to handle concurrent expansion
                var chunksNow = chunks;
                if (chunksNow.get(requiredChunkIndex) == null) {
                    chunksNow.compareAndSet(requiredChunkIndex, null, new long[CHUNK_WORDS]);
                }
                return;
            }
            // Need to grow the chunks array
            var newLength = Math.max(currentChunks.length() * 2, requiredChunkIndex + 1);
            var newChunks = new AtomicReferenceArray<long[]>(newLength);
            for (var i = 0; i < currentChunks.length(); i++) {
                newChunks.set(i, currentChunks.get(i));
            }
            newChunks.set(requiredChunkIndex, new long[CHUNK_WORDS]);
            if (CHUNKS_UPDATER.compareAndSet(this, currentChunks, newChunks)) {
                return;
            }
            // Another thread grew it first — retry
        }
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
}
