package io.memris.storage.heap;

import io.memris.kernel.RowId;

import java.util.concurrent.ConcurrentHashMap;

/**
 * String key to RowId index with generation tracking for O(1) lookups.
 * <p>
 * Thread-safe index mapping String IDs to RowIds with generation counters.
 * Used for primary key lookups in generated tables with free-list reuse.
 * <p>
 * <b>Generation tracking:</b>
 * When slots are reused after deletion, the generation counter prevents
 * stale reference bugs by validating row slot hasn't been reused.
 * <p>
 * <b>Thread-safety:</b>
 * <ul>
 *   <li>Multiple threads can safely call put, get, remove concurrently</li>
 *   <li>Uses ConcurrentHashMap for lock-free reads</li>
 * </ul>
 */
public final class StringIdIndex {

    private final ConcurrentHashMap<String, RowIdAndGeneration> index;

    public StringIdIndex(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive: " + initialCapacity);
        }
        this.index = new ConcurrentHashMap<>(initialCapacity);
    }

    public int size() {
        return index.size();
    }

    public RowId get(String key) {
        RowIdAndGeneration rag = index.get(key);
        return rag != null ? rag.rowId() : null;
    }

    /**
     * Get RowId and generation together for validation.
     */
    public RowIdAndGeneration getWithGeneration(String key) {
        return index.get(key);
    }

    public void put(String key, RowId rowId) {
        index.put(key, new RowIdAndGeneration(rowId, 0L));
    }

    public void put(String key, RowId rowId, long generation) {
        index.put(key, new RowIdAndGeneration(rowId, generation));
    }

    public void remove(String key) {
        index.remove(key);
    }

    public boolean hasKey(String key) {
        return index.containsKey(key);
    }

    public void clear() {
        index.clear();
    }

    public static final record RowIdAndGeneration(RowId rowId, long generation) {}
}
