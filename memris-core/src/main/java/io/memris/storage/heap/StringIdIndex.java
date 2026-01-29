package io.memris.storage.heap;

import io.memris.kernel.RowId;

import java.util.concurrent.ConcurrentHashMap;

/**
 * String key to RowId index for O(1) lookups.
 */
public final class StringIdIndex {

    private final ConcurrentHashMap<String, RowId> index;

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
        return index.get(key);
    }

    public void put(String key, RowId rowId) {
        index.put(key, rowId);
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
}
