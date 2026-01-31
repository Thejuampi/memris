package io.memris.index;

import io.memris.kernel.MutableRowIdSet;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.kernel.RowIdSetFactory;
import io.memris.kernel.RowIdSets;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class HashIndex<K> {
    private final ConcurrentHashMap<K, MutableRowIdSet> index = new ConcurrentHashMap<>();
    private final RowIdSetFactory setFactory;

    public HashIndex() {
        this(RowIdSetFactory.defaultFactory());
    }

    public HashIndex(RowIdSetFactory setFactory) {
        this.setFactory = Objects.requireNonNull(setFactory, "setFactory");
    }

    public void add(K key, RowId rowId) {
        if (key == null) {
            throw new IllegalArgumentException("key required");
        }
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        index.compute(key, (ignored, existing) -> {
            MutableRowIdSet set = existing == null ? setFactory.create(1) : existing;
            set.add(rowId);
            return setFactory.maybeUpgrade(set);
        });
    }

    /**
     * Remove all rows for the given key from the index.
     * @param key The key whose rows should be removed
     */
    public void removeAll(K key) {
        if (key == null) {
            throw new IllegalArgumentException("key required");
        }
        index.remove(key);
    }

    public void remove(K key, RowId rowId) {
        if (key == null || rowId == null) {
            return;
        }
        index.computeIfPresent(key, (ignored, existing) -> {
            existing.remove(rowId);
            return existing.size() == 0 ? null : existing;
        });
    }

    /**
     * Clear all entries from the index (O(1) operation).
     * Used for deleteAll() to avoid iterating over all entities.
     */
    public void clear() {
        index.clear();
    }

    public RowIdSet lookup(K key) {
        if (key == null) {
            return RowIdSets.empty();
        }
        MutableRowIdSet set = index.get(key);
        return set == null ? RowIdSets.empty() : set;
    }

    public int size() {
        return index.size();
    }

    /**
     * Get the underlying map for iteration (used by compaction).
     * Returns a copy of the entries to avoid concurrent modification issues.
     */
    public java.util.Map<K, MutableRowIdSet> entries() {
        return new java.util.HashMap<>(index);
    }
}
