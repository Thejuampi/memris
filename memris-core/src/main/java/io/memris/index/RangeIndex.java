package io.memris.index;

import io.memris.kernel.LongEnumerator;
import io.memris.kernel.MutableRowIdSet;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.kernel.RowIdSetFactory;
import io.memris.kernel.RowIdSets;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public final class RangeIndex<K extends Comparable<K>> {
    private final ConcurrentSkipListMap<K, MutableRowIdSet> index = new ConcurrentSkipListMap<>();
    private final RowIdSetFactory setFactory;

    public RangeIndex() {
        this(RowIdSetFactory.defaultFactory());
    }

    public RangeIndex(RowIdSetFactory setFactory) {
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

    public RowIdSet lookup(K key) {
        if (key == null) {
            return RowIdSets.empty();
        }
        MutableRowIdSet set = index.get(key);
        return set == null ? RowIdSets.empty() : set;
    }

    public RowIdSet between(K lowerInclusive, K upperInclusive) {
        if (lowerInclusive == null || upperInclusive == null) {
            return RowIdSets.empty();
        }
        return collect(index.subMap(lowerInclusive, true, upperInclusive, true));
    }

    public RowIdSet greaterThan(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.tailMap(value, false));
    }

    public RowIdSet greaterThanOrEqual(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.tailMap(value, true));
    }

    public RowIdSet lessThan(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.headMap(value, false));
    }

    public RowIdSet lessThanOrEqual(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.headMap(value, true));
    }

    public int size() {
        return index.size();
    }

    private RowIdSet collect(NavigableMap<K, MutableRowIdSet> map) {
        if (map.isEmpty()) {
            return RowIdSets.empty();
        }
        int expected = 0;
        for (MutableRowIdSet set : map.values()) {
            expected += set.size();
        }
        MutableRowIdSet result = setFactory.create(expected);
        for (Map.Entry<K, MutableRowIdSet> entry : map.entrySet()) {
            LongEnumerator e = entry.getValue().enumerator();
            while (e.hasNext()) {
                result.add(RowId.fromLong(e.nextLong()));
            }
            result = setFactory.maybeUpgrade(result);
        }
        return result;
    }
}
