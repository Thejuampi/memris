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

    public void remove(K key, RowId rowId) {
        if (key == null || rowId == null) {
            return;
        }
        index.computeIfPresent(key, (ignored, existing) -> {
            existing.remove(rowId);
            return existing.size() == 0 ? null : existing;
        });
    }

    public RowIdSet lookup(K key) {
        if (key == null) {
            return RowIdSets.empty();
        }
        var set = index.get(key);
        return set == null ? RowIdSets.empty() : set;
    }

    public RowIdSet lookup(K key, java.util.function.Predicate<RowId> filter) {
        if (key == null) {
            return RowIdSets.empty();
        }
        MutableRowIdSet set = index.get(key);
        if (set == null) {
            return RowIdSets.empty();
        }
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public RowIdSet between(K lowerInclusive, K upperInclusive) {
        if (lowerInclusive == null || upperInclusive == null) {
            return RowIdSets.empty();
        }
        return collect(index.subMap(lowerInclusive, true, upperInclusive, true));
    }

    public RowIdSet between(K lowerInclusive, K upperInclusive, java.util.function.Predicate<RowId> filter) {
        if (lowerInclusive == null || upperInclusive == null) {
            return RowIdSets.empty();
        }
        RowIdSet set = collect(index.subMap(lowerInclusive, true, upperInclusive, true));
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public RowIdSet greaterThan(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.tailMap(value, false));
    }

    public RowIdSet greaterThan(K value, java.util.function.Predicate<RowId> filter) {
        if (value == null) {
            return RowIdSets.empty();
        }
        RowIdSet set = collect(index.tailMap(value, false));
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public RowIdSet greaterThanOrEqual(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.tailMap(value, true));
    }

    public RowIdSet greaterThanOrEqual(K value, java.util.function.Predicate<RowId> filter) {
        if (value == null) {
            return RowIdSets.empty();
        }
        RowIdSet set = collect(index.tailMap(value, true));
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public RowIdSet lessThan(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.headMap(value, false));
    }

    public RowIdSet lessThan(K value, java.util.function.Predicate<RowId> filter) {
        if (value == null) {
            return RowIdSets.empty();
        }
        RowIdSet set = collect(index.headMap(value, false));
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public RowIdSet lessThanOrEqual(K value) {
        if (value == null) {
            return RowIdSets.empty();
        }
        return collect(index.headMap(value, true));
    }

    public RowIdSet lessThanOrEqual(K value, java.util.function.Predicate<RowId> filter) {
        if (value == null) {
            return RowIdSets.empty();
        }
        RowIdSet set = collect(index.headMap(value, true));
        if (filter == null) {
            return set;
        }
        return filterSet(set, filter);
    }

    public int size() {
        return index.size();
    }

    public void clear() {
        index.clear();
    }

    private RowIdSet collect(NavigableMap<K, MutableRowIdSet> map) {
        if (map.isEmpty()) {
            return RowIdSets.empty();
        }
        var expected = 0;
        for (var set : map.values()) {
            expected += set.size();
        }
        var result = setFactory.create(expected);
        for (var entry : map.entrySet()) {
            var e = entry.getValue().enumerator();
            while (e.hasNext()) {
                result.add(RowId.fromLong(e.nextLong()));
            }
            result = setFactory.maybeUpgrade(result);
        }
        return result;
    }

    private RowIdSet filterSet(RowIdSet set, java.util.function.Predicate<RowId> filter) {
        var result = setFactory.create(1);
        var e = set.enumerator();
        while (e.hasNext()) {
            var rowId = RowId.fromLong(e.nextLong());
            if (filter.test(rowId)) {
                result.add(rowId);
                result = setFactory.maybeUpgrade(result);
            }
        }
        return result;
    }
}
