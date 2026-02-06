package io.memris.index;

import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;

public final class CompositeRangeIndex {
    private final RangeIndex<CompositeKey> delegate = new RangeIndex<>();

    public void add(CompositeKey key, RowId rowId) {
        delegate.add(key, rowId);
    }

    public void remove(CompositeKey key, RowId rowId) {
        delegate.remove(key, rowId);
    }

    public RowIdSet lookup(CompositeKey key, java.util.function.Predicate<RowId> filter) {
        return delegate.lookup(key, filter);
    }

    public RowIdSet between(CompositeKey lowerInclusive, CompositeKey upperInclusive,
            java.util.function.Predicate<RowId> filter) {
        return delegate.between(lowerInclusive, upperInclusive, filter);
    }

    public RowIdSet greaterThan(CompositeKey value, java.util.function.Predicate<RowId> filter) {
        return delegate.greaterThan(value, filter);
    }

    public RowIdSet greaterThanOrEqual(CompositeKey value, java.util.function.Predicate<RowId> filter) {
        return delegate.greaterThanOrEqual(value, filter);
    }

    public RowIdSet lessThan(CompositeKey value, java.util.function.Predicate<RowId> filter) {
        return delegate.lessThan(value, filter);
    }

    public RowIdSet lessThanOrEqual(CompositeKey value, java.util.function.Predicate<RowId> filter) {
        return delegate.lessThanOrEqual(value, filter);
    }

    public void clear() {
        delegate.clear();
    }
}
