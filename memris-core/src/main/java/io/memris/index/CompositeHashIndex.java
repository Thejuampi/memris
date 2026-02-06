package io.memris.index;

import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;

public final class CompositeHashIndex {
    private final HashIndex<CompositeKey> delegate = new HashIndex<>();

    public void add(CompositeKey key, RowId rowId) {
        delegate.add(key, rowId);
    }

    public void remove(CompositeKey key, RowId rowId) {
        delegate.remove(key, rowId);
    }

    public RowIdSet lookup(CompositeKey key, java.util.function.Predicate<RowId> filter) {
        return delegate.lookup(key, filter);
    }

    public void clear() {
        delegate.clear();
    }
}
