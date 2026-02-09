package io.memris.runtime.dispatch;

import io.memris.storage.Selection;

public final class InIndexSelector {

    private InIndexSelector() {
    }

    @FunctionalInterface
    public interface EqIndexLookup {
        int[] query(Object value);
    }

    @FunctionalInterface
    public interface SelectionBuilder {
        Selection fromRows(int[] rows);
    }

    public static Selection select(Object value, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        Iterable<?> iterable = null;
        if (value instanceof Iterable<?> it) {
            iterable = it;
        } else if (value instanceof Object[] arr) {
            iterable = java.util.Arrays.asList(arr);
        }

        if (iterable == null) {
            if (value instanceof int[] ints) {
                Selection combined = null;
                for (var item : ints) {
                    var rows = lookup.query(item);
                    if (rows == null) {
                        return null;
                    }
                    var next = selectionBuilder.fromRows(rows);
                    combined = combined == null ? next : combined.union(next);
                }
                return combined;
            }
            if (value instanceof long[] longs) {
                Selection combined = null;
                for (var item : longs) {
                    var rows = lookup.query(item);
                    if (rows == null) {
                        return null;
                    }
                    var next = selectionBuilder.fromRows(rows);
                    combined = combined == null ? next : combined.union(next);
                }
                return combined;
            }
            return null;
        }

        Selection combined = null;
        for (var item : iterable) {
            var rows = lookup.query(item);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }
}
