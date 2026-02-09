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
        return switch (value) {
            case int[] ints -> unionIntValues(ints, lookup, selectionBuilder);
            case long[] longs -> unionLongValues(longs, lookup, selectionBuilder);
            case Object[] objects -> unionObjectValues(objects, lookup, selectionBuilder);
            case Iterable<?> iterable -> unionIterableValues(iterable, lookup, selectionBuilder);
            default -> null;
        };
    }

    private static Selection unionIntValues(int[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }

    private static Selection unionLongValues(long[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }

    private static Selection unionObjectValues(Object[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }

    private static Selection unionIterableValues(Iterable<?> values,
            EqIndexLookup lookup,
            SelectionBuilder selectionBuilder) {
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }
}
