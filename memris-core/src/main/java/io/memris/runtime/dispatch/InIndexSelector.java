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
            case null -> selectionBuilder.fromRows(new int[0]);
            case int[] ints -> unionIntValues(ints, lookup, selectionBuilder);
            case long[] longs -> unionLongValues(longs, lookup, selectionBuilder);
            case byte[] bytes -> unionByteValues(bytes, lookup, selectionBuilder);
            case short[] shorts -> unionShortValues(shorts, lookup, selectionBuilder);
            case char[] chars -> unionCharValues(chars, lookup, selectionBuilder);
            case boolean[] booleans -> unionBooleanValues(booleans, lookup, selectionBuilder);
            case float[] floats -> unionFloatValues(floats, lookup, selectionBuilder);
            case double[] doubles -> unionDoubleValues(doubles, lookup, selectionBuilder);
            case Object[] objects -> unionObjectValues(objects, lookup, selectionBuilder);
            case Iterable<?> iterable -> unionIterableValues(iterable, lookup, selectionBuilder);
            default -> null;
        };
    }

    private static Selection unionIntValues(int[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionLongValues(long[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionByteValues(byte[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionShortValues(short[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionCharValues(char[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionBooleanValues(boolean[] values,
            EqIndexLookup lookup,
            SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionFloatValues(float[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionDoubleValues(double[] values,
            EqIndexLookup lookup,
            SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }

    private static Selection unionObjectValues(Object[] values, EqIndexLookup lookup, SelectionBuilder selectionBuilder) {
        if (values.length == 0) {
            return selectionBuilder.fromRows(new int[0]);
        }
        Selection combined = null;
        for (var value : values) {
            var rows = lookup.query(value);
            if (rows == null) {
                return null;
            }
            var next = selectionBuilder.fromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
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
        return combined != null ? combined : selectionBuilder.fromRows(new int[0]);
    }
}
