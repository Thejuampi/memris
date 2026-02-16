package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.runtime.InArgumentDecoder;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Double values.
 * 
 * <p>
 * Doubles are stored using sortable long encoding that preserves numerical
 * ordering for all values including negative numbers. This allows range
 * comparisons
 * to work correctly.
 * 
 * <p>
 * Supports all standard comparison operators with correct numerical ordering.
 */
public class DoubleTypeHandler extends AbstractTypeHandler<Double> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_DOUBLE;
    }

    @Override
    public Class<Double> getJavaType() {
        return Double.class;
    }

    @Override
    public Double convertValue(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot convert null to Double");
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Cannot convert " + value.getClass() + " to Double", e);
            }
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert " + value.getClass() + " to Double");
        }
    }
    /**
     * Execute IN with a list of values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, java.util.List<?> values) {
        long[] bits = InArgumentDecoder.toLongArrayStrict(TypeCodes.TYPE_DOUBLE, values);
        return createSelection(table, table.scanInLong(columnIndex, bits));
    }

    /**
     * Convert double to sortable long bits for storage/comparison.
     */
    private long doubleToLongBits(double value) {
        return FloatEncoding.doubleToSortableLong(value);
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Double value, boolean ignoreCase) {
        if (value == null) return createSelection(table, new int[0]);
        long longValue = doubleToLongBits(value);
        return createSelection(table, table.scanEqualsLong(columnIndex, longValue));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Double value) {
        // Uses sortable encoding - comparison works correctly for all values
        long bits = doubleToLongBits(value);
        return createSelection(table, table.scanBetweenLong(columnIndex, bits + 1, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Double value) {
        long bits = doubleToLongBits(value);
        return createSelection(table, table.scanBetweenLong(columnIndex, bits, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Double value) {
        long bits = doubleToLongBits(value);
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, bits - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Double value) {
        long bits = doubleToLongBits(value);
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, bits));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Double value) {
        throw new UnsupportedOperationException(
                "BETWEEN for Double requires a double[2] array, use executeBetweenRange");
    }

    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, double min, double max) {
        long minBits = doubleToLongBits(min);
        long maxBits = doubleToLongBits(max);
        return createSelection(table, table.scanBetweenLong(columnIndex, minBits, maxBits));
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Double value) {
        long longValue = doubleToLongBits(value);
        return createSelection(table, table.scanInLong(columnIndex, new long[] { longValue }));
    }

    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, double[] values) {
        long[] bits = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = doubleToLongBits(values[i]);
        }
        return createSelection(table, table.scanInLong(columnIndex, bits));
    }
}
