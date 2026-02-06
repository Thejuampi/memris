package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Float values.
 * 
 * <p>
 * Floats are stored using sortable integer encoding that preserves numerical
 * ordering for all values including negative numbers. This allows range
 * comparisons
 * to work correctly.
 * 
 * <p>
 * Supports equality and comparison operators with correct numerical ordering.
 */
public class FloatTypeHandler extends AbstractTypeHandler<Float> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_FLOAT;
    }

    @Override
    public Class<Float> getJavaType() {
        return Float.class;
    }

    @Override
    public Float convertValue(Object value) {
        return switch (value) {
            case Float v -> v;
            case Number number -> number.floatValue();
            default -> throw new IllegalArgumentException(
                    "Cannot convert " + value.getClass() + " to Float");
        };
    }

    /**
     * Convert float to sortable int bits for storage/comparison.
     */
    private int floatToSortableInt(float value) {
        return FloatEncoding.floatToSortableInt(value);
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Float value, boolean ignoreCase) {
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanEqualsInt(columnIndex, bits));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Float value) {
        // Uses sortable encoding - comparison works correctly for all values
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, bits + 1, Integer.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, bits, Integer.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, bits - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, bits));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Float value) {
        throw new UnsupportedOperationException(
                "BETWEEN for Float is handled by HeapRuntimeKernel using range arguments");
    }

    /**
     * Execute between with explicit range using sortable encoding.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, float min, float max) {
        int minBits = floatToSortableInt(min);
        int maxBits = floatToSortableInt(max);
        return createSelection(table, table.scanBetweenInt(columnIndex, minBits, maxBits));
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToSortableInt(value);
        return createSelection(table, table.scanInInt(columnIndex, new int[] { bits }));
    }

    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, float[] values) {
        int[] bits = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = floatToSortableInt(values[i]);
        }
        return createSelection(table, table.scanInInt(columnIndex, bits));
    }

    /**
     * Check if value is NaN.
     */
    public Selection executeIsNaN(GeneratedTable table, int columnIndex) {
        int nanBits = floatToSortableInt(Float.NaN);
        return createSelection(table, table.scanEqualsInt(columnIndex, nanBits));
    }

    /**
     * Check if value is infinite (positive or negative).
     */
    public Selection executeIsInfinite(GeneratedTable table, int columnIndex) {
        int posInfBits = floatToSortableInt(Float.POSITIVE_INFINITY);
        int negInfBits = floatToSortableInt(Float.NEGATIVE_INFINITY);

        int[] posInfMatches = table.scanEqualsInt(columnIndex, posInfBits);
        int[] negInfMatches = table.scanEqualsInt(columnIndex, negInfBits);

        // Union of both
        int[] allMatches = new int[posInfMatches.length + negInfMatches.length];
        System.arraycopy(posInfMatches, 0, allMatches, 0, posInfMatches.length);
        System.arraycopy(negInfMatches, 0, allMatches, posInfMatches.length, negInfMatches.length);

        return createSelection(table, allMatches);
    }
}
