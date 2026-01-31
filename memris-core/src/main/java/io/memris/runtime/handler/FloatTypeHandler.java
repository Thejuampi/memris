package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Float values.
 * 
 * <p>Floats are stored as integers using {@link Float#floatToIntBits(float)}
 * to preserve exact bit patterns (handles NaN, infinity, -0.0, etc.).
 * Supports equality and comparison operators.
 * 
 * <p>Note: Range comparisons use the raw bit representation, which works correctly
 * for positive numbers but may give unexpected results for special values (NaN, etc.).
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
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Float");
        }
    }
    
    /**
     * Convert float to int bits for storage/comparison.
     */
    private int floatToIntBits(float value) {
        return Float.floatToIntBits(value);
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Float value, boolean ignoreCase) {
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanEqualsInt(columnIndex, bits));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Float value) {
        // Note: This compares bit patterns, not numeric values
        // For positive numbers, bit order matches numeric order
        // For negative numbers and special values, results may be unexpected
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, bits + 1, Integer.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, bits, Integer.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, bits - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, bits));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Float value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Float requires a float[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     * Note: Uses bit comparison, which may not match numeric comparison for all values.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, float min, float max) {
        int minBits = floatToIntBits(min);
        int maxBits = floatToIntBits(max);
        return createSelection(table, table.scanBetweenInt(columnIndex, minBits, maxBits));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Float value) {
        int bits = floatToIntBits(value);
        return createSelection(table, table.scanInInt(columnIndex, new int[]{bits}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, float[] values) {
        int[] bits = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = floatToIntBits(values[i]);
        }
        return createSelection(table, table.scanInInt(columnIndex, bits));
    }
    
    /**
     * Check if value is NaN.
     */
    public Selection executeIsNaN(GeneratedTable table, int columnIndex) {
        int nanBits = floatToIntBits(Float.NaN);
        return createSelection(table, table.scanEqualsInt(columnIndex, nanBits));
    }
    
    /**
     * Check if value is infinite (positive or negative).
     */
    public Selection executeIsInfinite(GeneratedTable table, int columnIndex) {
        int posInfBits = floatToIntBits(Float.POSITIVE_INFINITY);
        int negInfBits = floatToIntBits(Float.NEGATIVE_INFINITY);
        
        int[] posInfMatches = table.scanEqualsInt(columnIndex, posInfBits);
        int[] negInfMatches = table.scanEqualsInt(columnIndex, negInfBits);
        
        // Union of both
        int[] allMatches = new int[posInfMatches.length + negInfMatches.length];
        System.arraycopy(posInfMatches, 0, allMatches, 0, posInfMatches.length);
        System.arraycopy(negInfMatches, 0, allMatches, posInfMatches.length, negInfMatches.length);
        
        return createSelection(table, allMatches);
    }
}
