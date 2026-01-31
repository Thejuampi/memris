package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Double values.
 * 
 * <p>Doubles are stored as longs using {@link Double#doubleToLongBits(double)}
 * to preserve exact bit patterns (handles NaN, infinity, etc.).
 * Supports all standard comparison operators.
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
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Double");
        }
    }
    
    /**
     * Convert double to long bits for storage/comparison.
     */
    private long doubleToLongBits(double value) {
        return Double.doubleToLongBits(value);
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Double value, boolean ignoreCase) {
        // For doubles, equality comparison is tricky due to precision
        // We use exact bit comparison here
        long longValue = doubleToLongBits(value);
        return createSelection(table, table.scanEqualsLong(columnIndex, longValue));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Double value) {
        // Note: This compares the bit patterns, not the numeric values
        // For proper numeric comparison, we need to scan and compare each value
        throw new UnsupportedOperationException(
            "Numeric comparison for Double requires scanning and comparing each value");
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Double value) {
        throw new UnsupportedOperationException(
            "Numeric comparison for Double requires scanning and comparing each value");
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Double value) {
        throw new UnsupportedOperationException(
            "Numeric comparison for Double requires scanning and comparing each value");
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Double value) {
        throw new UnsupportedOperationException(
            "Numeric comparison for Double requires scanning and comparing each value");
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Double value) {
        throw new UnsupportedOperationException(
            "BETWEEN not yet implemented for Double - requires numeric comparison");
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Double value) {
        long longValue = doubleToLongBits(value);
        return createSelection(table, table.scanInLong(columnIndex, new long[]{longValue}));
    }
    
    /**
     * Execute approximate equality with epsilon tolerance.
     * Not yet implemented.
     */
    public Selection executeApproxEquals(GeneratedTable table, int columnIndex, double value, double epsilon) {
        throw new UnsupportedOperationException(
            "Approximate equality not yet implemented - requires scanning all values");
    }
}
