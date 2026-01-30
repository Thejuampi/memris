package io.memris.spring.runtime.handlers;

import io.memris.spring.TypeCodes;
import io.memris.spring.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Short values.
 * 
 * <p>Shorts are stored as integers in PageColumnInt.
 * Supports all standard comparison operators.
 */
public class ShortTypeHandler extends AbstractTypeHandler<Short> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_SHORT;
    }
    
    @Override
    public Class<Short> getJavaType() {
        return Short.class;
    }
    
    @Override
    public Short convertValue(Object value) {
        if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof Number) {
            return ((Number) value).shortValue();
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Short");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Short value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsInt(columnIndex, value.intValue()));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Short value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value + 1, Short.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Short value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value, Short.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Short value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Short.MIN_VALUE, value - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Short value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Short.MIN_VALUE, value));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Short value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Short requires a short[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, short min, short max) {
        return createSelection(table, table.scanBetweenInt(columnIndex, min, max));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Short value) {
        return createSelection(table, table.scanInInt(columnIndex, new int[]{value.intValue()}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, short[] values) {
        int[] intValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            intValues[i] = values[i];
        }
        return createSelection(table, table.scanInInt(columnIndex, intValues));
    }
}
