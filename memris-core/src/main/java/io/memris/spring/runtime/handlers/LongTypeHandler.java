package io.memris.spring.runtime.handlers;

import io.memris.spring.TypeCodes;
import io.memris.spring.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Long values.
 * 
 * <p>Supports all standard comparison operators for 64-bit integers.
 * Uses {@link GeneratedTable} scan methods optimized for long columns.
 */
public class LongTypeHandler extends AbstractTypeHandler<Long> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_LONG;
    }
    
    @Override
    public Class<Long> getJavaType() {
        return Long.class;
    }
    
    @Override
    public Long convertValue(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Long");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Long value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsLong(columnIndex, value));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Long value) {
        return createSelection(table, table.scanBetweenLong(columnIndex, value + 1, Long.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Long value) {
        return createSelection(table, table.scanBetweenLong(columnIndex, value, Long.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Long value) {
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, value - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Long value) {
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, value));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Long value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Long requires a long[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, long min, long max) {
        return createSelection(table, table.scanBetweenLong(columnIndex, min, max));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Long value) {
        // Single value IN - treat as equality
        return createSelection(table, table.scanInLong(columnIndex, new long[]{value}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, long[] values) {
        return createSelection(table, table.scanInLong(columnIndex, values));
    }
}
