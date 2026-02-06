package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Integer values.
 * 
 * <p>Supports all standard comparison operators for 32-bit integers.
 * Uses {@link GeneratedTable} scan methods optimized for int columns.
 */
public class IntTypeHandler extends AbstractTypeHandler<Integer> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_INT;
    }
    
    @Override
    public Class<Integer> getJavaType() {
        return Integer.class;
    }
    
    @Override
    public Integer convertValue(Object value) {
        return switch (value) {
            case Integer i -> i;
            case Number number -> number.intValue();
            default -> throw new IllegalArgumentException(
                    "Cannot convert " + value.getClass() + " to Integer");
        };
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Integer value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsInt(columnIndex, value));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Integer value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value + 1, Integer.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Integer value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value, Integer.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Integer value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, value - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Integer value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, value));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Integer value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Integer requires an int[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, int min, int max) {
        return createSelection(table, table.scanBetweenInt(columnIndex, min, max));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Integer value) {
        // Single value IN - treat as equality
        return createSelection(table, table.scanInInt(columnIndex, new int[]{value}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, int[] values) {
        return createSelection(table, table.scanInInt(columnIndex, values));
    }
    
    @Override
    protected Selection executeIsNull(GeneratedTable table, int columnIndex) {
        int[] rows = table.scanAll();
        int[] nullRows = new int[rows.length];
        int count = 0;
        for (int row : rows) {
            if (!table.isPresent(columnIndex, row)) {
                nullRows[count++] = row;
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(nullRows, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }
}
