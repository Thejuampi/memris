package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Byte values.
 * 
 * <p>Bytes are stored as integers in PageColumnInt.
 * Supports all standard comparison operators.
 */
public class ByteTypeHandler extends AbstractTypeHandler<Byte> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_BYTE;
    }
    
    @Override
    public Class<Byte> getJavaType() {
        return Byte.class;
    }
    
    @Override
    public Byte convertValue(Object value) {
        if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof Number) {
            return ((Number) value).byteValue();
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Byte");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Byte value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsInt(columnIndex, value.intValue()));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Byte value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value + 1, Byte.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Byte value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, value, Byte.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Byte value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Byte.MIN_VALUE, value - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Byte value) {
        return createSelection(table, table.scanBetweenInt(columnIndex, Byte.MIN_VALUE, value));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Byte value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Byte requires a byte[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, byte min, byte max) {
        return createSelection(table, table.scanBetweenInt(columnIndex, min, max));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Byte value) {
        return createSelection(table, table.scanInInt(columnIndex, new int[]{value.intValue()}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, byte[] values) {
        int[] intValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            intValues[i] = values[i];
        }
        return createSelection(table, table.scanInInt(columnIndex, intValues));
    }
}
