package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Character values.
 * 
 * <p>Characters are stored as integers (char code) in PageColumnInt.
 * Supports equality and comparison operators.
 */
public class CharTypeHandler extends AbstractTypeHandler<Character> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_CHAR;
    }
    
    @Override
    public Class<Character> getJavaType() {
        return Character.class;
    }
    
    @Override
    public Character convertValue(Object value) {
        if (value instanceof Character) {
            return (Character) value;
        } else if (value instanceof Number) {
            return (char) ((Number) value).intValue();
        } else if (value instanceof String && ((String) value).length() == 1) {
            return ((String) value).charAt(0);
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Character");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Character value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsInt(columnIndex, value.charValue()));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Character value) {
        int charCode = value.charValue();
        return createSelection(table, table.scanBetweenInt(columnIndex, charCode + 1, Character.MAX_VALUE));
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Character value) {
        int charCode = value.charValue();
        return createSelection(table, table.scanBetweenInt(columnIndex, charCode, Character.MAX_VALUE));
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Character value) {
        int charCode = value.charValue();
        return createSelection(table, table.scanBetweenInt(columnIndex, Character.MIN_VALUE, charCode - 1));
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Character value) {
        int charCode = value.charValue();
        return createSelection(table, table.scanBetweenInt(columnIndex, Character.MIN_VALUE, charCode));
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Character value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Character requires a char[2] array, use executeBetweenRange");
    }
    
    /**
     * Execute between with explicit range.
     */
    public Selection executeBetweenRange(GeneratedTable table, int columnIndex, char min, char max) {
        return createSelection(table, table.scanBetweenInt(columnIndex, min, max));
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Character value) {
        return createSelection(table, table.scanInInt(columnIndex, new int[]{(int) value.charValue()}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, char[] values) {
        int[] intValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            intValues[i] = values[i];
        }
        return createSelection(table, table.scanInInt(columnIndex, intValues));
    }
}
