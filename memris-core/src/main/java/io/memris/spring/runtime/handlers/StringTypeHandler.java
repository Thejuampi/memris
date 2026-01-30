package io.memris.spring.runtime.handlers;

import io.memris.spring.TypeCodes;
import io.memris.spring.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for String values.
 * 
 * <p>Supports equality, case-insensitive equality, and set operations.
 * Pattern matching operators (LIKE, CONTAINING) are not yet implemented.
 */
public class StringTypeHandler extends AbstractTypeHandler<String> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_STRING;
    }
    
    @Override
    public Class<String> getJavaType() {
        return String.class;
    }
    
    @Override
    public String convertValue(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to String");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, String value, boolean ignoreCase) {
        if (ignoreCase) {
            return createSelection(table, table.scanEqualsStringIgnoreCase(columnIndex, value));
        } else {
            return createSelection(table, table.scanEqualsString(columnIndex, value));
        }
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Greater-than comparison not supported for String type");
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Greater-than-or-equal comparison not supported for String type");
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Less-than comparison not supported for String type");
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Less-than-or-equal comparison not supported for String type");
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "BETWEEN not supported for String type");
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, String value) {
        // Single value IN - treat as equality
        return createSelection(table, table.scanInString(columnIndex, new String[]{value}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, String[] values) {
        return createSelection(table, table.scanInString(columnIndex, values));
    }
    
    /**
     * Execute pattern matching with LIKE operator.
     * Not yet implemented.
     */
    public Selection executeLike(GeneratedTable table, int columnIndex, String pattern, boolean ignoreCase) {
        throw new UnsupportedOperationException("LIKE operator not yet implemented for String");
    }
    
    /**
     * Execute pattern matching with CONTAINING operator.
     * Not yet implemented.
     */
    public Selection executeContaining(GeneratedTable table, int columnIndex, String substring, boolean ignoreCase) {
        throw new UnsupportedOperationException("CONTAINING operator not yet implemented for String");
    }
    
    /**
     * Execute pattern matching with STARTING_WITH operator.
     * Not yet implemented.
     */
    public Selection executeStartingWith(GeneratedTable table, int columnIndex, String prefix, boolean ignoreCase) {
        throw new UnsupportedOperationException("STARTING_WITH operator not yet implemented for String");
    }
    
    /**
     * Execute pattern matching with ENDING_WITH operator.
     * Not yet implemented.
     */
    public Selection executeEndingWith(GeneratedTable table, int columnIndex, String suffix, boolean ignoreCase) {
        throw new UnsupportedOperationException("ENDING_WITH operator not yet implemented for String");
    }
}
