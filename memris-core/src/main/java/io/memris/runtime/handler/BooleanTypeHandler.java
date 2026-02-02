package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for Boolean values.
 * 
 * <p>Booleans are stored as integers (0 = false, 1 = true).
 * Supports equality and boolean-specific operators (IS_TRUE, IS_FALSE).
 */
public class BooleanTypeHandler extends AbstractTypeHandler<Boolean> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_BOOLEAN;
    }
    
    @Override
    public Class<Boolean> getJavaType() {
        return Boolean.class;
    }
    
    @Override
    public Boolean convertValue(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to Boolean");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Boolean value, boolean ignoreCase) {
        int intValue = value ? 1 : 0;
        return createSelection(table, table.scanEqualsInt(columnIndex, intValue));
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Boolean value) {
        throw new UnsupportedOperationException(
            "Greater-than comparison not supported for Boolean type");
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Boolean value) {
        throw new UnsupportedOperationException(
            "Greater-than-or-equal comparison not supported for Boolean type");
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Boolean value) {
        throw new UnsupportedOperationException(
            "Less-than comparison not supported for Boolean type");
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Boolean value) {
        throw new UnsupportedOperationException(
            "Less-than-or-equal comparison not supported for Boolean type");
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Boolean value) {
        throw new UnsupportedOperationException(
            "BETWEEN not supported for Boolean type");
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Boolean value) {
        int intValue = value ? 1 : 0;
        return createSelection(table, table.scanInInt(columnIndex, new int[]{intValue}));
    }

    @Override
    public Selection executeCondition(GeneratedTable table, int columnIndex,
                                      io.memris.query.LogicalQuery.Operator operator, Boolean value, boolean ignoreCase) {
        return switch (operator) {
            case IS_TRUE -> executeIsTrue(table, columnIndex);
            case IS_FALSE -> executeIsFalse(table, columnIndex);
            default -> super.executeCondition(table, columnIndex, operator, value, ignoreCase);
        };
    }
    
    /**
     * Execute IS_TRUE check.
     * Returns all rows where the boolean column is true.
     */
    public Selection executeIsTrue(GeneratedTable table, int columnIndex) {
        return createSelection(table, table.scanEqualsInt(columnIndex, 1));
    }
    
    /**
     * Execute IS_FALSE check.
     * Returns all rows where the boolean column is false.
     */
    public Selection executeIsFalse(GeneratedTable table, int columnIndex) {
        return createSelection(table, table.scanEqualsInt(columnIndex, 0));
    }
}
