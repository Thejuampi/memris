package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.math.BigDecimal;

public class BigDecimalTypeHandler extends AbstractTypeHandler<BigDecimal> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_BIG_DECIMAL;
    }

    @Override
    public Class<BigDecimal> getJavaType() {
        return BigDecimal.class;
    }

    @Override
    public BigDecimal convertValue(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to BigDecimal");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, BigDecimal value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsString(columnIndex, value.toString()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, BigDecimal value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigDecimal");
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, BigDecimal value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigDecimal");
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, BigDecimal value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigDecimal");
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, BigDecimal value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigDecimal");
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, BigDecimal value) {
        throw new UnsupportedOperationException("BETWEEN not supported for BigDecimal");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, BigDecimal value) {
        return createSelection(table, table.scanInString(columnIndex, new String[]{value.toString()}));
    }
}
