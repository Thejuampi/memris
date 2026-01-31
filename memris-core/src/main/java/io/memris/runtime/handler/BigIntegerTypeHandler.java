package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.math.BigInteger;

public class BigIntegerTypeHandler extends AbstractTypeHandler<BigInteger> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_BIG_INTEGER;
    }

    @Override
    public Class<BigInteger> getJavaType() {
        return BigInteger.class;
    }

    @Override
    public BigInteger convertValue(Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to BigInteger");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, BigInteger value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsString(columnIndex, value.toString()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, BigInteger value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigInteger");
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, BigInteger value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigInteger");
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, BigInteger value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigInteger");
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, BigInteger value) {
        throw new UnsupportedOperationException("Comparison operators not supported for BigInteger");
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, BigInteger value) {
        throw new UnsupportedOperationException("BETWEEN not supported for BigInteger");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, BigInteger value) {
        return createSelection(table, table.scanInString(columnIndex, new String[]{value.toString()}));
    }
}
