package io.memris.spring.runtime.handlers;

import io.memris.spring.TypeCodes;
import io.memris.spring.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.util.Date;

public class DateTypeHandler extends AbstractTypeHandler<Date> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_DATE;
    }

    @Override
    public Class<Date> getJavaType() {
        return Date.class;
    }

    @Override
    public Date convertValue(Object value) {
        if (value instanceof Date) {
            return (Date) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to Date");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Date value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsLong(columnIndex, value.getTime()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Date value) {
        long epoch = value.getTime();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch + 1, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Date value) {
        long epoch = value.getTime();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Date value) {
        long epoch = value.getTime();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Date value) {
        long epoch = value.getTime();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Date value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Date requires two arguments, use kernel BETWEEN support");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Date value) {
        return createSelection(table, table.scanInLong(columnIndex, new long[]{value.getTime()}));
    }
}
