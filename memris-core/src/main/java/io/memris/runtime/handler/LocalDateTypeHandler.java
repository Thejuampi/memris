package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.time.LocalDate;

public class LocalDateTypeHandler extends AbstractTypeHandler<LocalDate> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_LOCAL_DATE;
    }

    @Override
    public Class<LocalDate> getJavaType() {
        return LocalDate.class;
    }

    @Override
    public LocalDate convertValue(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to LocalDate");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, LocalDate value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsLong(columnIndex, value.toEpochDay()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, LocalDate value) {
        long epochDay = value.toEpochDay();
        return createSelection(table, table.scanBetweenLong(columnIndex, epochDay + 1, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, LocalDate value) {
        long epochDay = value.toEpochDay();
        return createSelection(table, table.scanBetweenLong(columnIndex, epochDay, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, LocalDate value) {
        long epochDay = value.toEpochDay();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epochDay - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, LocalDate value) {
        long epochDay = value.toEpochDay();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epochDay));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, LocalDate value) {
        throw new UnsupportedOperationException(
            "BETWEEN for LocalDate requires two arguments, use kernel BETWEEN support");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, LocalDate value) {
        return createSelection(table, table.scanInLong(columnIndex, new long[]{value.toEpochDay()}));
    }
}
