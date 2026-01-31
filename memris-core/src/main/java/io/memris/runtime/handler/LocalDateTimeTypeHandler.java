package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class LocalDateTimeTypeHandler extends AbstractTypeHandler<LocalDateTime> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_LOCAL_DATE_TIME;
    }

    @Override
    public Class<LocalDateTime> getJavaType() {
        return LocalDateTime.class;
    }

    @Override
    public LocalDateTime convertValue(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to LocalDateTime");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, LocalDateTime value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsLong(columnIndex, value.toInstant(ZoneOffset.UTC).toEpochMilli()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, LocalDateTime value) {
        long epoch = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch + 1, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, LocalDateTime value) {
        long epoch = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, LocalDateTime value) {
        long epoch = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, LocalDateTime value) {
        long epoch = value.toInstant(ZoneOffset.UTC).toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, LocalDateTime value) {
        throw new UnsupportedOperationException(
            "BETWEEN for LocalDateTime requires two arguments, use kernel BETWEEN support");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, LocalDateTime value) {
        return createSelection(table, table.scanInLong(columnIndex, new long[]{value.toInstant(ZoneOffset.UTC).toEpochMilli()}));
    }
}
