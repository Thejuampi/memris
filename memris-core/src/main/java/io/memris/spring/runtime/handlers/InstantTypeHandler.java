package io.memris.spring.runtime.handlers;

import io.memris.spring.TypeCodes;
import io.memris.spring.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

import java.time.Instant;

public class InstantTypeHandler extends AbstractTypeHandler<Instant> {

    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_INSTANT;
    }

    @Override
    public Class<Instant> getJavaType() {
        return Instant.class;
    }

    @Override
    public Instant convertValue(Object value) {
        if (value instanceof Instant) {
            return (Instant) value;
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to Instant");
    }

    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, Instant value, boolean ignoreCase) {
        return createSelection(table, table.scanEqualsLong(columnIndex, value.toEpochMilli()));
    }

    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Instant value) {
        long epoch = value.toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch + 1, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Instant value) {
        long epoch = value.toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, epoch, Long.MAX_VALUE));
    }

    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, Instant value) {
        long epoch = value.toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch - 1));
    }

    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Instant value) {
        long epoch = value.toEpochMilli();
        return createSelection(table, table.scanBetweenLong(columnIndex, Long.MIN_VALUE, epoch));
    }

    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, Instant value) {
        throw new UnsupportedOperationException(
            "BETWEEN for Instant requires two arguments, use kernel BETWEEN support");
    }

    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, Instant value) {
        return createSelection(table, table.scanInLong(columnIndex, new long[]{value.toEpochMilli()}));
    }
}
