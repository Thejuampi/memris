package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.storage.SimpleTable;

import java.util.HashSet;
import java.util.Set;

public final class JoinExecutorManyToMany implements JoinExecutor<Object, Object> {
    private final SimpleTable joinTable;
    private final String joinColumn;
    private final String inverseJoinColumn;
    private final int sourceIdColumnIndex;
    private final byte sourceIdTypeCode;
    private final int targetIdColumnIndex;
    private final byte targetIdTypeCode;
    private final LogicalQuery.Join.JoinType joinType;

    public JoinExecutorManyToMany(SimpleTable joinTable,
                                  String joinColumn,
                                  String inverseJoinColumn,
                                  int sourceIdColumnIndex,
                                  byte sourceIdTypeCode,
                                  int targetIdColumnIndex,
                                  byte targetIdTypeCode,
                                  LogicalQuery.Join.JoinType joinType) {
        this.joinTable = joinTable;
        this.joinColumn = joinColumn;
        this.inverseJoinColumn = inverseJoinColumn;
        this.sourceIdColumnIndex = sourceIdColumnIndex;
        this.sourceIdTypeCode = sourceIdTypeCode;
        this.targetIdColumnIndex = targetIdColumnIndex;
        this.targetIdTypeCode = targetIdTypeCode;
        this.joinType = joinType;
    }

    @Override
    public Selection filterJoin(GeneratedTable sourceTable,
                                GeneratedTable targetTable,
                                Selection sourceSelection,
                                Selection targetSelection) {
        if (joinType == LogicalQuery.Join.JoinType.LEFT) {
            return sourceSelection != null ? sourceSelection : selectAll(sourceTable);
        }

        if (joinType != LogicalQuery.Join.JoinType.INNER) {
            throw new UnsupportedOperationException("Join type not supported: " + joinType);
        }

        if (joinTable == null) {
            return new SelectionImpl(new long[0]);
        }

        Set<Object> allowedTargets = null;
        if (targetSelection != null) {
            var targetRefs = targetSelection.toRefArray();
            allowedTargets = new HashSet<>(Math.max(16, targetRefs.length * 2));
            for (var ref : targetRefs) {
                var row = io.memris.storage.Selection.index(ref);
                var id = readId(targetTable, targetIdColumnIndex, targetIdTypeCode, row);
                if (id != null) {
                    allowedTargets.add(id);
                }
            }
            if (allowedTargets.isEmpty()) {
                return new SelectionImpl(new long[0]);
            }
        }

        var joinCol = joinTable.column(joinColumn);
        var inverseCol = joinTable.column(inverseJoinColumn);
        if (joinCol == null || inverseCol == null) {
            return new SelectionImpl(new long[0]);
        }

        var joinRowCount = joinTable.rowCount();
        var allowedSources = new HashSet<>(Math.max(16, (int) Math.min(joinRowCount * 2, Integer.MAX_VALUE)));
        for (var i = 0; i < joinRowCount; i++) {
            var rowId = new io.memris.kernel.RowId(i >>> 16, i & 0xFFFF);
            var joinValue = joinCol.get(rowId);
            if (joinValue == null) {
                continue;
            }
            var inverseValue = inverseCol.get(rowId);
            if (inverseValue == null) {
                continue;
            }
            if (allowedTargets != null && !allowedTargets.contains(inverseValue)) {
                continue;
            }
            allowedSources.add(joinValue);
        }

        if (allowedSources.isEmpty()) {
            return new SelectionImpl(new long[0]);
        }

        if (sourceSelection == null) {
            var rows = sourceTable.scanAll();
            var packed = new long[rows.length];
            var count = 0;
            for (var row : rows) {
                if (!sourceTable.isPresent(sourceIdColumnIndex, row)) {
                    continue;
                }
                var sourceId = readId(sourceTable, sourceIdColumnIndex, sourceIdTypeCode, row);
                if (sourceId != null && allowedSources.contains(sourceId)) {
                    packed[count++] = io.memris.storage.Selection.pack(row, sourceTable.rowGeneration(row));
                }
            }
            return new SelectionImpl(trim(packed, count));
        }

        var refs = sourceSelection.toRefArray();
        var packed2 = new long[refs.length];
        var count2 = 0;
        for (var ref : refs) {
            var row = io.memris.storage.Selection.index(ref);
            if (row < 0) {
                continue;
            }
            if (!sourceTable.isPresent(sourceIdColumnIndex, row)) {
                continue;
            }
            var sourceId = readId(sourceTable, sourceIdColumnIndex, sourceIdTypeCode, row);
            if (sourceId != null && allowedSources.contains(sourceId)) {
                packed2[count2++] = io.memris.storage.Selection.pack(row, sourceTable.rowGeneration(row));
            }
        }
        return new SelectionImpl(trim(packed2, count2));
    }

    private Object readId(GeneratedTable table, int columnIndex, byte typeCode, int rowIndex) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> Long.valueOf(table.readLong(columnIndex, rowIndex));
            case TypeCodes.TYPE_INT -> Integer.valueOf(table.readInt(columnIndex, rowIndex));
            case TypeCodes.TYPE_SHORT -> Short.valueOf((short) table.readInt(columnIndex, rowIndex));
            case TypeCodes.TYPE_BYTE -> Byte.valueOf((byte) table.readInt(columnIndex, rowIndex));
            case TypeCodes.TYPE_STRING -> table.readString(columnIndex, rowIndex);
            default -> Long.valueOf(table.readLong(columnIndex, rowIndex));
        };
    }

    private Selection selectAll(GeneratedTable table) {
        var rows = table.scanAll();
        var packed = new long[rows.length];
        for (var i = 0; i < rows.length; i++) {
            var rowIndex = rows[i];
            packed[i] = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
        }
        return new SelectionImpl(packed);
    }

    private long[] trim(long[] values, int size) {
        if (size == values.length) {
            return values;
        }
        long[] trimmed = new long[size];
        System.arraycopy(values, 0, trimmed, 0, size);
        return trimmed;
    }
}
