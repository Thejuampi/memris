package io.memris.spring.runtime;

import io.memris.spring.TypeCodes;
import io.memris.spring.plan.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;

public final class JoinExecutorImpl implements JoinExecutor<Object, Object> {
    private final int sourceColumnIndex;
    private final int targetColumnIndex;
    private final byte fkTypeCode;
    private final LogicalQuery.Join.JoinType joinType;

    public JoinExecutorImpl(int sourceColumnIndex, int targetColumnIndex, byte fkTypeCode, LogicalQuery.Join.JoinType joinType) {
        this.sourceColumnIndex = sourceColumnIndex;
        this.targetColumnIndex = targetColumnIndex;
        this.fkTypeCode = fkTypeCode;
        this.joinType = joinType;
    }

    @Override
    public Selection filterJoin(GeneratedTable sourceTable, GeneratedTable targetTable, Selection sourceSelection, Selection targetSelection) {
        if (joinType == LogicalQuery.Join.JoinType.LEFT) {
            return sourceSelection != null ? sourceSelection : selectAll(sourceTable);
        }

        boolean[] targetAllowed = null;
        if (targetSelection != null) {
            int size = Math.toIntExact(targetTable.allocatedCount());
            targetAllowed = new boolean[size];
            long[] refs = targetSelection.toRefArray();
            for (long ref : refs) {
                int row = io.memris.storage.Selection.index(ref);
                if (row >= 0 && row < size) {
                    targetAllowed[row] = true;
                }
            }
        }

        long[] sourceRefs;
        if (sourceSelection == null) {
            int[] rows = sourceTable.scanAll();
            sourceRefs = new long[rows.length];
            long gen = sourceTable.currentGeneration();
            for (int i = 0; i < rows.length; i++) {
                sourceRefs[i] = io.memris.storage.Selection.pack(rows[i], gen);
            }
        } else {
            sourceRefs = sourceSelection.toRefArray();
        }

        long gen = sourceTable.currentGeneration();
        long[] matched = new long[sourceRefs.length];
        int count = 0;

        for (long ref : sourceRefs) {
            int sourceRow = io.memris.storage.Selection.index(ref);
            long fkValue = readFkValue(sourceTable, sourceRow);
            long targetRef = targetTable.lookupById(fkValue);
            if (targetRef < 0) {
                continue;
            }
            int targetRow = io.memris.storage.Selection.index(targetRef);
            if (targetAllowed != null && (targetRow < 0 || targetRow >= targetAllowed.length || !targetAllowed[targetRow])) {
                continue;
            }
            matched[count++] = io.memris.storage.Selection.pack(sourceRow, gen);
        }

        long[] packed = new long[count];
        System.arraycopy(matched, 0, packed, 0, count);
        return new SelectionImpl(packed);
    }

    private long readFkValue(GeneratedTable sourceTable, int rowIndex) {
        return switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> sourceTable.readLong(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_INT -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_SHORT -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_BYTE -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            default -> sourceTable.readLong(sourceColumnIndex, rowIndex);
        };
    }

    private Selection selectAll(GeneratedTable table) {
        int[] rows = table.scanAll();
        long[] packed = new long[rows.length];
        long gen = table.currentGeneration();
        for (int i = 0; i < rows.length; i++) {
            packed[i] = io.memris.storage.Selection.pack(rows[i], gen);
        }
        return new SelectionImpl(packed);
    }
}
