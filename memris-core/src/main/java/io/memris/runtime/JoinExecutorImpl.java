package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;

import java.util.Arrays;

public final class JoinExecutorImpl implements JoinExecutor<Object, Object> {
    private final int sourceColumnIndex;
    private final int targetColumnIndex;
    private final boolean targetColumnIsId;
    private final byte fkTypeCode;
    private final LogicalQuery.Join.JoinType joinType;

    public JoinExecutorImpl(int sourceColumnIndex, int targetColumnIndex, boolean targetColumnIsId, byte fkTypeCode, LogicalQuery.Join.JoinType joinType) {
        this.sourceColumnIndex = sourceColumnIndex;
        this.targetColumnIndex = targetColumnIndex;
        this.targetColumnIsId = targetColumnIsId;
        this.fkTypeCode = fkTypeCode;
        this.joinType = joinType;
    }

    @Override
    public Selection filterJoin(GeneratedTable sourceTable, GeneratedTable targetTable, Selection sourceSelection, Selection targetSelection) {
        if (joinType == LogicalQuery.Join.JoinType.LEFT) {
            return sourceSelection != null ? sourceSelection : selectAll(sourceTable);
        }

        if (joinType != LogicalQuery.Join.JoinType.INNER) {
            throw new UnsupportedOperationException("Join type not supported: " + joinType);
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
            for (int i = 0; i < rows.length; i++) {
                int rowIndex = rows[i];
                sourceRefs[i] = io.memris.storage.Selection.pack(rowIndex, sourceTable.rowGeneration(rowIndex));
            }
        } else {
            sourceRefs = sourceSelection.toRefArray();
        }
        long[] matched = new long[sourceRefs.length];
        int count = 0;

        long[] targetValues = null;
        if (!targetColumnIsId) {
            targetValues = buildTargetValueSet(targetTable, targetSelection);
            if (targetValues.length == 0) {
                return new SelectionImpl(new long[0]);
            }
        }

        for (long ref : sourceRefs) {
            int sourceRow = io.memris.storage.Selection.index(ref);
            if (!sourceTable.isPresent(sourceColumnIndex, sourceRow)) {
                continue;
            }
            long fkValue = readFkValue(sourceTable, sourceRow);

            if (targetColumnIsId) {
                long targetRef = targetTable.lookupById(fkValue);
                if (targetRef < 0) {
                    continue;
                }
                int targetRow = io.memris.storage.Selection.index(targetRef);
                if (targetAllowed != null && (targetRow < 0 || targetRow >= targetAllowed.length || !targetAllowed[targetRow])) {
                    continue;
                }
            } else {
                if (!contains(targetValues, fkValue)) {
                    continue;
                }
            }

            matched[count++] = io.memris.storage.Selection.pack(sourceRow, sourceTable.rowGeneration(sourceRow));
        }

        long[] packed = new long[count];
        System.arraycopy(matched, 0, packed, 0, count);
        return new SelectionImpl(packed);
    }

    private long[] buildTargetValueSet(GeneratedTable targetTable, Selection targetSelection) {
        int[] rows;
        if (targetSelection != null) {
            rows = targetSelection.toIntArray();
        } else {
            rows = targetTable.scanAll();
        }
        if (rows.length == 0) {
            return new long[0];
        }

        long[] values = new long[rows.length];
        int count = 0;
        for (int row : rows) {
            if (!targetTable.isPresent(targetColumnIndex, row)) {
                continue;
            }
            values[count++] = readTargetValue(targetTable, row);
        }
        if (count == 0) {
            return new long[0];
        }
        values = Arrays.copyOf(values, count);
        Arrays.sort(values);
        return dedupe(values);
    }

    private long readTargetValue(GeneratedTable targetTable, int rowIndex) {
        return switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> targetTable.readLong(targetColumnIndex, rowIndex);
            case TypeCodes.TYPE_INT -> targetTable.readInt(targetColumnIndex, rowIndex);
            case TypeCodes.TYPE_SHORT -> targetTable.readInt(targetColumnIndex, rowIndex);
            case TypeCodes.TYPE_BYTE -> targetTable.readInt(targetColumnIndex, rowIndex);
            default -> targetTable.readLong(targetColumnIndex, rowIndex);
        };
    }

    private static boolean contains(long[] sortedValues, long target) {
        int lo = 0;
        int hi = sortedValues.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long value = sortedValues[mid];
            if (value < target) {
                lo = mid + 1;
            } else if (value > target) {
                hi = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }

    private static long[] dedupe(long[] values) {
        if (values.length < 2) {
            return values;
        }
        int count = 1;
        for (int i = 1; i < values.length; i++) {
            if (values[i] != values[i - 1]) {
                values[count++] = values[i];
            }
        }
        return Arrays.copyOf(values, count);
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
        for (int i = 0; i < rows.length; i++) {
            int rowIndex = rows[i];
            packed[i] = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
        }
        return new SelectionImpl(packed);
    }
}
