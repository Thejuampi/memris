package io.memris.spring.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.LogicalQuery;

public final class HeapRuntimeKernel {

    private final GeneratedTable table;

    public HeapRuntimeKernel(GeneratedTable table) {
        this.table = table;
    }

    public long rowCount() {
        return table.liveCount();
    }

    public long allocatedCount() {
        return table.allocatedCount();
    }

    public int columnCount() {
        return table.columnCount();
    }

    public byte typeCodeAt(int columnIndex) {
        return table.typeCodeAt(columnIndex);
    }

    public GeneratedTable table() {
        return table;
    }

    public Selection executeCondition(CompiledQuery.CompiledCondition cc, Object[] args) {
        int columnIndex = cc.columnIndex();
        LogicalQuery.Operator operator = cc.operator();
        Object value = args[cc.argumentIndex()];

        byte typeCode = table.typeCodeAt(columnIndex);
        if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
            return executeLongCondition(columnIndex, operator, (Long) value);
        } else if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
            return executeIntCondition(columnIndex, operator, (Integer) value);
        } else if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
            return executeStringCondition(columnIndex, operator, (String) value, cc.ignoreCase());
        } else {
            throw new IllegalArgumentException("Unsupported type: " + typeCode);
        }
    }

    private Selection executeLongCondition(int colIdx, LogicalQuery.Operator op, Object value) {
        long longValue = (value instanceof Long) ? (Long) value : ((Number) value).longValue();
        return switch (op) {
            case EQ -> createSelection(table.scanEqualsLong(colIdx, longValue));
            case NE -> {
                int[] all = table.scanAll();
                int[] matches = table.scanEqualsLong(colIdx, longValue);
                yield subtractSelections(all, matches);
            }
            case GT -> createSelection(table.scanBetweenLong(colIdx, longValue + 1, Long.MAX_VALUE));
            case LT -> createSelection(table.scanBetweenLong(colIdx, Long.MIN_VALUE, longValue - 1));
            case GTE -> createSelection(table.scanBetweenLong(colIdx, longValue, Long.MAX_VALUE));
            case LTE -> createSelection(table.scanBetweenLong(colIdx, Long.MIN_VALUE, longValue));
            case BETWEEN -> throw new UnsupportedOperationException("BETWEEN not yet implemented");
            case IN -> {
                if (value instanceof long[] arr) {
                    yield createSelection(table.scanInLong(colIdx, arr));
                } else if (value instanceof Long single) {
                    yield createSelection(table.scanInLong(colIdx, new long[]{single}));
                } else {
                    throw new IllegalArgumentException("IN operator expects long[] or Long, got: " + value.getClass());
                }
            }
            case NOT_IN -> {
                int[] all = table.scanAll();
                int[] inSet;
                if (value instanceof long[] arr) {
                    inSet = table.scanInLong(colIdx, arr);
                } else if (value instanceof Long single) {
                    inSet = table.scanInLong(colIdx, new long[]{single});
                } else {
                    throw new IllegalArgumentException("NOT_IN operator expects long[] or Long, got: " + value.getClass());
                }
                yield subtractSelections(all, inSet);
            }
            default -> throw new UnsupportedOperationException("Operator: " + op);
        };
    }

    private Selection executeIntCondition(int colIdx, LogicalQuery.Operator op, Integer value) {
        return switch (op) {
            case EQ -> createSelection(table.scanEqualsInt(colIdx, value));
            case NE -> {
                int[] all = table.scanAll();
                int[] matches = table.scanEqualsInt(colIdx, value);
                yield subtractSelections(all, matches);
            }
            case IN -> createSelection(table.scanInInt(colIdx, new int[]{value}));
            case NOT_IN -> {
                int[] all = table.scanAll();
                int[] inSet = table.scanInInt(colIdx, new int[]{value});
                yield subtractSelections(all, inSet);
            }
            default -> throw new UnsupportedOperationException("Operator: " + op);
        };
    }

    private Selection executeStringCondition(int colIdx, LogicalQuery.Operator op, String value, boolean ignoreCase) {
        return switch (op) {
            case EQ -> createSelection(table.scanEqualsString(colIdx, value));
            case IGNORE_CASE_EQ -> createSelection(table.scanEqualsStringIgnoreCase(colIdx, value));
            case IN -> createSelection(table.scanInString(colIdx, new String[]{value}));
            case NOT_IN -> {
                int[] all = table.scanAll();
                int[] inSet = table.scanInString(colIdx, new String[]{value});
                yield subtractSelections(all, inSet);
            }
            default -> throw new UnsupportedOperationException("Operator: " + op);
        };
    }

    private Selection createSelection(int[] indices) {
        long[] packed = new long[indices.length];
        for (int i = 0; i < indices.length; i++) {
            packed[i] = Selection.pack(indices[i], table.currentGeneration());
        }
        return new SelectionImpl(packed);
    }

    private Selection subtractSelections(int[] all, int[] toRemove) {
        long[] allPacked = new long[all.length];
        for (int i = 0; i < all.length; i++) {
            allPacked[i] = Selection.pack(all[i], table.currentGeneration());
        }
        Selection allSel = new SelectionImpl(allPacked);

        long[] removePacked = new long[toRemove.length];
        for (int i = 0; i < toRemove.length; i++) {
            removePacked[i] = Selection.pack(toRemove[i], table.currentGeneration());
        }
        Selection removeSel = new SelectionImpl(removePacked);

        return allSel.intersect(removeSel);
    }
}
