package io.memris.query;

import io.memris.kernel.PlanNode;
import io.memris.kernel.selection.IntEnumerator;
import io.memris.kernel.selection.IntSelection;
import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public final class SimpleExecutor {
    private final FfmTable[] tables;

    public SimpleExecutor(FfmTable... tables) {
        this.tables = tables;
    }

    public SelectionVector execute(PlanNode plan) {
        return switch (plan) {
            case PlanNode.Scan scan -> executeScan(scan);
            case PlanNode.Filter filter -> executeFilter(filter);
            case PlanNode.Sort sort -> executeSort(sort);
            case PlanNode.Limit limit -> executeLimit(limit);
            default -> throw new UnsupportedOperationException("Unsupported plan: " + plan);
        };
    }

    private SelectionVector executeScan(PlanNode.Scan scan) {
        FfmTable table = findTable(scan.table().name());
        if (table == null) {
            return SelectionVectorFactory.defaultFactory().create(0);
        }
        return table.scanAll(SelectionVectorFactory.defaultFactory());
    }

    private SelectionVector executeFilter(PlanNode.Filter filter) {
        SelectionVector input = execute(filter.child());
        return input.filter(filter.predicate(), SelectionVectorFactory.defaultFactory());
    }

    private SelectionVector executeSort(PlanNode.Sort sort) {
        SelectionVector input = execute(sort.child());
        if (input.size() <= 1) {
            return input;
        }
        int[] indices = input.toIntArray();
        String sortColumn = sort.orderings().get(0).column();
        boolean desc = sort.orderings().get(0).direction() == PlanNode.Direction.DESC;
        FfmTable table = findTableByColumn(sortColumn);
        if (table == null) {
            return input;
        }
        final FfmTable fTable = table;
        final String fCol = sortColumn;
        Integer[] boxed = new Integer[indices.length];
        for (int i = 0; i < indices.length; i++) {
            boxed[i] = indices[i];
        }
        Comparator<Integer> cmp = desc 
            ? (a, b) -> Integer.compare(fTable.getInt(fCol, b), fTable.getInt(fCol, a))
            : (a, b) -> Integer.compare(fTable.getInt(fCol, a), fTable.getInt(fCol, b));
        java.util.Arrays.sort(boxed, cmp);
        IntSelection sorted = new IntSelection(indices.length);
        for (int idx : boxed) {
            sorted.add(idx);
        }
        return sorted;
    }

    private SelectionVector executeLimit(PlanNode.Limit limit) {
        SelectionVector input = execute(limit.child());
        if (limit.limit() >= input.size()) {
            return input;
        }
        IntSelection limited = new IntSelection((int) limit.limit());
        IntEnumerator e = input.enumerator();
        long count = 0;
        while (e.hasNext() && count < limit.limit()) {
            if (limit.offset() <= count) {
                limited.add(e.nextInt());
            } else {
                e.nextInt();
            }
            count++;
        }
        return limited;
    }

    private FfmTable findTable(String name) {
        for (FfmTable table : tables) {
            if (table.name().equals(name)) {
                return table;
            }
        }
        return null;
    }

    private FfmTable findTableByColumn(String column) {
        for (FfmTable table : tables) {
            if (table.column(column) != null) {
                return table;
            }
        }
        return null;
    }
}
