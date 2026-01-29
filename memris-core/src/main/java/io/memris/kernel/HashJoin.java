package io.memris.kernel;

import io.memris.kernel.selection.IntEnumerator;
import io.memris.kernel.selection.IntSelection;
import io.memris.kernel.MutableSelectionVector;
import io.memris.kernel.SelectionVector;
import io.memris.kernel.SelectionVectorFactory;

import java.util.HashMap;
import java.util.Map;

public final class HashJoin {
    private final Map<Object, MutableSelectionVector> buildTable = new HashMap<>();
    private int buildRowCount;

    public void build(SelectionVector leftRows, int[] leftKeys, Object[] leftValues) {
        IntEnumerator e = leftRows.enumerator();
        while (e.hasNext()) {
            int rowIdx = e.nextInt();
            Object key = leftKeys[rowIdx];
            MutableSelectionVector rows = buildTable.computeIfAbsent(key, k -> new IntSelection(4));
            rows.add(rowIdx);
            buildRowCount++;
        }
    }

    public SelectionVector probe(SelectionVector rightRows, int[] rightKeys, SelectionVectorFactory factory) {
        MutableSelectionVector result = factory.create((int) (rightRows.size() * 0.1));
        IntEnumerator e = rightRows.enumerator();
        while (e.hasNext()) {
            int rowIdx = e.nextInt();
            Object key = rightKeys[rowIdx];
            MutableSelectionVector match = buildTable.get(key);
            if (match != null) {
                IntEnumerator me = match.enumerator();
                while (me.hasNext()) {
                    result.add(rowIdx);
                    me.nextInt();
                }
            }
        }
        return result;
    }

    public int buildRowCount() {
        return buildRowCount;
    }

    public int distinctKeys() {
        return buildTable.size();
    }
}
