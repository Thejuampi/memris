package io.memris.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.query.LogicalQuery;

/**
 * Abstract base class for type handlers providing common functionality.
 * 
 * <p>
 * Subclasses only need to implement the type-specific scan methods.
 * Common operations like NOT, IN, and NOT_IN are handled by this base class.
 * 
 * @param <T> the Java type this handler supports
 */
public abstract class AbstractTypeHandler<T> implements TypeHandler<T> {

    @Override
    public Selection executeCondition(GeneratedTable table, int columnIndex,
            LogicalQuery.Operator operator, T value, boolean ignoreCase) {
        return switch (operator) {
            case EQ -> executeEquals(table, columnIndex, value, ignoreCase);
            case NE -> executeNotEquals(table, columnIndex, value);
            case GT -> executeGreaterThan(table, columnIndex, value);
            case GTE -> executeGreaterThanOrEqual(table, columnIndex, value);
            case LT -> executeLessThan(table, columnIndex, value);
            case LTE -> executeLessThanOrEqual(table, columnIndex, value);
            case BETWEEN -> executeBetween(table, columnIndex, value);
            case IN -> executeIn(table, columnIndex, value);
            case NOT_IN -> executeNotIn(table, columnIndex, value);
            case IS_NULL -> executeIsNull(table, columnIndex);
            case NOT_NULL -> executeIsNotNull(table, columnIndex);
            default -> throw new UnsupportedOperationException(
                    "Operator " + operator + " not supported for type " + getJavaType().getSimpleName());
        };
    }

    /**
     * Execute equality comparison.
     */
    protected abstract Selection executeEquals(GeneratedTable table, int columnIndex, T value, boolean ignoreCase);

    /**
     * Execute not-equals comparison (default: all minus matches).
     */
    protected Selection executeNotEquals(GeneratedTable table, int columnIndex, T value) {
        int[] all = table.scanAll();
        Selection matches = executeEquals(table, columnIndex, value, false);
        return subtractSelections(table, all, matches);
    }

    /**
     * Execute greater-than comparison.
     */
    protected abstract Selection executeGreaterThan(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute greater-than-or-equal comparison.
     */
    protected abstract Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute less-than comparison.
     */
    protected abstract Selection executeLessThan(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute less-than-or-equal comparison.
     */
    protected abstract Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute between comparison.
     * Value should be an array of two elements [min, max].
     */
    protected abstract Selection executeBetween(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute IN (set membership) comparison.
     */
    protected abstract Selection executeIn(GeneratedTable table, int columnIndex, T value);

    /**
     * Execute NOT IN comparison (default: all minus in set).
     */
    protected Selection executeNotIn(GeneratedTable table, int columnIndex, T value) {
        int[] all = table.scanAll();
        Selection inSet = executeIn(table, columnIndex, value);
        return subtractSelections(table, all, inSet);
    }

    /**
     * Execute IS NULL check.
     * Default implementation scans all and filters nulls.
     */
    protected Selection executeIsNull(GeneratedTable table, int columnIndex) {
        int[] rows = table.scanAll();
        int[] nullRows = new int[rows.length];
        int count = 0;
        for (int row : rows) {
            if (!table.isPresent(columnIndex, row)) {
                nullRows[count++] = row;
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(nullRows, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }

    /**
     * Execute IS NOT NULL check.
     * Default implementation: all rows minus null rows.
     */
    protected Selection executeIsNotNull(GeneratedTable table, int columnIndex) {
        Selection nullRows = executeIsNull(table, columnIndex);
        int[] all = table.scanAll();
        return subtractSelections(table, all, nullRows);
    }

    /**
     * Helper method to create a Selection from int[] row indices.
     */
    protected Selection createSelection(GeneratedTable table, int[] indices) {
        return SelectionImpl.fromScanIndices(table, indices);
    }

    /**
     * Helper method to subtract one selection from another.
     */
    protected Selection subtractSelections(GeneratedTable table, int[] all, Selection toRemove) {
        long[] allPacked = new long[all.length];
        for (int i = 0; i < all.length; i++) {
            int rowIndex = all[i];
            allPacked[i] = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
        }
        Selection allSel = new SelectionImpl(allPacked);
        return allSel.subtract(toRemove);
    }

    /**
     * Helper method to subtract a selection from an int array.
     */
    protected Selection subtractSelections(GeneratedTable table, int[] all, int[] toRemove) {
        return subtractSelections(table, all, createSelection(table, toRemove));
    }
}
