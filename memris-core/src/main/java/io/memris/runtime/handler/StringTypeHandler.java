package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.runtime.AbstractTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Type handler for String values.
 * 
 * <p>Supports equality, case-insensitive equality, and set operations.
 * Pattern matching operators (LIKE, CONTAINING) are not yet implemented.
 */
public class StringTypeHandler extends AbstractTypeHandler<String> {
    
    @Override
    public byte getTypeCode() {
        return TypeCodes.TYPE_STRING;
    }
    
    @Override
    public Class<String> getJavaType() {
        return String.class;
    }
    
    @Override
    public String convertValue(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else {
            throw new IllegalArgumentException(
                "Cannot convert " + value.getClass() + " to String");
        }
    }
    
    @Override
    protected Selection executeEquals(GeneratedTable table, int columnIndex, String value, boolean ignoreCase) {
        if (ignoreCase) {
            return createSelection(table, table.scanEqualsStringIgnoreCase(columnIndex, value));
        } else {
            return createSelection(table, table.scanEqualsString(columnIndex, value));
        }
    }
    
    @Override
    protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Greater-than comparison not supported for String type");
    }
    
    @Override
    protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Greater-than-or-equal comparison not supported for String type");
    }
    
    @Override
    protected Selection executeLessThan(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Less-than comparison not supported for String type");
    }
    
    @Override
    protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "Less-than-or-equal comparison not supported for String type");
    }
    
    @Override
    protected Selection executeBetween(GeneratedTable table, int columnIndex, String value) {
        throw new UnsupportedOperationException(
            "BETWEEN not supported for String type");
    }

    @Override
    public Selection executeCondition(GeneratedTable table, int columnIndex,
                                      io.memris.query.LogicalQuery.Operator operator, String value, boolean ignoreCase) {
        return switch (operator) {
            case LIKE -> executeLike(table, columnIndex, value, ignoreCase);
            case NOT_LIKE -> executeNotLike(table, columnIndex, value, ignoreCase);
            case CONTAINING -> executeContaining(table, columnIndex, value, ignoreCase);
            case NOT_CONTAINING -> executeNotContaining(table, columnIndex, value, ignoreCase);
            case STARTING_WITH -> executeStartingWith(table, columnIndex, value, ignoreCase);
            case NOT_STARTING_WITH -> executeNotStartingWith(table, columnIndex, value, ignoreCase);
            case ENDING_WITH -> executeEndingWith(table, columnIndex, value, ignoreCase);
            case NOT_ENDING_WITH -> executeNotEndingWith(table, columnIndex, value, ignoreCase);
            default -> super.executeCondition(table, columnIndex, operator, value, ignoreCase);
        };
    }
    
    @Override
    protected Selection executeIn(GeneratedTable table, int columnIndex, String value) {
        // Single value IN - treat as equality
        return createSelection(table, table.scanInString(columnIndex, new String[]{value}));
    }
    
    /**
     * Execute IN with multiple values.
     */
    public Selection executeIn(GeneratedTable table, int columnIndex, String[] values) {
        return createSelection(table, table.scanInString(columnIndex, values));
    }
    
    /**
     * Execute pattern matching with LIKE operator.
     * Not yet implemented.
     */
    public Selection executeLike(GeneratedTable table, int columnIndex, String pattern, boolean ignoreCase) {
        if (pattern == null) {
            return createSelection(table, new int[0]);
        }
        String effectivePattern = ignoreCase ? pattern.toLowerCase() : pattern;
        int[] rows = table.scanAll();
        int[] matches = new int[rows.length];
        int count = 0;
        for (int row : rows) {
            String value = table.readString(columnIndex, row);
            if (value == null) {
                continue;
            }
            String candidate = ignoreCase ? value.toLowerCase() : value;
            if (matchesLike(candidate, effectivePattern)) {
                matches[count++] = row;
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(matches, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }

    public Selection executeNotLike(GeneratedTable table, int columnIndex, String pattern, boolean ignoreCase) {
        int[] all = table.scanAll();
        Selection like = executeLike(table, columnIndex, pattern, ignoreCase);
        return subtractSelections(table, all, like);
    }
    
    /**
     * Execute pattern matching with CONTAINING operator.
     * Not yet implemented.
     */
    public Selection executeContaining(GeneratedTable table, int columnIndex, String substring, boolean ignoreCase) {
        if (substring == null) {
            return createSelection(table, new int[0]);
        }
        int[] rows = table.scanAll();
        int[] matches = new int[rows.length];
        int count = 0;
        if (ignoreCase) {
            String needle = substring.toLowerCase();
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.toLowerCase().contains(needle)) {
                    matches[count++] = row;
                }
            }
        } else {
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.contains(substring)) {
                    matches[count++] = row;
                }
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(matches, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }

    public Selection executeNotContaining(GeneratedTable table, int columnIndex, String substring, boolean ignoreCase) {
        int[] all = table.scanAll();
        Selection containing = executeContaining(table, columnIndex, substring, ignoreCase);
        return subtractSelections(table, all, containing);
    }
    
    @Override
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
     * Execute pattern matching with STARTING_WITH operator.
     * Not yet implemented.
     */
    public Selection executeStartingWith(GeneratedTable table, int columnIndex, String prefix, boolean ignoreCase) {
        if (prefix == null) {
            return createSelection(table, new int[0]);
        }
        int[] rows = table.scanAll();
        int[] matches = new int[rows.length];
        int count = 0;
        if (ignoreCase) {
            String needle = prefix.toLowerCase();
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.toLowerCase().startsWith(needle)) {
                    matches[count++] = row;
                }
            }
        } else {
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.startsWith(prefix)) {
                    matches[count++] = row;
                }
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(matches, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }
    
    /**
     * Execute pattern matching with ENDING_WITH operator.
     * Not yet implemented.
     */
    public Selection executeEndingWith(GeneratedTable table, int columnIndex, String suffix, boolean ignoreCase) {
        if (suffix == null) {
            return createSelection(table, new int[0]);
        }
        int[] rows = table.scanAll();
        int[] matches = new int[rows.length];
        int count = 0;
        if (ignoreCase) {
            String needle = suffix.toLowerCase();
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.toLowerCase().endsWith(needle)) {
                    matches[count++] = row;
                }
            }
        } else {
            for (int row : rows) {
                String value = table.readString(columnIndex, row);
                if (value != null && value.endsWith(suffix)) {
                    matches[count++] = row;
                }
            }
        }
        int[] trimmed = new int[count];
        System.arraycopy(matches, 0, trimmed, 0, count);
        return createSelection(table, trimmed);
    }

    public Selection executeNotStartingWith(GeneratedTable table, int columnIndex, String prefix, boolean ignoreCase) {
        int[] all = table.scanAll();
        Selection startingWith = executeStartingWith(table, columnIndex, prefix, ignoreCase);
        return subtractSelections(table, all, startingWith);
    }

    public Selection executeNotEndingWith(GeneratedTable table, int columnIndex, String suffix, boolean ignoreCase) {
        int[] all = table.scanAll();
        Selection endingWith = executeEndingWith(table, columnIndex, suffix, ignoreCase);
        return subtractSelections(table, all, endingWith);
    }

    @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
    private static boolean matchesLike(String value, String pattern) {
        int v = 0;
        int p = 0;
        int star = -1;
        int match = 0;
        while (v < value.length()) {
            if (p < pattern.length()) {
                char pc = pattern.charAt(p);
                if (pc == '_' || pc == value.charAt(v)) {
                    p++;
                    v++;
                    continue;
                }
                if (pc == '%') {
                    star = p;
                    match = v;
                    p++;
                    continue;
                }
            }
            if (star != -1) {
                p = star + 1;
                match++;
                v = match;
                continue;
            }
            return false;
        }
        while (p < pattern.length() && pattern.charAt(p) == '%') {
            p++;
        }
        return p == pattern.length();
    }
}
