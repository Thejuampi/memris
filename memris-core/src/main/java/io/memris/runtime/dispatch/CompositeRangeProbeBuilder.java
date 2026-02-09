package io.memris.runtime.dispatch;

import io.memris.index.CompositeKey;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;

public final class CompositeRangeProbeBuilder {

    private CompositeRangeProbeBuilder() {
    }

    static CompiledRangeShape compileShape(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int[] columnPositions) {
        if (columnPositions == null || columnPositions.length == 0) {
            return null;
        }

        var prefixConditionIndexes = new int[columnPositions.length];
        var prefixArgumentIndexes = new int[columnPositions.length];
        var prefix = 0;
        while (prefix < columnPositions.length) {
            var conditionIndex = findConditionIndex(conditions, start, end, columnPositions[prefix], LogicalQuery.Operator.EQ);
            if (conditionIndex < 0) {
                break;
            }
            var condition = conditions[conditionIndex];
            prefixConditionIndexes[prefix] = conditionIndex;
            prefixArgumentIndexes[prefix] = condition.argumentIndex();
            prefix++;
        }
        if (prefix == 0) {
            return null;
        }

        var trimmedPrefixConditionIndexes = new int[prefix];
        var trimmedPrefixArgumentIndexes = new int[prefix];
        System.arraycopy(prefixConditionIndexes, 0, trimmedPrefixConditionIndexes, 0, prefix);
        System.arraycopy(prefixArgumentIndexes, 0, trimmedPrefixArgumentIndexes, 0, prefix);

        if (prefix == columnPositions.length) {
            return new CompiledRangeShape(columnPositions.length,
                    trimmedPrefixConditionIndexes,
                    trimmedPrefixArgumentIndexes,
                    -1,
                    -1,
                    null,
                    true);
        }

        var rangeConditionIndex = findConditionIndexAny(conditions, start, end, columnPositions[prefix]);
        if (rangeConditionIndex < 0) {
            return new CompiledRangeShape(columnPositions.length,
                    trimmedPrefixConditionIndexes,
                    trimmedPrefixArgumentIndexes,
                    -1,
                    -1,
                    null,
                    false);
        }

        var range = conditions[rangeConditionIndex];
        return new CompiledRangeShape(columnPositions.length,
                trimmedPrefixConditionIndexes,
                trimmedPrefixArgumentIndexes,
                rangeConditionIndex,
                range.argumentIndex(),
                range.operator(),
                false);
    }

    public static CompositeRangeProbe build(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int[] columnPositions,
            Object[] args) {
        var shape = compileShape(conditions, start, end, columnPositions);
        if (shape == null) {
            return null;
        }
        var consumed = new boolean[end - start + 1];
        for (var conditionIndex : shape.prefixConditionIndexes()) {
            consumed[conditionIndex - start] = true;
        }

        var lower = new Object[columnPositions.length];
        var upper = new Object[columnPositions.length];
        for (var i = 0; i < shape.prefixArgumentIndexes().length; i++) {
            var value = args[shape.prefixArgumentIndexes()[i]];
            lower[i] = value;
            upper[i] = value;
        }

        var prefix = shape.prefixArgumentIndexes().length;
        if (shape.fullEquality()) {
            return new CompositeRangeProbe(Predicate.Operator.EQ,
                    CompositeKey.of(lower),
                    null,
                    consumed);
        }

        var operator = shape.rangeOperator();
        if (operator != null) {
            var value = args[shape.rangeArgumentIndex()];
            for (var i = prefix + 1; i < columnPositions.length; i++) {
                lower[i] = CompositeKey.minSentinel();
                upper[i] = CompositeKey.maxSentinel();
            }
            consumed[shape.rangeConditionIndex() - start] = true;
            return switch (operator) {
                case EQ -> {
                    lower[prefix] = value;
                    upper[prefix] = value;
                    yield new CompositeRangeProbe(Predicate.Operator.BETWEEN,
                            CompositeKey.of(lower),
                            CompositeKey.of(upper),
                            consumed);
                }
                case GT -> {
                    lower[prefix] = value;
                    upper[prefix] = CompositeKey.maxSentinel();
                    yield new CompositeRangeProbe(Predicate.Operator.GT, CompositeKey.of(lower), null, consumed);
                }
                case GTE -> {
                    lower[prefix] = value;
                    yield new CompositeRangeProbe(Predicate.Operator.GTE, CompositeKey.of(lower), null, consumed);
                }
                case LT -> {
                    upper[prefix] = value;
                    yield new CompositeRangeProbe(Predicate.Operator.LT, CompositeKey.of(upper), null, consumed);
                }
                case LTE -> {
                    upper[prefix] = value;
                    yield new CompositeRangeProbe(Predicate.Operator.LTE, CompositeKey.of(upper), null, consumed);
                }
                case BETWEEN -> {
                    var second = args[shape.rangeArgumentIndex() + 1];
                    lower[prefix] = value;
                    upper[prefix] = second;
                    yield new CompositeRangeProbe(Predicate.Operator.BETWEEN,
                            CompositeKey.of(lower),
                            CompositeKey.of(upper),
                            consumed);
                }
                default -> null;
            };
        }

        for (var i = prefix; i < columnPositions.length; i++) {
            lower[i] = CompositeKey.minSentinel();
            upper[i] = CompositeKey.maxSentinel();
        }
        return new CompositeRangeProbe(Predicate.Operator.BETWEEN,
                CompositeKey.of(lower),
                CompositeKey.of(upper),
                consumed);
    }

    record CompiledRangeShape(int width,
            int[] prefixConditionIndexes,
            int[] prefixArgumentIndexes,
            int rangeConditionIndex,
            int rangeArgumentIndex,
            LogicalQuery.Operator rangeOperator,
            boolean fullEquality) {

        int[] consumedOffsets(int start) {
            var consumed = rangeConditionIndex >= 0
                    ? new int[prefixConditionIndexes.length + 1]
                    : new int[prefixConditionIndexes.length];
            var index = 0;
            for (var conditionIndex : prefixConditionIndexes) {
                consumed[index++] = conditionIndex - start;
            }
            if (rangeConditionIndex >= 0) {
                consumed[index] = rangeConditionIndex - start;
            }
            return consumed;
        }
    }

    private static int findConditionIndex(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int columnIndex,
            LogicalQuery.Operator operator) {
        for (var i = start; i <= end; i++) {
            var condition = conditions[i];
            if (condition.columnIndex() == columnIndex && condition.operator() == operator && !condition.ignoreCase()) {
                return i;
            }
        }
        return -1;
    }

    private static int findConditionIndexAny(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int columnIndex) {
        for (var i = start; i <= end; i++) {
            var condition = conditions[i];
            if (condition.columnIndex() != columnIndex || condition.ignoreCase()) {
                continue;
            }
            switch (condition.operator()) {
                case EQ, GT, GTE, LT, LTE, BETWEEN -> {
                    return i;
                }
                default -> {
                }
            }
        }
        return -1;
    }
}
