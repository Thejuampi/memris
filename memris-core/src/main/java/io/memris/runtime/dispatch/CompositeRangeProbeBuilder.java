package io.memris.runtime.dispatch;

import io.memris.index.CompositeKey;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;

public final class CompositeRangeProbeBuilder {

    private CompositeRangeProbeBuilder() {
    }

    public static CompositeRangeProbe build(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int[] columnPositions,
            Object[] args) {
        var consumed = new boolean[end - start + 1];
        var lower = new Object[columnPositions.length];
        var upper = new Object[columnPositions.length];
        var prefix = 0;
        while (prefix < columnPositions.length) {
            var conditionIndex = findConditionIndex(conditions, start, end, columnPositions[prefix],
                    LogicalQuery.Operator.EQ);
            if (conditionIndex < 0) {
                break;
            }
            var condition = conditions[conditionIndex];
            var value = args[condition.argumentIndex()];
            lower[prefix] = value;
            upper[prefix] = value;
            consumed[conditionIndex - start] = true;
            prefix++;
        }
        if (prefix == 0) {
            return null;
        }
        if (prefix == columnPositions.length) {
            return new CompositeRangeProbe(Predicate.Operator.EQ,
                    CompositeKey.of(lower),
                    null,
                    consumed);
        }

        var rangeConditionIndex = findConditionIndexAny(conditions, start, end, columnPositions[prefix]);
        if (rangeConditionIndex >= 0) {
            var range = conditions[rangeConditionIndex];
            var operator = range.operator();
            var value = args[range.argumentIndex()];
            for (var i = prefix + 1; i < columnPositions.length; i++) {
                lower[i] = CompositeKey.minSentinel();
                upper[i] = CompositeKey.maxSentinel();
            }
            consumed[rangeConditionIndex - start] = true;
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
                    var second = args[range.argumentIndex() + 1];
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
