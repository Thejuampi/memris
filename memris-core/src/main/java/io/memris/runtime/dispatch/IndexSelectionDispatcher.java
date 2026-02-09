package io.memris.runtime.dispatch;

import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;

public final class IndexSelectionDispatcher {

    private IndexSelectionDispatcher() {
    }

    @FunctionalInterface
    public interface IndexQuery {
        int[] query(LogicalQuery.Operator operator, Object value);
    }

    @FunctionalInterface
    public interface InSelector {
        int[] query(Object value);
    }

    public static int[] selectRows(CompiledQuery.CompiledCondition condition,
            Object[] args,
            IndexQuery indexQuery,
            InSelector inSelector) {
        var operator = condition.operator();
        if (operator == LogicalQuery.Operator.IS_NULL || operator == LogicalQuery.Operator.NOT_NULL) {
            return null;
        }

        if (condition.argumentIndex() < 0 || condition.argumentIndex() >= args.length) {
            return null;
        }

        var value = args[condition.argumentIndex()];
        if (operator == LogicalQuery.Operator.IN) {
            return inSelector.query(value);
        }

        if (operator == LogicalQuery.Operator.BETWEEN) {
            if (condition.argumentIndex() + 1 >= args.length) {
                return null;
            }
            return indexQuery.query(operator,
                    new Object[] { args[condition.argumentIndex()], args[condition.argumentIndex() + 1] });
        }

        return switch (operator) {
            case EQ, GT, GTE, LT, LTE, STARTING_WITH, ENDING_WITH -> indexQuery.query(operator, value);
            default -> null;
        };
    }
}
