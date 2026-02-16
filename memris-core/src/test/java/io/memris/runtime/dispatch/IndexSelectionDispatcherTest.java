package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSelectionDispatcherTest {

    @Test
    @DisplayName("should dispatch BETWEEN with two arguments")
    void shouldDispatchBetweenWithTwoArguments() {
        var condition = CompiledQuery.CompiledCondition.of(1,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.BETWEEN,
                0,
                false,
                LogicalQuery.Combinator.OR);

        var rows = IndexSelectionDispatcher.selectRows(condition,
                new Object[] { 10, 20 },
                (operator, value) -> operator == LogicalQuery.Operator.BETWEEN
                        && value instanceof Object[] range
                        && range.length == 2
                        && (int) range[0] == 10
                        && (int) range[1] == 20
                                ? new int[] { 5 }
                                : null,
                value -> null);

        assertThat(rows).containsExactly(5);
    }

    @Test
    @DisplayName("should dispatch IN through dedicated selector")
    void shouldDispatchInThroughDedicatedSelector() {
        var condition = CompiledQuery.CompiledCondition.of(1,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.IN,
                0,
                false,
                LogicalQuery.Combinator.OR);

        var rows = IndexSelectionDispatcher.selectRows(condition,
                new Object[] { new int[] { 1, 2 } },
                (operator, value) -> null,
                value -> value instanceof int[] arr && arr.length == 2 ? new int[] { 7, 8 } : null);

        assertThat(rows).containsExactlyInAnyOrder(7, 8);
    }

    @Test
    @DisplayName("should return null for null-check operators and invalid argument indexes")
    void shouldReturnNullForNullChecksAndInvalidArgumentIndexes() {
        var isNull = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Operator.IS_NULL,
                0,
                false,
                LogicalQuery.Combinator.AND);
        var notNull = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Operator.NOT_NULL,
                0,
                false,
                LogicalQuery.Combinator.AND);
        var badIndex = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.EQ,
                2,
                false,
                LogicalQuery.Combinator.AND);

        assertThat(IndexSelectionDispatcher.selectRows(isNull, new Object[] { "x" }, (op, val) -> new int[] { 1 }, val -> new int[] { 2 }))
                .isNull();
        assertThat(IndexSelectionDispatcher.selectRows(notNull, new Object[] { "x" }, (op, val) -> new int[] { 1 }, val -> new int[] { 2 }))
                .isNull();
        assertThat(IndexSelectionDispatcher.selectRows(badIndex, new Object[] { "x" }, (op, val) -> new int[] { 1 }, val -> new int[] { 2 }))
                .isNull();
    }

    @Test
    @DisplayName("should return null for between with missing upper bound and unsupported operators")
    void shouldReturnNullForMissingBetweenArgAndUnsupportedOperator() {
        var between = CompiledQuery.CompiledCondition.of(1,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Operator.BETWEEN,
                0,
                false,
                LogicalQuery.Combinator.AND);

        var unsupported = CompiledQuery.CompiledCondition.of(1,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Operator.CONTAINING,
                0,
                false,
                LogicalQuery.Combinator.AND);

        assertThat(IndexSelectionDispatcher.selectRows(between,
                new Object[] { 10L },
                (operator, value) -> new int[] { 4 },
                value -> new int[] { 5 })).isNull();

        assertThat(IndexSelectionDispatcher.selectRows(unsupported,
                new Object[] { "a" },
                (operator, value) -> new int[] { 4 },
                value -> new int[] { 5 })).isNull();
    }

    @Test
    @DisplayName("should dispatch eq/gt/lte via index query")
    void shouldDispatchComparisonOperatorsViaIndexQuery() {
        var eq = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.EQ,
                0,
                false,
                LogicalQuery.Combinator.AND);
        var gt = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.GT,
                0,
                false,
                LogicalQuery.Combinator.AND);
        var lte = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.LTE,
                0,
                false,
                LogicalQuery.Combinator.AND);

        assertThat(IndexSelectionDispatcher.selectRows(eq, new Object[] { 10 }, (op, value) -> op == LogicalQuery.Operator.EQ ? new int[] { 1 } : null, v -> null))
                .containsExactly(1);
        assertThat(IndexSelectionDispatcher.selectRows(gt, new Object[] { 10 }, (op, value) -> op == LogicalQuery.Operator.GT ? new int[] { 2 } : null, v -> null))
                .containsExactly(2);
        assertThat(IndexSelectionDispatcher.selectRows(lte, new Object[] { 10 }, (op, value) -> op == LogicalQuery.Operator.LTE ? new int[] { 3 } : null, v -> null))
                .containsExactly(3);
    }
}
