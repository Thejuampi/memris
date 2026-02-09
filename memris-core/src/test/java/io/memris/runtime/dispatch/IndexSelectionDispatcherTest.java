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
}
