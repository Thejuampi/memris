package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionSelectionOrchestratorTest {

    @Test
    @DisplayName("should combine groups with AND and OR semantics")
    void shouldCombineGroupsWithAndAndOrSemantics() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(1,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.EQ,
                        0,
                        false,
                        LogicalQuery.Combinator.AND),
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.EQ,
                        1,
                        false,
                        LogicalQuery.Combinator.OR),
                CompiledQuery.CompiledCondition.of(3,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.EQ,
                        2,
                        false,
                        LogicalQuery.Combinator.OR)
        };

        var selection = ConditionSelectionOrchestrator.execute(conditions,
                null,
                new Object[0],
                (compiled, start, end, args, consumed) -> null,
                (compiled, executors, index, args) -> switch (index) {
                    case 0 -> selectionFromRows(new int[] { 1, 2 });
                    case 1 -> selectionFromRows(new int[] { 2, 3 });
                    case 2 -> selectionFromRows(new int[] { 9 });
                    default -> selectionFromRows(new int[0]);
                },
                () -> selectionFromRows(new int[] { 100 }));

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(2, 9);
    }

    private static Selection selectionFromRows(int[] rows) {
        var refs = new long[rows.length];
        for (var i = 0; i < rows.length; i++) {
            refs[i] = Selection.pack(rows[i], 1);
        }
        return new SelectionImpl(refs);
    }
}
