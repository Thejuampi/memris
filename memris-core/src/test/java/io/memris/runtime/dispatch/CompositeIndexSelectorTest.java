package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeIndexSelectorTest {

    @Test
    @DisplayName("should select rows from composite hash index")
    void shouldSelectRowsFromCompositeHashIndex() {
        var hash = new CompositeHashIndex();
        hash.add(CompositeKey.of(new Object[] { "us", 20 }), new RowId(0, 7));
        var plans = new CompositeIndexSelector.CompositeIndexPlan[] {
                new CompositeIndexSelector.CompositeIndexPlan("idx_region_code", new int[] { 1, 2 })
        };
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(1,
                        TypeCodes.TYPE_STRING,
                        LogicalQuery.Operator.EQ,
                        0,
                        false,
                        LogicalQuery.Combinator.AND),
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.EQ,
                        1,
                        false,
                        LogicalQuery.Combinator.OR)
        };
        var consumed = new boolean[2];

        var selection = CompositeIndexSelector.select(plans,
                Map.of("idx_region_code", hash),
                conditions,
                0,
                1,
                new Object[] { "us", 20 },
                consumed,
                CompositeIndexSelectorTest::query,
                CompositeIndexSelectorTest::selectionFromRows);

        assertThat(new ResultSummary(selection.toIntArray(), consumed))
                .usingRecursiveComparison()
                .isEqualTo(new ResultSummary(new int[] { 7 }, new boolean[] { true, true }));
    }

    @Test
    @DisplayName("should select rows from composite range index")
    void shouldSelectRowsFromCompositeRangeIndex() {
        var range = new CompositeRangeIndex();
        range.add(CompositeKey.of(new Object[] { "us", 10 }), new RowId(0, 5));
        range.add(CompositeKey.of(new Object[] { "us", 15 }), new RowId(0, 8));
        var plans = new CompositeIndexSelector.CompositeIndexPlan[] {
                new CompositeIndexSelector.CompositeIndexPlan("idx_region_score", new int[] { 1, 2 })
        };
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(1,
                        TypeCodes.TYPE_STRING,
                        LogicalQuery.Operator.EQ,
                        0,
                        false,
                        LogicalQuery.Combinator.AND),
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.GTE,
                        1,
                        false,
                        LogicalQuery.Combinator.OR)
        };
        var consumed = new boolean[2];

        var selection = CompositeIndexSelector.select(plans,
                Map.of("idx_region_score", range),
                conditions,
                0,
                1,
                new Object[] { "us", 12 },
                consumed,
                CompositeIndexSelectorTest::query,
                CompositeIndexSelectorTest::selectionFromRows);

        assertThat(new ResultSummary(selection.toIntArray(), consumed))
                .usingRecursiveComparison()
                .isEqualTo(new ResultSummary(new int[] { 8 }, new boolean[] { true, true }));
    }

    private static int[] query(Object index, Predicate.Operator operator, Object value) {
        return IndexProbeCompiler.compile(index).query(operator, value, rowId -> true);
    }

    private static Selection selectionFromRows(int[] rows) {
        var refs = new long[rows.length];
        for (var i = 0; i < rows.length; i++) {
            refs[i] = Selection.pack(rows[i], 1);
        }
        return new SelectionImpl(refs);
    }

    private record ResultSummary(int[] rows, boolean[] consumed) {
    }
}
