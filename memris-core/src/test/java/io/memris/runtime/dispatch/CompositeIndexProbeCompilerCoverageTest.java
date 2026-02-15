package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeIndexProbeCompilerCoverageTest {

    @Test
    void shouldReturnNoopProbeWhenInputsAreMissing() {
        var condition = condition(0, LogicalQuery.Operator.EQ, 0, LogicalQuery.Combinator.AND);
        var conditions = new CompiledQuery.CompiledCondition[] { condition };

        var probe1 = CompositeIndexProbeCompiler.compile(null, Map.of(), conditions, this::query, this::selectionFromRows);
        var probe2 = CompositeIndexProbeCompiler.compile(new CompositeIndexSelector.CompositeIndexPlan[0],
                Map.of(),
                conditions,
                this::query,
                this::selectionFromRows);
        var probe3 = CompositeIndexProbeCompiler.compile(
                new CompositeIndexSelector.CompositeIndexPlan[] {
                        new CompositeIndexSelector.CompositeIndexPlan("x", new int[] { 0, 1 })
                },
                null,
                conditions,
                this::query,
                this::selectionFromRows);

        assertThat(probe1.select(conditions, 0, 0, new Object[] { "a" }, new boolean[] { false })).isNull();
        assertThat(probe2.select(conditions, 0, 0, new Object[] { "a" }, new boolean[] { false })).isNull();
        assertThat(probe3.select(conditions, 0, 0, new Object[] { "a" }, new boolean[] { false })).isNull();
    }

    @Test
    void shouldCompileHashPlanAndRespectRuntimeConditionIdentity() {
        var hashIndex = new CompositeHashIndex();
        var plan = new CompositeIndexSelector.CompositeIndexPlan("idx_hash", new int[] { 0, 1 });
        var conditions = new CompiledQuery.CompiledCondition[] {
                condition(0, LogicalQuery.Operator.EQ, 0, LogicalQuery.Combinator.AND),
                condition(1, LogicalQuery.Operator.EQ, 1, LogicalQuery.Combinator.AND)
        };
        var expected = CompositeKey.of(new Object[] { "us", 10 });
        Object[] capturedValue = new Object[1];
        Predicate.Operator[] capturedOperator = new Predicate.Operator[1];

        var probe = CompositeIndexProbeCompiler.compile(
                new CompositeIndexSelector.CompositeIndexPlan[] { plan },
                Map.of("idx_hash", hashIndex),
                conditions,
                (index, operator, value) -> {
                    capturedOperator[0] = operator;
                    capturedValue[0] = value;
                    return expected.equals(value) ? new int[] { 4, 5 } : null;
                },
                this::selectionFromRows);

        var consumed = new boolean[] { false, false };
        var selected = probe.select(conditions, 0, 1, new Object[] { "us", 10 }, consumed);

        assertThat(selected.toIntArray()).containsExactly(4, 5);
        assertThat(consumed).containsExactly(true, true);
        assertThat(capturedOperator[0]).isEqualTo(Predicate.Operator.EQ);
        assertThat(capturedValue[0]).isEqualTo(expected);

        var mismatch = probe.select(
                new CompiledQuery.CompiledCondition[] { conditions[0], conditions[1] },
                0,
                1,
                new Object[] { "us", 10 },
                new boolean[] { false, false });
        assertThat(mismatch).isNull();
    }

    @Test
    void shouldCompileRangePlanForPrefixScanAndBetween() {
        var rangeIndex = new CompositeRangeIndex();
        var plan = new CompositeIndexSelector.CompositeIndexPlan("idx_range", new int[] { 0, 1 });
        var prefixOnly = new CompiledQuery.CompiledCondition[] {
                condition(0, LogicalQuery.Operator.EQ, 0, LogicalQuery.Combinator.AND)
        };

        Object[] capturedValue = new Object[1];
        Predicate.Operator[] capturedOperator = new Predicate.Operator[1];
        var probePrefix = CompositeIndexProbeCompiler.compile(
                new CompositeIndexSelector.CompositeIndexPlan[] { plan },
                Map.of("idx_range", rangeIndex),
                prefixOnly,
                (index, operator, value) -> {
                    capturedOperator[0] = operator;
                    capturedValue[0] = value;
                    return new int[] { 7 };
                },
                this::selectionFromRows);

        var consumedPrefix = new boolean[] { false };
        var prefixSelection = probePrefix.select(prefixOnly, 0, 0, new Object[] { "us" }, consumedPrefix);

        assertThat(prefixSelection.toIntArray()).containsExactly(7);
        assertThat(consumedPrefix).containsExactly(true);
        assertThat(capturedOperator[0]).isEqualTo(Predicate.Operator.BETWEEN);
        assertThat(capturedValue[0]).isInstanceOf(Object[].class);

        Object[] between = (Object[]) capturedValue[0];
        assertThat(between[0]).isEqualTo(CompositeKey.of(new Object[] { "us", CompositeKey.minSentinel() }));
        assertThat(between[1]).isEqualTo(CompositeKey.of(new Object[] { "us", CompositeKey.maxSentinel() }));
    }

    @Test
    void shouldCompileRangePlanForGtAndBetweenAndPreserveConsumedOnNullSelection() {
        var rangeIndex = new CompositeRangeIndex();
        var plan = new CompositeIndexSelector.CompositeIndexPlan("idx_range", new int[] { 0, 1 });

        var gtConditions = new CompiledQuery.CompiledCondition[] {
                condition(0, LogicalQuery.Operator.EQ, 0, LogicalQuery.Combinator.AND),
                condition(1, LogicalQuery.Operator.GT, 1, LogicalQuery.Combinator.AND)
        };
        var gtProbe = CompositeIndexProbeCompiler.compile(
                new CompositeIndexSelector.CompositeIndexPlan[] { plan },
                Map.of("idx_range", rangeIndex),
                gtConditions,
                (index, operator, value) -> null,
                this::selectionFromRows);
        var consumedGt = new boolean[] { false, false };
        var gtSelection = gtProbe.select(gtConditions, 0, 1, new Object[] { "us", 10 }, consumedGt);
        assertThat(gtSelection).isNull();
        assertThat(consumedGt).containsExactly(false, false);

        var betweenConditions = new CompiledQuery.CompiledCondition[] {
                condition(0, LogicalQuery.Operator.EQ, 0, LogicalQuery.Combinator.AND),
                condition(1, LogicalQuery.Operator.BETWEEN, 1, LogicalQuery.Combinator.AND)
        };
        Predicate.Operator[] capturedOperator = new Predicate.Operator[1];
        Object[] capturedValue = new Object[1];
        var betweenProbe = CompositeIndexProbeCompiler.compile(
                new CompositeIndexSelector.CompositeIndexPlan[] { plan },
                Map.of("idx_range", rangeIndex),
                betweenConditions,
                (index, operator, value) -> {
                    capturedOperator[0] = operator;
                    capturedValue[0] = value;
                    return new int[] { 9 };
                },
                this::selectionFromRows);
        var consumedBetween = new boolean[] { false, false };
        var betweenSelection = betweenProbe.select(
                betweenConditions,
                0,
                1,
                new Object[] { "us", 10, 20 },
                consumedBetween);

        assertThat(betweenSelection.toIntArray()).containsExactly(9);
        assertThat(consumedBetween).containsExactly(true, true);
        assertThat(capturedOperator[0]).isEqualTo(Predicate.Operator.BETWEEN);
        assertThat(capturedValue[0]).isInstanceOf(Object[].class);
    }

    private CompiledQuery.CompiledCondition condition(int columnIndex,
            LogicalQuery.Operator operator,
            int argumentIndex,
            LogicalQuery.Combinator combinator) {
        return CompiledQuery.CompiledCondition.of(columnIndex,
                TypeCodes.TYPE_INT,
                operator,
                argumentIndex,
                false,
                combinator);
    }

    private int[] query(Object index, Predicate.Operator operator, Object value) {
        return null;
    }

    private Selection selectionFromRows(int[] rows) {
        long[] refs = new long[rows.length];
        for (int i = 0; i < rows.length; i++) {
            refs[i] = Selection.pack(rows[i], 1);
        }
        return new SelectionImpl(refs);
    }
}
