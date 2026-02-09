package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.index.CompositeKey;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeRangeProbeBuilderTest {

    @Test
    @DisplayName("should build BETWEEN probe from eq prefix and range")
    void shouldBuildBetweenProbeFromEqPrefixAndRange() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(1,
                        TypeCodes.TYPE_STRING,
                        LogicalQuery.Operator.EQ,
                        0,
                        false,
                        LogicalQuery.Combinator.AND),
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.BETWEEN,
                        1,
                        false,
                        LogicalQuery.Combinator.OR)
        };
        var args = new Object[] { "us", 10, 20 };

        var probe = CompositeRangeProbeBuilder.build(conditions, 0, 1, new int[] { 1, 2 }, args);
        var summary = new ProbeSummary(
                probe.operator(),
                probe.lower(),
                probe.upper(),
                Arrays.toString(probe.consumed()));

        assertThat(summary).usingRecursiveComparison().isEqualTo(new ProbeSummary(
                Predicate.Operator.BETWEEN,
                CompositeKey.of(new Object[] { "us", 10 }),
                CompositeKey.of(new Object[] { "us", 20 }),
                "[true, true]"));
    }

    @Test
    @DisplayName("should return null without eq prefix")
    void shouldReturnNullWithoutEqPrefix() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.GTE,
                        0,
                        false,
                        LogicalQuery.Combinator.OR)
        };

        var probe = CompositeRangeProbeBuilder.build(conditions, 0, 0, new int[] { 1, 2 }, new Object[] { 10 });

        assertThat(probe).isNull();
    }

    private record ProbeSummary(Predicate.Operator operator, CompositeKey lower, CompositeKey upper, String consumed) {
    }
}
