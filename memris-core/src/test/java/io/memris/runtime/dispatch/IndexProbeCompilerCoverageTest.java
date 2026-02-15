package io.memris.runtime.dispatch;

import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.index.StringPrefixIndex;
import io.memris.index.StringSuffixIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexProbeCompilerCoverageTest {

    @Test
    void shouldHandleHashAndStringPatternUnsupportedCases() {
        var hash = new HashIndex<String>();
        hash.add("k", RowId.fromLong(1));
        var hashProbe = IndexProbeCompiler.compile(hash);
        assertThat(hashProbe.query(Predicate.Operator.EQ, "k", rowId -> true)).containsExactly(1);
        assertThat(hashProbe.query(Predicate.Operator.EQ, null, rowId -> true)).isNull();
        assertThat(hashProbe.query(Predicate.Operator.GT, "k", rowId -> true)).isNull();

        var prefix = new StringPrefixIndex();
        prefix.add("alpha", RowId.fromLong(2));
        var suffix = new StringSuffixIndex();
        suffix.add("omega", RowId.fromLong(3));

        var prefixProbe = IndexProbeCompiler.compile(prefix);
        var suffixProbe = IndexProbeCompiler.compile(suffix);
        assertThat(prefixProbe.query(Predicate.Operator.STARTING_WITH, "alp", rowId -> true)).containsExactly(2);
        assertThat(prefixProbe.query(Predicate.Operator.STARTING_WITH, 1, rowId -> true)).isNull();
        assertThat(prefixProbe.query(Predicate.Operator.EQ, "alpha", rowId -> true)).isNull();
        assertThat(suffixProbe.query(Predicate.Operator.ENDING_WITH, "ega", rowId -> true)).containsExactly(3);
        assertThat(suffixProbe.query(Predicate.Operator.ENDING_WITH, 1, rowId -> true)).isNull();
    }

    @Test
    void shouldHandleRangeOperatorsAndInvalidInputs() {
        var range = new RangeIndex<Integer>();
        range.add(10, RowId.fromLong(4));
        range.add(20, RowId.fromLong(5));
        range.add(30, RowId.fromLong(6));

        var probe = IndexProbeCompiler.compile(range);
        assertThat(probe.query(Predicate.Operator.EQ, 20, rowId -> true)).containsExactly(5);
        assertThat(probe.query(Predicate.Operator.GT, 20, rowId -> true)).containsExactly(6);
        assertThat(probe.query(Predicate.Operator.GTE, 20, rowId -> true)).containsExactlyInAnyOrder(5, 6);
        assertThat(probe.query(Predicate.Operator.LT, 20, rowId -> true)).containsExactly(4);
        assertThat(probe.query(Predicate.Operator.LTE, 20, rowId -> true)).containsExactlyInAnyOrder(4, 5);
        assertThat(probe.query(Predicate.Operator.BETWEEN, new Object[] { 10, 20 }, rowId -> true))
                .containsExactlyInAnyOrder(4, 5);

        assertThat(probe.query(Predicate.Operator.BETWEEN, new Object[] { 10 }, rowId -> true)).isNull();
        assertThatThrownBy(() -> probe.query(Predicate.Operator.BETWEEN, new Object[] { "a", 20 }, rowId -> true))
                .isInstanceOf(ClassCastException.class);
        assertThatThrownBy(() -> probe.query(Predicate.Operator.EQ, "not-comparable", rowId -> true))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void shouldHandleCompositeHashAndRangeOperatorsAndUnknownIndex() {
        var hash = new CompositeHashIndex();
        var key = CompositeKey.of(new Object[] { "us", 10 });
        hash.add(key, RowId.fromLong(7));
        var hashProbe = IndexProbeCompiler.compile(hash);
        assertThat(hashProbe.query(Predicate.Operator.EQ, key, rowId -> true)).containsExactly(7);
        assertThat(hashProbe.query(Predicate.Operator.EQ, "bad", rowId -> true)).isNull();

        var range = new CompositeRangeIndex();
        var key10 = CompositeKey.of(new Object[] { "us", 10 });
        var key20 = CompositeKey.of(new Object[] { "us", 20 });
        range.add(key10, RowId.fromLong(8));
        range.add(key20, RowId.fromLong(9));
        var rangeProbe = IndexProbeCompiler.compile(range);
        assertThat(rangeProbe.query(Predicate.Operator.EQ, key10, rowId -> true)).containsExactly(8);
        assertThat(rangeProbe.query(Predicate.Operator.GT, key10, rowId -> true)).containsExactly(9);
        assertThat(rangeProbe.query(Predicate.Operator.BETWEEN, new Object[] { key10, key20 }, rowId -> true))
                .containsExactlyInAnyOrder(8, 9);
        assertThat(rangeProbe.query(Predicate.Operator.BETWEEN, new Object[] { key10 }, rowId -> true)).isNull();
        assertThat(rangeProbe.query(Predicate.Operator.BETWEEN, new Object[] { "x", key20 }, rowId -> true)).isNull();

        var unknown = IndexProbeCompiler.compile(new Object());
        assertThat(unknown.query(Predicate.Operator.EQ, "x", rowId -> true)).isNull();
    }
}
