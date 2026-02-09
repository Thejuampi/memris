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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IndexProbeCompilerTest {

    @Test
    @DisplayName("should probe hash and range indexes by operator")
    void shouldProbeHashAndRangeIndexesByOperator() {
        var hash = new HashIndex<String>();
        hash.add("us", new RowId(0, 3));
        var range = new RangeIndex<Integer>();
        range.add(10, new RowId(0, 1));
        range.add(20, new RowId(0, 2));

        var hashRows = IndexProbeCompiler.compile(hash).query(Predicate.Operator.EQ, "us", rowId -> true);
        var rangeRows = IndexProbeCompiler.compile(range).query(Predicate.Operator.GT, 10, rowId -> true);

        assertThat(new ProbeResult(hashRows, rangeRows)).usingRecursiveComparison()
                .isEqualTo(new ProbeResult(new int[] { 3 }, new int[] { 2 }));
    }

    @Test
    @DisplayName("should probe prefix and suffix indexes")
    void shouldProbePrefixAndSuffixIndexes() {
        var prefix = new StringPrefixIndex();
        prefix.add("alpha", new RowId(0, 4));
        var suffix = new StringSuffixIndex();
        suffix.add("omega", new RowId(0, 9));

        var prefixRows = IndexProbeCompiler.compile(prefix)
                .query(Predicate.Operator.STARTING_WITH, "alp", rowId -> true);
        var suffixRows = IndexProbeCompiler.compile(suffix)
                .query(Predicate.Operator.ENDING_WITH, "ega", rowId -> true);

        assertThat(new ProbeResult(prefixRows, suffixRows)).usingRecursiveComparison()
                .isEqualTo(new ProbeResult(new int[] { 4 }, new int[] { 9 }));
    }

    @Test
    @DisplayName("should probe composite hash and range indexes")
    void shouldProbeCompositeHashAndRangeIndexes() {
        var hash = new CompositeHashIndex();
        hash.add(CompositeKey.of(new Object[] { "us", 10 }), new RowId(0, 6));
        var range = new CompositeRangeIndex();
        range.add(CompositeKey.of(new Object[] { "us", 10 }), new RowId(0, 11));
        range.add(CompositeKey.of(new Object[] { "us", 15 }), new RowId(0, 12));

        var hashRows = IndexProbeCompiler.compile(hash)
                .query(Predicate.Operator.EQ, CompositeKey.of(new Object[] { "us", 10 }), rowId -> true);
        var rangeRows = IndexProbeCompiler.compile(range)
                .query(Predicate.Operator.GTE, CompositeKey.of(new Object[] { "us", 12 }), rowId -> true);

        assertThat(new ProbeResult(hashRows, rangeRows)).usingRecursiveComparison()
                .isEqualTo(new ProbeResult(new int[] { 6 }, new int[] { 12 }));
    }

    private record ProbeResult(int[] first, int[] second) {
    }
}
