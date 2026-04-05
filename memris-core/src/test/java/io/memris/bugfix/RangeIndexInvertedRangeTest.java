package io.memris.bugfix;

import io.memris.index.RangeIndex;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class RangeIndexInvertedRangeTest {

    @Test
    void between_invertedRange_returnsEmpty() {
        var index = new RangeIndex<Long>();
        index.add(10L, RowId.fromLong(1));
        index.add(20L, RowId.fromLong(2));
        index.add(30L, RowId.fromLong(3));

        assertThatCode(() -> index.between(30L, 10L)).doesNotThrowAnyException();
        assertThat(index.between(30L, 10L).size()).isZero();
    }

    @Test
    void between_normalRange_returnsResults() {
        var index = new RangeIndex<Long>();
        index.add(10L, RowId.fromLong(1));
        index.add(20L, RowId.fromLong(2));
        index.add(30L, RowId.fromLong(3));

        var result = index.between(10L, 20L);
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void between_sameBounds_returnsSingleMatch() {
        var index = new RangeIndex<Long>();
        index.add(10L, RowId.fromLong(1));

        var result = index.between(10L, 10L);
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    void between_emptyIndex_invertedRange_noException() {
        var index = new RangeIndex<Long>();
        assertThatCode(() -> index.between(100L, 1L)).doesNotThrowAnyException();
    }
}
