package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeIndexTest {

    @Test
    void addStoresRowIds() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(10, RowId.fromLong(1L));
        index.add(10, RowId.fromLong(2L));

        assertThat(index.lookup(10).toLongArray())
                .containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void removeRemovesLastRow() {
        RangeIndex<Integer> index = new RangeIndex<>();
        RowId rowId = RowId.fromLong(3L);

        index.add(11, rowId);
        index.remove(11, rowId);

        assertThat(index.lookup(11).size()).isZero();
    }

    @Test
    void betweenReturnsInclusiveRange() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));

        assertThat(index.between(1, 2).toLongArray())
                .containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void greaterThanExcludesBoundary() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));

        assertThat(index.greaterThan(2).toLongArray())
                .containsExactlyInAnyOrder(30L);
    }

    @Test
    void greaterThanOrEqualIncludesBoundary() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));

        assertThat(index.greaterThanOrEqual(2).toLongArray())
                .containsExactlyInAnyOrder(20L, 30L);
    }

    @Test
    void lessThanExcludesBoundary() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));

        assertThat(index.lessThan(2).toLongArray())
                .containsExactlyInAnyOrder(10L);
    }

    @Test
    void lessThanOrEqualIncludesBoundary() {
        RangeIndex<Integer> index = new RangeIndex<>();

        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));

        assertThat(index.lessThanOrEqual(2).toLongArray())
                .containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void betweenNullReturnsEmpty() {
        RangeIndex<Integer> index = new RangeIndex<>();

        assertThat(index.between(null, 1).size()).isZero();
    }

    @Test
    void clearEmptiesIndex() {
        RangeIndex<Integer> index = new RangeIndex<>();
        index.add(1, RowId.fromLong(1L));
        index.add(2, RowId.fromLong(2L));

        index.clear();

        assertThat(index.size()).isZero();
    }

    @Test
    void addRejectsNullKey() {
        RangeIndex<Integer> index = new RangeIndex<>();

        assertThatThrownBy(() -> index.add(null, RowId.fromLong(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void addRejectsNullRowId() {
        RangeIndex<Integer> index = new RangeIndex<>();

        assertThatThrownBy(() -> index.add(1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void removeShouldIgnoreNullArguments() {
        RangeIndex<Integer> index = new RangeIndex<>();
        RowId rowId = RowId.fromLong(1L);
        index.add(1, rowId);

        index.remove(null, rowId);
        index.remove(1, null);

        assertThat(index.lookup(1).toLongArray()).containsExactly(1L);
    }

    @Test
    void lookupWithFilterShouldApplyFilterAndRespectNullFilter() {
        RangeIndex<Integer> index = new RangeIndex<>();
        index.add(10, RowId.fromLong(1L));
        index.add(10, RowId.fromLong(2L));

        assertThat(index.lookup(10, rowId -> rowId.value() == 2L).toLongArray())
                .containsExactly(2L);
        assertThat(index.lookup(10, null).toLongArray())
                .containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void rangeMethodsWithFilterShouldOnlyReturnMatchingRows() {
        RangeIndex<Integer> index = new RangeIndex<>();
        index.add(1, RowId.fromLong(11L));
        index.add(2, RowId.fromLong(22L));
        index.add(3, RowId.fromLong(33L));

        assertThat(index.between(1, 3, rowId -> rowId.value() >= 22).toLongArray())
                .containsExactlyInAnyOrder(22L, 33L);
        assertThat(index.greaterThan(1, rowId -> rowId.value() == 33L).toLongArray())
                .containsExactly(33L);
        assertThat(index.greaterThanOrEqual(2, rowId -> rowId.value() != 22L).toLongArray())
                .containsExactly(33L);
        assertThat(index.lessThan(3, rowId -> rowId.value() == 11L).toLongArray())
                .containsExactly(11L);
        assertThat(index.lessThanOrEqual(2, rowId -> rowId.value() >= 22L).toLongArray())
                .containsExactly(22L);
    }
}
