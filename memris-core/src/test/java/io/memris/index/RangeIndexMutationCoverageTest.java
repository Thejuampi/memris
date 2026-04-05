package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RangeIndexMutationCoverageTest {

    @Test
    void sizeReturnsKeyCount() {
        var index = new RangeIndex<Integer>();
        assertThat(index.size()).isZero();
        index.add(1, RowId.fromLong(1L));
        assertThat(index.size()).isEqualTo(1);
        index.add(2, RowId.fromLong(2L));
        assertThat(index.size()).isEqualTo(2);
    }

    @Test
    void betweenReversedBoundsReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        assertThat(index.between(5, 1).size()).isZero();
    }

    @Test
    void betweenNullUpperReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        assertThat(index.between(1, null).size()).isZero();
    }

    @Test
    void betweenSameLowerUpperReturnsExactMatch() {
        var index = new RangeIndex<Integer>();
        index.add(5, RowId.fromLong(50L));
        index.add(6, RowId.fromLong(60L));
        assertThat(index.between(5, 5).toLongArray()).containsExactly(50L);
    }

    @Test
    void betweenMultipleKeysCollectsAllRowIds() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(1, RowId.fromLong(11L));
        index.add(3, RowId.fromLong(30L));
        var result = index.between(1, 3);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 11L, 30L);
    }

    @Test
    void collectEmptySubMapReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        index.add(10, RowId.fromLong(1L));
        assertThat(index.between(1, 5).size()).isZero();
    }

    @Test
    void lookupNullKeyReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lookup(null).size()).isZero();
    }

    @Test
    void lookupMissingKeyReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lookup(999).size()).isZero();
    }

    @Test
    void greaterThanNullReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.greaterThan(null).size()).isZero();
    }

    @Test
    void lessThanNullReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lessThan(null).size()).isZero();
    }

    @Test
    void greaterThanOrEqualNullReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.greaterThanOrEqual(null).size()).isZero();
    }

    @Test
    void lessThanOrEqualNullReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lessThanOrEqual(null).size()).isZero();
    }

    @Test
    void removeWithNullArgumentsIsNoOp() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(1L));
        index.remove(null, RowId.fromLong(1L));
        index.remove(1, null);
        assertThat(index.lookup(1).size()).isEqualTo(1);
    }

    @Test
    void betweenWithFilterNullBoundsReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(1L));
        assertThat(index.between(null, 1, r -> true).size()).isZero();
        assertThat(index.between(1, null, r -> true).size()).isZero();
    }

    @Test
    void betweenWithFilterReversedBoundsReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(1L));
        assertThat(index.between(5, 1, r -> true).size()).isZero();
    }

    @Test
    void betweenWithFilterAppliesFilter() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.between(1, 3, r -> r.value() == 20L);
        assertThat(result.toLongArray()).containsExactly(20L);
    }

    @Test
    void betweenWithNullFilterReturnsAll() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.between(1, 2, null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void greaterThanWithFilterAppliesFilter() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.greaterThan(1, r -> r.value() == 30L);
        assertThat(result.toLongArray()).containsExactly(30L);
    }

    @Test
    void greaterThanWithNullFilterReturnsAll() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.greaterThan(1, null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(20L);
    }

    @Test
    void greaterThanOrEqualWithFilterAppliesFilter() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.greaterThanOrEqual(2, r -> r.value() == 20L);
        assertThat(result.toLongArray()).containsExactly(20L);
    }

    @Test
    void lessThanWithFilterAppliesFilter() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.lessThan(2, r -> r.value() == 10L);
        assertThat(result.toLongArray()).containsExactly(10L);
    }

    @Test
    void lessThanOrEqualWithFilterAppliesFilter() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.lessThanOrEqual(2, r -> r.value() == 10L);
        assertThat(result.toLongArray()).containsExactly(10L);
    }

    @Test
    void lookupWithFilterNullKeyReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lookup(null, r -> true).size()).isZero();
    }

    @Test
    void lookupWithFilterMissingKeyReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lookup(999, r -> true).size()).isZero();
    }

    @Test
    void greaterThanNullFilterReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.greaterThan(null, r -> true).size()).isZero();
    }

    @Test
    void greaterThanOrEqualNullFilterReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.greaterThanOrEqual(null, r -> true).size()).isZero();
    }

    @Test
    void lessThanNullFilterReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lessThan(null, r -> true).size()).isZero();
    }

    @Test
    void lessThanOrEqualNullFilterReturnsEmpty() {
        var index = new RangeIndex<Integer>();
        assertThat(index.lessThanOrEqual(null, r -> true).size()).isZero();
    }

    @Test
    void removeLastEntryForKeyRemovesKey() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.remove(1, RowId.fromLong(10L));
        assertThat(index.size()).isZero();
    }

    @Test
    void greaterThanOrEqualReturnsMatchingKeys() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.greaterThanOrEqual(2);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(20L, 30L);
    }

    @Test
    void lessThanReturnsMatchingKeys() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.lessThan(3);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void lessThanOrEqualReturnsMatchingKeys() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.lessThanOrEqual(2);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void greaterThanReturnsMatchingKeys() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.add(3, RowId.fromLong(30L));
        var result = index.greaterThan(1);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(20L, 30L);
    }

    @Test
    void removeAllDeletesKey() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.removeAll(1);
        assertThat(index.size()).isEqualTo(1);
        assertThat(index.lookup(1).size()).isZero();
    }

    @Test
    void clearRemovesAllKeys() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        index.clear();
        assertThat(index.size()).isZero();
    }

    @Test
    void betweenSameKeyBoundsReturnsExactMatch() {
        var index = new RangeIndex<Integer>();
        index.add(5, RowId.fromLong(50L));
        assertThat(index.between(5, 5).toLongArray()).containsExactly(50L);
    }

    @Test
    void removeOneOfTwoRowIdsLeavesOther() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(1, RowId.fromLong(11L));
        index.remove(1, RowId.fromLong(10L));
        assertThat(index.lookup(1).toLongArray()).containsExactly(11L);
    }

    @Test
    void greaterThanOrEqualWithNullFilterReturnsAll() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.greaterThanOrEqual(1, null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void lessThanWithNullFilterReturnsAll() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.lessThan(3, null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void lessThanOrEqualWithNullFilterReturnsAll() {
        var index = new RangeIndex<Integer>();
        index.add(1, RowId.fromLong(10L));
        index.add(2, RowId.fromLong(20L));
        var result = index.lessThanOrEqual(2, null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(10L, 20L);
    }
}
