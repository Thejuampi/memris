package io.memris.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SelectionImplMutationCoverageTest {

    @Test
    void emptySelectionHasSizeZero() {
        assertThat(SelectionImpl.EMPTY.size()).isZero();
    }

    @Test
    void emptySelectionContainsNothing() {
        assertThat(SelectionImpl.EMPTY.contains(1L)).isFalse();
    }

    @Test
    void emptySelectionToRefArrayIsEmpty() {
        assertThat(SelectionImpl.EMPTY.toRefArray()).isEmpty();
    }

    @Test
    void emptySelectionToIntArrayIsEmpty() {
        assertThat(SelectionImpl.EMPTY.toIntArray()).isEmpty();
    }

    @Test
    void singleElementSelection() {
        var sel = new SelectionImpl(new long[]{42L});
        assertThat(sel.size()).isEqualTo(1);
        assertThat(sel.contains(42L)).isTrue();
        assertThat(sel.contains(99L)).isFalse();
    }

    @Test
    void unsortedInputIsSorted() {
        var sel = new SelectionImpl(new long[]{30L, 10L, 20L});
        assertThat(sel.toRefArray()).containsExactly(10L, 20L, 30L);
    }

    @Test
    void containsBinarySearch() {
        var sel = new SelectionImpl(new long[]{10L, 20L, 30L, 40L, 50L});
        assertThat(sel.contains(10L)).isTrue();
        assertThat(sel.contains(30L)).isTrue();
        assertThat(sel.contains(50L)).isTrue();
        assertThat(sel.contains(15L)).isFalse();
        assertThat(sel.contains(55L)).isFalse();
    }

    @Test
    void unionMergesTwoSortedSets() {
        var a = new SelectionImpl(new long[]{1L, 3L, 5L});
        var b = new SelectionImpl(new long[]{2L, 4L, 6L});
        var result = a.union(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
    }

    @Test
    void unionWithOverlappingDeduplicates() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L});
        var b = new SelectionImpl(new long[]{2L, 3L, 4L});
        var result = a.union(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L, 4L);
    }

    @Test
    void unionWithEmpty() {
        var a = new SelectionImpl(new long[]{1L, 2L});
        var result = a.union(SelectionImpl.EMPTY);
        assertThat(result.toRefArray()).containsExactly(1L, 2L);
    }

    @Test
    void emptyUnionWithNonEmpty() {
        var b = new SelectionImpl(new long[]{1L, 2L});
        var result = SelectionImpl.EMPTY.union(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L);
    }

    @Test
    void intersectFindsCommon() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L});
        var b = new SelectionImpl(new long[]{2L, 3L, 4L});
        var result = a.intersect(b);
        assertThat(result.toRefArray()).containsExactly(2L, 3L);
    }

    @Test
    void intersectWithEmptyReturnsEmpty() {
        var a = new SelectionImpl(new long[]{1L, 2L});
        var result = a.intersect(SelectionImpl.EMPTY);
        assertThat(result.size()).isZero();
    }

    @Test
    void intersectNoOverlapReturnsEmpty() {
        var a = new SelectionImpl(new long[]{1L, 2L});
        var b = new SelectionImpl(new long[]{3L, 4L});
        var result = a.intersect(b);
        assertThat(result.size()).isZero();
    }

    @Test
    void subtractRemovesElements() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L, 4L});
        var b = new SelectionImpl(new long[]{2L, 4L});
        var result = a.subtract(b);
        assertThat(result.toRefArray()).containsExactly(1L, 3L);
    }

    @Test
    void subtractWithEmptyReturnsOriginal() {
        var a = new SelectionImpl(new long[]{1L, 2L});
        var result = a.subtract(SelectionImpl.EMPTY);
        assertThat(result.toRefArray()).containsExactly(1L, 2L);
    }

    @Test
    void subtractFromEmptyReturnsEmpty() {
        var b = new SelectionImpl(new long[]{1L, 2L});
        var result = SelectionImpl.EMPTY.subtract(b);
        assertThat(result.size()).isZero();
    }

    @Test
    void subtractTrailingElements() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L, 4L, 5L});
        var b = new SelectionImpl(new long[]{4L, 5L});
        var result = a.subtract(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void toIntArrayExtractsIndices() {
        var sel = new SelectionImpl(new long[]{42L});
        assertThat(sel.toIntArray()).containsExactly(42);
    }

    @Test
    void preSortedConstructorSkipsSort() {
        var sel = new SelectionImpl(new long[]{1L, 2L, 3L}, true);
        assertThat(sel.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void preSortedFalseTriggersSort() {
        var sel = new SelectionImpl(new long[]{3L, 1L, 2L}, false);
        assertThat(sel.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void ensureSortedShortArrayReturnedAsIs() {
        var sel = new SelectionImpl(new long[]{1L});
        assertThat(sel.toRefArray()).containsExactly(1L);
    }

    @Test
    void fromScanIndicesEmptyReturnsEmpty() {
        var result = SelectionImpl.fromScanIndices(null, new int[0]);
        assertThat(result).isSameAs(SelectionImpl.EMPTY);
    }

    @Test
    void ensureSortedReversedArrayTriggersSort() {
        var sel = new SelectionImpl(new long[]{3L, 2L, 1L});
        assertThat(sel.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void containsExercisesBinarySearchHighBranch() {
        var sel = new SelectionImpl(new long[]{10L, 20L, 30L, 40L, 50L});
        assertThat(sel.contains(15L)).isFalse();
        assertThat(sel.contains(5L)).isFalse();
        assertThat(sel.contains(55L)).isFalse();
        assertThat(sel.contains(35L)).isFalse();
    }

    @Test
    void subtractWhereBExhaustedFirstLeavesTrailingA() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L, 4L, 5L});
        var b = new SelectionImpl(new long[]{1L, 2L});
        var result = a.subtract(b);
        assertThat(result.toRefArray()).containsExactly(3L, 4L, 5L);
    }

    @Test
    void unionWhereAExhaustedFirstLeavesTrailingB() {
        var a = new SelectionImpl(new long[]{1L, 2L});
        var b = new SelectionImpl(new long[]{3L, 4L, 5L});
        var result = a.union(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void intersectWithAllMatchReturnsAll() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L});
        var b = new SelectionImpl(new long[]{1L, 2L, 3L});
        var result = a.intersect(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void subtractAllMatchReturnsEmpty() {
        var a = new SelectionImpl(new long[]{1L, 2L, 3L});
        var b = new SelectionImpl(new long[]{1L, 2L, 3L});
        var result = a.subtract(b);
        assertThat(result.size()).isZero();
    }

    @Test
    void trimShrinksArrayWhenFewerResults() {
        var a = new SelectionImpl(new long[]{1L, 3L, 5L, 7L});
        var b = new SelectionImpl(new long[]{2L, 4L, 6L, 8L});
        var result = a.intersect(b);
        assertThat(result.size()).isZero();
    }

    @Test
    void ensureSortedTwoElementReversedArrayTriggersSort() {
        var sel = new SelectionImpl(new long[]{2L, 1L});
        assertThat(sel.toRefArray()).containsExactly(1L, 2L);
    }

    @Test
    void unionTrailingBAfterAExhausts() {
        var a = new SelectionImpl(new long[]{3L, 4L, 5L});
        var b = new SelectionImpl(new long[]{1L, 2L});
        var result = a.union(b);
        assertThat(result.toRefArray()).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    @Test
    void subtractBExhaustsBeforeATrailingElementsPreserved() {
        var a = new SelectionImpl(new long[]{1L, 5L});
        var b = new SelectionImpl(new long[]{3L});
        var result = a.subtract(b);
        assertThat(result.toRefArray()).containsExactly(1L, 5L);
    }

    @Test
    void subtractBAdvancePastA() {
        var a = new SelectionImpl(new long[]{10L});
        var b = new SelectionImpl(new long[]{1L, 2L, 3L});
        var result = a.subtract(b);
        assertThat(result.toRefArray()).containsExactly(10L);
    }

    @Test
    void intersectBExhaustsBeforeA() {
        var a = new SelectionImpl(new long[]{1L, 2L, 5L});
        var b = new SelectionImpl(new long[]{2L});
        var result = a.intersect(b);
        assertThat(result.toRefArray()).containsExactly(2L);
    }
}
