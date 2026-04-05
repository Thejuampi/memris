package io.memris.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RowIdArraySetMutationCoverageTest {

    @Test
    void addAndContains() {
        var set = new RowIdArraySet();
        var rowId = RowId.fromLong(42L);
        set.add(rowId);
        assertThat(set.contains(rowId)).isTrue();
    }

    @Test
    void containsAbsentReturnsFalse() {
        var set = new RowIdArraySet();
        assertThat(set.contains(RowId.fromLong(99L))).isFalse();
    }

    @Test
    void containsNullReturnsFalse() {
        var set = new RowIdArraySet();
        assertThat(set.contains(null)).isFalse();
    }

    @Test
    void addNullThrows() {
        var set = new RowIdArraySet();
        try {
            set.add(null);
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void removeValue() {
        var set = new RowIdArraySet();
        var rowId = RowId.fromLong(42L);
        set.add(rowId);
        set.remove(rowId);
        assertThat(set.contains(rowId)).isFalse();
        assertThat(set.size()).isZero();
    }

    @Test
    void removeNullIsNoOp() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.remove(null);
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void removeAbsentIsNoOp() {
        var set = new RowIdArraySet();
        set.remove(RowId.fromLong(1L));
        assertThat(set.size()).isZero();
    }

    @Test
    void addDuplicateIsIdempotent() {
        var set = new RowIdArraySet();
        var rowId = RowId.fromLong(42L);
        set.add(rowId);
        set.add(rowId);
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void sizeTracksMultipleValues() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));
        set.add(RowId.fromLong(3L));
        assertThat(set.size()).isEqualTo(3);
    }

    @Test
    void toLongArrayReturnsAllValues() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(10L));
        set.add(RowId.fromLong(20L));
        set.add(RowId.fromLong(30L));
        assertThat(set.toLongArray()).containsExactlyInAnyOrder(10L, 20L, 30L);
    }

    @Test
    void enumeratorIteratesAll() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));
        var e = set.enumerator();
        var count = 0;
        while (e.hasNext()) {
            e.nextLong();
            count++;
        }
        assertThat(count).isEqualTo(2);
    }

    @Test
    void enumeratorExhaustedThrows() {
        var set = new RowIdArraySet();
        var e = set.enumerator();
        assertThat(e.hasNext()).isFalse();
        try {
            e.nextLong();
            assertThat(false).isTrue();
        } catch (java.util.NoSuchElementException ex) {
            assertThat(ex).isInstanceOf(java.util.NoSuchElementException.class);
        }
    }

    @Test
    void initialCapacityNegativeThrows() {
        try {
            new RowIdArraySet(-1);
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void manyValuesInDifferentStripes() {
        var set = new RowIdArraySet();
        for (long i = 0; i < 100; i++) {
            set.add(RowId.fromLong(i));
        }
        assertThat(set.size()).isEqualTo(100);
        for (long i = 0; i < 100; i++) {
            assertThat(set.contains(RowId.fromLong(i))).isTrue();
        }
    }

    @Test
    void removeAllThenVerifyEmpty() {
        var set = new RowIdArraySet();
        for (long i = 0; i < 20; i++) {
            set.add(RowId.fromLong(i));
        }
        for (long i = 0; i < 20; i++) {
            set.remove(RowId.fromLong(i));
        }
        assertThat(set.size()).isZero();
        assertThat(set.toLongArray()).isEmpty();
    }

    @Test
    void removeMiddleElement() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));
        set.add(RowId.fromLong(3L));
        set.remove(RowId.fromLong(2L));
        assertThat(set.contains(RowId.fromLong(1L))).isTrue();
        assertThat(set.contains(RowId.fromLong(2L))).isFalse();
        assertThat(set.contains(RowId.fromLong(3L))).isTrue();
    }

    @Test
    void removeFirstElementOfStripe() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));
        set.add(RowId.fromLong(3L));
        set.remove(RowId.fromLong(1L));
        assertThat(set.contains(RowId.fromLong(1L))).isFalse();
        assertThat(set.contains(RowId.fromLong(2L))).isTrue();
        assertThat(set.contains(RowId.fromLong(3L))).isTrue();
        assertThat(set.size()).isEqualTo(2);
    }

    @Test
    void removeLastElementOfStripe() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));
        set.add(RowId.fromLong(3L));
        set.remove(RowId.fromLong(3L));
        assertThat(set.contains(RowId.fromLong(1L))).isTrue();
        assertThat(set.contains(RowId.fromLong(3L))).isFalse();
        assertThat(set.size()).isEqualTo(2);
    }

    @Test
    void toLongArrayAfterRemove() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(10L));
        set.add(RowId.fromLong(20L));
        set.add(RowId.fromLong(30L));
        set.remove(RowId.fromLong(20L));
        assertThat(set.toLongArray()).containsExactlyInAnyOrder(10L, 30L);
    }

    @Test
    void removeSingleElementLeavesEmptyStripe() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(42L));
        set.remove(RowId.fromLong(42L));
        assertThat(set.size()).isZero();
        assertThat(set.toLongArray()).isEmpty();
    }

    @Test
    void constructorAcceptsZeroCapacity() {
        var set = new RowIdArraySet(0);
        set.add(RowId.fromLong(1L));
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void removeNonFirstFromStripeReturnsCorrectElements() {
        var set = new RowIdArraySet();
        for (long i = 0; i < 100; i++) {
            set.add(RowId.fromLong(i));
        }
        set.remove(RowId.fromLong(50L));
        assertThat(set.contains(RowId.fromLong(50L))).isFalse();
        assertThat(set.size()).isEqualTo(99);
        for (long i = 0; i < 100; i++) {
            if (i != 50) {
                assertThat(set.contains(RowId.fromLong(i))).isTrue();
            }
        }
    }
}
