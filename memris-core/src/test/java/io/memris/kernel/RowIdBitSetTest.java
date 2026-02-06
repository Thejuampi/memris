package io.memris.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdBitSetTest {

    @Test
    void shouldStartEmpty() {
        var set = new RowIdBitSet();
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldRejectNullAdd() {
        var set = new RowIdBitSet();
        assertThatThrownBy(() -> set.add(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldContainAddedElement() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(42));
        assertThat(set.contains(RowId.fromLong(42))).isTrue();
    }

    @Test
    void shouldNotContainAbsentElement() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(42));
        assertThat(set.contains(RowId.fromLong(99))).isFalse();
    }

    @Test
    void shouldBeIdempotentForDuplicates() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldMaintainSizeAfterDuplicateAdds() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        assertThat(set.toLongArray()).containsExactly(1L);
    }

    @Test
    void shouldRemoveElement() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.add(RowId.fromLong(3));
        set.remove(RowId.fromLong(2));
        assertThat(set.size()).isEqualTo(2);
    }

    @Test
    void shouldRemainConsistentAfterRemove() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.add(RowId.fromLong(3));
        set.remove(RowId.fromLong(2));
        assertThat(set.toLongArray()).containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    void shouldBeIdempotentForRemove() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldIgnoreRemoveOfAbsentElement() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(99));
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldHandleWordBoundary63() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(63));
        assertThat(set.contains(RowId.fromLong(63))).isTrue();
    }

    @Test
    void shouldHandleWordBoundary64() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(64));
        assertThat(set.contains(RowId.fromLong(64))).isTrue();
    }

    @Test
    void shouldHandleWordBoundary65() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(65));
        assertThat(set.contains(RowId.fromLong(65))).isTrue();
    }

    @Test
    void shouldHandleChunkBoundary4095() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(4095));
        assertThat(set.contains(RowId.fromLong(4095))).isTrue();
    }

    @Test
    void shouldHandleChunkBoundary4096() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(4096));
        assertThat(set.contains(RowId.fromLong(4096))).isTrue();
    }

    @Test
    void shouldHandleChunkBoundary4097() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(4097));
        assertThat(set.contains(RowId.fromLong(4097))).isTrue();
    }

    @Test
    void shouldReturnNullSafeForContains() {
        var set = new RowIdBitSet();
        assertThat(set.contains(null)).isFalse();
    }

    @Test
    void shouldHandleNullRemoveWithoutException() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.remove(null);
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldEnumeratorProvideSnapshot() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        var enumerator = set.enumerator();
        set.add(RowId.fromLong(3));
        set.remove(RowId.fromLong(1));

        var values = new java.util.ArrayList<Long>();
        while (enumerator.hasNext()) {
            values.add(enumerator.nextLong());
        }
        assertThat(values).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void shouldHandleLargeRowIds() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(10000));
        set.add(RowId.fromLong(20000));
        set.add(RowId.fromLong(50000));
        assertThat(set.size()).isEqualTo(3);
    }

    @Test
    void shouldContainLargeRowId10000() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(10000));
        assertThat(set.contains(RowId.fromLong(10000))).isTrue();
    }

    @Test
    void shouldContainLargeRowId20000() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(20000));
        assertThat(set.contains(RowId.fromLong(20000))).isTrue();
    }

    @Test
    void shouldContainLargeRowId50000() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(50000));
        assertThat(set.contains(RowId.fromLong(50000))).isTrue();
    }

    @Test
    void shouldRejectNegativeRowId() {
        var set = new RowIdBitSet();
        assertThatThrownBy(() -> set.add(RowId.fromLong(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    @Test
    void shouldRejectTooLargeRowId() {
        var set = new RowIdBitSet();
        assertThatThrownBy(() -> set.add(RowId.fromLong(Integer.MAX_VALUE + 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
    }

    @Test
    void shouldToLongArrayReturnSnapshot() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        var array = set.toLongArray();
        set.add(RowId.fromLong(3));
        assertThat(array).hasSize(2);
    }

    @Test
    void shouldSnapshotNotAffectSet() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.toLongArray();
        set.add(RowId.fromLong(3));
        assertThat(set.size()).isEqualTo(3);
    }

    @Test
    void shouldMaintainSetSemanticsUnderChurn() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldNotContainRemovedDuplicate() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.contains(RowId.fromLong(1))).isFalse();
    }

    @Test
    void shouldHandleManyAddRemoveCycles() {
        var set = new RowIdBitSet();
        for (var i = 0; i < 1000; i++) {
            set.add(RowId.fromLong(i));
        }
        for (var i = 0; i < 1000; i += 2) {
            set.remove(RowId.fromLong(i));
        }
        assertThat(set.size()).isEqualTo(500);
    }

    @Test
    void shouldContainOddElementsAfterBulkRemove() {
        var set = new RowIdBitSet();
        for (var i = 0; i < 1000; i++) {
            set.add(RowId.fromLong(i));
        }
        for (var i = 0; i < 1000; i += 2) {
            set.remove(RowId.fromLong(i));
        }
        assertThat(set.toLongArray())
                .containsExactlyInAnyOrder(java.util.stream.LongStream.range(0, 500).mapToObj(i -> i * 2 + 1).toArray(Long[]::new));
    }

    @Test
    void shouldNotContainEvenElementsAfterBulkRemove() {
        var set = new RowIdBitSet();
        for (var i = 0; i < 1000; i++) {
            set.add(RowId.fromLong(i));
        }
        for (var i = 0; i < 1000; i += 2) {
            set.remove(RowId.fromLong(i));
        }
        assertThat(set.toLongArray())
                .doesNotContain(java.util.stream.LongStream.range(0, 500).mapToObj(i -> i * 2).toArray(Long[]::new));
    }
}
