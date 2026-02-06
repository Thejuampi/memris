package io.memris.kernel;

import org.junit.jupiter.api.Test;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdArraySetTest {

    @Test
    void shouldStartEmpty() {
        var set = new RowIdArraySet();
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldRejectNullAdd() {
        var set = new RowIdArraySet();
        assertThatThrownBy(() -> set.add(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldContainAddedElement() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(42));
        assertThat(set.contains(RowId.fromLong(42))).isTrue();
    }

    @Test
    void shouldNotContainAbsentElement() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(42));
        assertThat(set.contains(RowId.fromLong(99))).isFalse();
    }

    @Test
    void shouldBeIdempotentForDuplicates() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldMaintainSizeAfterDuplicateAdds() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        assertThat(set.toLongArray()).containsExactly(1L);
    }

    @Test
    void shouldRemoveElement() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.add(RowId.fromLong(3));
        set.remove(RowId.fromLong(2));
        assertThat(set.size()).isEqualTo(2);
    }

    @Test
    void shouldRemainConsistentAfterRemove() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.add(RowId.fromLong(3));
        set.remove(RowId.fromLong(2));
        assertThat(set.toLongArray()).containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    void shouldBeIdempotentForRemove() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldIgnoreRemoveOfAbsentElement() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(99));
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldReturnNullSafeForContains() {
        var set = new RowIdArraySet();
        assertThat(set.contains(null)).isFalse();
    }

    @Test
    void shouldHandleNullRemoveWithoutException() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.remove(null);
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void shouldEnumeratorProvideSnapshot() {
        var set = new RowIdArraySet();
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
    void shouldGrowCapacityAsNeeded() {
        var set = new RowIdArraySet(2);
        for (var i = 0; i < 100; i++) {
            set.add(RowId.fromLong(i));
        }
        assertThat(set.size()).isEqualTo(100);
    }

    @Test
    void shouldContainAllElementsAfterGrowth() {
        var set = new RowIdArraySet(2);
        for (var i = 0; i < 100; i++) {
            set.add(RowId.fromLong(i));
        }
        assertThat(set.toLongArray()).containsExactlyInAnyOrder(LongStream.range(0, 100).boxed().toArray(Long[]::new));
    }

    @Test
    void shouldToLongArrayReturnSnapshot() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        var array = set.toLongArray();
        set.add(RowId.fromLong(3));
        assertThat(array).hasSize(2);
    }

    @Test
    void shouldSnapshotNotAffectSet() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(2));
        set.toLongArray();
        set.add(RowId.fromLong(3));
        assertThat(set.size()).isEqualTo(3);
    }

    @Test
    void shouldMaintainSetSemanticsUnderChurn() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.size()).isZero();
    }

    @Test
    void shouldNotContainRemovedDuplicate() {
        var set = new RowIdArraySet();
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.add(RowId.fromLong(1));
        set.remove(RowId.fromLong(1));
        assertThat(set.contains(RowId.fromLong(1))).isFalse();
    }
}
