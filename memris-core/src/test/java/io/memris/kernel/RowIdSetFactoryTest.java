package io.memris.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdSetFactoryTest {

    @Test
    void createUsesArraySetForSmallExpectedSize() {
        RowIdSetFactory factory = new RowIdSetFactory(4);

        assertThat(factory.create(1)).isInstanceOf(RowIdArraySet.class);
    }

    @Test
    void createUsesBitSetAtThreshold() {
        RowIdSetFactory factory = new RowIdSetFactory(2);

        assertThat(factory.create(2)).isInstanceOf(RowIdBitSet.class);
    }

    @Test
    void maybeUpgradeReturnsBitSetWhenThresholdReached() {
        RowIdSetFactory factory = new RowIdSetFactory(2);
        MutableRowIdSet set = factory.create(1);

        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(2L));

        assertThat(factory.maybeUpgrade(set)).isInstanceOf(RowIdBitSet.class);
    }

    @Test
    void maybeUpgradeCopiesValuesIntoUpgradedSet() {
        RowIdSetFactory factory = new RowIdSetFactory(2);
        MutableRowIdSet set = factory.create(1);

        set.add(RowId.fromLong(5L));
        set.add(RowId.fromLong(9L));

        assertThat(factory.maybeUpgrade(set).toLongArray())
                .containsExactlyInAnyOrder(5L, 9L);
    }

    @Test
    void maybeUpgradePreservesRowIdZero() {
        RowIdSetFactory factory = new RowIdSetFactory(2);
        MutableRowIdSet set = factory.create(1);

        set.add(RowId.fromLong(0L));
        set.add(RowId.fromLong(1L));

        assertThat(factory.maybeUpgrade(set).toLongArray())
                .containsExactlyInAnyOrder(0L, 1L);
    }

    @Test
    void maybeUpgradeSkipsBitSetWhenRowIdExceedsMaxInt() {
        var factory = new RowIdSetFactory(2);
        var set = factory.create(1);

        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong((long) Integer.MAX_VALUE + 1));

        var upgraded = factory.maybeUpgrade(set);
        assertThat(upgraded).isInstanceOf(RowIdArraySet.class);
        assertThat(upgraded.toLongArray()).containsExactlyInAnyOrder(1L, (long) Integer.MAX_VALUE + 1);
    }

    @Test
    void bitSetRejectsRowIdAboveMaxInt() {
        var set = new RowIdBitSet();
        assertThatThrownBy(() -> set.add(RowId.fromLong((long) Integer.MAX_VALUE + 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
    }

    @Test
    void bitSetAcceptsMaxIntRowId() {
        var set = new RowIdBitSet();
        set.add(RowId.fromLong(Integer.MAX_VALUE));
        assertThat(set.contains(RowId.fromLong(Integer.MAX_VALUE))).isTrue();
    }

    @Test
    void maybeUpgradeUpgradesWithMaxIntRowId() {
        var factory = new RowIdSetFactory(2);
        var set = factory.create(1);
        set.add(RowId.fromLong(1L));
        set.add(RowId.fromLong(Integer.MAX_VALUE));
        var upgraded = factory.maybeUpgrade(set);
        assertThat(upgraded).isInstanceOf(RowIdBitSet.class);
        assertThat(upgraded.toLongArray()).containsExactlyInAnyOrder(1L, (long) Integer.MAX_VALUE);
    }
}
