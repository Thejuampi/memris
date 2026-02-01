package io.memris.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
