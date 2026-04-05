package io.memris.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SelectionImplToRefArrayImmutabilityTest {

    @Test
    void toRefArrayShouldReturnDefensiveCopy() {
        var selection = new SelectionImpl(new long[]{1L, 2L, 3L});
        var array = selection.toRefArray();
        array[0] = 999L;
        assertThat(selection.toRefArray()).containsExactly(1L, 2L, 3L);
    }

    @Test
    void toRefArrayModificationShouldNotAffectContains() {
        var selection = new SelectionImpl(new long[]{10L, 20L, 30L});
        var array = selection.toRefArray();
        array[0] = 0L;
        assertThat(selection.contains(10L)).isTrue();
    }

    @Test
    void toRefArrayModificationShouldNotAffectSize() {
        var selection = new SelectionImpl(new long[]{5L, 10L});
        var array = selection.toRefArray();
        array[0] = 999L;
        assertThat(selection.size()).isEqualTo(2);
    }
}
