package io.memris.kernel;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdSetsTest {

    @Test
    void emptyEnumerator_behaviour() {
        RowIdSet empty = RowIdSets.empty();

        assertThat(empty.size()).isZero();
        assertThat(empty.contains(new RowId(1, 1))).isFalse();
        assertThat(empty.toLongArray()).isEmpty();

        LongEnumerator e = empty.enumerator();
        assertThat(e.hasNext()).isFalse();
        assertThatThrownBy(e::nextLong).isInstanceOf(NoSuchElementException.class);
    }
}
