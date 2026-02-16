package io.memris.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowIdTest {

    @Test
    void compareEqualsAndAccessors() {
        RowId left = new RowId(1, 2);
        RowId right = new RowId(1, 3);
        RowId same = RowId.fromLong(left.value());

        assertThat(left.compareTo(right)).isNegative();
        assertThat(right.compareTo(left)).isPositive();
        assertThat(left.equals(same)).isTrue();
        assertThat(left.equals(null)).isFalse();
        assertThat(left.equals("x")).isFalse();
        assertThat(left.page()).isEqualTo(1L);
        assertThat(left.offset()).isEqualTo(2);
    }

    @Test
    void validatesBounds() {
        assertThatThrownBy(() -> new RowId(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RowId(1, -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
