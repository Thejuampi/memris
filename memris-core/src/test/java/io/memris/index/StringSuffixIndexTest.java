package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringSuffixIndexTest {

    @Test
    void endsWithFindsMatchingRows() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));
        index.add("jones", RowId.fromLong(2L));
        index.add("brown", RowId.fromLong(3L));

        assertThat(index.endsWith("th").toLongArray()).containsExactly(1L);
    }

    @Test
    void addRejectsNullKey() {
        var index = new StringSuffixIndex();

        assertThatThrownBy(() -> index.add(null, RowId.fromLong(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void addRejectsNullRowId() {
        var index = new StringSuffixIndex();

        assertThatThrownBy(() -> index.add("key", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void removeSilentlyIgnoresNullKey() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));

        index.remove(null, RowId.fromLong(1L));

        assertThat(index.endsWith("th").toLongArray()).containsExactly(1L);
    }

    @Test
    void removeSilentlyIgnoresNullRowId() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));

        index.remove("smith", null);

        assertThat(index.endsWith("th").toLongArray()).containsExactly(1L);
    }

    @Test
    void endsWithNullReturnsEmpty() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));

        assertThat(index.endsWith(null).size()).isZero();
    }

    @Test
    void notEndsWithNullDoesNotThrow() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));

        var result = index.notEndsWith(null, new int[]{1});
        assertThat(result.size()).isZero();
    }
}
