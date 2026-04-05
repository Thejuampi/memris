package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringPrefixIndexTest {

    @Test
    void startsWithEmptyStringReturnsAllIndexedRows() {
        var index = new StringPrefixIndex();
        index.add("hello", RowId.fromLong(1L));
        index.add("world", RowId.fromLong(2L));

        assertThat(index.startsWith("").toLongArray())
                .containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void addEmptyStringIsReachableViaStartsWith() {
        var index = new StringPrefixIndex();
        index.add("", RowId.fromLong(1L));
        index.add("hello", RowId.fromLong(2L));

        assertThat(index.startsWith("").toLongArray())
                .containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void startsWithEmptyStringReturnsAllAfterRemoval() {
        var index = new StringPrefixIndex();
        index.add("hello", RowId.fromLong(1L));
        index.add("world", RowId.fromLong(2L));
        index.remove("hello", RowId.fromLong(1L));

        assertThat(index.startsWith("").toLongArray())
                .containsExactly(2L);
    }

    @Test
    void removeEmptyStringWorks() {
        var index = new StringPrefixIndex();
        index.add("", RowId.fromLong(1L));
        index.add("hello", RowId.fromLong(2L));
        index.remove("", RowId.fromLong(1L));

        assertThat(index.startsWith("").toLongArray())
                .containsExactly(2L);
    }

    @Test
    void addRejectsNullKey() {
        var index = new StringPrefixIndex();

        assertThatThrownBy(() -> index.add(null, RowId.fromLong(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void addRejectsNullRowId() {
        var index = new StringPrefixIndex();

        assertThatThrownBy(() -> index.add("key", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void removeSilentlyIgnoresNullKey() {
        var index = new StringPrefixIndex();
        index.add("key", RowId.fromLong(1L));

        index.remove(null, RowId.fromLong(1L));

        assertThat(index.startsWith("key").toLongArray()).containsExactly(1L);
    }

    @Test
    void removeSilentlyIgnoresNullRowId() {
        var index = new StringPrefixIndex();
        index.add("key", RowId.fromLong(1L));

        index.remove("key", null);

        assertThat(index.startsWith("key").toLongArray()).containsExactly(1L);
    }

    @Test
    void startsWithNullReturnsEmpty() {
        var index = new StringPrefixIndex();
        index.add("hello", RowId.fromLong(1L));

        assertThat(index.startsWith(null).size()).isZero();
    }

    @Test
    void normalPrefixLookupStillWorks() {
        var index = new StringPrefixIndex();
        index.add("hello", RowId.fromLong(1L));
        index.add("help", RowId.fromLong(2L));
        index.add("world", RowId.fromLong(3L));

        assertThat(index.startsWith("hel").toLongArray())
                .containsExactlyInAnyOrder(1L, 2L);
    }
}
