package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashIndexTest {

    @Test
    void addStoresRowIds() {
        HashIndex<String> index = new HashIndex<>();
        RowId first = RowId.fromLong(1L);
        RowId second = RowId.fromLong(2L);

        index.add("key", first);
        index.add("key", second);

        assertThat(index.lookup("key").toLongArray())
                .containsExactlyInAnyOrder(first.value(), second.value());
    }

    @Test
    void removeRemovesLastRow() {
        HashIndex<String> index = new HashIndex<>();
        RowId rowId = RowId.fromLong(7L);

        index.add("key", rowId);
        index.remove("key", rowId);

        assertThat(index.lookup("key").size()).isZero();
    }

    @Test
    void removeAllClearsKey() {
        HashIndex<String> index = new HashIndex<>();
        RowId rowId = RowId.fromLong(3L);

        index.add("key", rowId);
        index.removeAll("key");

        assertThat(index.lookup("key").size()).isZero();
    }

    @Test
    void clearEmptiesIndex() {
        HashIndex<String> index = new HashIndex<>();
        index.add("alpha", RowId.fromLong(1L));
        index.add("beta", RowId.fromLong(2L));

        index.clear();

        assertThat(index.size()).isZero();
    }

    @Test
    void addRejectsNullKey() {
        HashIndex<String> index = new HashIndex<>();

        assertThatThrownBy(() -> index.add(null, RowId.fromLong(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
