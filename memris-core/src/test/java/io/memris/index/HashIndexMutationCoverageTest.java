package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HashIndexMutationCoverageTest {

    @Test
    void lookupNullKeyReturnsEmpty() {
        var index = new HashIndex<String>();
        assertThat(index.lookup(null).size()).isZero();
    }

    @Test
    void lookupMissingKeyReturnsEmpty() {
        var index = new HashIndex<String>();
        assertThat(index.lookup("missing").size()).isZero();
    }

    @Test
    void sizeReturnsCorrectCount() {
        var index = new HashIndex<String>();
        assertThat(index.size()).isZero();
        index.add("a", RowId.fromLong(1L));
        assertThat(index.size()).isEqualTo(1);
        index.add("b", RowId.fromLong(2L));
        assertThat(index.size()).isEqualTo(2);
    }

    @Test
    void removeWithNullKeyIsNoOp() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        index.remove(null, RowId.fromLong(1L));
        assertThat(index.lookup("key").size()).isEqualTo(1);
    }

    @Test
    void removeWithNullRowIdIsNoOp() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        index.remove("key", null);
        assertThat(index.lookup("key").size()).isEqualTo(1);
    }

    @Test
    void removeOneOfMultipleLeavesRest() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        index.add("key", RowId.fromLong(2L));
        index.add("key", RowId.fromLong(3L));

        index.remove("key", RowId.fromLong(2L));

        assertThat(index.lookup("key").toLongArray())
                .containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    void lookupWithFilterFiltersResults() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        index.add("key", RowId.fromLong(2L));
        index.add("key", RowId.fromLong(3L));

        var filtered = index.lookup("key", r -> r.value() > 1);
        assertThat(filtered.toLongArray()).containsExactlyInAnyOrder(2L, 3L);
    }

    @Test
    void lookupWithNullFilterReturnsAll() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        index.add("key", RowId.fromLong(2L));

        var result = index.lookup("key", null);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void lookupWithFilterNullKeyReturnsEmpty() {
        var index = new HashIndex<String>();
        assertThat(index.lookup(null, r -> true).size()).isZero();
    }

    @Test
    void lookupWithFilterMissingKeyReturnsEmpty() {
        var index = new HashIndex<String>();
        assertThat(index.lookup("missing", r -> true).size()).isZero();
    }

    @Test
    void entriesReturnsDefensiveCopy() {
        var index = new HashIndex<String>();
        index.add("key", RowId.fromLong(1L));
        var entries = index.entries();
        index.clear();
        assertThat(entries).hasSize(1);
    }

    @Test
    void addRejectsNullRowId() {
        var index = new HashIndex<String>();
        try {
            index.add("key", null);
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
