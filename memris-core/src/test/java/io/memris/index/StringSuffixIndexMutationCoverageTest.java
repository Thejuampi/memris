package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringSuffixIndexMutationCoverageTest {

    @Test
    void endsWithNullReturnsEmpty() {
        var index = new StringSuffixIndex();
        assertThat(index.endsWith(null).size()).isZero();
    }

    @Test
    void endsWithMatchesSuffix() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));
        index.add("jones", RowId.fromLong(2L));
        index.add("worth", RowId.fromLong(3L));
        var result = index.endsWith("th");
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    void endsWithNonMatchingReturnsEmpty() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));
        assertThat(index.endsWith("xyz").size()).isZero();
    }

    @Test
    void endsWithFilterAppliesPredicate() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));
        index.add("worth", RowId.fromLong(2L));
        var result = index.endsWith("th", r -> r.value() == 1L);
        assertThat(result.toLongArray()).containsExactly(1L);
    }

    @Test
    void endsWithFilterNullReturnsEmpty() {
        var index = new StringSuffixIndex();
        assertThat(index.endsWith(null, r -> true).size()).isZero();
    }

    @Test
    void notEndsWithNullReturnsEmpty() {
        var index = new StringSuffixIndex();
        assertThat(index.notEndsWith(null, new int[]{1}).size()).isZero();
    }

    @Test
    void notEndsWithReturnsComplement() {
        var index = new StringSuffixIndex();
        index.add("smith", RowId.fromLong(1L));
        index.add("jones", RowId.fromLong(2L));
        var result = index.notEndsWith("th", new int[]{1, 2});
        assertThat(result.toLongArray()).containsExactly(2L);
    }

    @Test
    void removeWithNullKeyIsNoOp() {
        var index = new StringSuffixIndex();
        index.add("key", RowId.fromLong(1L));
        index.remove(null, RowId.fromLong(1L));
        assertThat(index.size()).isPositive();
    }

    @Test
    void removeWithNullRowIdIsNoOp() {
        var index = new StringSuffixIndex();
        index.add("key", RowId.fromLong(1L));
        index.remove("key", null);
        assertThat(index.size()).isPositive();
    }

    @Test
    void addRejectsNullKey() {
        var index = new StringSuffixIndex();
        try {
            index.add(null, RowId.fromLong(1L));
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void clearEmptiesIndex() {
        var index = new StringSuffixIndex();
        index.add("key", RowId.fromLong(1L));
        index.clear();
        assertThat(index.size()).isZero();
    }

    @Test
    void ignoreCaseSuffixLookup() {
        var index = new StringSuffixIndex(true);
        index.add("Hello", RowId.fromLong(1L));
        var result = index.endsWith("LO");
        assertThat(result.toLongArray()).containsExactly(1L);
    }
}
