package io.memris.index;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringPrefixIndexMutationCoverageTest {

    @Test
    void startsWithNullReturnsEmpty() {
        var index = new StringPrefixIndex();
        assertThat(index.startsWith(null).size()).isZero();
    }

    @Test
    void startsWithEmptyPrefixReturnsAll() {
        var index = new StringPrefixIndex();
        index.add("abc", RowId.fromLong(1L));
        index.add("def", RowId.fromLong(2L));
        var result = index.startsWith("");
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void startsWithFiltersByPrefix() {
        var index = new StringPrefixIndex();
        index.add("apple", RowId.fromLong(1L));
        index.add("apricot", RowId.fromLong(2L));
        index.add("banana", RowId.fromLong(3L));
        var result = index.startsWith("ap");
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void startsWithNonMatchingPrefixReturnsEmpty() {
        var index = new StringPrefixIndex();
        index.add("apple", RowId.fromLong(1L));
        assertThat(index.startsWith("zz").size()).isZero();
    }

    @Test
    void addEmptyStringKey() {
        var index = new StringPrefixIndex();
        index.add("", RowId.fromLong(1L));
        var result = index.startsWith("");
        assertThat(result.toLongArray()).containsExactly(1L);
    }

    @Test
    void removeEmptyStringKey() {
        var index = new StringPrefixIndex();
        index.add("", RowId.fromLong(1L));
        index.remove("", RowId.fromLong(1L));
        assertThat(index.startsWith("").size()).isZero();
    }

    @Test
    void removeWithNullKeyIsNoOp() {
        var index = new StringPrefixIndex();
        index.add("key", RowId.fromLong(1L));
        index.remove(null, RowId.fromLong(1L));
        assertThat(index.size()).isPositive();
    }

    @Test
    void removeWithNullRowIdIsNoOp() {
        var index = new StringPrefixIndex();
        index.add("key", RowId.fromLong(1L));
        index.remove("key", null);
        assertThat(index.size()).isPositive();
    }

    @Test
    void startsWithFilterAppliesPredicate() {
        var index = new StringPrefixIndex();
        index.add("abc", RowId.fromLong(1L));
        index.add("abd", RowId.fromLong(2L));
        index.add("abe", RowId.fromLong(3L));
        var result = index.startsWith("ab", r -> r.value() != 2L);
        assertThat(result.toLongArray()).containsExactlyInAnyOrder(1L, 3L);
    }

    @Test
    void startsWithFilterNullFilterReturnsAll() {
        var index = new StringPrefixIndex();
        index.add("abc", RowId.fromLong(1L));
        var result = index.startsWith("ab", null);
        assertThat(result.toLongArray()).containsExactly(1L);
    }

    @Test
    void startsWithFilterEmptyResultReturnsEmpty() {
        var index = new StringPrefixIndex();
        var result = index.startsWith("zz", r -> true);
        assertThat(result.size()).isZero();
    }

    @Test
    void clearEmptiesIndex() {
        var index = new StringPrefixIndex();
        index.add("key", RowId.fromLong(1L));
        index.clear();
        assertThat(index.size()).isZero();
    }

    @Test
    void notStartsWithReturnsComplement() {
        var index = new StringPrefixIndex();
        index.add("abc", RowId.fromLong(1L));
        index.add("def", RowId.fromLong(2L));
        var result = index.notStartsWith("ab", new int[]{1, 2});
        assertThat(result.toLongArray()).containsExactly(2L);
    }

    @Test
    void addRejectsNullKey() {
        var index = new StringPrefixIndex();
        try {
            index.add(null, RowId.fromLong(1L));
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void addRejectsNullRowId() {
        var index = new StringPrefixIndex();
        try {
            index.add("key", null);
            assertThat(false).isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void ignoreCasePrefixLookup() {
        var index = new StringPrefixIndex(true);
        index.add("Hello", RowId.fromLong(1L));
        var result = index.startsWith("hel");
        assertThat(result.toLongArray()).containsExactly(1L);
    }

    @Test
    void sizeReturnsPrefixCount() {
        var index = new StringPrefixIndex();
        index.add("ab", RowId.fromLong(1L));
        assertThat(index.size()).isEqualTo(2);
    }

    @Test
    void removeOneOfTwoSharedPrefixLeavesOther() {
        var index = new StringPrefixIndex();
        index.add("abc", RowId.fromLong(1L));
        index.add("abd", RowId.fromLong(2L));
        index.remove("abc", RowId.fromLong(1L));
        assertThat(index.startsWith("ab").toLongArray()).containsExactly(2L);
    }
}
