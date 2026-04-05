package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TDD tests for PageColumnString.
 * String column storage with scan operations.
 */
class PageColumnStringTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        assertThat(column.publishedCount()).isEqualTo(0);
    }

    @Test
    void setAndGet() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        column.set(0, "hello");
        column.publish(1);

        assertThat(column.get(0)).isEqualTo("hello");
    }

    @Test
    void getReturnsNullForUnpublished() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        // Not published yet
        assertThat(column.get(0)).isNull();
    }

    @Test
    void setMultipleValues() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        column.set(0, "one");
        column.set(1, "two");
        column.set(2, "three");
        column.publish(3);

        assertThat(new String[]{column.get(0), column.get(1), column.get(2)}).containsExactly("one", "two", "three");
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        assertThat(column.publishedCount()).isEqualTo(0);

        column.publish(1);
        assertThat(column.publishedCount()).isEqualTo(1);

        column.publish(5);
        assertThat(column.publishedCount()).isEqualTo(5);

        // Can't decrease published
        column.publish(3);
        assertThat(column.publishedCount()).isEqualTo(5);
    }

    @Test
    void scanEqualsReturnsMatchingOffsets() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "apple");
        column.set(5, "banana");
        column.set(10, "apple");
        column.set(15, "cherry");
        column.publish(20);

        int[] matches = column.scanEquals("apple", 20);

        assertThat(matches).containsExactly(0, 10);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "apple");
        column.set(1, "banana");
        column.publish(2);

        int[] matches = column.scanEquals("cherry", 2);

        assertThat(matches).isEmpty();
    }

    @Test
    void scanRespectsPublishedCount() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "apple");
        column.set(1, "apple");
        column.set(2, "apple");
        column.set(3, "apple");
        column.publish(2); // Only publish first 2

        int[] matches = column.scanEquals("apple", 4);

        // Should only scan published range
        assertThat(matches).containsExactly(0, 1);
    }

    @Test
    void scanInReturnsMatchingOffsets() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "apple");
        column.set(1, "banana");
        column.set(2, "cherry");
        column.set(3, "date");
        column.publish(4);

        int[] matches = column.scanIn(new String[]{"banana", "cherry", "fig"}, 4);

        assertThat(matches).containsExactly(1, 2);
    }

    @Test
    void scanEqualsIgnoreCase() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "Apple");
        column.set(1, "BANANA");
        column.set(2, "cherry");
        column.set(3, "APPLE");
        column.publish(4);

        int[] matches = column.scanEqualsIgnoreCase("apple", 4);

        assertThat(matches).containsExactly(0, 3);
    }

    @Test
    void scanNullMatchesExplicitlySetNullButNotNeverWritten() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "hello");
        column.setNull(1);
        // offset 2: never written
        column.set(3, "world");
        column.setNull(4);
        column.publish(5);

        int[] matches = column.scanEquals(null, 5);

        assertThat(matches).containsExactly(1, 4);
    }

    @Test
    void setNullIsNotPresentAndReturnsNull() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.setNull(0);
        column.publish(1);

        assertThat(column.isPresent(0)).isFalse();
        assertThat(column.get(0)).isNull();
    }

    @Test
    void neverWrittenIsNotPresent() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(1, "value");
        column.publish(2);

        assertThat(column.isPresent(0)).isFalse();
    }

    @Test
    void constructorRejectsZeroCapacity() {
        assertThatThrownBy(() -> new PageColumnString(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void scanCrossPageBoundary() {
        var col = new PageColumnString(4, 4, 4);
        col.set(3, "x");
        col.set(4, "x");
        col.set(12, "x");
        col.set(15, "x");
        col.publish(16);
        assertThat(col.scanEquals("x", 16)).containsExactlyInAnyOrder(3, 4, 12, 15);
    }

    @Test
    void publishSameValueIsNoOp() {
        var col = new PageColumnString(4, 4, 4);
        col.publish(8);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
    }

    @Test
    void scanLastPageOnly() {
        var col = new PageColumnString(4, 4, 4);
        col.set(12, "a");
        col.set(13, "a");
        col.set(14, "a");
        col.set(15, "a");
        col.publish(16);
        assertThat(col.scanEquals("a", 16)).containsExactlyInAnyOrder(12, 13, 14, 15);
    }

    @Test
    void getRejectsExactCapacity() {
        var col = new PageColumnString(4);
        assertThatThrownBy(() -> col.get(4))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("offset out of range");
    }

    @Test
    void threeArgConstructorRejectsZeroMaxPages() {
        assertThatThrownBy(() -> new PageColumnString(4, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPages must be positive");
    }

    @Test
    void threeArgConstructorRejectsZeroInitialPages() {
        assertThatThrownBy(() -> new PageColumnString(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialPages must be positive");
    }
}
