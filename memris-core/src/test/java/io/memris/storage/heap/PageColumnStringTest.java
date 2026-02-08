package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
