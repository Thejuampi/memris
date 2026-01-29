package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for PageColumnString.
 * String column storage with scan operations.
 */
class PageColumnStringTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        assertEquals(0, column.publishedCount());
    }

    @Test
    void setAndGet() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        column.set(0, "hello");
        column.publish(1);

        assertEquals("hello", column.get(0));
    }

    @Test
    void getReturnsNullForUnpublished() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        // Not published yet
        assertNull(column.get(0));
    }

    @Test
    void setMultipleValues() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        column.set(0, "one");
        column.set(1, "two");
        column.set(2, "three");
        column.publish(3);

        assertEquals("one", column.get(0));
        assertEquals("two", column.get(1));
        assertEquals("three", column.get(2));
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        assertEquals(0, column.publishedCount());

        column.publish(1);
        assertEquals(1, column.publishedCount());

        column.publish(5);
        assertEquals(5, column.publishedCount());

        // Can't decrease published
        column.publish(3);
        assertEquals(5, column.publishedCount());
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

        assertArrayEquals(new int[]{0, 10}, matches);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);

        column.set(0, "apple");
        column.set(1, "banana");
        column.publish(2);

        int[] matches = column.scanEquals("cherry", 2);

        assertArrayEquals(new int[]{}, matches);
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
        assertArrayEquals(new int[]{0, 1}, matches);
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

        assertArrayEquals(new int[]{1, 2}, matches);
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

        assertArrayEquals(new int[]{0, 3}, matches);
    }
}
