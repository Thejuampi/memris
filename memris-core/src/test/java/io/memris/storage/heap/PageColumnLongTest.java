package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TDD tests for PageColumnLong.
 * Primitive long column storage with SIMD-capable scan operations.
 */
class PageColumnLongTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        assertEquals(0, column.publishedCount());
    }

    @Test
    void setAndGet() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        column.set(0, 42L);
        column.publish(1);

        assertEquals(42L, column.get(0));
    }

    @Test
    void getReturnsDefaultForUnpublished() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        // Not published yet
        assertEquals(0L, column.get(0));
    }

    @Test
    void setMultipleValues() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        column.set(0, 1L);
        column.set(1, 2L);
        column.set(2, 3L);
        column.publish(3);

        assertEquals(1L, column.get(0));
        assertEquals(2L, column.get(1));
        assertEquals(3L, column.get(2));
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

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
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(5, 10L);
        column.set(10, 10L);
        column.set(15, 99L);
        column.publish(20);

        int[] matches = column.scanEquals(10L, 20);

        assertArrayEquals(new int[]{0, 5, 10}, matches);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 20L);
        column.publish(2);

        int[] matches = column.scanEquals(99L, 2);

        assertArrayEquals(new int[]{}, matches);
    }

    @Test
    void scanRespectsPublishedCount() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 10L);
        column.set(2, 10L);
        column.set(3, 10L);
        column.publish(2); // Only publish first 2

        int[] matches = column.scanEquals(10L, 4);

        // Should only scan published range
        assertArrayEquals(new int[]{0, 1}, matches);
    }

    @Test
    void scanGreaterThan() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 50L);
        column.set(2, 30L);
        column.set(3, 5L);
        column.publish(4);

        int[] matches = column.scanGreaterThan(20L, 4);

        // Both 50 and 30 are > 20
        assertArrayEquals(new int[]{1, 2}, matches);
    }

    @Test
    void scanBetween() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 25L);
        column.set(2, 30L);
        column.set(3, 5L);
        column.set(4, 35L);
        column.publish(5);

        int[] matches = column.scanBetween(10L, 30L, 5);

        assertArrayEquals(new int[]{0, 1, 2}, matches);
    }

    @Test
    void scanInReturnsMatchingOffsets() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.set(3, 99L);
        column.publish(4);

        int[] matches = column.scanIn(new long[]{20L, 30L, 99L}, 4);

        assertArrayEquals(new int[]{1, 2, 3}, matches);
    }
}
