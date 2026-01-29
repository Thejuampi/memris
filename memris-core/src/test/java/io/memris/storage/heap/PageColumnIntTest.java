package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TDD tests for PageColumnInt.
 * Primitive int column storage with SIMD-capable scan operations.
 */
class PageColumnIntTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        assertEquals(0, column.publishedCount());
    }

    @Test
    void setAndGet() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        column.set(0, 42);
        column.publish(1);

        assertEquals(42, column.get(0));
    }

    @Test
    void getReturnsDefaultForUnpublished() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        // Not published yet
        assertEquals(0, column.get(0));
    }

    @Test
    void setMultipleValues() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        column.set(0, 1);
        column.set(1, 2);
        column.set(2, 3);
        column.publish(3);

        assertEquals(1, column.get(0));
        assertEquals(2, column.get(1));
        assertEquals(3, column.get(2));
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

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
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(5, 10);
        column.set(10, 10);
        column.set(15, 99);
        column.publish(20);

        int[] matches = column.scanEquals(10, 20);

        assertArrayEquals(new int[]{0, 5, 10}, matches);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);

        int[] matches = column.scanEquals(99, 2);

        assertArrayEquals(new int[]{}, matches);
    }

    @Test
    void scanRespectsPublishedCount() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 10);
        column.set(2, 10);
        column.set(3, 10);
        column.publish(2); // Only publish first 2

        int[] matches = column.scanEquals(10, 4);

        // Should only scan published range
        assertArrayEquals(new int[]{0, 1}, matches);
    }

    @Test
    void scanGreaterThan() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 50);
        column.set(2, 30);
        column.set(3, 5);
        column.publish(4);

        int[] matches = column.scanGreaterThan(20, 4);

        // Both 50 and 30 are > 20
        assertArrayEquals(new int[]{1, 2}, matches);
    }

    @Test
    void scanBetween() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 25);
        column.set(2, 30);
        column.set(3, 5);
        column.set(4, 35);
        column.publish(5);

        int[] matches = column.scanBetween(10, 30, 5);

        assertArrayEquals(new int[]{0, 1, 2}, matches);
    }

    @Test
    void scanInReturnsMatchingOffsets() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 99);
        column.publish(4);

        int[] matches = column.scanIn(new int[]{20, 30, 99}, 4);

        assertArrayEquals(new int[]{1, 2, 3}, matches);
    }
}
