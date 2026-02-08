package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD tests for PageColumnInt.
 * Primitive int column storage with SIMD-capable scan operations.
 */
class PageColumnIntTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        assertThat(column.publishedCount()).isEqualTo(0);
    }

    @Test
    void setAndGet() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        column.set(0, 42);
        column.publish(1);

        assertThat(column.get(0)).isEqualTo(42);
    }

    @Test
    void getReturnsDefaultForUnpublished() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        // Not published yet
        assertThat(column.get(0)).isEqualTo(0);
    }

    @Test
    void setMultipleValues() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        column.set(0, 1);
        column.set(1, 2);
        column.set(2, 3);
        column.publish(3);

        assertThat(new int[]{column.get(0), column.get(1), column.get(2)}).containsExactly(1, 2, 3);
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

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
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(5, 10);
        column.set(10, 10);
        column.set(15, 99);
        column.publish(20);

        int[] matches = column.scanEquals(10, 20);

        assertThat(matches).containsExactly(0, 5, 10);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);

        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);

        int[] matches = column.scanEquals(99, 2);

        assertThat(matches).isEmpty();
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
        assertThat(matches).containsExactly(0, 1);
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
        assertThat(matches).containsExactly(1, 2);
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

        assertThat(matches).containsExactly(0, 1, 2);
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

        assertThat(matches).containsExactly(1, 2, 3);
    }
}
