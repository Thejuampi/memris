package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD tests for PageColumnLong.
 * Primitive long column storage with SIMD-capable scan operations.
 */
class PageColumnLongTest {

    private static final int PAGE_SIZE = 64;

    @Test
    void newColumnHasZeroPublished() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        assertThat(column.publishedCount()).isEqualTo(0);
    }

    @Test
    void setAndGet() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        column.set(0, 42L);
        column.publish(1);

        assertThat(column.get(0)).isEqualTo(42L);
    }

    @Test
    void getReturnsDefaultForUnpublished() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        // Not published yet
        assertThat(column.get(0)).isEqualTo(0L);
    }

    @Test
    void setMultipleValues() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        column.set(0, 1L);
        column.set(1, 2L);
        column.set(2, 3L);
        column.publish(3);

        assertThat(new long[]{column.get(0), column.get(1), column.get(2)}).containsExactly(1L, 2L, 3L);
    }

    @Test
    void publishedCountMonotonic() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

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
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(5, 10L);
        column.set(10, 10L);
        column.set(15, 99L);
        column.publish(20);

        int[] matches = column.scanEquals(10L, 20);

        assertThat(matches).containsExactly(0, 5, 10);
    }

    @Test
    void scanEqualsReturnsEmptyForNoMatch() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);

        column.set(0, 10L);
        column.set(1, 20L);
        column.publish(2);

        int[] matches = column.scanEquals(99L, 2);

        assertThat(matches).isEmpty();
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
        assertThat(matches).containsExactly(0, 1);
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
        assertThat(matches).containsExactly(1, 2);
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

        assertThat(matches).containsExactly(0, 1, 2);
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

        assertThat(matches).containsExactly(1, 2, 3);
    }

    @Test
    void scanGreaterThanOrEqualAndLessThanOrEqual() {
        PageColumnLong column = new PageColumnLong(4, 2);
        column.set(0, 5L);
        column.set(1, 10L);
        column.set(2, 15L);
        column.set(3, 20L);
        column.publish(4);

        assertThat(column.scanGreaterThanOrEqual(10L, 4)).containsExactly(1, 2, 3);
        assertThat(column.scanLessThanOrEqual(10L, 4)).containsExactly(0, 1);
    }

    @Test
    void twoArgConstructorCreatesPagedCapacity() {
        PageColumnLong column = new PageColumnLong(8, 3);

        assertThat(column.capacity()).isEqualTo(24);
        column.set(15, 123L);
        column.publish(16);
        assertThat(column.get(15)).isEqualTo(123L);
    }
}
