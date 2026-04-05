package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void scanGreaterThanOrEqualAndLessThanOrEqual() {
        PageColumnInt column = new PageColumnInt(4, 2);
        column.set(0, 5);
        column.set(1, 10);
        column.set(2, 15);
        column.set(3, 20);
        column.publish(4);

        assertThat(column.scanGreaterThanOrEqual(10, 4)).containsExactly(1, 2, 3);
        assertThat(column.scanLessThanOrEqual(10, 4)).containsExactly(0, 1);
    }

    @Test
    void twoArgConstructorCreatesPagedCapacity() {
        PageColumnInt column = new PageColumnInt(8, 3);

        assertThat(column.capacity()).isEqualTo(24);
        column.set(15, 123);
        column.publish(16);
        assertThat(column.get(15)).isEqualTo(123);
    }

    @Test
    void scanInWithDuplicateTargetsReturnsDistinctOffsets() {
        var column = new PageColumnInt(64);
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.publish(3);

        var matches = column.scanIn(new int[]{20, 20, 20}, 3);

        assertThat(matches).containsExactly(1);
    }

    @Test
    void scanInWithSingleTarget() {
        var column = new PageColumnInt(64);
        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);

        var matches = column.scanIn(new int[]{20}, 2);

        assertThat(matches).containsExactly(1);
    }

    @Test
    void scanInWithManyTargets() {
        var column = new PageColumnInt(64);
        for (var i = 0; i < 64; i++) {
            column.set(i, i);
        }
        column.publish(64);

        var targets = new int[32];
        for (var i = 0; i < 32; i++) {
            targets[i] = i * 2;
        }
        var matches = column.scanIn(targets, 64);

        assertThat(matches).hasSize(32);
    }

    @Test
    void scanInWithUnsortedTargetsStillMatches() {
        var column = new PageColumnInt(64);
        column.set(0, 50);
        column.set(1, 10);
        column.set(2, 30);
        column.publish(3);

        var matches = column.scanIn(new int[]{30, 10, 50}, 3);

        assertThat(matches).containsExactly(0, 1, 2);
    }

    @Test
    void constructorRejectsZeroCapacity() {
        assertThatThrownBy(() -> new PageColumnInt(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsZeroPageSize() {
        assertThatThrownBy(() -> new PageColumnInt(0, 4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsZeroMaxPages() {
        assertThatThrownBy(() -> new PageColumnInt(4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsZeroInitialPages() {
        assertThatThrownBy(() -> new PageColumnInt(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void scanCrossPageBoundary() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(3, 10);
        col.set(4, 10);
        col.set(12, 10);
        col.set(15, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(3, 4, 12, 15);
    }

    @Test
    void publishAtExactCapacity() {
        var col = new PageColumnInt(4, 4, 4);
        col.publish(16);
        assertThat(col.publishedCount()).isEqualTo(16);
    }

    @Test
    void publishSameValueIsNoOp() {
        var col = new PageColumnInt(4, 4, 4);
        col.publish(8);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
    }

    @Test
    void scanLastPageOnly() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(12, 10);
        col.set(13, 10);
        col.set(14, 10);
        col.set(15, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(12, 13, 14, 15);
    }

    @Test
    void scanWithNullPageInMiddle() {
        var col = new PageColumnInt(4, 4, 1);
        col.set(0, 10);
        col.set(5, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void getRejectsExactCapacity() {
        var col = new PageColumnInt(4);
        assertThatThrownBy(() -> col.get(4))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("offset out of range");
    }

    @Test
    void setRejectsExactCapacity() {
        var col = new PageColumnInt(4);
        assertThatThrownBy(() -> col.set(4, 1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("offset out of range");
    }

    @Test
    void threeArgConstructorRejectsZeroPageSize() {
        assertThatThrownBy(() -> new PageColumnInt(0, 4, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pageSize must be positive");
    }

    @Test
    void threeArgConstructorRejectsZeroMaxPages() {
        assertThatThrownBy(() -> new PageColumnInt(4, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPages must be positive");
    }

    @Test
    void threeArgConstructorRejectsZeroInitialPages() {
        assertThatThrownBy(() -> new PageColumnInt(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialPages must be positive");
    }

    @Test
    void publishAtExactCapacitySucceeds() {
        var col = new PageColumnInt(4);
        col.publish(4);
        assertThat(col.publishedCount()).isEqualTo(4);
    }

    @Test
    void trimResultsPartialFill() {
        var col = new PageColumnInt(64);
        col.set(0, 10);
        col.set(1, 20);
        col.publish(2);
        var result = col.scanEquals(10, 2);
        assertThat(result).containsExactly(0);
        assertThat(result.length).isEqualTo(1);
    }
}
