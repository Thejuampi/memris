package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageColumnLongMutationCoverageTest {

    @Test
    void singlePageConstructorBoundary() {
        assertThatThrownBy(() -> new PageColumnLong(0))
                .isInstanceOf(IllegalArgumentException.class);
        var col = new PageColumnLong(1);
        assertThat(col.capacity()).isEqualTo(1);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesOne() {
        var col = new PageColumnLong(4, 1);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesZero() {
        assertThatThrownBy(() -> new PageColumnLong(4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorBoundaryPageSizeOne() {
        var col = new PageColumnLong(1, 4, 4);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorInitialPagesOne() {
        assertThatThrownBy(() -> new PageColumnLong(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsInitialExceedsMax() {
        assertThatThrownBy(() -> new PageColumnLong(4, 2, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsNegativePageSize() {
        assertThatThrownBy(() -> new PageColumnLong(-1, 4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getOffsetBoundary() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 10L);
        col.set(15, 20L);
        assertThat(col.get(0)).isEqualTo(10L);
        assertThat(col.get(15)).isEqualTo(20L);
        assertThatThrownBy(() -> col.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.get(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void isPresentBoundary() {
        var col = new PageColumnLong(4, 4, 4);
        assertThat(col.isPresent(0)).isFalse();
        col.set(0, 10L);
        assertThat(col.isPresent(0)).isTrue();
        assertThatThrownBy(() -> col.isPresent(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.isPresent(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setNullBoundary() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(5, 42L);
        col.setNull(5);
        assertThat(col.isPresent(5)).isFalse();
        assertThat(col.get(5)).isEqualTo(0L);
        assertThatThrownBy(() -> col.setNull(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.set(-1, 1L))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void publishMonotonicAndBoundary() {
        var col = new PageColumnLong(4, 4, 4);
        assertThat(col.publishedCount()).isEqualTo(0);
        col.publish(1);
        assertThat(col.publishedCount()).isEqualTo(1);
        col.publish(1);
        assertThat(col.publishedCount()).isEqualTo(1);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
        col.publish(4);
        assertThat(col.publishedCount()).isEqualTo(8);
        assertThatThrownBy(() -> col.publish(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.publish(17))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void scanEqualsMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 10L);
        col.set(5, 10L);
        col.set(10, 20L);
        col.set(15, 10L);
        col.publish(16);
        assertThat(col.scanEquals(10L, 16)).containsExactlyInAnyOrder(0, 5, 15);
        assertThat(col.scanEquals(20L, 16)).containsExactly(10);
        assertThat(col.scanEquals(99L, 16)).isEmpty();
    }

    @Test
    void scanGreaterThanMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanGreaterThan(20L, 16)).containsExactlyInAnyOrder(10, 15);
    }

    @Test
    void scanLessThanMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanLessThan(20L, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanGreaterThanOrEqualMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanGreaterThanOrEqual(15L, 16)).containsExactlyInAnyOrder(5, 10, 15);
    }

    @Test
    void scanLessThanOrEqualMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanLessThanOrEqual(15L, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanBetweenMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanBetween(10L, 25L, 16)).containsExactlyInAnyOrder(5, 10);
    }

    @Test
    void scanInMultiPage() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 5L);
        col.set(5, 15L);
        col.set(10, 25L);
        col.set(15, 35L);
        col.publish(16);
        assertThat(col.scanIn(new long[]{5L, 25L}, 16)).containsExactlyInAnyOrder(0, 10);
    }

    @Test
    void scanInEdgeCases() {
        var col = new PageColumnLong(4, 4, 4);
        assertThat(col.scanIn(null, 16)).isEmpty();
        assertThat(col.scanIn(new long[]{}, 16)).isEmpty();
    }

    @Test
    void scanWithSparseDataSkipsNulls() {
        var col = new PageColumnLong(4, 4, 2);
        col.set(0, 10L);
        col.set(7, 10L);
        col.publish(16);
        assertThat(col.scanEquals(10L, 16)).containsExactlyInAnyOrder(0, 7);
    }

    @Test
    void scanRespectsPublishedLimit() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 10L);
        col.set(5, 10L);
        col.set(10, 10L);
        col.publish(6);
        assertThat(col.scanEquals(10L, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanRespectsLimitParameter() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 10L);
        col.set(5, 10L);
        col.publish(16);
        assertThat(col.scanEquals(10L, 4)).containsExactly(0);
    }

    @Test
    void getReturnsZeroForUnpublishedInMultiPage() {
        var col = new PageColumnLong(4, 4, 1);
        col.set(0, 42L);
        assertThat(col.get(5)).isEqualTo(0L);
    }

    @Test
    void publishCASLoopWorksConcurrently() throws Exception {
        var col = new PageColumnLong(1024);
        var threads = new Thread[4];
        for (int t = 0; t < 4; t++) {
            int base = t * 100 + 100;
            threads[t] = new Thread(() -> col.publish(base));
        }
        for (var thread : threads) thread.start();
        for (var thread : threads) thread.join();
        assertThat(col.publishedCount()).isEqualTo(400);
    }

    @Test
    void dynamicPageAllocationBeyondInitialPages() {
        var col = new PageColumnLong(4, 4, 1);
        col.set(0, 10L);
        col.set(4, 20L);
        col.set(8, 30L);
        col.set(12, 40L);
        col.publish(16);
        assertThat(col.get(0)).isEqualTo(10L);
        assertThat(col.get(4)).isEqualTo(20L);
        assertThat(col.get(8)).isEqualTo(30L);
        assertThat(col.get(12)).isEqualTo(40L);
    }

    @Test
    void scanFillsPartialResultsThenTrims() {
        var col = new PageColumnLong(4, 4, 4);
        col.set(0, 10L);
        col.set(5, 10L);
        col.set(10, 10L);
        col.publish(16);
        assertThat(col.scanEquals(10L, 16)).hasSize(3);
    }

    @Test
    void publishAtExactPageBoundary() {
        var col = new PageColumnLong(4, 4, 4);
        col.publish(4);
        assertThat(col.publishedCount()).isEqualTo(4);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
    }
}
