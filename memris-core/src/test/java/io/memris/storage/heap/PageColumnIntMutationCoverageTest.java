package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageColumnIntMutationCoverageTest {

    @Test
    void singlePageConstructorBoundary() {
        assertThatThrownBy(() -> new PageColumnInt(0))
                .isInstanceOf(IllegalArgumentException.class);
        var col = new PageColumnInt(1);
        assertThat(col.capacity()).isEqualTo(1);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesOne() {
        var col = new PageColumnInt(4, 1);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesZero() {
        assertThatThrownBy(() -> new PageColumnInt(4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorBoundaryPageSizeOne() {
        var col = new PageColumnInt(1, 4, 4);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorInitialPagesOne() {
        assertThatThrownBy(() -> new PageColumnInt(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsInitialExceedsMax() {
        assertThatThrownBy(() -> new PageColumnInt(4, 2, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsNegativePageSize() {
        assertThatThrownBy(() -> new PageColumnInt(-1, 4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getOffsetBoundary() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 10);
        col.set(15, 20);
        assertThat(col.get(0)).isEqualTo(10);
        assertThat(col.get(15)).isEqualTo(20);
        assertThatThrownBy(() -> col.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.get(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void isPresentBoundary() {
        var col = new PageColumnInt(4, 4, 4);
        assertThat(col.isPresent(0)).isFalse();
        col.set(0, 10);
        assertThat(col.isPresent(0)).isTrue();
        assertThatThrownBy(() -> col.isPresent(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.isPresent(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setNullBoundary() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(5, 42);
        col.setNull(5);
        assertThat(col.isPresent(5)).isFalse();
        assertThat(col.get(5)).isEqualTo(0);
        assertThatThrownBy(() -> col.setNull(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.set(-1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void publishMonotonicAndBoundary() {
        var col = new PageColumnInt(4, 4, 4);
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
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 10);
        col.set(5, 10);
        col.set(10, 20);
        col.set(15, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(0, 5, 15);
        assertThat(col.scanEquals(20, 16)).containsExactly(10);
        assertThat(col.scanEquals(99, 16)).isEmpty();
    }

    @Test
    void scanGreaterThanMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanGreaterThan(20, 16)).containsExactlyInAnyOrder(10, 15);
    }

    @Test
    void scanLessThanMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanLessThan(20, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanGreaterThanOrEqualMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanGreaterThanOrEqual(15, 16)).containsExactlyInAnyOrder(5, 10, 15);
    }

    @Test
    void scanLessThanOrEqualMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanLessThanOrEqual(15, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanBetweenMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanBetween(10, 25, 16)).containsExactlyInAnyOrder(5, 10);
    }

    @Test
    void scanInMultiPage() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 5);
        col.set(5, 15);
        col.set(10, 25);
        col.set(15, 35);
        col.publish(16);
        assertThat(col.scanIn(new int[]{5, 25}, 16)).containsExactlyInAnyOrder(0, 10);
    }

    @Test
    void scanInEdgeCases() {
        var col = new PageColumnInt(4, 4, 4);
        assertThat(col.scanIn(null, 16)).isEmpty();
        assertThat(col.scanIn(new int[]{}, 16)).isEmpty();
    }

    @Test
    void scanWithSparseDataSkipsNulls() {
        var col = new PageColumnInt(4, 4, 2);
        col.set(0, 10);
        col.set(7, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(0, 7);
    }

    @Test
    void scanRespectsPublishedLimit() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 10);
        col.set(5, 10);
        col.set(10, 10);
        col.publish(6);
        assertThat(col.scanEquals(10, 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanRespectsLimitParameter() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 10);
        col.set(5, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 4)).containsExactly(0);
    }

    @Test
    void getReturnsZeroForUnpublishedInMultiPage() {
        var col = new PageColumnInt(4, 4, 1);
        col.set(0, 42);
        assertThat(col.get(5)).isEqualTo(0);
    }

    @Test
    void publishCASLoopWorksConcurrently() throws Exception {
        var col = new PageColumnInt(1024);
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
        var col = new PageColumnInt(4, 4, 1);
        col.set(0, 10);
        col.set(4, 20);
        col.set(8, 30);
        col.set(12, 40);
        col.publish(16);
        assertThat(col.get(0)).isEqualTo(10);
        assertThat(col.get(4)).isEqualTo(20);
        assertThat(col.get(8)).isEqualTo(30);
        assertThat(col.get(12)).isEqualTo(40);
    }

    @Test
    void scanFillsPartialResultsThenTrims() {
        var col = new PageColumnInt(4, 4, 4);
        col.set(0, 10);
        col.set(5, 10);
        col.set(10, 10);
        col.publish(16);
        assertThat(col.scanEquals(10, 16)).hasSize(3);
    }

    @Test
    void publishAtExactPageBoundary() {
        var col = new PageColumnInt(4, 4, 4);
        col.publish(4);
        assertThat(col.publishedCount()).isEqualTo(4);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
    }
}
