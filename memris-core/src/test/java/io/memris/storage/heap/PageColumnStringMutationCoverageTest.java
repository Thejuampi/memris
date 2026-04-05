package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageColumnStringMutationCoverageTest {

    @Test
    void singlePageConstructorBoundary() {
        assertThatThrownBy(() -> new PageColumnString(0))
                .isInstanceOf(IllegalArgumentException.class);
        var col = new PageColumnString(1);
        assertThat(col.capacity()).isEqualTo(1);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesOne() {
        var col = new PageColumnString(4, 1);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorBoundaryMaxPagesZero() {
        assertThatThrownBy(() -> new PageColumnString(4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorBoundaryPageSizeOne() {
        var col = new PageColumnString(1, 4, 4);
        assertThat(col.capacity()).isEqualTo(4);
    }

    @Test
    void multiPageConstructorInitialPagesOne() {
        assertThatThrownBy(() -> new PageColumnString(4, 4, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsInitialExceedsMax() {
        assertThatThrownBy(() -> new PageColumnString(4, 2, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiPageConstructorRejectsNegativePageSize() {
        assertThatThrownBy(() -> new PageColumnString(-1, 4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getOffsetBoundary() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "a");
        col.set(15, "b");
        assertThat(col.get(0)).isEqualTo("a");
        assertThat(col.get(15)).isEqualTo("b");
        assertThatThrownBy(() -> col.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.get(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void isPresentBoundary() {
        var col = new PageColumnString(4, 4, 4);
        assertThat(col.isPresent(0)).isFalse();
        col.set(0, "x");
        assertThat(col.isPresent(0)).isTrue();
        assertThatThrownBy(() -> col.isPresent(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.isPresent(16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setNullBoundary() {
        var col = new PageColumnString(4, 4, 4);
        col.set(5, "hello");
        col.setNull(5);
        assertThat(col.isPresent(5)).isFalse();
        assertThat(col.get(5)).isNull();
        assertThatThrownBy(() -> col.setNull(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> col.set(-1, "x"))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void publishMonotonicAndBoundary() {
        var col = new PageColumnString(4, 4, 4);
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
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "hello");
        col.set(5, "hello");
        col.set(10, "world");
        col.set(15, "hello");
        col.publish(16);
        assertThat(col.scanEquals("hello", 16)).containsExactlyInAnyOrder(0, 5, 15);
        assertThat(col.scanEquals("world", 16)).containsExactly(10);
        assertThat(col.scanEquals("xyz", 16)).isEmpty();
    }

    @Test
    void scanEqualsNullFindsNulls() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "a");
        col.setNull(5);
        col.publish(16);
        assertThat(col.scanEquals(null, 16)).containsExactly(5);
    }

    @Test
    void scanEqualsIgnoreCaseMultiPage() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "Hello");
        col.set(5, "HELLO");
        col.set(10, "world");
        col.publish(16);
        assertThat(col.scanEqualsIgnoreCase("hello", 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanEqualsIgnoreCaseNullFindsNulls() {
        var col = new PageColumnString(4, 4, 4);
        col.setNull(0);
        col.publish(1);
        assertThat(col.scanEqualsIgnoreCase(null, 16)).containsExactly(0);
    }

    @Test
    void scanInMultiPage() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "a");
        col.set(5, "b");
        col.set(10, "c");
        col.set(15, "d");
        col.publish(16);
        assertThat(col.scanIn(new String[]{"a", "c"}, 16)).containsExactlyInAnyOrder(0, 10);
    }

    @Test
    void scanInEdgeCases() {
        var col = new PageColumnString(4, 4, 4);
        assertThat(col.scanIn(null, 16)).isEmpty();
        assertThat(col.scanIn(new String[]{}, 16)).isEmpty();
    }

    @Test
    void scanWithSparseDataSkipsNulls() {
        var col = new PageColumnString(4, 4, 2);
        col.set(0, "hello");
        col.set(7, "hello");
        col.publish(16);
        assertThat(col.scanEquals("hello", 16)).containsExactlyInAnyOrder(0, 7);
    }

    @Test
    void scanRespectsPublishedLimit() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "a");
        col.set(5, "a");
        col.set(10, "a");
        col.publish(6);
        assertThat(col.scanEquals("a", 16)).containsExactlyInAnyOrder(0, 5);
    }

    @Test
    void scanRespectsLimitParameter() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "a");
        col.set(5, "a");
        col.publish(16);
        assertThat(col.scanEquals("a", 4)).containsExactly(0);
    }

    @Test
    void getReturnsNullForUnpublishedInMultiPage() {
        var col = new PageColumnString(4, 4, 1);
        col.set(0, "x");
        assertThat(col.get(5)).isNull();
    }

    @Test
    void setExplicitNullDistinguishedFromUnset() {
        var col = new PageColumnString(4, 4, 4);
        col.setNull(0);
        col.publish(2);
        assertThat(col.scanEquals(null, 16)).containsExactly(0);
        assertThat(col.isPresent(0)).isFalse();
        assertThat(col.get(0)).isNull();
    }

    @Test
    void publishCASLoopWorksConcurrently() throws Exception {
        var col = new PageColumnString(1024);
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
        var col = new PageColumnString(4, 4, 1);
        col.set(0, "a");
        col.set(4, "b");
        col.set(8, "c");
        col.set(12, "d");
        col.publish(16);
        assertThat(col.get(0)).isEqualTo("a");
        assertThat(col.get(4)).isEqualTo("b");
        assertThat(col.get(8)).isEqualTo("c");
        assertThat(col.get(12)).isEqualTo("d");
    }

    @Test
    void scanFillsPartialResultsThenTrims() {
        var col = new PageColumnString(4, 4, 4);
        col.set(0, "hello");
        col.set(5, "hello");
        col.set(10, "hello");
        col.publish(16);
        assertThat(col.scanEquals("hello", 16)).hasSize(3);
    }

    @Test
    void publishAtExactPageBoundary() {
        var col = new PageColumnString(4, 4, 4);
        col.publish(4);
        assertThat(col.publishedCount()).isEqualTo(4);
        col.publish(8);
        assertThat(col.publishedCount()).isEqualTo(8);
    }
}
