package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LockFreeFreeListTest {

    @Test
    void pushThenPopReturnsLIFO() {
        var list = new LockFreeFreeList();
        list.push(1);
        list.push(2);
        list.push(3);
        assertThat(list.pop()).isEqualTo(3);
        assertThat(list.pop()).isEqualTo(2);
        assertThat(list.pop()).isEqualTo(1);
    }

    @Test
    void popReturnsMinusOneWhenEmpty() {
        var list = new LockFreeFreeList();
        assertThat(list.pop()).isEqualTo(-1);
    }

    @Test
    void isEmptyReflectsState() {
        var list = new LockFreeFreeList();
        assertThat(list.isEmpty()).isTrue();
        list.push(1);
        assertThat(list.isEmpty()).isFalse();
        list.pop();
        assertThat(list.isEmpty()).isTrue();
    }

    @Test
    void sizeTracksPushAndPop() {
        var list = new LockFreeFreeList();
        list.push(10);
        list.push(20);
        assertThat(list.size()).isEqualTo(2);
        list.pop();
        assertThat(list.size()).isEqualTo(1);
        list.pop();
        assertThat(list.size()).isZero();
    }

    @Test
    void concurrentPushPopDoesNotLoseEntries() throws Exception {
        var list = new LockFreeFreeList();
        var threads = new Thread[4];
        var pushed = new java.util.concurrent.atomic.AtomicInteger();

        for (var t = 0; t < threads.length; t++) {
            var start = t * 250;
            threads[t] = new Thread(() -> {
                for (var i = start; i < start + 250; i++) {
                    list.push(i);
                    pushed.incrementAndGet();
                }
            });
        }
        for (var thread : threads) thread.start();
        for (var thread : threads) thread.join();

        var count = 0;
        while (list.pop() != -1) count++;
        assertThat(count).isEqualTo(pushed.get());
    }
}
