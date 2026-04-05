package io.memris.bugfix;

import io.memris.storage.heap.AbstractTable;
import io.memris.kernel.RowId;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class AllocateRowIdCapacityRaceTest {

    static class TestTable extends AbstractTable {
        TestTable(int pageSize, int maxPages) {
            super("test", pageSize, maxPages);
        }

        RowId allocate() {
            return allocateRowId();
        }
    }

    @Test
    void allocateRowId_concurrentAtCapacity_exactlyCapacitySucceed() throws Exception {
        var pageSize = 4;
        var maxPages = 1;
        var table = new TestTable(pageSize, maxPages);
        var capacity = pageSize * maxPages;
        var threadCount = capacity + 10;
        var barrier = new CyclicBarrier(threadCount);
        var successes = new ConcurrentLinkedQueue<RowId>();
        var capacityExceeded = new AtomicInteger(0);

        var threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    var rowId = table.allocate();
                    successes.add(rowId);
                } catch (IllegalStateException e) {
                    if (e.getMessage().contains("capacity")) {
                        capacityExceeded.incrementAndGet();
                    }
                } catch (Exception ignored) {}
            });
            threads[i].start();
        }

        for (var t : threads) {
            t.join(5000);
        }

        assertThat(successes.size()).isEqualTo(capacity);
        assertThat(capacityExceeded.get()).isEqualTo(threadCount - capacity);
    }

    @RepeatedTest(5)
    void allocateRowId_concurrentOvershootThenFreeListReuse() throws Exception {
        var pageSize = 2;
        var maxPages = 1;
        var table = new TestTable(pageSize, maxPages);
        var capacity = pageSize * maxPages;

        var threadCount = capacity + 8;
        var barrier = new CyclicBarrier(threadCount);
        var allocated = new ConcurrentLinkedQueue<RowId>();
        var threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    var rowId = table.allocate();
                    allocated.add(rowId);
                } catch (IllegalStateException ignored) {}
                catch (Exception ignored) {}
            });
            threads[i].start();
        }

        for (var t : threads) {
            t.join(5000);
        }

        assertThat(allocated.size()).isEqualTo(capacity);

        for (var rowId : allocated) {
            var rowIndex = (int) rowId.page() * pageSize + rowId.offset();
            table.tombstone(rowId, table.rowGeneration(rowIndex));
        }

        for (int i = 0; i < capacity; i++) {
            assertThatCode(table::allocate).doesNotThrowAnyException();
        }
    }

    @Test
    void allocateRowId_noOvershoot_nextRowIdStaysAtCapacity() throws Exception {
        var pageSize = 4;
        var maxPages = 1;
        var table = new TestTable(pageSize, maxPages);
        var capacity = pageSize * maxPages;

        var threadCount = capacity * 3;
        var barrier = new CyclicBarrier(threadCount);
        var threads = new Thread[threadCount];
        var successes = new ConcurrentLinkedQueue<RowId>();

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    successes.add(table.allocate());
                } catch (Exception ignored) {}
            });
            threads[i].start();
        }

        for (var t : threads) {
            t.join(5000);
        }

        assertThat(successes.size()).isEqualTo(capacity);
        assertThat(table.allocatedCount()).isEqualTo(capacity);
    }
}
