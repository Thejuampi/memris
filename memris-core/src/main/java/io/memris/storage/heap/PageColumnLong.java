package io.memris.storage.heap;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Primitive long column storage with SIMD-capable scan operations.
 * <p>
 * Uses paged arrays with CAS-based page publication and volatile published
 * watermark for safe concurrent reads. Scan operations enable SIMD
 * vectorization for high throughput.
 * <p>
 * <b>Thread-safety:</b>
 * <ul>
 *   <li>Writers: single-writer per offset expected (or external synchronization)</li>
 *   <li>Readers: many concurrent readers via volatile published semantics</li>
 *   <li>Publish: monotonic increment (never decreases)</li>
 * </ul>
 */
public final class PageColumnLong {

    private final int pageSize;
    private final int maxPages;
    private final int capacity;
    private final AtomicReferenceArray<ColumnPage> pages;
    private volatile int published;
    private static final AtomicIntegerFieldUpdater<PageColumnLong> PUBLISHED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PageColumnLong.class, "published");

    private static final class ColumnPage {
        private final long[] values;
        private final byte[] present;

        private ColumnPage(int pageSize) {
            this.values = new long[pageSize];
            this.present = new byte[pageSize];
        }
    }

    /**
     * Create a PageColumnLong.
     *
     * @param capacity page capacity (must be positive)
     */
    public PageColumnLong(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.pageSize = capacity;
        this.maxPages = 1;
        this.capacity = capacity;
        this.pages = new AtomicReferenceArray<>(1);
        getOrCreatePage(0);
        this.published = 0;
    }

    public PageColumnLong(int pageSize, int maxPages) {
        this(pageSize, maxPages, 1);
    }

    public PageColumnLong(int pageSize, int maxPages, int initialPages) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive: " + pageSize);
        }
        if (maxPages <= 0) {
            throw new IllegalArgumentException("maxPages must be positive: " + maxPages);
        }
        if (initialPages <= 0) {
            throw new IllegalArgumentException("initialPages must be positive: " + initialPages);
        }
        if (initialPages > maxPages) {
            throw new IllegalArgumentException("initialPages exceeds maxPages: " + initialPages);
        }
        long total = (long) pageSize * (long) maxPages;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("capacity exceeds Integer.MAX_VALUE: " + total);
        }
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.capacity = (int) total;
        this.pages = new AtomicReferenceArray<>(maxPages);
        for (int pageId = 0; pageId < initialPages; pageId++) {
            getOrCreatePage(pageId);
        }
        this.published = 0;
    }

    /**
     * Get the number of published (readable) slots.
     *
     * @return published count
     */
    public int publishedCount() {
        return published;
    }

    /**
     * Get primitive long value at offset.
     *
     * @param offset the offset
     * @return the long value (0 if unpublished)
     */
    public long get(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        ColumnPage page = pages.get(pageId);
        if (page == null || page.present[pageOffset] == 0) {
            return 0L;
        }
        return page.values[pageOffset];
    }

    /**
     * Check presence at offset.
     */
    public boolean isPresent(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        ColumnPage page = pages.get(pageId);
        return page != null && page.present[pageOffset] != 0;
    }

    /**
     * Set primitive long value at offset.
     *
     * @param offset the offset
     * @param value the long value
     */
    public void set(int offset, long value) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        ColumnPage page = getOrCreatePage(pageId);
        page.values[pageOffset] = value;
        page.present[pageOffset] = 1;
    }

    /**
     * Set null at offset.
     */
    public void setNull(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        ColumnPage page = getOrCreatePage(pageId);
        page.present[pageOffset] = 0;
    }

    /**
     * Publish slots up to the given offset.
     * <p>
     * Makes rows visible to concurrent readers via volatile write.
     * Published count is monotonic - never decreases.
     *
     * @param newPublished the new published count
     */
    public void publish(int newPublished) {
        if (newPublished < 0 || newPublished > capacity) {
            throw new IndexOutOfBoundsException("newPublished out of range: " + newPublished);
        }
        int current;
        do {
            current = this.published;
            if (newPublished <= current) {
                return;
            }
        } while (!PUBLISHED_UPDATER.compareAndSet(this, current, newPublished));
    }

    /**
     * Scan for values equal to target.
     * <p>
     * Uses loop unrolling (8x) for improved performance with branchless comparison.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanEquals(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && data[i] == target) results[found++] = base + i;
                if (present[i + 1] != 0 && data[i + 1] == target) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && data[i + 2] == target) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && data[i + 3] == target) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && data[i + 4] == target) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && data[i + 5] == target) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && data[i + 6] == target) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && data[i + 7] == target) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && data[i] == target) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        if (found < results.length) {
            int[] trimmed = new int[found];
            System.arraycopy(results, 0, trimmed, 0, found);
            return trimmed;
        }
        return results;
    }

    /**
     * Scan for values less than target.
     * <p>
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanLessThan(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && data[i] < target) results[found++] = base + i;
                if (present[i + 1] != 0 && data[i + 1] < target) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && data[i + 2] < target) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && data[i + 3] < target) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && data[i + 4] < target) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && data[i + 5] < target) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && data[i + 6] < target) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && data[i + 7] < target) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && data[i] < target) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values greater than target.
     * <p>
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanGreaterThan(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && data[i] > target) results[found++] = base + i;
                if (present[i + 1] != 0 && data[i + 1] > target) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && data[i + 2] > target) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && data[i + 3] > target) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && data[i + 4] > target) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && data[i + 5] > target) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && data[i + 6] > target) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && data[i + 7] > target) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && data[i] > target) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values greater than or equal to target.
     * <p>
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanGreaterThanOrEqual(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && data[i] >= target) results[found++] = base + i;
                if (present[i + 1] != 0 && data[i + 1] >= target) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && data[i + 2] >= target) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && data[i + 3] >= target) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && data[i + 4] >= target) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && data[i + 5] >= target) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && data[i + 6] >= target) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && data[i + 7] >= target) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && data[i] >= target) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values less than or equal to target.
     * <p>
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanLessThanOrEqual(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && data[i] <= target) results[found++] = base + i;
                if (present[i + 1] != 0 && data[i + 1] <= target) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && data[i + 2] <= target) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && data[i + 3] <= target) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && data[i + 4] <= target) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && data[i + 5] <= target) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && data[i + 6] <= target) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && data[i + 7] <= target) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && data[i] <= target) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values in range [lower, upper] (inclusive).
     * <p>
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param lower   lower bound (inclusive)
     * @param upper   upper bound (inclusive)
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanBetween(long lower, long upper, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                long v0 = data[i];
                if (present[i] != 0 && v0 >= lower && v0 <= upper) results[found++] = base + i;
                long v1 = data[i + 1];
                if (present[i + 1] != 0 && v1 >= lower && v1 <= upper) results[found++] = base + i + 1;
                long v2 = data[i + 2];
                if (present[i + 2] != 0 && v2 >= lower && v2 <= upper) results[found++] = base + i + 2;
                long v3 = data[i + 3];
                if (present[i + 3] != 0 && v3 >= lower && v3 <= upper) results[found++] = base + i + 3;
                long v4 = data[i + 4];
                if (present[i + 4] != 0 && v4 >= lower && v4 <= upper) results[found++] = base + i + 4;
                long v5 = data[i + 5];
                if (present[i + 5] != 0 && v5 >= lower && v5 <= upper) results[found++] = base + i + 5;
                long v6 = data[i + 6];
                if (present[i + 6] != 0 && v6 >= lower && v6 <= upper) results[found++] = base + i + 6;
                long v7 = data[i + 7];
                if (present[i + 7] != 0 && v7 >= lower && v7 <= upper) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                long value = data[i];
                if (present[i] != 0 && value >= lower && value <= upper) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values IN a collection.
     * <p>
     * Uses HashSet for O(1) target lookup instead of O(n*m) nested loop.
     * Uses loop unrolling (8x) for improved performance.
     *
     * @param targets the target values
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanIn(long[] targets, int limit) {
        if (targets == null || targets.length == 0) {
            return new int[0];
        }

        // Use HashSet for O(1) lookup - more efficient for large target sets
        java.util.HashSet<Long> targetSet = new java.util.HashSet<>(targets.length * 2);
        for (long target : targets) {
            targetSet.add(target);
        }

        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            ColumnPage page = pages.get(pageId);
            if (page == null) {
                remaining -= pageLimit;
                continue;
            }
            byte[] present = page.present;
            long[] data = page.values;

            // Unrolled loop: process 8 elements per iteration
            int i = 0;
            int unrollLimit = pageLimit - 7;
            for (; i < unrollLimit; i += 8) {
                if (present[i] != 0 && targetSet.contains(data[i])) results[found++] = base + i;
                if (present[i + 1] != 0 && targetSet.contains(data[i + 1])) results[found++] = base + i + 1;
                if (present[i + 2] != 0 && targetSet.contains(data[i + 2])) results[found++] = base + i + 2;
                if (present[i + 3] != 0 && targetSet.contains(data[i + 3])) results[found++] = base + i + 3;
                if (present[i + 4] != 0 && targetSet.contains(data[i + 4])) results[found++] = base + i + 4;
                if (present[i + 5] != 0 && targetSet.contains(data[i + 5])) results[found++] = base + i + 5;
                if (present[i + 6] != 0 && targetSet.contains(data[i + 6])) results[found++] = base + i + 6;
                if (present[i + 7] != 0 && targetSet.contains(data[i + 7])) results[found++] = base + i + 7;
            }
            // Handle remaining elements
            for (; i < pageLimit; i++) {
                if (present[i] != 0 && targetSet.contains(data[i])) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
        }

        return trimResults(results, found);
    }

    /**
     * Trim results array to actual found count.
     */
    private int[] trimResults(int[] results, int found) {
        if (found == 0) {
            return new int[0];
        }
        if (found < results.length) {
            int[] trimmed = new int[found];
            System.arraycopy(results, 0, trimmed, 0, found);
            return trimmed;
        }
        return results;
    }

    /**
     * Get the column capacity.
     *
     * @return capacity
     */
    public int capacity() {
        return capacity;
    }

    private ColumnPage getOrCreatePage(int pageId) {
        if (pageId < 0 || pageId >= maxPages) {
            throw new IndexOutOfBoundsException("pageId out of range: " + pageId);
        }
        ColumnPage existing = pages.get(pageId);
        if (existing != null) {
            return existing;
        }
        ColumnPage created = new ColumnPage(pageSize);
        if (pages.compareAndSet(pageId, null, created)) {
            return created;
        }
        return pages.get(pageId);
    }
}
