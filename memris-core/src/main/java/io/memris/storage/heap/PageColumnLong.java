package io.memris.storage.heap;

/**
 * Primitive long column storage with SIMD-capable scan operations.
 * <p>
 * Uses long[] array with volatile published watermark for safe concurrent reads.
 * Scan operations enable SIMD vectorization for high throughput.
 * <p>
 * <b>Thread-safety:</b>
 * <ul>
 *   <li>Writers: single-writer per offset expected (or external synchronization)</li>
 *   <li>Readers: many concurrent readers via volatile published semantics</li>
 *   <li>Publish: monotonic increment (never decreases)</li>
 * </ul>
 */
public final class PageColumnLong {

    private final long[] data;
    private final byte[] present;
    private volatile int published;
    private final int capacity;

    /**
     * Create a PageColumnLong.
     *
     * @param capacity page capacity (must be positive)
     */
    public PageColumnLong(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.data = new long[capacity];
        this.present = new byte[capacity];
        this.published = 0;
        this.capacity = capacity;
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
        if (present[offset] == 0) {
            return 0L;
        }
        return data[offset];
    }

    /**
     * Check presence at offset.
     */
    public boolean isPresent(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        return present[offset] != 0;
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
        data[offset] = value;
        present[offset] = 1;
    }

    /**
     * Set null at offset.
     */
    public void setNull(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        present[offset] = 0;
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
        // Monotonic publish - only increase
        int current = this.published;
        if (newPublished > current) {
            this.published = newPublished;
        }
    }

    /**
     * Scan for values equal to target.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanEquals(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] == target) {
                results[found++] = i;
            }
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
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanLessThan(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] < target) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values greater than target.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanGreaterThan(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] > target) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values greater than or equal to target.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanGreaterThanOrEqual(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] >= target) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values less than or equal to target.
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanLessThanOrEqual(long target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] <= target) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values in range [lower, upper] (inclusive).
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

        for (int i = 0; i < count; i++) {
            long value = data[i];
            if (present[i] != 0 && value >= lower && value <= upper) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values IN a collection.
     * <p>
     * Uses HashSet for O(1) target lookup instead of O(n*m) nested loop.
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

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && targetSet.contains(data[i])) {
                results[found++] = i;
            }
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
}
