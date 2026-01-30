package io.memris.storage.heap;

/**
 * Primitive int column storage with SIMD-capable scan operations.
 * <p>
 * Uses int[] array with volatile published watermark for safe concurrent reads.
 * Scan operations enable SIMD vectorization for high throughput.
 * <p>
 * <b>Thread-safety:</b>
 * <ul>
 *   <li>Writers: single-writer per offset expected (or external synchronization)</li>
 *   <li>Readers: many concurrent readers via volatile published semantics</li>
 *   <li>Publish: monotonic increment (never decreases)</li>
 * </ul>
 */
public final class PageColumnInt {

    private final int[] data;
    private final byte[] present;
    private volatile int published;
    private final int capacity;

    /**
     * Create a PageColumnInt.
     *
     * @param capacity page capacity (must be positive)
     */
    public PageColumnInt(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.data = new int[capacity];
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
     * Get primitive int value at offset.
     *
     * @param offset the offset
     * @return the int value (0 if unpublished)
     */
    public int get(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        if (present[offset] == 0) {
            return 0;
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
     * Set primitive int value at offset.
     *
     * @param offset the offset
     * @param value the int value
     */
    public void set(int offset, int value) {
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
    public int[] scanEquals(int target, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && data[i] == target) {
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
    public int[] scanGreaterThan(int target, int limit) {
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
     * Scan for values in range [lower, upper] (inclusive).
     *
     * @param lower   lower bound (inclusive)
     * @param upper   upper bound (inclusive)
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanBetween(int lower, int upper, int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            int value = data[i];
            if (present[i] != 0 && value >= lower && value <= upper) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values IN a collection.
     *
     * @param targets the target values
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanIn(int[] targets, int limit) {
        int count = Math.min(published, limit);

        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] == 0) {
                continue;
            }
            int value = data[i];
            for (int target : targets) {
                if (value == target) {
                    results[found++] = i;
                    break;
                }
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
