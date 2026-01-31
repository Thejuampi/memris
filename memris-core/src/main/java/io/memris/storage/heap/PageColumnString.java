package io.memris.storage.heap;

/**
 * String column storage with scan operations.
 * <p>
 * Uses String[] array with volatile published watermark for safe concurrent reads.
 * Scan operations enable direct column access without row allocation.
 * <p>
 * <b>Thread-safety:</b>
 * <ul>
 *   <li>Writers: single-writer per offset expected (or external synchronization)</li>
 *   <li>Readers: many concurrent readers via volatile published semantics</li>
 *   <li>Publish: monotonic increment (never decreases)</li>
 * </ul>
 */
public final class PageColumnString {

    private final String[] data;
    private final byte[] present;
    private volatile int published;
    private final int capacity;

    /**
     * Create a PageColumnString.
     *
     * @param capacity page capacity (must be positive)
     */
    public PageColumnString(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.data = new String[capacity];
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
     * Get String value at offset.
     *
     * @param offset the offset
     * @return the String value (null if unpublished)
     */
    public String get(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        if (present[offset] == 0) {
            return null;
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
     * Set String value at offset.
     *
     * @param offset the offset
     * @param value the String value
     */
    public void set(int offset, String value) {
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
        data[offset] = null;
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
    public int[] scanEquals(String target, int limit) {
        if (target == null) {
            return scanNull(limit);
        }

        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] != 0 && target.equals(data[i])) {
                results[found++] = i;
            }
        }

        return trimResults(results, found);
    }

    /**
     * Scan for values equal to target (case-insensitive).
     *
     * @param target  the target value
     * @param limit   maximum offset to scan (published count)
     * @return array of matching offsets
     */
    public int[] scanEqualsIgnoreCase(String target, int limit) {
        if (target == null) {
            return scanNull(limit);
        }

        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        String lowerTarget = target.toLowerCase();
        for (int i = 0; i < count; i++) {
            if (present[i] == 0) {
                continue;
            }
            String value = data[i];
            if (value != null && value.toLowerCase().equals(lowerTarget)) {
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
    public int[] scanIn(String[] targets, int limit) {
        if (targets == null || targets.length == 0) {
            return new int[0];
        }

        // Use HashSet for O(1) lookup - more efficient for large target sets
        java.util.HashSet<String> targetSet = new java.util.HashSet<>(targets.length * 2);
        for (String target : targets) {
            if (target != null) {
                targetSet.add(target);
            }
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
     * Scan for null values.
     */
    private int[] scanNull(int limit) {
        int count = Math.min(published, limit);
        int[] results = new int[count];
        int found = 0;

        for (int i = 0; i < count; i++) {
            if (present[i] == 0) {
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
