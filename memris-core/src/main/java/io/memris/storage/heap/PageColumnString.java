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

    private final int pageSize;
    private final int maxPages;
    private final int capacity;
    private final String[][] dataPages;
    private final byte[][] presentPages;
    private volatile int published;
    private final Object pageAllocationLock = new Object();

    /**
     * Create a PageColumnString.
     *
     * @param capacity page capacity (must be positive)
     */
    public PageColumnString(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.pageSize = capacity;
        this.maxPages = 1;
        this.capacity = capacity;
        this.dataPages = new String[1][];
        this.presentPages = new byte[1][];
        allocatePage(0);
        this.published = 0;
    }

    public PageColumnString(int pageSize, int maxPages) {
        this(pageSize, maxPages, 1);
    }

    public PageColumnString(int pageSize, int maxPages, int initialPages) {
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
        this.dataPages = new String[maxPages][];
        this.presentPages = new byte[maxPages][];
        for (int pageId = 0; pageId < initialPages; pageId++) {
            allocatePage(pageId);
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
     * Get String value at offset.
     *
     * @param offset the offset
     * @return the String value (null if unpublished)
     */
    public String get(int offset) {
        if (offset < 0 || offset >= capacity) {
            throw new IndexOutOfBoundsException("offset out of range: " + offset);
        }
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        byte[] present = presentPages[pageId];
        if (present == null || present[pageOffset] == 0) {
            return null;
        }
        return dataPages[pageId][pageOffset];
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
        byte[] present = presentPages[pageId];
        return present != null && present[pageOffset] != 0;
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
        int pageId = offset / pageSize;
        int pageOffset = offset % pageSize;
        ensurePage(pageId);
        dataPages[pageId][pageOffset] = value;
        presentPages[pageId][pageOffset] = 1;
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
        ensurePage(pageId);
        dataPages[pageId][pageOffset] = null;
        presentPages[pageId][pageOffset] = 0;
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

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            byte[] present = presentPages[pageId];
            if (present == null) {
                remaining -= pageLimit;
                continue;
            }
            String[] data = dataPages[pageId];
            for (int i = 0; i < pageLimit; i++) {
                if (present[i] != 0 && target.equals(data[i])) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
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
        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            byte[] present = presentPages[pageId];
            if (present == null) {
                remaining -= pageLimit;
                continue;
            }
            String[] data = dataPages[pageId];
            for (int i = 0; i < pageLimit; i++) {
                if (present[i] == 0) {
                    continue;
                }
                String value = data[i];
                if (value != null && value.toLowerCase().equals(lowerTarget)) {
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

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            byte[] present = presentPages[pageId];
            if (present == null) {
                remaining -= pageLimit;
                continue;
            }
            String[] data = dataPages[pageId];
            for (int i = 0; i < pageLimit; i++) {
                if (present[i] != 0 && targetSet.contains(data[i])) {
                    results[found++] = base + i;
                }
            }
            remaining -= pageLimit;
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

        int remaining = count;
        for (int pageId = 0; pageId < maxPages && remaining > 0; pageId++) {
            int pageLimit = Math.min(pageSize, remaining);
            int base = pageId * pageSize;
            byte[] present = presentPages[pageId];
            if (present == null) {
                remaining -= pageLimit;
                continue;
            }
            for (int i = 0; i < pageLimit; i++) {
                if (present[i] == 0) {
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

    private void ensurePage(int pageId) {
        if (dataPages[pageId] != null && presentPages[pageId] != null) {
            return;
        }
        allocatePage(pageId);
    }

    private void allocatePage(int pageId) {
        if (pageId < 0 || pageId >= maxPages) {
            throw new IndexOutOfBoundsException("pageId out of range: " + pageId);
        }
        if (dataPages[pageId] != null && presentPages[pageId] != null) {
            return;
        }
        synchronized (pageAllocationLock) {
            if (dataPages[pageId] == null) {
                dataPages[pageId] = new String[pageSize];
            }
            if (presentPages[pageId] == null) {
                presentPages[pageId] = new byte[pageSize];
            }
        }
    }
}
