package io.memris.kernel;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Lock-free row ID set for small cardinalities.
 * <p>
 * Uses striped copy-on-write arrays and CAS pointer swaps to avoid locks.
 * Writers contend only within a stripe; reads are wait-free snapshots.
 * <p>
 * <b>Contract:</b>
 * <ul>
 *   <li>This is a <b>set</b> - no duplicates allowed. add() is idempotent.</li>
 *   <li>remove() is idempotent - no-op when value absent.</li>
 *   <li>size() returns the cardinality (unique count).</li>
 *   <li>enumerator() is snapshot-at-creation and never fails due to concurrent mutation.</li>
 *   <li>Iteration order is unspecified.</li>
 * </ul>
 * <p>
 * <b>Write path:</b> CAS replace of stripe arrays. No blocking lock acquisition.
 * <p>
 * <b>Read path:</b> Wait-free direct reads over immutable stripe snapshots.
 */
public final class RowIdArraySet implements MutableRowIdSet {
    private static final int STRIPE_COUNT = 16;
    private static final int STRIPE_MASK = STRIPE_COUNT - 1;

    private final AtomicReferenceArray<long[]> stripes;

    public RowIdArraySet() {
        this.stripes = new AtomicReferenceArray<>(STRIPE_COUNT);
        for (var i = 0; i < STRIPE_COUNT; i++) {
            stripes.set(i, new long[0]);
        }
    }

    public RowIdArraySet(int initialCapacity) {
        this();
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative");
        }
    }

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }

        var target = rowId.value();
        var stripeIndex = stripeFor(target);
        while (true) {
            var current = stripes.get(stripeIndex);
            if (containsInStripe(current, target)) {
                return;
            }

            var next = Arrays.copyOf(current, current.length + 1);
            next[current.length] = target;
            if (stripes.compareAndSet(stripeIndex, current, next)) {
                return;
            }
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }

        var target = rowId.value();
        var stripeIndex = stripeFor(target);
        while (true) {
            var current = stripes.get(stripeIndex);
            var index = indexOfInStripe(current, target);
            if (index < 0) {
                return;
            }

            var next = new long[current.length - 1];
            if (index > 0) {
                System.arraycopy(current, 0, next, 0, index);
            }
            if (index < current.length - 1) {
                System.arraycopy(current, index + 1, next, index, current.length - index - 1);
            }
            if (stripes.compareAndSet(stripeIndex, current, next)) {
                return;
            }
        }
    }

    @Override
    public int size() {
        var total = 0;
        for (var i = 0; i < STRIPE_COUNT; i++) {
            total += stripes.get(i).length;
        }
        return total;
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }

        var target = rowId.value();
        var stripe = stripes.get(stripeFor(target));
        return containsInStripe(stripe, target);
    }

    @Override
    public long[] toLongArray() {
        var snapshot = new long[STRIPE_COUNT][];
        var total = 0;
        for (var i = 0; i < STRIPE_COUNT; i++) {
            var stripe = stripes.get(i);
            snapshot[i] = stripe;
            total += stripe.length;
        }

        var result = new long[total];
        var offset = 0;
        for (var stripe : snapshot) {
            System.arraycopy(stripe, 0, result, offset, stripe.length);
            offset += stripe.length;
        }
        return result;
    }

    @Override
    public LongEnumerator enumerator() {
        var snapshot = toLongArray();
        return new LongEnumerator() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < snapshot.length;
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return snapshot[index++];
            }
        };
    }

    private static int stripeFor(long value) {
        var hash = value ^ (value >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= hash >>> 33;
        return (int) (hash & STRIPE_MASK);
    }

    private static boolean containsInStripe(long[] stripe, long value) {
        for (var entry : stripe) {
            if (entry == value) {
                return true;
            }
        }
        return false;
    }

    private static int indexOfInStripe(long[] stripe, long value) {
        for (var i = 0; i < stripe.length; i++) {
            if (stripe[i] == value) {
                return i;
            }
        }
        return -1;
    }
}
