package io.memris.storage.ffm;

import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.kernel.selection.SelectionVectors;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class FfmLongColumn {
    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

    private final String name;
    private final MemorySegment segment;
    private final int capacity;

    public FfmLongColumn(String name, Arena arena, int capacity) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (arena == null) {
            throw new IllegalArgumentException("arena required");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        long bytes = (long) capacity * ValueLayout.JAVA_LONG.byteSize();
        this.name = name;
        this.segment = arena.allocate(bytes, ValueLayout.JAVA_LONG.byteAlignment());
        this.capacity = capacity;
    }

    public String name() { return name; }

    public int capacity() {
        return capacity;
    }

    public void set(int index, long value) {
        checkIndex(index);
        segment.setAtIndex(ValueLayout.JAVA_LONG, index, value);
    }

    public long get(int index) {
        checkIndex(index);
        return segment.getAtIndex(ValueLayout.JAVA_LONG, index);
    }

    public SelectionVector scanEquals(long value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory required");
        }
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> mask = vector.eq(value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_LONG, i) == value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanBetween(long lower, long upper, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory required");
        }
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> geMask = vector.compare(VectorOperators.GE, lower);
            VectorMask<Long> leMask = vector.compare(VectorOperators.LE, upper);
            VectorMask<Long> mask = geMask.and(leMask);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            long v = segment.getAtIndex(ValueLayout.JAVA_LONG, i);
            if (v >= lower && v <= upper) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanGreaterThan(long value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> mask = vector.compare(VectorOperators.GT, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_LONG, i) > value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanGreaterThanOrEqual(long value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> mask = vector.compare(VectorOperators.GE, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_LONG, i) >= value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanLessThan(long value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> mask = vector.compare(VectorOperators.LT, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_LONG, i) < value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanLessThanOrEqual(long value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            LongVector vector = LongVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_LONG.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Long> mask = vector.compare(VectorOperators.LE, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_LONG, i) <= value) {
                selection.add(i);
            }
        }
        return selection;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index out of range: " + index);
        }
    }
}
