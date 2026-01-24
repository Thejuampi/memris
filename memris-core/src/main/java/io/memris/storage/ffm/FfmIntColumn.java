package io.memris.storage.ffm;

import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.kernel.selection.SelectionVectors;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class FfmIntColumn {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    private final String name;
    private final MemorySegment segment;
    private final int capacity;

    public FfmIntColumn(String name, Arena arena, int capacity) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (arena == null) {
            throw new IllegalArgumentException("arena required");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        long bytes = (long) capacity * ValueLayout.JAVA_INT.byteSize();
        this.name = name;
        this.segment = arena.allocate(bytes, ValueLayout.JAVA_INT.byteAlignment());
        this.capacity = capacity;
    }

    public String name() { return name; }

    public int capacity() {
        return capacity;
    }

    public void set(int index, int value) {
        checkIndex(index);
        segment.setAtIndex(ValueLayout.JAVA_INT, index, value);
    }

    public int get(int index) {
        checkIndex(index);
        return segment.getAtIndex(ValueLayout.JAVA_INT, index);
    }

    public SelectionVector scanEquals(int value, int rowCount, SelectionVectorFactory factory) {
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
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> mask = vector.eq(value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_INT, i) == value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanBetween(int lower, int upper, int rowCount, SelectionVectorFactory factory) {
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
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> geMask = vector.compare(VectorOperators.GE, lower);
            VectorMask<Integer> leMask = vector.compare(VectorOperators.LE, upper);
            VectorMask<Integer> mask = geMask.and(leMask);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            int v = segment.getAtIndex(ValueLayout.JAVA_INT, i);
            if (v >= lower && v <= upper) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanGreaterThan(int value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> mask = vector.compare(VectorOperators.GT, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_INT, i) > value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanGreaterThanOrEqual(int value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> mask = vector.compare(VectorOperators.GE, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_INT, i) >= value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanLessThan(int value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> mask = vector.compare(VectorOperators.LT, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_INT, i) < value) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanLessThanOrEqual(int value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) return SelectionVectors.empty();
        if (rowCount > capacity) throw new IllegalArgumentException("rowCount exceeds capacity");
        if (factory == null) throw new IllegalArgumentException("factory required");
        MutableSelectionVector selection = factory.create(0);
        int loopBound = SPECIES.loopBound(rowCount);
        int i = 0;
        for (; i < loopBound; i += SPECIES.length()) {
            IntVector vector = IntVector.fromMemorySegment(SPECIES, segment,
                    (long) i * ValueLayout.JAVA_INT.byteSize(), ByteOrder.nativeOrder());
            VectorMask<Integer> mask = vector.compare(VectorOperators.LE, value);
            long lanes = mask.toLong();
            while (lanes != 0L) {
                int lane = Long.numberOfTrailingZeros(lanes);
                selection.add(i + lane);
                lanes &= (lanes - 1);
            }
            selection = factory.maybeUpgrade(selection);
        }
        for (; i < rowCount; i++) {
            if (segment.getAtIndex(ValueLayout.JAVA_INT, i) <= value) {
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
