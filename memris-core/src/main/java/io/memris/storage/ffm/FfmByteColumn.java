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

public final class FfmByteColumn {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    private final String name;
    private final MemorySegment segment;
    private final int capacity;

    public FfmByteColumn(String name, Arena arena, int capacity) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (arena == null) {
            throw new IllegalArgumentException("arena required");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        long bytes = (long) capacity * ValueLayout.JAVA_BYTE.byteSize();
        this.name = name;
        this.segment = arena.allocate(bytes, ValueLayout.JAVA_BYTE.byteAlignment());
        this.capacity = capacity;
    }

    public String name() { return name; }

    public int capacity() {
        return capacity;
    }

    public void set(int index, byte value) {
        checkIndex(index);
        segment.setAtIndex(ValueLayout.JAVA_BYTE, index, value);
    }

    public byte get(int index) {
        checkIndex(index);
        return segment.getAtIndex(ValueLayout.JAVA_BYTE, index);
    }

    public SelectionVector scanEquals(byte value, int rowCount, SelectionVectorFactory factory) {
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
                    (long) i * ValueLayout.JAVA_BYTE.byteSize(), ByteOrder.nativeOrder());
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
            if (segment.getAtIndex(ValueLayout.JAVA_BYTE, i) == value) {
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
