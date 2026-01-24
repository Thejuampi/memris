package io.memris.storage.ffm;

import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.kernel.selection.SelectionVectors;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

public final class FfmStringColumn {
    private static final int OFFSET_SIZE = 4;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;

    private final Arena arena;
    private final MemorySegment offsets;
    private final MemorySegment data;
    private final int capacity;

    public FfmStringColumn(Arena arena, int capacity) {
        if (arena == null) {
            throw new IllegalArgumentException("arena required");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.arena = arena;
        long offsetBytes = (long) (capacity + 1) * OFFSET_SIZE;
        this.offsets = arena.allocate(offsetBytes, ValueLayout.JAVA_INT.byteAlignment());
        this.data = arena.allocate(offsetBytes * 8);
        this.capacity = capacity;
        offsets.setAtIndex(ValueLayout.JAVA_INT, 0, 0);
    }

    public int capacity() {
        return capacity;
    }

    public void set(int index, String value) {
        checkIndex(index);
        int currentOffset = offsets.getAtIndex(ValueLayout.JAVA_INT, index);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            data.setAtIndex(BYTE, currentOffset + i, bytes[i]);
        }
        offsets.setAtIndex(ValueLayout.JAVA_INT, index + 1, currentOffset + bytes.length);
    }

    public String get(int index) {
        checkIndex(index);
        int start = offsets.getAtIndex(ValueLayout.JAVA_INT, index);
        int end = offsets.getAtIndex(ValueLayout.JAVA_INT, index + 1);
        int length = end - start;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = data.getAtIndex(BYTE, start + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public SelectionVector scanEquals(String value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        byte[] targetBytes = value.getBytes(StandardCharsets.UTF_8);
        MutableSelectionVector selection = factory.create(0);
        for (int i = 0; i < rowCount; i++) {
            int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
            int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
            int length = end - start;
            if (length == targetBytes.length) {
                boolean match = true;
                for (int j = 0; j < length; j++) {
                    if (data.getAtIndex(BYTE, start + j) != targetBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    selection.add(i);
                }
            }
        }
        return selection;
    }

    public SelectionVector scanStartingWith(String prefix, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        int prefixLength = prefixBytes.length;
        MutableSelectionVector selection = factory.create(0);
        for (int i = 0; i < rowCount; i++) {
            int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
            int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
            int length = end - start;
            if (length >= prefixLength) {
                boolean match = true;
                for (int j = 0; j < prefixLength; j++) {
                    if (data.getAtIndex(BYTE, start + j) != prefixBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    selection.add(i);
                }
            }
        }
        return selection;
    }

    public SelectionVector scanEndingWith(String suffix, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        byte[] suffixBytes = suffix.getBytes(StandardCharsets.UTF_8);
        int suffixLength = suffixBytes.length;
        MutableSelectionVector selection = factory.create(0);
        for (int i = 0; i < rowCount; i++) {
            int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
            int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
            int length = end - start;
            if (length >= suffixLength) {
                boolean match = true;
                int offset = length - suffixLength;
                for (int j = 0; j < suffixLength; j++) {
                    if (data.getAtIndex(BYTE, start + offset + j) != suffixBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    selection.add(i);
                }
            }
        }
        return selection;
    }

    public SelectionVector scanLike(String pattern, int rowCount, SelectionVectorFactory factory) {
        // Handle LIKE pattern - support % (any characters) and _ (single character)
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        MutableSelectionVector selection = factory.create(0);
        for (int i = 0; i < rowCount; i++) {
            int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
            int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
            int length = end - start;
            if (matchesPattern(data, start, length, patternBytes)) {
                selection.add(i);
            }
        }
        return selection;
    }

    public SelectionVector scanEqualsIgnoreCase(String value, int rowCount, SelectionVectorFactory factory) {
        if (rowCount <= 0) {
            return SelectionVectors.empty();
        }
        if (rowCount > capacity) {
            throw new IllegalArgumentException("rowCount exceeds capacity: " + rowCount);
        }
        String lowerValue = value.toLowerCase(java.util.Locale.ROOT);
        byte[] targetBytes = lowerValue.getBytes(StandardCharsets.UTF_8);
        MutableSelectionVector selection = factory.create(0);
        for (int i = 0; i < rowCount; i++) {
            int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
            int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
            int length = end - start;
            if (length == targetBytes.length) {
                boolean match = true;
                for (int j = 0; j < length; j++) {
                    // Convert to lowercase for comparison
                    byte b = data.getAtIndex(BYTE, start + j);
                    if (b >= 'A' && b <= 'Z') {
                        b = (byte) (b + 32); // to lowercase
                    }
                    if (b != targetBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    selection.add(i);
                }
            }
        }
        return selection;
    }

    /**
     * Check if a string matches a LIKE pattern.
     * Supports: % (zero or more characters), _ (exactly one character)
     */
    private static boolean matchesPattern(MemorySegment data, int start, int length, byte[] pattern) {
        return matchesPatternRecursive(data, start, length, pattern, 0, 0);
    }

    private static boolean matchesPatternRecursive(MemorySegment data, int strStart, int strLen, byte[] pattern, int patternIdx, int strIdx) {
        // End of pattern - match if at end of string
        if (patternIdx == pattern.length) {
            return strIdx == strLen;
        }

        byte p = pattern[patternIdx];

        // % - matches zero or more characters
        if (p == '%') {
            // Try matching zero characters
            if (matchesPatternRecursive(data, strStart, strLen, pattern, patternIdx + 1, strIdx)) {
                return true;
            }
            // Try matching one or more characters
            if (strIdx < strLen) {
                return matchesPatternRecursive(data, strStart, strLen, pattern, patternIdx, strIdx + 1);
            }
            return false;
        }

        // _ - matches exactly one character
        if (p == '_') {
            if (strIdx >= strLen) {
                return false;
            }
            return matchesPatternRecursive(data, strStart, strLen, pattern, patternIdx + 1, strIdx + 1);
        }

        // Regular character - must match exactly
        if (strIdx >= strLen) {
            return false;
        }
        byte s = data.getAtIndex(BYTE, strStart + strIdx);
        if (s != p) {
            return false;
        }
        return matchesPatternRecursive(data, strStart, strLen, pattern, patternIdx + 1, strIdx + 1);
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index out of range: " + index);
        }
    }
}
