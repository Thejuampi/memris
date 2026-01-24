package io.memris.kernel.selection;

public final class SelectionVectorFactory {
    private static final int DEFAULT_BITSET_THRESHOLD = 4096;

    private final int bitSetThreshold;

    public SelectionVectorFactory(int bitSetThreshold) {
        if (bitSetThreshold <= 0) {
            throw new IllegalArgumentException("bitSetThreshold must be positive");
        }
        this.bitSetThreshold = bitSetThreshold;
    }

    public static SelectionVectorFactory defaultFactory() {
        return new SelectionVectorFactory(DEFAULT_BITSET_THRESHOLD);
    }

    public MutableSelectionVector create(int expectedSize) {
        if (expectedSize >= bitSetThreshold) {
            return new BitsetSelection();
        }
        return new IntSelection(Math.max(expectedSize, 16));
    }

    public MutableSelectionVector maybeUpgrade(MutableSelectionVector selection) {
        if (selection instanceof IntSelection intSelection && intSelection.size() >= bitSetThreshold) {
            BitsetSelection upgraded = new BitsetSelection();
            for (int rowIndex : intSelection.toIntArray()) {
                upgraded.add(rowIndex);
            }
            return upgraded;
        }
        return selection;
    }

    public int bitSetThreshold() {
        return bitSetThreshold;
    }
}
