package io.memris.bugfix;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SelectionFromScanIndicesSortBugTest {

    static class StubTable implements GeneratedTable {
        private final long[] generations;

        StubTable(long[] generations) {
            this.generations = generations;
        }

        @Override public long rowGeneration(int rowIndex) {
            return generations[rowIndex];
        }
        @Override public int columnCount() { return 0; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return 0; }
        @Override public long liveCount() { return 0; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return null; }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) {}
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) {}
        @Override public boolean isLive(long ref) { return false; }
        @Override public long currentGeneration() { return 0; }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[0]; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return false; }
    }

    @Test
    void fromScanIndices_withDifferentGenerations_shouldBeSorted() {
        var indices = new int[]{0, 1, 2};
        var generations = new long[]{5L, 2L, 3L};
        var table = new StubTable(generations);

        var sel = SelectionImpl.fromScanIndices(table, indices);

        var ref0 = Selection.pack(0, 5L);
        var ref1 = Selection.pack(1, 2L);
        var ref2 = Selection.pack(2, 3L);

        assertThat(sel.contains(ref0)).isTrue();
        assertThat(sel.contains(ref1)).isTrue();
        assertThat(sel.contains(ref2)).isTrue();
    }

    @Test
    void fromScanIndices_mutationTest_shouldNotContainUnrelatedRef() {
        var indices = new int[]{0, 1};
        var generations = new long[]{5L, 2L};
        var table = new StubTable(generations);

        var sel = SelectionImpl.fromScanIndices(table, indices);

        var unrelated = Selection.pack(0, 99L);
        assertThat(sel.contains(unrelated)).isFalse();
    }
}
