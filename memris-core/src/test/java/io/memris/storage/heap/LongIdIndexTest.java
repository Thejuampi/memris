package io.memris.storage.heap;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD tests for LongIdIndex.
 * Long key to RowId index for O(1) lookups.
 */
class LongIdIndexTest {

    @Test
    void newIndexHasZeroSize() {
        LongIdIndex index = new LongIdIndex(16);
        assertThat(index.size()).isEqualTo(0);
    }

    @Test
    void putAndGet() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);

        assertThat(index.get(42L)).isEqualTo(rowId);
    }

    @Test
    void getReturnsNullForMissingKey() {
        LongIdIndex index = new LongIdIndex(16);

        assertThat(index.get(999L)).isNull();
    }

    @Test
    void putUpdatesExistingKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId1 = new RowId(0, 5);
        RowId rowId2 = new RowId(1, 10);

        index.put(42L, rowId1, 1L);
        index.put(42L, rowId2, 2L); // Update

        assertThat(new IndexSnapshot(index.get(42L), index.size())).usingRecursiveComparison()
                .isEqualTo(new IndexSnapshot(rowId2, 1));
    }

    @Test
    void sizeTracksNumberOfKeys() {
        LongIdIndex index = new LongIdIndex(16);

        assertThat(index.size()).isEqualTo(0);

        index.put(1L, new RowId(0, 0), 1L);
        assertThat(index.size()).isEqualTo(1);

        index.put(2L, new RowId(0, 1), 1L);
        assertThat(index.size()).isEqualTo(2);

        // Same key doesn't increase size
        index.put(1L, new RowId(1, 0), 2L);
        assertThat(index.size()).isEqualTo(2);
    }

    @Test
    void removeDeletesKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);
        assertThat(index.size()).isEqualTo(1);

        index.remove(42L);
        assertThat(new IndexSnapshot(index.get(42L), index.size())).usingRecursiveComparison()
                .isEqualTo(new IndexSnapshot(null, 0));
    }

    @Test
    void removeMissingKeyDoesNothing() {
        LongIdIndex index = new LongIdIndex(16);

        index.remove(999L); // Should not throw
        assertThat(index.size()).isEqualTo(0);
    }

    @Test
    void hasKeyReturnsTrueForExistingKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);

        assertThat(new KeySnapshot(index.hasKey(42L), index.hasKey(999L))).usingRecursiveComparison()
                .isEqualTo(new KeySnapshot(true, false));
    }

    @Test
    void multipleKeysCanBeStored() {
        LongIdIndex index = new LongIdIndex(16);

        index.put(1L, new RowId(0, 0), 1L);
        index.put(2L, new RowId(0, 1), 1L);
        index.put(3L, new RowId(0, 2), 1L);

        assertThat(new MultiSnapshot(index.get(1L), index.get(2L), index.get(3L), index.size()))
                .usingRecursiveComparison()
                .isEqualTo(new MultiSnapshot(new RowId(0, 0), new RowId(0, 1), new RowId(0, 2), 3));
    }

    @Test
    void handlesNegativeLongs() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(-1L, rowId, 1L);

        assertThat(index.get(-1L)).isEqualTo(rowId);
    }

    private record IndexSnapshot(RowId rowId, int size) {
    }

    private record KeySnapshot(boolean hasExistingKey, boolean hasMissingKey) {
    }

    private record MultiSnapshot(RowId first, RowId second, RowId third, int size) {
    }
}
