package io.memris.storage.heap;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for LongIdIndex.
 * Long key to RowId index for O(1) lookups.
 */
class LongIdIndexTest {

    @Test
    void newIndexHasZeroSize() {
        LongIdIndex index = new LongIdIndex(16);
        assertEquals(0, index.size());
    }

    @Test
    void putAndGet() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);

        assertEquals(rowId, index.get(42L));
    }

    @Test
    void getReturnsNullForMissingKey() {
        LongIdIndex index = new LongIdIndex(16);

        assertNull(index.get(999L));
    }

    @Test
    void putUpdatesExistingKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId1 = new RowId(0, 5);
        RowId rowId2 = new RowId(1, 10);

        index.put(42L, rowId1, 1L);
        index.put(42L, rowId2, 2L); // Update

        assertEquals(rowId2, index.get(42L));
        assertEquals(1, index.size());
    }

    @Test
    void sizeTracksNumberOfKeys() {
        LongIdIndex index = new LongIdIndex(16);

        assertEquals(0, index.size());

        index.put(1L, new RowId(0, 0), 1L);
        assertEquals(1, index.size());

        index.put(2L, new RowId(0, 1), 1L);
        assertEquals(2, index.size());

        // Same key doesn't increase size
        index.put(1L, new RowId(1, 0), 2L);
        assertEquals(2, index.size());
    }

    @Test
    void removeDeletesKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);
        assertEquals(1, index.size());

        index.remove(42L);
        assertEquals(0, index.size());
        assertNull(index.get(42L));
    }

    @Test
    void removeMissingKeyDoesNothing() {
        LongIdIndex index = new LongIdIndex(16);

        index.remove(999L); // Should not throw
        assertEquals(0, index.size());
    }

    @Test
    void hasKeyReturnsTrueForExistingKey() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(42L, rowId, 1L);

        assertTrue(index.hasKey(42L));
        assertFalse(index.hasKey(999L));
    }

    @Test
    void multipleKeysCanBeStored() {
        LongIdIndex index = new LongIdIndex(16);

        index.put(1L, new RowId(0, 0), 1L);
        index.put(2L, new RowId(0, 1), 1L);
        index.put(3L, new RowId(0, 2), 1L);

        assertEquals(new RowId(0, 0), index.get(1L));
        assertEquals(new RowId(0, 1), index.get(2L));
        assertEquals(new RowId(0, 2), index.get(3L));
        assertEquals(3, index.size());
    }

    @Test
    void handlesNegativeLongs() {
        LongIdIndex index = new LongIdIndex(16);
        RowId rowId = new RowId(0, 5);

        index.put(-1L, rowId, 1L);

        assertEquals(rowId, index.get(-1L));
    }
}
