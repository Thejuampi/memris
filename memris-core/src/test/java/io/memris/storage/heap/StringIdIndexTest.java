package io.memris.storage.heap;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StringIdIndexTest {
    @Test
    void putAndGet_returnsRowId() {
        StringIdIndex index = new StringIdIndex(8);
        RowId rowId = new RowId(1, 2);
        index.put("foo", rowId);
        assertThat(index.get("foo")).isEqualTo(rowId);
    }

    @Test
    void getWithGeneration_returnsRowIdAndGeneration() {
        StringIdIndex index = new StringIdIndex(8);
        RowId rowId = new RowId(3, 4);
        index.put("bar", rowId, 42L);
        var rag = index.getWithGeneration("bar");
        assertThat(rag.rowId()).isEqualTo(rowId);
        assertThat(rag.generation()).isEqualTo(42L);
    }

    @Test
    void remove_clearsEntry() {
        StringIdIndex index = new StringIdIndex(8);
        RowId rowId = new RowId(5, 6);
        index.put("baz", rowId);
        index.remove("baz");
        assertThat(index.get("baz")).isNull();
    }

    @Test
    void clear_removesAllEntries() {
        StringIdIndex index = new StringIdIndex(8);
        index.put("a", new RowId(1, 1));
        index.put("b", new RowId(2, 2));
        index.clear();
        assertThat(index.get("a")).isNull();
        assertThat(index.get("b")).isNull();
    }
}