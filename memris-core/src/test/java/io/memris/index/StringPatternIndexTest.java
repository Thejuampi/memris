package io.memris.index;

import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for StringPrefixIndex and StringSuffixIndex.
 */
class StringPatternIndexTest {

    private StringPrefixIndex prefixIndex;
    private StringSuffixIndex suffixIndex;

    @BeforeEach
    void setUp() {
        prefixIndex = new StringPrefixIndex();
        suffixIndex = new StringSuffixIndex();
    }

    @Test
    void prefixIndex_startsWith_shouldReturnMatchingRows() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        RowId rowId3 = RowId.fromLong(3);
        
        prefixIndex.add("apple", rowId1);
        prefixIndex.add("application", rowId2);
        prefixIndex.add("banana", rowId3);
        
        // When
        RowIdSet result = prefixIndex.startsWith("app");
        
        // Then
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.contains(rowId1)).isTrue();
        assertThat(result.contains(rowId2)).isTrue();
        assertThat(result.contains(rowId3)).isFalse();
    }

    @Test
    void prefixIndex_startsWith_emptyPrefix_shouldReturnEmpty() {
        // Given - empty prefix is not indexed by default, returns empty set
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        
        prefixIndex.add("apple", rowId1);
        prefixIndex.add("banana", rowId2);
        
        // When - empty prefix returns empty set (by design)
        RowIdSet result = prefixIndex.startsWith("");
        
        // Then
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    void prefixIndex_startsWith_noMatch_shouldReturnEmpty() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        prefixIndex.add("apple", rowId1);
        
        // When
        RowIdSet result = prefixIndex.startsWith("z");
        
        // Then
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    void prefixIndex_remove_shouldRemoveRow() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        
        prefixIndex.add("apple", rowId1);
        prefixIndex.add("application", rowId2);
        
        // When
        prefixIndex.remove("apple", rowId1);
        RowIdSet result = prefixIndex.startsWith("app");
        
        // Then
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.contains(rowId2)).isTrue();
    }

    @Test
    void suffixIndex_endsWith_shouldReturnMatchingRows() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        RowId rowId3 = RowId.fromLong(3);
        
        suffixIndex.add("smith", rowId1);
        suffixIndex.add("jones", rowId2);
        suffixIndex.add("brown", rowId3);
        
        // When
        RowIdSet result = suffixIndex.endsWith("th");
        
        // Then
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.contains(rowId1)).isTrue();
        assertThat(result.contains(rowId2)).isFalse();
    }

    @Test
    void suffixIndex_endsWith_multipleMatches_shouldReturnAll() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        RowId rowId3 = RowId.fromLong(3);
        
        suffixIndex.add("smith", rowId1);
        suffixIndex.add("myth", rowId2);
        suffixIndex.add("brown", rowId3);
        
        // When
        RowIdSet result = suffixIndex.endsWith("th");
        
        // Then
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.contains(rowId1)).isTrue();
        assertThat(result.contains(rowId2)).isTrue();
    }

    @Test
    void suffixIndex_remove_shouldRemoveRow() {
        // Given
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        
        suffixIndex.add("smith", rowId1);
        suffixIndex.add("myth", rowId2);
        
        // When
        suffixIndex.remove("smith", rowId1);
        RowIdSet result = suffixIndex.endsWith("th");
        
        // Then
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.contains(rowId2)).isTrue();
    }
}
