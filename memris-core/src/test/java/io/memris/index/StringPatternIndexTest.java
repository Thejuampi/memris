package io.memris.index;

import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.kernel.RowIdSetFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void prefixIndex_notStartsWith_shouldReturnComplement() {
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        RowId rowId3 = RowId.fromLong(3);

        prefixIndex.add("apple", rowId1);
        prefixIndex.add("banana", rowId2);
        prefixIndex.add("apricot", rowId3);

        RowIdSet result = prefixIndex.notStartsWith("ap", new int[] { 1, 2, 3 });

        assertThat(result.contains(rowId1)).isFalse();
        assertThat(result.contains(rowId3)).isFalse();
        assertThat(result.contains(rowId2)).isTrue();
    }

    @Test
    void suffixIndex_notEndsWith_shouldUseCustomConstructorAndComplement() {
        StringSuffixIndex custom = new StringSuffixIndex(true, new RowIdSetFactory(2));
        RowId rowId1 = RowId.fromLong(1);
        RowId rowId2 = RowId.fromLong(2);
        RowId rowId3 = RowId.fromLong(3);

        custom.add("Smith", rowId1);
        custom.add("Jones", rowId2);
        custom.add("MYTH", rowId3);

        RowIdSet result = custom.notEndsWith("th", new int[] { 1, 2, 3 });
        assertThat(result.contains(rowId2)).isTrue();
        assertThat(result.contains(rowId1)).isFalse();
        assertThat(result.contains(rowId3)).isFalse();
    }

    @Test
    void suffixIndex_add_requiresNonNullKey() {
        assertThatThrownBy(() -> suffixIndex.add(null, RowId.fromLong(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key required");
    }
}
