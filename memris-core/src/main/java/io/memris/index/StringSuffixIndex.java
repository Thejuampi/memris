package io.memris.index;

import io.memris.kernel.MutableRowIdSet;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.kernel.RowIdSetFactory;
import io.memris.kernel.RowIdSets;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reverse string index for efficient ENDING_WITH queries.
 * 
 * <p>This index stores strings in reverse order, enabling O(k) lookup time for suffix queries
 * by treating them as prefix queries on the reversed strings. This provides significantly
 * faster ENDING_WITH performance compared to O(n) full table scans.
 * 
 * <p>Example usage:
 * <pre>
 * // Index creation
 * StringSuffixIndex index = new StringSuffixIndex();
 * 
 * // Add entries
 * index.add("smith", rowId1);
 * index.add("jones", rowId2);
 * index.add("brown", rowId3);
 * 
 * // Query - O(k) lookup (k = suffix length)
 * RowIdSet results = index.endsWith("th");  // Returns rowId1 (smith ends with "th")
 * </pre>
 * 
 * <p><strong>Performance characteristics:</strong>
 * <ul>
 *   <li>endsWith(): O(k) where k = suffix length, plus O(m) to collect m matches</li>
 *   <li>add(): O(k) where k = string length</li>
 *   <li>remove(): O(k) where k = string length</li>
 *   <li>Memory: O(total characters in all indexed strings)</li>
 * </ul>
 * 
 * <p><strong>Implementation notes:</strong>
 * <ul>
 *   <li>Stores strings in reverse order internally</li>
 *   <li>Uses a trie (prefix tree) for efficient prefix matching on reversed strings</li>
 *   <li>Case sensitivity is configurable at construction time</li>
 *   <li>Thread-safe using ConcurrentHashMap</li>
 * </ul>
 * 
 * @since 1.0
 * @author Memris Team
 * @see io.memris.runtime.handler.StringTypeHandler
 * @see io.memris.core.Index.IndexType#SUFFIX
 */
public final class StringSuffixIndex {
    
    private final StringPrefixIndex reversedIndex;
    
    /**
     * Creates a suffix index with default settings (case-sensitive).
     */
    public StringSuffixIndex() {
        this(false);
    }
    
    /**
     * Creates a suffix index with specified case sensitivity.
     * 
     * @param ignoreCase if true, all lookups are case-insensitive
     */
    public StringSuffixIndex(boolean ignoreCase) {
        this.reversedIndex = new StringPrefixIndex(ignoreCase);
    }
    
    /**
     * Creates a suffix index with specified settings.
     * 
     * @param ignoreCase if true, all lookups are case-insensitive
     * @param setFactory factory for creating RowIdSet instances
     */
    public StringSuffixIndex(boolean ignoreCase, RowIdSetFactory setFactory) {
        this.reversedIndex = new StringPrefixIndex(ignoreCase, setFactory);
    }
    
    /**
     * Adds a string and its associated row ID to the index.
     * 
     * <p>The string is stored in reverse order internally to enable efficient
     * suffix queries.
     * 
     * @param key the string to index (must not be null)
     * @param rowId the row ID associated with this string
     * @throws IllegalArgumentException if key is null
     */
    public void add(String key, RowId rowId) {
        if (key == null) {
            throw new IllegalArgumentException("key required");
        }
        String reversed = new StringBuilder(key).reverse().toString();
        reversedIndex.add(reversed, rowId);
    }
    
    /**
     * Removes a row ID from the index for the given key.
     * 
     * @param key the string whose row ID should be removed
     * @param rowId the row ID to remove
     */
    public void remove(String key, RowId rowId) {
        if (key == null || rowId == null) {
            return;
        }
        String reversed = new StringBuilder(key).reverse().toString();
        reversedIndex.remove(reversed, rowId);
    }
    
    /**
     * Finds all row IDs for strings that end with the given suffix.
     * 
     * <p>This is the primary query method for ENDING_WITH operations.
     * Time complexity: O(k + m) where k = suffix length, m = number of matches.
     * 
     * @param suffix the suffix to search for (must not be null)
     * @return a RowIdSet containing all matching row IDs
     */
    public RowIdSet endsWith(String suffix) {
        if (suffix == null) {
            return RowIdSets.empty();
        }
        String reversed = new StringBuilder(suffix).reverse().toString();
        return reversedIndex.startsWith(reversed);
    }
    
    /**
     * Finds all row IDs for strings that do NOT end with the given suffix.
     * 
     * @param suffix the suffix to exclude
     * @param allRowIds all row IDs in the table (for subtraction)
     * @return a RowIdSet containing all non-matching row IDs
     */
    public RowIdSet notEndsWith(String suffix, int[] allRowIds) {
        return reversedIndex.notStartsWith(
            new StringBuilder(suffix).reverse().toString(), 
            allRowIds
        );
    }
    
    /**
     * Returns the number of unique strings in the index.
     */
    public int size() {
        return reversedIndex.size();
    }
    
    /**
     * Clears all entries from the index.
     */
    public void clear() {
        reversedIndex.clear();
    }
}
