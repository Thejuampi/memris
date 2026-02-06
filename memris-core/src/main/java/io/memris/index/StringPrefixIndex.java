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
 * Optimized prefix index using HashMap for O(1) prefix lookups.
 * 
 * <p>This is simpler and faster than a trie for STARTING_WITH queries.
 * It maintains a map from each possible prefix length to the set of row IDs.
 * 
 * <p>Performance: O(k) to generate all prefixes + O(1) lookup per prefix length.
 */
public final class StringPrefixIndex {
    
    // Map from prefix to set of row IDs
    private final ConcurrentHashMap<String, MutableRowIdSet> prefixMap;
    private final RowIdSetFactory setFactory;
    private final boolean ignoreCase;
    
    public StringPrefixIndex() {
        this(false);
    }
    
    public StringPrefixIndex(boolean ignoreCase) {
        this(ignoreCase, RowIdSetFactory.defaultFactory());
    }
    
    public StringPrefixIndex(boolean ignoreCase, RowIdSetFactory setFactory) {
        this.prefixMap = new ConcurrentHashMap<>();
        this.ignoreCase = ignoreCase;
        this.setFactory = Objects.requireNonNull(setFactory, "setFactory");
    }
    
    public void add(String key, RowId rowId) {
        if (key == null || rowId == null) {
            return;
        }
        
        String normalizedKey = normalize(key);
        
        // Add rowId for every prefix of the key
        for (int i = 1; i <= normalizedKey.length(); i++) {
            var prefix = normalizedKey.substring(0, i);
            var set = prefixMap.computeIfAbsent(prefix, ignored -> setFactory.create(4));
            set.add(rowId);
            var upgraded = setFactory.maybeUpgrade(set);
            if (upgraded != set) {
                prefixMap.replace(prefix, set, upgraded);
            }
        }
    }
    
    public void remove(String key, RowId rowId) {
        if (key == null || rowId == null) {
            return;
        }
        
        String normalizedKey = normalize(key);
        
        // Remove rowId from every prefix of the key
        for (int i = 1; i <= normalizedKey.length(); i++) {
            var prefix = normalizedKey.substring(0, i);
            var set = prefixMap.get(prefix);
            if (set == null) {
                continue;
            }
            set.remove(rowId);
            if (set.size() == 0) {
                prefixMap.remove(prefix, set);
            }
        }
    }
    
    public RowIdSet startsWith(String prefix) {
        if (prefix == null) {
            return RowIdSets.empty();
        }
        
        String normalizedPrefix = normalize(prefix);
        MutableRowIdSet result = prefixMap.get(normalizedPrefix);
        
        return result == null ? RowIdSets.empty() : result;
    }
    
    public RowIdSet notStartsWith(String prefix, int[] allRowIds) {
        RowIdSet startsWith = startsWith(prefix);
        
        MutableRowIdSet allSet = setFactory.create(allRowIds.length);
        for (int rowId : allRowIds) {
            allSet.add(RowId.fromLong(rowId));
        }
        
        // Subtract matching rows
        MutableRowIdSet result = setFactory.create(allSet.size());
        io.memris.kernel.LongEnumerator e = allSet.enumerator();
        while (e.hasNext()) {
            RowId rowId = RowId.fromLong(e.nextLong());
            if (!startsWith.contains(rowId)) {
                result.add(rowId);
            }
        }
        return result;
    }
    
    public int size() {
        return prefixMap.size();
    }
    
    public void clear() {
        prefixMap.clear();
    }
    
    private String normalize(String key) {
        return ignoreCase ? key.toLowerCase(Locale.ROOT) : key;
    }
}
