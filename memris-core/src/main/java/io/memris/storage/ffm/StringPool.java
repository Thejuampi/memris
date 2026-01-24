package io.memris.storage.ffm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * String pool for memory-efficient string storage.
 * Strings are stored once and referenced by integer handles.
 * Updates to string fields create new pool entries (immutable strings).
 */
public final class StringPool {
    private final List<String> strings = new ArrayList<>();
    private final Map<String, Integer> stringToHandle = new HashMap<>();
    private int nextHandle = 1;  // 0 = null, 1+ = actual strings

    /**
     * Get or create a string handle.
     * @param str The string value (can be null)
     * @return Integer handle (0 = null, 1+ = string index)
     */
    public int getHandle(String str) {
        if (str == null) {
            return 0;  // Null marker
        }

        Integer existing = stringToHandle.get(str);
        if (existing != null) {
            return existing;  // Reuse existing handle
        }

        // Add new string to pool
        strings.add(str);
        int handle = nextHandle++;
        stringToHandle.put(str, handle);
        return handle;
    }

    /**
     * Get string value by handle.
     * @param handle Integer handle
     * @return String value or null if handle is 0
     */
    public String getString(int handle) {
        if (handle == 0) {
            return null;
        }
        if (handle < 1 || handle >= nextHandle) {
            throw new IndexOutOfBoundsException("Invalid string handle: " + handle);
        }
        return strings.get(handle - 1);  // Handle 1 maps to index 0
    }

    /**
     * Get the number of strings in the pool.
     */
    public int size() {
        return strings.size();
    }

    /**
     * Clear all strings (for testing).
     */
    public void clear() {
        strings.clear();
        stringToHandle.clear();
        nextHandle = 1;
    }
}
