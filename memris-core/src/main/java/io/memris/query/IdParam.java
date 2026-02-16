package io.memris.query;

/**
 * Marker type for ID parameters in built-in method matching.
 * <p>
 * This is a planner-only marker used in MethodKey matching to indicate
 * "any ID type" without using the broad Object.class wildcard.
 * <p>
 * At planning time, this allows us to match findById(Long), findById(UUID),
 * findById(String), etc. while avoiding accidental matches for methods that
 * happen to take Object but aren't ID operations.
 * <p>
 * This has zero runtime overhead - it's only used during method introspection
 * at repository creation time (build-time, not hot-path).
 *
 * @see MethodKey
 * @see QueryPlanner
 */
public final class IdParam {

    private IdParam() {
        // Utility class - no instances
    }

    /**
     * Check if a type is a valid ID parameter type.
     * <p>
     * Valid ID types are non-primitive types (anything that can be an entity ID).
     * Primitives are excluded because IDs are always objects.
     *
     * @param type the type to check
     * @return true if the type is a valid ID parameter type
     */
    public static boolean isValidIdType(Class<?> type) {
        // Exclude primitives
        if (type.isPrimitive()) {
            return false;
        }
        // Iterable is typically a batch operation, not a single ID
        if (java.lang.Iterable.class.isAssignableFrom(type)) {
            return false;
        }
        // Arrays are batch operations
        return !type.isArray();
        // Everything else (Long, UUID, String, value objects) is valid
    }
}
