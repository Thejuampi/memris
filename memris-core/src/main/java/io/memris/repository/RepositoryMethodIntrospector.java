package io.memris.repository;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Shared utility for deterministic method extraction and ordering from repository interfaces.
 * <p>
 * This ensures stable queryId assignment across multiple repository creations
 * by always returning methods in the same order (sorted by name and parameter types).
 * <p>
 * <b>CRITICAL:</b> This is the ONLY place where query methods are extracted.
 * Both Scaffolder and Emitter MUST use this exact same extractor to maintain
 * positional binding correctness.
 * <p>
 * <b>Positional binding:</b> The i-th method in the sorted array corresponds
 * to the i-th CompiledQuery in RepositoryPlan.queries().
 * <p>
 * <b>Filtering rules:</b>
 * <ul>
 *   <li>Excludes default methods (interface defaults with implementations)</li>
 *   <li>Excludes bridge methods (generics erasure artifacts)</li>
 *   <li>Excludes synthetic methods (compiler-generated)</li>
 *   <li>Excludes Object methods (toString, hashCode, etc.)</li>
 *   <li>Includes ONLY abstract methods (methods we must implement)</li>
 * </ul>
 */
public final class RepositoryMethodIntrospector {

    private RepositoryMethodIntrospector() {
        // Utility class
    }

    /**
     * Extract all query methods from a repository interface in deterministic order.
     * <p>
     * This is the SINGLE SOURCE OF TRUTH for method extraction.
     * Any changes here must be applied consistently across all callers.
     *
     * @param repositoryInterface the repository interface
     * @return sorted array of abstract methods to implement
     */
    public static Method[] extractQueryMethods(Class<?> repositoryInterface) {
        return Arrays.stream(repositoryInterface.getMethods())
                .filter(RepositoryMethodIntrospector::isQueryMethod)
                .sorted(METHOD_ORDER)
                .toArray(Method[]::new);
    }

    /**
     * Filter to identify query methods we need to implement.
     * <p>
     * This centralizes the filtering logic to ensure consistency across
     * scaffolder and emitter.
     */
    private static boolean isQueryMethod(Method method) {
        // Exclude default methods (they have implementations)
        if (method.isDefault()) {
            return false;
        }

        // Exclude bridge methods (generics erasure artifacts)
        if (method.isBridge()) {
            return false;
        }

        // Exclude synthetic methods (compiler-generated)
        if (method.isSynthetic()) {
            return false;
        }

        // Exclude Object methods (inherited from java.lang.Object)
        if (method.getDeclaringClass() == Object.class) {
            return false;
        }

        // Include ONLY abstract methods (methods we MUST implement)
        // This filters out Spring infrastructure methods that may have default implementations
        return Modifier.isAbstract(method.getModifiers());
    }

    /**
     * Comparator for deterministic method ordering.
     * <p>
     * Sorts by method name first, then by parameter types to ensure
     * consistent ordering across different JVM implementations.
     */
    private static final Comparator<Method> METHOD_ORDER = (m1, m2) -> {
        var nameCompare = m1.getName().compareTo(m2.getName());
        if (nameCompare != 0) {
            return nameCompare;
        }

        // Same name - compare parameter types
        var params1 = m1.getParameterTypes();
        var params2 = m2.getParameterTypes();

        return compareParameterTypes(params1, params2);
    };

    /**
     * Compare parameter types for deterministic ordering.
     */
    private static int compareParameterTypes(Class<?>[] params1, Class<?>[] params2) {
        var lenCompare = Integer.compare(params1.length, params2.length);
        if (lenCompare != 0) {
            return lenCompare;
        }

        for (int i = 0; i < params1.length; i++) {
            var typeCompare = params1[i].getName().compareTo(params2[i].getName());
            if (typeCompare != 0) {
                return typeCompare;
            }
        }

        return 0;
    }

    /**
     * Create a method key for built-in operation matching.
     * <p>
     * This combines method name and parameter types into a single key
     * for exact signature matching (handles overloads correctly).
     */
    public static MethodKey methodKeyOf(Method method) {
        return new MethodKey(method.getName(), List.of(method.getParameterTypes()));
    }

    /**
     * Method key for exact signature matching.
     * <p>
     * Used for built-in operation lookup to correctly handle overloads.
     */
    public record MethodKey(String name, List<Class<?>> parameterTypes) {

        /**
         * Create a MethodKey from a Method.
         */
        public static MethodKey of(Method method) {
            return new MethodKey(method.getName(), List.of(method.getParameterTypes()));
        }

        /**
         * Get the method name.
         */
        public String methodName() {
            return name;
        }

        /**
         * Check if this key matches the given method.
         * <p>
         * Uses isAssignableFrom() to handle subtype relationships.
         * This allows findById(Long.class) to match a key defined with Object.class.
         * <p>
         * Special handling for IdParam marker: matches any valid ID type
         * (non-primitive, non-Iterable, non-array).
         */
        public boolean matches(Method method) {
            if (!name.equals(method.getName())) {
                return false;
            }

            var paramTypes = method.getParameterTypes();
            if (parameterTypes.size() != paramTypes.length) {
                return false;
            }

            for (int i = 0; i < paramTypes.length; i++) {
                var expected = parameterTypes.get(i);
                var actual = paramTypes[i];

                // Special handling for IdParam marker
                if (expected == io.memris.query.IdParam.class) {
                    if (!io.memris.query.IdParam.isValidIdType(actual)) {
                        return false;
                    }
                    // Valid ID type matches - continue to next parameter
                    continue;
                }

                // Regular isAssignableFrom matching
                if (!expected.isAssignableFrom(actual)) {
                    return false;
                }
            }

            return true;
        }
    }
}
