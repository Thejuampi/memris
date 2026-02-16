package io.memris.query;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Method key for exact signature matching in query planning.
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

            if (expected == IdParam.class) {
                if (!IdParam.isValidIdType(actual)) {
                    return false;
                }
                continue;
            }

            if (!expected.isAssignableFrom(actual)) {
                return false;
            }
        }

        return true;
    }
}
