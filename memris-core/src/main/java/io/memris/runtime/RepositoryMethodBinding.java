package io.memris.runtime;

import io.memris.query.CompiledQuery;

/**
 * Pre-bound metadata for a repository method.
 */
public final class RepositoryMethodBinding {

    private final CompiledQuery query;
    private final int[] parameterIndices;
    private final Object[] boundValues;
    private final int slotCount;
    private final ArgResolver resolver;

    private static final Object[] EMPTY_ARGS = new Object[0];

    @FunctionalInterface
    private interface ArgResolver {
        Object[] resolve(Object[] args);
    }

    private RepositoryMethodBinding(CompiledQuery query, int[] parameterIndices, Object[] boundValues, int slotCount) {
        this.query = query;
        this.parameterIndices = parameterIndices;
        this.boundValues = boundValues;
        this.slotCount = slotCount;
        this.resolver = buildResolver(parameterIndices, boundValues, slotCount);
    }

    public static RepositoryMethodBinding of(CompiledQuery query) {
        int[] paramIndices = query.parameterIndices();
        Object[] bound = query.boundValues();
        int slots = 0;
        if (paramIndices != null) {
            slots = Math.max(slots, paramIndices.length);
        }
        if (bound != null) {
            slots = Math.max(slots, bound.length);
        }
        return new RepositoryMethodBinding(query, paramIndices, bound, slots);
    }

    public static RepositoryMethodBinding[] fromQueries(CompiledQuery[] queries) {
        RepositoryMethodBinding[] bindings = new RepositoryMethodBinding[queries.length];
        for (int i = 0; i < queries.length; i++) {
            bindings[i] = of(queries[i]);
        }
        return bindings;
    }

    public CompiledQuery query() {
        return query;
    }

    public Object[] resolveArgs(Object[] args) {
        return resolver.resolve(args);
    }

    private static ArgResolver buildResolver(int[] parameterIndices, Object[] boundValues, int slotCount) {
        if (slotCount == 0) {
            return args -> args != null ? args : EMPTY_ARGS;
        }
        boolean hasParams = parameterIndices != null && parameterIndices.length > 0;
        boolean hasBounds = boundValues != null && boundValues.length > 0;
        if (!hasParams) {
            return args -> {
                Object[] resolved = new Object[slotCount];
                if (hasBounds) {
                    int copyLen = Math.min(boundValues.length, slotCount);
                    System.arraycopy(boundValues, 0, resolved, 0, copyLen);
                }
                return resolved;
            };
        }
        return args -> {
            Object[] resolved = new Object[slotCount];
            for (int i = 0; i < slotCount; i++) {
                int methodIndex = (parameterIndices != null && i < parameterIndices.length) ? parameterIndices[i] : -1;
                if (methodIndex >= 0) {
                    resolved[i] = args[methodIndex];
                    continue;
                }
                if (boundValues != null && i < boundValues.length) {
                    resolved[i] = boundValues[i];
                }
            }
            return resolved;
        };
    }
}
