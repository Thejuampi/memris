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

    private RepositoryMethodBinding(CompiledQuery query, int[] parameterIndices, Object[] boundValues, int slotCount) {
        this.query = query;
        this.parameterIndices = parameterIndices;
        this.boundValues = boundValues;
        this.slotCount = slotCount;
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
        if (slotCount == 0) {
            return args != null ? args : new Object[0];
        }
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
    }
}
