package io.memris.runtime;

import io.memris.core.ColumnAccessPlan;

final class PropertyPathAccessor {

    private final ColumnAccessPlan plan;

    private PropertyPathAccessor(ColumnAccessPlan plan) {
        this.plan = plan;
    }

    static PropertyPathAccessor compile(Class<?> rootType, String path) {
        return new PropertyPathAccessor(ColumnAccessPlan.compile(rootType, path));
    }

    Object get(Object root) {
        return plan.get(root);
    }

    void set(Object root, Object value) {
        plan.set(root, value);
    }
}
