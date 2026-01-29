package io.memris.spring.runtime;

import io.memris.spring.MemrisRepositoryFactory;

import java.lang.reflect.Method;

/**
 * Hot-path query execution engine.
 * <p>
 * RepositoryRuntime owns the RepositoryPlan (built ONCE at repository creation)
 * and provides typed entrypoints for query execution.
 *
 * @param <T> the entity type
 */
public final class RepositoryRuntime<T> {

    private final RepositoryPlan<T> plan;
    private final MemrisRepositoryFactory factory;

    /**
     * Create a RepositoryRuntime from a RepositoryPlan.
     *
     * @param plan  the compiled repository plan
     * @param factory the repository factory (for index queries)
     */
    public RepositoryRuntime(RepositoryPlan<T> plan, MemrisRepositoryFactory factory) {
        if (plan == null) {
            throw new IllegalArgumentException("plan required");
        }
        this.plan = plan;
        this.factory = factory;
    }

    /**
     * Execute a method call - placeholder for now.
     */
    public Object executeMethod(Method method, Object[] args) {
        // TODO: Implement method execution based on compiled queries
        throw new UnsupportedOperationException("Method execution not yet implemented: " + method.getName());
    }

    /**
     * Get the repository plan.
     */
    public RepositoryPlan<T> plan() {
        return plan;
    }
}
