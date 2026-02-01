package io.memris.runtime;

@FunctionalInterface
public interface OrderExecutor {
    int[] apply(RepositoryRuntime<?> runtime, int[] rows);
}
