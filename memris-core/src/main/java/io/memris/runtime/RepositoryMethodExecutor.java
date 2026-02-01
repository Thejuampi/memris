package io.memris.runtime;

@FunctionalInterface
public interface RepositoryMethodExecutor {
    Object execute(RepositoryRuntime<?> runtime, Object[] args);
}
