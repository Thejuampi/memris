package io.memris.runtime;

@FunctionalInterface
public interface ProjectionExecutor {
    Object materialize(RepositoryRuntime<?> runtime, int rowIndex);
}
