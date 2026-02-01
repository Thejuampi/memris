package io.memris.runtime;

import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;

public final class ConditionExecutor {
    @FunctionalInterface
    public interface Selector {
        Selection execute(RepositoryRuntime<?> runtime, Object[] args);
    }

    private final LogicalQuery.Combinator nextCombinator;
    private final Selector selector;

    public ConditionExecutor(LogicalQuery.Combinator nextCombinator, Selector selector) {
        this.nextCombinator = nextCombinator;
        this.selector = selector;
    }

    public Selection execute(RepositoryRuntime<?> runtime, Object[] args) {
        return selector.execute(runtime, args);
    }

    public LogicalQuery.Combinator nextCombinator() {
        return nextCombinator;
    }
}
