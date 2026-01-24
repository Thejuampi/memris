package io.memris.kernel;

public interface Executor {
    RowIdSet execute(PlanNode plan);
}
