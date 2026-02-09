package io.memris.runtime.dispatch;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.runtime.HeapRuntimeKernel;

@FunctionalInterface
public interface DirectConditionExecutor {

    Selection execute(GeneratedTable table, HeapRuntimeKernel kernel, Object[] args);
}
