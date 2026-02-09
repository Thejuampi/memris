package io.memris.runtime.dispatch;

import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.runtime.ConditionExecutor;
import io.memris.storage.Selection;

public final class ConditionSelectionOrchestrator {

    private ConditionSelectionOrchestrator() {
    }

    @FunctionalInterface
    public interface CompositeSelector {
        Selection select(CompiledQuery.CompiledCondition[] conditions,
                int start,
                int end,
                Object[] args,
                boolean[] consumed);
    }

    @FunctionalInterface
    public interface ConditionSelector {
        Selection select(CompiledQuery.CompiledCondition[] conditions,
                ConditionExecutor[] executors,
                int index,
                Object[] args);
    }

    @FunctionalInterface
    public interface ScanAllSupplier {
        Selection scanAll();
    }

    public static Selection execute(CompiledQuery.CompiledCondition[] conditions,
            ConditionExecutor[] executors,
            Object[] args,
            CompositeSelector compositeSelector,
            ConditionSelector conditionSelector,
            ScanAllSupplier scanAllSupplier) {
        var combined = (Selection) null;
        for (var groupStart = 0; groupStart < conditions.length;) {
            var groupEnd = groupStart;
            while (groupEnd < conditions.length - 1
                    && conditions[groupEnd].nextCombinator() != LogicalQuery.Combinator.OR) {
                groupEnd++;
            }
            var groupSelection = executeGroup(conditions,
                    executors,
                    groupStart,
                    groupEnd,
                    args,
                    compositeSelector,
                    conditionSelector,
                    scanAllSupplier);
            combined = (combined == null) ? groupSelection : combined.union(groupSelection);
            groupStart = groupEnd + 1;
        }
        return combined;
    }

    private static Selection executeGroup(CompiledQuery.CompiledCondition[] conditions,
            ConditionExecutor[] executors,
            int start,
            int end,
            Object[] args,
            CompositeSelector compositeSelector,
            ConditionSelector conditionSelector,
            ScanAllSupplier scanAllSupplier) {
        var consumed = new boolean[end - start + 1];
        var current = compositeSelector.select(conditions, start, end, args, consumed);
        for (var i = start; i <= end; i++) {
            if (consumed[i - start]) {
                continue;
            }
            var next = conditionSelector.select(conditions, executors, i, args);
            current = current == null ? next : current.intersect(next);
        }
        if (current == null) {
            return scanAllSupplier.scanAll();
        }
        return current;
    }
}
