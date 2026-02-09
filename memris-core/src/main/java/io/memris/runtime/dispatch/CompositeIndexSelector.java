package io.memris.runtime.dispatch;

import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;
import java.util.Map;

public final class CompositeIndexSelector {

    private CompositeIndexSelector() {
    }

    @FunctionalInterface
    public interface IndexLookup {
        int[] query(Object index, Predicate.Operator operator, Object value);
    }

    @FunctionalInterface
    public interface SelectionBuilder {
        Selection fromRows(int[] rows);
    }

    public record CompositeIndexPlan(String indexName, int[] columnPositions) {
    }

    public static Selection select(CompositeIndexPlan[] plans,
            Map<String, Object> indexes,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            Object[] args,
            boolean[] consumed,
            IndexLookup indexLookup,
            SelectionBuilder selectionBuilder) {
        if (plans == null || plans.length == 0 || indexes == null) {
            return null;
        }
        for (var plan : plans) {
            if (plan.columnPositions().length < 2) {
                continue;
            }
            var index = indexes.get(plan.indexName());
            if (index == null) {
                continue;
            }

            if (index instanceof CompositeHashIndex) {
                var keyParts = new Object[plan.columnPositions().length];
                var localConsumed = new boolean[consumed.length];
                var match = true;
                for (var i = 0; i < plan.columnPositions().length; i++) {
                    var conditionIndex = findConditionIndex(conditions,
                            start,
                            end,
                            plan.columnPositions()[i],
                            LogicalQuery.Operator.EQ);
                    if (conditionIndex < 0) {
                        match = false;
                        break;
                    }
                    var condition = conditions[conditionIndex];
                    keyParts[i] = args[condition.argumentIndex()];
                    localConsumed[conditionIndex - start] = true;
                }
                if (!match) {
                    continue;
                }
                var rows = indexLookup.query(index, Predicate.Operator.EQ, CompositeKey.of(keyParts));
                if (rows != null) {
                    System.arraycopy(localConsumed, 0, consumed, 0, consumed.length);
                    return selectionBuilder.fromRows(rows);
                }
                continue;
            }

            if (index instanceof CompositeRangeIndex) {
                var probe = CompositeRangeProbeBuilder.build(conditions, start, end, plan.columnPositions(), args);
                if (probe == null) {
                    continue;
                }
                var value = probe.upper() == null
                        ? probe.lower()
                        : new Object[] { probe.lower(), probe.upper() };
                var rows = indexLookup.query(index, probe.operator(), value);
                if (rows != null) {
                    System.arraycopy(probe.consumed(), 0, consumed, 0, consumed.length);
                    return selectionBuilder.fromRows(rows);
                }
            }
        }
        return null;
    }

    private static int findConditionIndex(CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            int columnIndex,
            LogicalQuery.Operator operator) {
        for (var i = start; i <= end; i++) {
            var condition = conditions[i];
            if (condition.columnIndex() == columnIndex && condition.operator() == operator && !condition.ignoreCase()) {
                return i;
            }
        }
        return -1;
    }
}
