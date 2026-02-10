package io.memris.runtime.dispatch;

import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
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
        var probe = CompositeIndexProbeCompiler.compile(plans, indexes, conditions, indexLookup, selectionBuilder);
        return probe.select(conditions, start, end, args, consumed);
    }
}
