package io.memris.runtime.dispatch;

import io.memris.index.CompositeKey;
import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeRangeIndex;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.Selection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public final class CompositeIndexProbeCompiler {

    private CompositeIndexProbeCompiler() {
    }

    @FunctionalInterface
    public interface CompositeIndexProbe {
        Selection select(CompiledQuery.CompiledCondition[] conditions,
                int start,
                int end,
                Object[] args,
                boolean[] consumed);
    }

    private static final CompositeIndexProbe NOOP = (conditions, start, end, args, consumed) -> null;

    public static CompositeIndexProbe compile(CompositeIndexSelector.CompositeIndexPlan[] plans,
            Map<String, Object> indexes,
            CompiledQuery.CompiledCondition[] conditions,
            CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder) {
        if (plans == null || plans.length == 0 || indexes == null || conditions == null || conditions.length == 0) {
            return NOOP;
        }

        var groupedPlans = new HashMap<Long, GroupProbe>();
        for (var group = 0; group < conditions.length;) {
            var start = group;
            var end = group;
            while (end < conditions.length - 1 && conditions[end].nextCombinator() != LogicalQuery.Combinator.OR) {
                end++;
            }
            var probe = compileGroup(plans, indexes, conditions, start, end, indexLookup, selectionBuilder);
            if (probe != null) {
                groupedPlans.put(groupKey(start, end), probe);
            }
            group = end + 1;
        }

        if (groupedPlans.isEmpty()) {
            return NOOP;
        }

        return (runtimeConditions, start, end, args, consumed) -> {
            if (runtimeConditions != conditions) {
                return null;
            }
            var probe = groupedPlans.get(groupKey(start, end));
            return probe == null ? null : probe.select(args, consumed);
        };
    }

    private static GroupProbe compileGroup(CompositeIndexSelector.CompositeIndexPlan[] plans,
            Map<String, Object> indexes,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder) {
        var probes = new ArrayList<PlanProbe>();
        for (var plan : plans) {
            if (plan.columnPositions().length < 2) {
                continue;
            }
            var index = indexes.get(plan.indexName());
            if (index == null) {
                continue;
            }

            var compiled = compilePlan(index,
                    plan.columnPositions(),
                    conditions,
                    start,
                    end,
                    indexLookup,
                    selectionBuilder);
            if (compiled != null) {
                probes.add(compiled);
            }
        }

        if (probes.isEmpty()) {
            return null;
        }

        return (args, consumed) -> {
            for (var probe : probes) {
                var selected = probe.select(args, consumed);
                if (selected != null) {
                    return selected;
                }
            }
            return null;
        };
    }

    private static PlanProbe compilePlan(Object index,
            int[] columnPositions,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder) {
        return switch (index) {
            case CompositeHashIndex hashIndex -> compileHashPlan(hashIndex,
                    columnPositions,
                    conditions,
                    start,
                    end,
                    indexLookup,
                    selectionBuilder);
            case CompositeRangeIndex rangeIndex -> compileRangePlan(rangeIndex,
                    columnPositions,
                    conditions,
                    start,
                    end,
                    indexLookup,
                    selectionBuilder);
            default -> null;
        };
    }

    private static PlanProbe compileHashPlan(Object index,
            int[] columnPositions,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder) {
        var argumentIndexes = new int[columnPositions.length];
        var consumedOffsets = new int[columnPositions.length];
        for (var i = 0; i < columnPositions.length; i++) {
            var conditionIndex = findConditionIndex(conditions,
                    start,
                    end,
                    columnPositions[i],
                    LogicalQuery.Operator.EQ);
            if (conditionIndex < 0) {
                return null;
            }
            var condition = conditions[conditionIndex];
            argumentIndexes[i] = condition.argumentIndex();
            consumedOffsets[i] = conditionIndex - start;
        }

        var scratch = ThreadLocal.withInitial(() -> new HashScratch(columnPositions.length));
        return (args, consumed) -> {
            var local = scratch.get();
            for (var i = 0; i < argumentIndexes.length; i++) {
                local.keyParts[i] = args[argumentIndexes[i]];
            }
            var rows = indexLookup.query(index, Predicate.Operator.EQ, CompositeKey.of(local.keyParts));
            if (rows == null) {
                return null;
            }
            for (var offset : consumedOffsets) {
                consumed[offset] = true;
            }
            return selectionBuilder.fromRows(rows);
        };
    }

    private static PlanProbe compileRangePlan(Object index,
            int[] columnPositions,
            CompiledQuery.CompiledCondition[] conditions,
            int start,
            int end,
            CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder) {
        var shape = CompositeRangeProbeBuilder.compileShape(conditions, start, end, columnPositions);
        if (shape == null || shape.prefixArgumentIndexes().length == 0) {
            return null;
        }
        var consumedOffsets = shape.consumedOffsets(start);

        var scratch = ThreadLocal.withInitial(() -> new RangeScratch(columnPositions.length));
        return (args, consumed) -> {
            var local = scratch.get();
            var width = shape.width();
            for (var i = 0; i < width; i++) {
                local.lower[i] = null;
                local.upper[i] = null;
            }

            var prefixArgumentIndexes = shape.prefixArgumentIndexes();
            for (var i = 0; i < prefixArgumentIndexes.length; i++) {
                var value = args[prefixArgumentIndexes[i]];
                local.lower[i] = value;
                local.upper[i] = value;
            }

            var prefix = shape.prefixArgumentIndexes().length;
            if (shape.fullEquality()) {
                return select(indexLookup,
                        selectionBuilder,
                        index,
                        Predicate.Operator.EQ,
                        CompositeKey.of(local.lower),
                        consumed,
                        consumedOffsets);
            }

            var rangeOperator = shape.rangeOperator();
            if (rangeOperator == null) {
                for (var i = prefix; i < width; i++) {
                    local.lower[i] = CompositeKey.minSentinel();
                    local.upper[i] = CompositeKey.maxSentinel();
                }
                local.between[0] = CompositeKey.of(local.lower);
                local.between[1] = CompositeKey.of(local.upper);
                return select(indexLookup,
                        selectionBuilder,
                        index,
                        Predicate.Operator.BETWEEN,
                        local.between,
                        consumed,
                        consumedOffsets);
            }

            for (var i = prefix + 1; i < width; i++) {
                local.lower[i] = CompositeKey.minSentinel();
                local.upper[i] = CompositeKey.maxSentinel();
            }

            var first = args[shape.rangeArgumentIndex()];
            return switch (rangeOperator) {
                case EQ -> {
                    local.lower[prefix] = first;
                    local.upper[prefix] = first;
                    local.between[0] = CompositeKey.of(local.lower);
                    local.between[1] = CompositeKey.of(local.upper);
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.BETWEEN,
                            local.between,
                            consumed,
                            consumedOffsets);
                }
                case GT -> {
                    local.lower[prefix] = first;
                    local.upper[prefix] = CompositeKey.maxSentinel();
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.GT,
                            CompositeKey.of(local.lower),
                            consumed,
                            consumedOffsets);
                }
                case GTE -> {
                    local.lower[prefix] = first;
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.GTE,
                            CompositeKey.of(local.lower),
                            consumed,
                            consumedOffsets);
                }
                case LT -> {
                    local.upper[prefix] = first;
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.LT,
                            CompositeKey.of(local.upper),
                            consumed,
                            consumedOffsets);
                }
                case LTE -> {
                    local.upper[prefix] = first;
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.LTE,
                            CompositeKey.of(local.upper),
                            consumed,
                            consumedOffsets);
                }
                case BETWEEN -> {
                    var second = args[shape.rangeArgumentIndex() + 1];
                    local.lower[prefix] = first;
                    local.upper[prefix] = second;
                    local.between[0] = CompositeKey.of(local.lower);
                    local.between[1] = CompositeKey.of(local.upper);
                    yield select(indexLookup,
                            selectionBuilder,
                            index,
                            Predicate.Operator.BETWEEN,
                            local.between,
                            consumed,
                            consumedOffsets);
                }
                default -> null;
            };
        };
    }

    private static Selection select(CompositeIndexSelector.IndexLookup indexLookup,
            CompositeIndexSelector.SelectionBuilder selectionBuilder,
            Object index,
            Predicate.Operator operator,
            Object value,
            boolean[] consumed,
            int[] consumedOffsets) {
        var rows = indexLookup.query(index, operator, value);
        if (rows == null) {
            return null;
        }
        for (var offset : consumedOffsets) {
            consumed[offset] = true;
        }
        return selectionBuilder.fromRows(rows);
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

    private static long groupKey(int start, int end) {
        return (((long) start) << 32) | (end & 0xffffffffL);
    }

    @FunctionalInterface
    private interface GroupProbe {
        Selection select(Object[] args, boolean[] consumed);
    }

    @FunctionalInterface
    private interface PlanProbe {
        Selection select(Object[] args, boolean[] consumed);
    }

    private static final class HashScratch {
        private final Object[] keyParts;

        private HashScratch(int width) {
            this.keyParts = new Object[width];
        }
    }

    private static final class RangeScratch {
        private final Object[] lower;
        private final Object[] upper;
        private final Object[] between;

        private RangeScratch(int width) {
            this.lower = new Object[width];
            this.upper = new Object[width];
            this.between = new Object[2];
        }
    }
}
