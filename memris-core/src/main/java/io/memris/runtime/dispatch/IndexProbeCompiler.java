package io.memris.runtime.dispatch;

import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.index.StringPrefixIndex;
import io.memris.index.StringSuffixIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;

public final class IndexProbeCompiler {

    private IndexProbeCompiler() {
    }

    @FunctionalInterface
    public interface IndexProbe {
        int[] query(Predicate.Operator operator, Object value, java.util.function.Predicate<RowId> validator);
    }

    public static IndexProbe compile(Object index) {
        return switch (index) {
            case HashIndex hashIndex -> (operator, value, validator) ->
                operator == Predicate.Operator.EQ && value != null
                        ? rowIdSetToIntArray(hashIndex.lookup(value, validator))
                        : null;
            case RangeIndex rangeIndex -> (operator, value, validator) -> {
                if (value instanceof Comparable<?> comparable) {
                    @SuppressWarnings("rawtypes")
                    var comp = (Comparable) comparable;
                    return switch (operator) {
                        case EQ -> rowIdSetToIntArray(rangeIndex.lookup(comp, validator));
                        case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(comp, validator));
                        case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(comp, validator));
                        case LT -> rowIdSetToIntArray(rangeIndex.lessThan(comp, validator));
                        case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(comp, validator));
                        default -> null;
                    };
                }
                if (operator == Predicate.Operator.BETWEEN
                        && value instanceof Object[] range
                        && range.length >= 2
                        && range[0] instanceof Comparable<?> lowerComparable
                        && range[1] instanceof Comparable<?> upperComparable) {
                    @SuppressWarnings("rawtypes")
                    var lower = (Comparable) lowerComparable;
                    @SuppressWarnings("rawtypes")
                    var upper = (Comparable) upperComparable;
                    return rowIdSetToIntArray(rangeIndex.between(lower, upper, validator));
                }
                return null;
            };
            case StringPrefixIndex prefixIndex -> (operator, value, validator) ->
                operator == Predicate.Operator.STARTING_WITH && value instanceof String s
                        ? rowIdSetToIntArray(prefixIndex.startsWith(s))
                        : null;
            case StringSuffixIndex suffixIndex -> (operator, value, validator) ->
                operator == Predicate.Operator.ENDING_WITH && value instanceof String s
                        ? rowIdSetToIntArray(suffixIndex.endsWith(s))
                        : null;
            case CompositeHashIndex hashIndex -> (operator, value, validator) ->
                operator == Predicate.Operator.EQ && value instanceof CompositeKey key
                        ? rowIdSetToIntArray(hashIndex.lookup(key, validator))
                        : null;
            case CompositeRangeIndex rangeIndex -> (operator, value, validator) -> {
                if (value instanceof CompositeKey key) {
                    return switch (operator) {
                        case EQ -> rowIdSetToIntArray(rangeIndex.lookup(key, validator));
                        case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(key, validator));
                        case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(key, validator));
                        case LT -> rowIdSetToIntArray(rangeIndex.lessThan(key, validator));
                        case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(key, validator));
                        default -> null;
                    };
                }
                if (operator == Predicate.Operator.BETWEEN
                        && value instanceof Object[] range
                        && range.length >= 2
                        && range[0] instanceof CompositeKey lower
                        && range[1] instanceof CompositeKey upper) {
                    return rowIdSetToIntArray(rangeIndex.between(lower, upper, validator));
                }
                return null;
            };
            default -> (operator, value, validator) -> null;
        };
    }

    private static int[] rowIdSetToIntArray(RowIdSet rowIdSet) {
        if (rowIdSet == null || rowIdSet.size() == 0) {
            return new int[0];
        }
        var longArray = rowIdSet.toLongArray();
        var intArray = new int[longArray.length];
        for (var i = 0; i < longArray.length; i++) {
            intArray[i] = (int) longArray[i];
        }
        return intArray;
    }
}
