package io.memris.runtime.codegen;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Generates row-level residual condition evaluators for compiled query programs.
 */
public final class ConditionRowEvaluatorGenerator {

    private static final ConcurrentHashMap<String, RowConditionEvaluator> CACHE = new ConcurrentHashMap<>();
    private static final AtomicLong CLASS_COUNTER = new AtomicLong();

    private ConditionRowEvaluatorGenerator() {
    }

    @FunctionalInterface
    public interface RowConditionEvaluator {
        boolean matches(GeneratedTable table, int rowIndex, Object[] args);
    }

    public static RowConditionEvaluator generate(CompiledQuery.CompiledCondition condition, boolean primitiveNonNull) {
        if (!isSupported(condition)) {
            return null;
        }
        var key = condition.columnIndex() + ":" + condition.typeCode() + ":" + condition.operator() + ":"
                + condition.argumentIndex() + ":" + condition.ignoreCase() + ":" + primitiveNonNull;
        return CACHE.computeIfAbsent(key, ignored -> doGenerate(condition, primitiveNonNull));
    }

    private static boolean isSupported(CompiledQuery.CompiledCondition condition) {
        var operator = condition.operator();
        return switch (operator) {
            case EQ, NE, IN, NOT_IN, IGNORE_CASE_EQ, GT, GTE, LT, LTE, BETWEEN, IS_NULL, NOT_NULL, BEFORE, AFTER -> true;
            default -> false;
        };
    }

    private static RowConditionEvaluator doGenerate(CompiledQuery.CompiledCondition condition, boolean primitiveNonNull) {
        MatcherDelegate delegate = createDelegate(condition, primitiveNonNull);
        if (delegate == null) {
            return null;
        }
        if (!RuntimeExecutorGenerator.isEnabled()) {
            return delegate::matches;
        }
        try {
            String className = "io.memris.runtime.codegen.RowConditionEvaluator$Gen" + CLASS_COUNTER.incrementAndGet();
            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(RowConditionEvaluator.class)
                    .name(className);
            builder = builder.method(named("matches")).intercept(MethodDelegation.to(delegate));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(ConditionRowEvaluatorGenerator.class.getClassLoader()).getLoaded();
                return (RowConditionEvaluator) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception ignored) {
            return delegate::matches;
        }
    }

    private interface MatcherDelegate {
        boolean matches(GeneratedTable table, int rowIndex, Object[] args);
    }

    private static MatcherDelegate createDelegate(CompiledQuery.CompiledCondition condition, boolean primitiveNonNull) {
        var operator = normalizeOperator(condition.operator());
        int columnIndex = condition.columnIndex();
        int argumentIndex = condition.argumentIndex();
        byte typeCode = condition.typeCode();
        boolean ignoreCase = condition.ignoreCase() || condition.operator() == LogicalQuery.Operator.IGNORE_CASE_EQ;

        if (operator == LogicalQuery.Operator.IS_NULL || operator == LogicalQuery.Operator.NOT_NULL) {
            return new NullMatcher(columnIndex, primitiveNonNull, operator == LogicalQuery.Operator.IS_NULL);
        }

        return switch (operator) {
            case EQ -> createEqMatcher(columnIndex, argumentIndex, typeCode, ignoreCase, primitiveNonNull);
            case NE -> new InverseMatcher(createEqMatcher(columnIndex, argumentIndex, typeCode, ignoreCase, primitiveNonNull));
            case IN -> createInMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
            case NOT_IN -> new InverseMatcher(createInMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull));
            case GT, GTE, LT, LTE -> createRangeMatcher(columnIndex, argumentIndex, typeCode, operator, primitiveNonNull);
            case BETWEEN -> createBetweenMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
            default -> null;
        };
    }

    private static LogicalQuery.Operator normalizeOperator(LogicalQuery.Operator operator) {
        return switch (operator) {
            case IGNORE_CASE_EQ -> LogicalQuery.Operator.EQ;
            case BEFORE -> LogicalQuery.Operator.LT;
            case AFTER -> LogicalQuery.Operator.GT;
            default -> operator;
        };
    }

    private static MatcherDelegate createEqMatcher(int columnIndex,
            int argumentIndex,
            byte typeCode,
            boolean ignoreCase,
            boolean primitiveNonNull) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER -> new EqStringMatcher(columnIndex,
                            argumentIndex,
                            ignoreCase,
                            primitiveNonNull);
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE,
                    TypeCodes.TYPE_DOUBLE -> new EqLongMatcher(columnIndex,
                            argumentIndex,
                            typeCode,
                            primitiveNonNull);
            default -> new EqIntMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
        };
    }

    private static MatcherDelegate createRangeMatcher(int columnIndex,
            int argumentIndex,
            byte typeCode,
            LogicalQuery.Operator operator,
            boolean primitiveNonNull) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE,
                    TypeCodes.TYPE_DOUBLE -> new RangeLongMatcher(columnIndex,
                            argumentIndex,
                            typeCode,
                            operator,
                            primitiveNonNull);
            default -> new RangeIntMatcher(columnIndex, argumentIndex, typeCode, operator, primitiveNonNull);
        };
    }

    private static MatcherDelegate createBetweenMatcher(int columnIndex,
            int argumentIndex,
            byte typeCode,
            boolean primitiveNonNull) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE,
                    TypeCodes.TYPE_DOUBLE -> new BetweenLongMatcher(columnIndex,
                            argumentIndex,
                            typeCode,
                            primitiveNonNull);
            default -> new BetweenIntMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
        };
    }

    private static MatcherDelegate createInMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER -> new InStringMatcher(columnIndex, argumentIndex, primitiveNonNull);
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE,
                    TypeCodes.TYPE_DOUBLE -> new InLongMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
            default -> new InIntMatcher(columnIndex, argumentIndex, typeCode, primitiveNonNull);
        };
    }

    public static final class InverseMatcher implements MatcherDelegate {
        private final MatcherDelegate delegate;

        private InverseMatcher(MatcherDelegate delegate) {
            this.delegate = delegate;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            if (delegate == null) {
                return false;
            }
            return !delegate.matches(table, rowIndex, args);
        }
    }

    public static final class NullMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final boolean primitiveNonNull;
        private final boolean expectNull;

        private NullMatcher(int columnIndex, boolean primitiveNonNull, boolean expectNull) {
            this.columnIndex = columnIndex;
            this.primitiveNonNull = primitiveNonNull;
            this.expectNull = expectNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            if (primitiveNonNull) {
                return !expectNull;
            }
            boolean present = table.isPresent(columnIndex, rowIndex);
            return expectNull ? !present : present;
        }
    }

    public static final class EqStringMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final boolean ignoreCase;
        private final boolean primitiveNonNull;

        private EqStringMatcher(int columnIndex, int argumentIndex, boolean ignoreCase, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.ignoreCase = ignoreCase;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object argument = argAt(args, argumentIndex);
            String expected = argument != null ? argument.toString() : null;
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return expected == null;
            }
            String actual = table.readString(columnIndex, rowIndex);
            if (actual == null || expected == null) {
                return java.util.Objects.equals(actual, expected);
            }
            return ignoreCase ? actual.equalsIgnoreCase(expected) : actual.equals(expected);
        }
    }

    public static final class EqLongMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private EqLongMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object argument = argAt(args, argumentIndex);
            if (argument == null) {
                return !primitiveNonNull && !table.isPresent(columnIndex, rowIndex);
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            long actual = table.readLong(columnIndex, rowIndex);
            long expected = toLong(typeCode, argument);
            return actual == expected;
        }
    }

    public static final class EqIntMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private EqIntMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object argument = argAt(args, argumentIndex);
            if (argument == null) {
                return !primitiveNonNull && !table.isPresent(columnIndex, rowIndex);
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            int actual = table.readInt(columnIndex, rowIndex);
            int expected = toInt(typeCode, argument);
            return actual == expected;
        }
    }

    public static final class RangeLongMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final LogicalQuery.Operator operator;
        private final boolean primitiveNonNull;

        private RangeLongMatcher(int columnIndex,
                int argumentIndex,
                byte typeCode,
                LogicalQuery.Operator operator,
                boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.operator = operator;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object argument = argAt(args, argumentIndex);
            if (argument == null) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            long actual = table.readLong(columnIndex, rowIndex);
            long expected = toLong(typeCode, argument);
            return switch (operator) {
                case GT -> actual > expected;
                case GTE -> actual >= expected;
                case LT -> actual < expected;
                case LTE -> actual <= expected;
                default -> false;
            };
        }
    }

    public static final class RangeIntMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final LogicalQuery.Operator operator;
        private final boolean primitiveNonNull;

        private RangeIntMatcher(int columnIndex,
                int argumentIndex,
                byte typeCode,
                LogicalQuery.Operator operator,
                boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.operator = operator;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object argument = argAt(args, argumentIndex);
            if (argument == null) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            int actual = table.readInt(columnIndex, rowIndex);
            int expected = toInt(typeCode, argument);
            return switch (operator) {
                case GT -> actual > expected;
                case GTE -> actual >= expected;
                case LT -> actual < expected;
                case LTE -> actual <= expected;
                default -> false;
            };
        }
    }

    public static final class BetweenLongMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private BetweenLongMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object lowerArgument = argAt(args, argumentIndex);
            Object upperArgument = argAt(args, argumentIndex + 1);
            if (lowerArgument == null || upperArgument == null) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            long actual = table.readLong(columnIndex, rowIndex);
            long lower = toLong(typeCode, lowerArgument);
            long upper = toLong(typeCode, upperArgument);
            return actual >= lower && actual <= upper;
        }
    }

    public static final class BetweenIntMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private BetweenIntMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            Object lowerArgument = argAt(args, argumentIndex);
            Object upperArgument = argAt(args, argumentIndex + 1);
            if (lowerArgument == null || upperArgument == null) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            int actual = table.readInt(columnIndex, rowIndex);
            int lower = toInt(typeCode, lowerArgument);
            int upper = toInt(typeCode, upperArgument);
            return actual >= lower && actual <= upper;
        }
    }

    public static final class InStringMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final boolean primitiveNonNull;

        private InStringMatcher(int columnIndex, int argumentIndex, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            var values = toStringArray(argAt(args, argumentIndex));
            if (values.length == 0) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            var actual = table.readString(columnIndex, rowIndex);
            for (var value : values) {
                if (value == null ? actual == null : value.equals(actual)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static final class InLongMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private InLongMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            var values = toLongArray(typeCode, argAt(args, argumentIndex));
            if (values.length == 0) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            var actual = table.readLong(columnIndex, rowIndex);
            for (var value : values) {
                if (actual == value) {
                    return true;
                }
            }
            return false;
        }
    }

    public static final class InIntMatcher implements MatcherDelegate {
        private final int columnIndex;
        private final int argumentIndex;
        private final byte typeCode;
        private final boolean primitiveNonNull;

        private InIntMatcher(int columnIndex, int argumentIndex, byte typeCode, boolean primitiveNonNull) {
            this.columnIndex = columnIndex;
            this.argumentIndex = argumentIndex;
            this.typeCode = typeCode;
            this.primitiveNonNull = primitiveNonNull;
        }

        @Override
        @RuntimeType
        public boolean matches(GeneratedTable table, int rowIndex, Object[] args) {
            var values = toIntArray(typeCode, argAt(args, argumentIndex));
            if (values.length == 0) {
                return false;
            }
            if (!primitiveNonNull && !table.isPresent(columnIndex, rowIndex)) {
                return false;
            }
            var actual = table.readInt(columnIndex, rowIndex);
            for (var value : values) {
                if (actual == value) {
                    return true;
                }
            }
            return false;
        }
    }

    private static Object argAt(Object[] args, int index) {
        if (index < 0 || args == null || index >= args.length) {
            return null;
        }
        return args[index];
    }

    private static long toLong(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INSTANT -> ((Instant) value).toEpochMilli();
            case TypeCodes.TYPE_LOCAL_DATE -> ((LocalDate) value).toEpochDay();
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
            case TypeCodes.TYPE_DATE -> ((Date) value).getTime();
            case TypeCodes.TYPE_DOUBLE -> FloatEncoding.doubleToSortableLong(((Number) value).doubleValue());
            default -> ((Number) value).longValue();
        };
    }

    private static int toInt(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_BOOLEAN -> Boolean.TRUE.equals(value) ? 1 : 0;
            case TypeCodes.TYPE_CHAR -> (value instanceof Character c) ? c : value.toString().charAt(0);
            case TypeCodes.TYPE_FLOAT -> FloatEncoding.floatToSortableInt(((Number) value).floatValue());
            default -> ((Number) value).intValue();
        };
    }

    private static long[] toLongArray(byte typeCode, Object value) {
        if (value == null) {
            return new long[0];
        }
        if (value instanceof long[] longs) {
            return longs;
        }
        if (value instanceof int[] ints) {
            var result = new long[ints.length];
            for (var i = 0; i < ints.length; i++) {
                result[i] = ints[i];
            }
            return result;
        }
        if (value instanceof Object[] objects) {
            var result = new long[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = toLong(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var values = new java.util.ArrayList<Long>();
            for (var item : iterable) {
                values.add(toLong(typeCode, item));
            }
            var result = new long[values.size()];
            for (var i = 0; i < values.size(); i++) {
                result[i] = values.get(i);
            }
            return result;
        }
        return new long[] { toLong(typeCode, value) };
    }

    private static int[] toIntArray(byte typeCode, Object value) {
        if (value == null) {
            return new int[0];
        }
        if (value instanceof int[] ints) {
            return ints;
        }
        if (value instanceof Object[] objects) {
            var result = new int[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = toInt(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var values = new java.util.ArrayList<Integer>();
            for (var item : iterable) {
                values.add(toInt(typeCode, item));
            }
            var result = new int[values.size()];
            for (var i = 0; i < values.size(); i++) {
                result[i] = values.get(i);
            }
            return result;
        }
        return new int[] { toInt(typeCode, value) };
    }

    private static String[] toStringArray(Object value) {
        if (value == null) {
            return new String[0];
        }
        if (value instanceof String[] strings) {
            return strings;
        }
        if (value instanceof Object[] objects) {
            var result = new String[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = objects[i] != null ? objects[i].toString() : null;
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var values = new java.util.ArrayList<String>();
            for (var item : iterable) {
                values.add(item != null ? item.toString() : null);
            }
            return values.toArray(String[]::new);
        }
        return new String[] { value.toString() };
    }

    static void clearCache() {
        CACHE.clear();
    }
}
