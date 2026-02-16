package io.memris.runtime.codegen;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.runtime.InArgumentDecoder;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import io.memris.core.MemrisConfiguration;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Generates specialized runtime executors using ByteBuddy.
 * <p>
 * This generator creates type-specialized implementations at build time,
 * eliminating runtime type switches on hot paths. Generated classes are
 * cached globally for reuse across repositories.
 * <p>
 * Key executor types generated:
 * <ul>
 * <li>{@code FieldValueReader} - reads column values with type-specific
 * logic</li>
 * <li>{@code FkReader} - reads foreign key values</li>
 * <li>{@code TargetRowResolver} - resolves target rows for projections</li>
 * <li>{@code GroupingValueReader} - reads grouping keys for aggregations</li>
 * <li>{@code BetweenExecutor} - executes BETWEEN predicates</li>
 * <li>{@code InListExecutor} - executes IN-list predicates</li>
 * </ul>
 * <p>
 * Feature toggle: Set system property {@code memris.codegen.enabled=false}
 * to disable code generation and use runtime branching instead.
 */
public final class RuntimeExecutorGenerator {

    private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();
    private final AtomicLong classCounter = new AtomicLong(0);

    /**
     * System property to disable code generation (deprecated, use
     * MemrisConfiguration)
     */
    public static final String CODEGEN_ENABLED_PROPERTY = "memris.codegen.enabled";

    private final MemrisConfiguration configuration;

    public RuntimeExecutorGenerator() {
        this(null);
    }

    public RuntimeExecutorGenerator(MemrisConfiguration config) {
        configuration = config;
    }

    /**
     * Check if code generation is enabled.
     * Uses MemrisConfiguration if set, otherwise falls back to system property.
     * Defaults to true; set -Dmemris.codegen.enabled=false to disable via property,
     * or use MemrisConfiguration.builder().codegenEnabled(false).build()
     * programmatically.
     */
    public boolean isEnabled() {
        if (configuration != null) {
            return configuration.codegenEnabled();
        }
        return !"false".equalsIgnoreCase(System.getProperty(CODEGEN_ENABLED_PROPERTY, "true"));
    }

    // ========================================================================
    // FieldValueReader Generation
    // ========================================================================

    /**
     * Functional interface for reading field values from a table.
     */
    @FunctionalInterface
    public interface FieldValueReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    /**
     * Generate a specialized FieldValueReader for the given column and type.
     */
    public FieldValueReader generateFieldValueReader(int columnIndex, byte typeCode,
            TypeConverter<?, ?> converter) {
        String converterKey = converter != null ? "_C" + System.identityHashCode(converter) : "";
        String key = "FVR_" + columnIndex + "_" + typeCode + converterKey;

        return (FieldValueReader) cache.computeIfAbsent(key,
                k -> doGenerateFieldValueReader(columnIndex, typeCode, converter));
    }

    private FieldValueReader doGenerateFieldValueReader(int columnIndex, byte typeCode,
            TypeConverter<?, ?> converter) {
        // Equivalent generated Java (simplified):
        // final class FieldValueReader$GenN implements FieldValueReader {
        // public Object read(GeneratedTable table, int rowIndex) {
        // return interceptor.read(table, rowIndex);
        // }
        // }
        String className = "io.memris.runtime.codegen.FieldValueReader$Gen" + classCounter.incrementAndGet();

        try {
            // Select the appropriate interceptor based on type code
            Object interceptor = createFieldValueReaderInterceptor(columnIndex, typeCode, converter);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(FieldValueReader.class)
                    .name(className);

            builder = builder.method(named("read"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (FieldValueReader) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate FieldValueReader for column " + columnIndex, e);
        }
    }

    private static Object createFieldValueReaderInterceptor(int columnIndex, byte typeCode,
            TypeConverter<?, ?> converter) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                new StringFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_LONG ->
                new LongFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE, TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                new LongFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_INT ->
                new IntFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_BOOLEAN ->
                new BooleanFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_BYTE ->
                new ByteFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_SHORT ->
                new ShortFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_CHAR ->
                new CharFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_FLOAT ->
                new FloatFieldReader(columnIndex, converter);
            case TypeCodes.TYPE_DOUBLE ->
                new DoubleFieldReader(columnIndex, converter);
            default ->
                new IntFieldReader(columnIndex, converter);
        };
    }

    // Type-specific interceptor classes for FieldValueReader

    public static class StringFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public StringFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = table.readString(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class LongFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public LongFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = table.readLong(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class IntFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public IntFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = table.readInt(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class BooleanFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public BooleanFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = table.readInt(columnIndex, rowIndex) != 0;
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class ByteFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public ByteFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = (byte) table.readInt(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class ShortFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public ShortFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = (short) table.readInt(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class CharFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public CharFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = (char) table.readInt(columnIndex, rowIndex);
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class FloatFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public FloatFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, rowIndex));
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    public static class DoubleFieldReader {
        private final int columnIndex;
        private final TypeConverter<?, ?> converter;

        public DoubleFieldReader(int columnIndex, TypeConverter<?, ?> converter) {
            this.columnIndex = columnIndex;
            this.converter = converter;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            Object value = FloatEncoding.sortableLongToDouble(table.readLong(columnIndex, rowIndex));
            return converter != null ? applyConverter(converter, value) : value;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object applyConverter(TypeConverter<?, ?> converter, Object value) {
        return ((TypeConverter<Object, Object>) converter).fromStorage(value);
    }

    // ========================================================================
    // FkReader Generation
    // ========================================================================

    /**
     * Functional interface for reading foreign key values from a table.
     */
    @FunctionalInterface
    public interface FkReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    /**
     * Generate a specialized FkReader for the given column and type.
     */
    public FkReader generateFkReader(int columnIndex, byte typeCode) {
        String key = "FK_" + columnIndex + "_" + typeCode;
        return (FkReader) cache.computeIfAbsent(key, k -> doGenerateFkReader(columnIndex, typeCode));
    }

    private FkReader doGenerateFkReader(int columnIndex, byte typeCode) {
        // Equivalent generated Java (simplified):
        // final class FkReader$GenN implements FkReader {
        // public Object read(GeneratedTable table, int rowIndex) {
        // return interceptor.read(table, rowIndex);
        // }
        // }
        String className = "io.memris.runtime.codegen.FkReader$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createFkReaderInterceptor(columnIndex, typeCode);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(FkReader.class)
                    .name(className);

            builder = builder.method(named("read"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (FkReader) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate FkReader for column " + columnIndex, e);
        }
    }

    private static Object createFkReaderInterceptor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING -> new StringFkReader(columnIndex);
            case TypeCodes.TYPE_LONG -> new LongFkReader(columnIndex);
            case TypeCodes.TYPE_INT -> new IntFkReader(columnIndex);
            default -> new LongFkReader(columnIndex);
        };
    }

    // Type-specific interceptor classes for FkReader

    public static class StringFkReader {
        private final int columnIndex;

        public StringFkReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            return table.readString(columnIndex, rowIndex);
        }
    }

    public static class LongFkReader {
        private final int columnIndex;

        public LongFkReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            return table.readLong(columnIndex, rowIndex);
        }
    }

    public static class IntFkReader {
        private final int columnIndex;

        public IntFkReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            if (!table.isPresent(columnIndex, rowIndex)) {
                return null;
            }
            return table.readInt(columnIndex, rowIndex);
        }
    }

    // ========================================================================
    // TargetRowResolver Generation
    // ========================================================================

    /**
     * Functional interface for resolving target row indices.
     */
    @FunctionalInterface
    public interface TargetRowResolver {
        int resolve(GeneratedTable table, Object fkValue);
    }

    /**
     * Generate a specialized TargetRowResolver.
     */
    public TargetRowResolver generateTargetRowResolver(
            boolean targetColumnIsId, byte fkTypeCode, int targetColumnIndex) {
        String key = "TRR_" + targetColumnIsId + "_" + fkTypeCode + "_" + targetColumnIndex;
        return (TargetRowResolver) cache.computeIfAbsent(key,
                k -> doGenerateTargetRowResolver(targetColumnIsId, fkTypeCode, targetColumnIndex));
    }

    private TargetRowResolver doGenerateTargetRowResolver(
            boolean targetColumnIsId, byte fkTypeCode, int targetColumnIndex) {
        // Equivalent generated Java (simplified):
        // final class TargetRowResolver$GenN implements TargetRowResolver {
        // public int resolve(GeneratedTable table, Object fkValue) {
        // return interceptor.resolve(table, fkValue);
        // }
        // }
        String className = "io.memris.runtime.codegen.TargetRowResolver$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createTargetRowResolverInterceptor(targetColumnIsId, fkTypeCode, targetColumnIndex);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(TargetRowResolver.class)
                    .name(className);

            builder = builder.method(named("resolve"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (TargetRowResolver) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate TargetRowResolver", e);
        }
    }

    private static Object createTargetRowResolverInterceptor(
            boolean targetColumnIsId, byte fkTypeCode, int targetColumnIndex) {
        if (targetColumnIsId) {
            return switch (fkTypeCode) {
                case TypeCodes.TYPE_STRING -> new StringIdResolver();
                case TypeCodes.TYPE_LONG -> new LongIdResolver();
                case TypeCodes.TYPE_INT -> new IntIdResolver();
                default -> new LongIdResolver();
            };
        } else {
            return switch (fkTypeCode) {
                case TypeCodes.TYPE_STRING -> new StringColumnResolver(targetColumnIndex);
                case TypeCodes.TYPE_LONG -> new LongColumnResolver(targetColumnIndex);
                case TypeCodes.TYPE_INT -> new IntColumnResolver(targetColumnIndex);
                default -> new LongColumnResolver(targetColumnIndex);
            };
        }
    }

    // Type-specific interceptor classes for TargetRowResolver (ID-based)

    public static class StringIdResolver {
        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            long ref = table.lookupByIdString((String) fkValue);
            return ref >= 0 ? (int) (ref & 0xFFFFFFFFL) : -1;
        }
    }

    public static class LongIdResolver {
        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            long ref = table.lookupById(((Number) fkValue).longValue());
            return ref >= 0 ? (int) (ref & 0xFFFFFFFFL) : -1;
        }
    }

    public static class IntIdResolver {
        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            long ref = table.lookupById(((Number) fkValue).intValue());
            return ref >= 0 ? (int) (ref & 0xFFFFFFFFL) : -1;
        }
    }

    // Type-specific interceptor classes for TargetRowResolver (column-based)

    public static class StringColumnResolver {
        private final int targetColumnIndex;

        public StringColumnResolver(int targetColumnIndex) {
            this.targetColumnIndex = targetColumnIndex;
        }

        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            int[] matches = table.scanEqualsString(targetColumnIndex, (String) fkValue);
            return matches.length > 0 ? matches[0] : -1;
        }
    }

    public static class LongColumnResolver {
        private final int targetColumnIndex;

        public LongColumnResolver(int targetColumnIndex) {
            this.targetColumnIndex = targetColumnIndex;
        }

        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            int[] matches = table.scanEqualsLong(targetColumnIndex, ((Number) fkValue).longValue());
            return matches.length > 0 ? matches[0] : -1;
        }
    }

    public static class IntColumnResolver {
        private final int targetColumnIndex;

        public IntColumnResolver(int targetColumnIndex) {
            this.targetColumnIndex = targetColumnIndex;
        }

        @RuntimeType
        public int resolve(GeneratedTable table, Object fkValue) {
            if (fkValue == null)
                return -1;
            int[] matches = table.scanEqualsInt(targetColumnIndex, ((Number) fkValue).intValue());
            return matches.length > 0 ? matches[0] : -1;
        }
    }

    // ========================================================================
    // GroupingValueReader Generation
    // ========================================================================

    /**
     * Functional interface for reading grouping key values from a table.
     * Similar to FieldValueReader but without converter support.
     */
    @FunctionalInterface
    public interface GroupingValueReader {
        Object read(GeneratedTable table, int rowIndex);
    }

    /**
     * Generate a specialized GroupingValueReader for the given column and type.
     */
    public GroupingValueReader generateGroupingValueReader(int columnIndex, byte typeCode) {
        if (!isEnabled()) {
            return createFallbackGroupingValueReader(columnIndex, typeCode);
        }

        String key = "GVR_" + columnIndex + "_" + typeCode;
        return (GroupingValueReader) cache.computeIfAbsent(key,
                k -> doGenerateGroupingValueReader(columnIndex, typeCode));
    }

    private GroupingValueReader doGenerateGroupingValueReader(int columnIndex, byte typeCode) {
        // Equivalent generated Java (simplified):
        // final class GroupingValueReader$GenN implements GroupingValueReader {
        // public Object read(GeneratedTable table, int rowIndex) {
        // return interceptor.read(table, rowIndex);
        // }
        // }
        String className = "io.memris.runtime.codegen.GroupingValueReader$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createGroupingValueReaderInterceptor(columnIndex, typeCode);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(GroupingValueReader.class)
                    .name(className);

            builder = builder.method(named("read"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (GroupingValueReader) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            return createFallbackGroupingValueReader(columnIndex, typeCode);
        }
    }

    private static Object createGroupingValueReaderInterceptor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                new StringGroupingReader(columnIndex);
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE ->
                new LongGroupingReader(columnIndex);
            case TypeCodes.TYPE_INT ->
                new IntGroupingReader(columnIndex);
            case TypeCodes.TYPE_BOOLEAN ->
                new BooleanGroupingReader(columnIndex);
            case TypeCodes.TYPE_BYTE ->
                new ByteGroupingReader(columnIndex);
            case TypeCodes.TYPE_SHORT ->
                new ShortGroupingReader(columnIndex);
            case TypeCodes.TYPE_CHAR ->
                new CharGroupingReader(columnIndex);
            case TypeCodes.TYPE_FLOAT ->
                new FloatGroupingReader(columnIndex);
            case TypeCodes.TYPE_DOUBLE ->
                new DoubleGroupingReader(columnIndex);
            default ->
                new IntGroupingReader(columnIndex);
        };
    }

    // Grouping reader interceptor classes

    public static class StringGroupingReader {
        private final int columnIndex;

        public StringGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? table.readString(columnIndex, rowIndex) : null;
        }
    }

    public static class LongGroupingReader {
        private final int columnIndex;

        public LongGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? table.readLong(columnIndex, rowIndex) : null;
        }
    }

    public static class IntGroupingReader {
        private final int columnIndex;

        public IntGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? table.readInt(columnIndex, rowIndex) : null;
        }
    }

    public static class BooleanGroupingReader {
        private final int columnIndex;

        public BooleanGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? table.readInt(columnIndex, rowIndex) != 0 : null;
        }
    }

    public static class ByteGroupingReader {
        private final int columnIndex;

        public ByteGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? (byte) table.readInt(columnIndex, rowIndex) : null;
        }
    }

    public static class ShortGroupingReader {
        private final int columnIndex;

        public ShortGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? (short) table.readInt(columnIndex, rowIndex) : null;
        }
    }

    public static class CharGroupingReader {
        private final int columnIndex;

        public CharGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex) ? (char) table.readInt(columnIndex, rowIndex) : null;
        }
    }

    public static class FloatGroupingReader {
        private final int columnIndex;

        public FloatGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex)
                    ? FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, rowIndex))
                    : null;
        }
    }

    public static class DoubleGroupingReader {
        private final int columnIndex;

        public DoubleGroupingReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.isPresent(columnIndex, rowIndex)
                    ? FloatEncoding.sortableLongToDouble(table.readLong(columnIndex, rowIndex))
                    : null;
        }
    }

    private static GroupingValueReader createFallbackGroupingValueReader(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex) ? table.readString(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex) ? table.readLong(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_INT ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex) ? table.readInt(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_BOOLEAN ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex) ? table.readInt(columnIndex, rowIndex) != 0
                        : null;
            case TypeCodes.TYPE_BYTE ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex)
                        ? (byte) table.readInt(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_SHORT ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex)
                        ? (short) table.readInt(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_CHAR ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex)
                        ? (char) table.readInt(columnIndex, rowIndex)
                        : null;
            case TypeCodes.TYPE_FLOAT ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex)
                        ? FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, rowIndex))
                        : null;
            case TypeCodes.TYPE_DOUBLE ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex)
                        ? FloatEncoding.sortableLongToDouble(table.readLong(columnIndex, rowIndex))
                        : null;
            default ->
                (table, rowIndex) -> table.isPresent(columnIndex, rowIndex) ? table.readInt(columnIndex, rowIndex)
                        : null;
        };
    }

    // ========================================================================
    // BetweenExecutor Generation
    // ========================================================================

    @FunctionalInterface
    public interface BetweenExecutor {
        Selection execute(GeneratedTable table, int argIndex, Object[] args);
    }

    public BetweenExecutor generateBetweenExecutor(int columnIndex, byte typeCode) {
        if (!isEnabled()) {
            return createFallbackBetweenExecutor(columnIndex, typeCode);
        }

        String key = "BETWEEN_" + columnIndex + "_" + typeCode;
        return (BetweenExecutor) cache.computeIfAbsent(key,
                k -> doGenerateBetweenExecutor(columnIndex, typeCode));
    }

    private BetweenExecutor doGenerateBetweenExecutor(int columnIndex, byte typeCode) {
        // Equivalent generated Java (simplified):
        // final class BetweenExecutor$GenN implements BetweenExecutor {
        // public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
        // return interceptor.execute(table, argIndex, args);
        // }
        // }
        String className = "io.memris.runtime.codegen.BetweenExecutor$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createBetweenExecutorInterceptor(columnIndex, typeCode);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(BetweenExecutor.class)
                    .name(className);

            builder = builder.method(named("execute"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (BetweenExecutor) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            return createFallbackBetweenExecutor(columnIndex, typeCode);
        }
    }

    private static Object createBetweenExecutorInterceptor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> new LongBetweenExecutor(columnIndex);
            case TypeCodes.TYPE_INT, TypeCodes.TYPE_BYTE, TypeCodes.TYPE_SHORT -> new IntBetweenExecutor(columnIndex);
            case TypeCodes.TYPE_CHAR -> new CharBetweenExecutor(columnIndex);
            case TypeCodes.TYPE_FLOAT -> new FloatBetweenExecutor(columnIndex);
            case TypeCodes.TYPE_DOUBLE -> new DoubleBetweenExecutor(columnIndex);
            case TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE, TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                new TemporalBetweenExecutor(columnIndex, typeCode);
            default -> new UnsupportedBetweenExecutor(typeCode);
        };
    }

    public static class LongBetweenExecutor {
        private final int columnIndex;

        public LongBetweenExecutor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            long min = ((Number) args[argIndex]).longValue();
            long max = ((Number) args[argIndex + 1]).longValue();
            return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
        }
    }

    public static class IntBetweenExecutor {
        private final int columnIndex;

        public IntBetweenExecutor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            int min = ((Number) args[argIndex]).intValue();
            int max = ((Number) args[argIndex + 1]).intValue();
            return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
        }
    }

    public static class CharBetweenExecutor {
        private final int columnIndex;

        public CharBetweenExecutor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            Object minObj = args[argIndex];
            Object maxObj = args[argIndex + 1];
            int min = (minObj instanceof Character c) ? c : minObj.toString().charAt(0);
            int max = (maxObj instanceof Character c) ? c : maxObj.toString().charAt(0);
            return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
        }
    }

    public static class FloatBetweenExecutor {
        private final int columnIndex;

        public FloatBetweenExecutor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            int min = FloatEncoding.floatToSortableInt(((Number) args[argIndex]).floatValue());
            int max = FloatEncoding.floatToSortableInt(((Number) args[argIndex + 1]).floatValue());
            return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
        }
    }

    public static class DoubleBetweenExecutor {
        private final int columnIndex;

        public DoubleBetweenExecutor(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            long min = FloatEncoding.doubleToSortableLong(((Number) args[argIndex]).doubleValue());
            long max = FloatEncoding.doubleToSortableLong(((Number) args[argIndex + 1]).doubleValue());
            return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
        }
    }

    public static class TemporalBetweenExecutor {
        private final int columnIndex;
        private final byte typeCode;

        public TemporalBetweenExecutor(int columnIndex, byte typeCode) {
            this.columnIndex = columnIndex;
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            if (argIndex + 1 >= args.length) {
                throw new IllegalArgumentException("BETWEEN requires two arguments");
            }
            long min = convertToEpochLong(typeCode, args[argIndex]);
            long max = convertToEpochLong(typeCode, args[argIndex + 1]);
            return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
        }
    }

    public static class UnsupportedBetweenExecutor {
        private final byte typeCode;

        public UnsupportedBetweenExecutor(byte typeCode) {
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, int argIndex, Object[] args) {
            throw new UnsupportedOperationException("BETWEEN not supported for type code: " + typeCode);
        }
    }

    private static BetweenExecutor createFallbackBetweenExecutor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    long min = ((Number) args[argIndex]).longValue();
                    long max = ((Number) args[argIndex + 1]).longValue();
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
                };
            case TypeCodes.TYPE_INT, TypeCodes.TYPE_BYTE, TypeCodes.TYPE_SHORT ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    int min = ((Number) args[argIndex]).intValue();
                    int max = ((Number) args[argIndex + 1]).intValue();
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
                };
            case TypeCodes.TYPE_CHAR ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    Object minObj = args[argIndex];
                    Object maxObj = args[argIndex + 1];
                    int min = (minObj instanceof Character c) ? c : minObj.toString().charAt(0);
                    int max = (maxObj instanceof Character c) ? c : maxObj.toString().charAt(0);
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
                };
            case TypeCodes.TYPE_FLOAT ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    int min = FloatEncoding.floatToSortableInt(((Number) args[argIndex]).floatValue());
                    int max = FloatEncoding.floatToSortableInt(((Number) args[argIndex + 1]).floatValue());
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenInt(columnIndex, min, max));
                };
            case TypeCodes.TYPE_DOUBLE ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    long min = FloatEncoding.doubleToSortableLong(((Number) args[argIndex]).doubleValue());
                    long max = FloatEncoding.doubleToSortableLong(((Number) args[argIndex + 1]).doubleValue());
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
                };
            case TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE, TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                (table, argIndex, args) -> {
                    if (argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires two arguments");
                    }
                    long min = convertToEpochLong(typeCode, args[argIndex]);
                    long max = convertToEpochLong(typeCode, args[argIndex + 1]);
                    return SelectionImpl.fromScanIndices(table, table.scanBetweenLong(columnIndex, min, max));
                };
            default ->
                (table, argIndex, args) -> {
                    throw new UnsupportedOperationException("BETWEEN not supported for type code: " + typeCode);
                };
        };
    }

    // ========================================================================
    // InListExecutor Generation
    // ========================================================================

    @FunctionalInterface
    public interface InListExecutor {
        Selection execute(GeneratedTable table, Object value);
    }

    public InListExecutor generateInListExecutor(int columnIndex, byte typeCode) {
        if (!isEnabled()) {
            return createFallbackInListExecutor(columnIndex, typeCode);
        }

        String key = "IN_" + columnIndex + "_" + typeCode;
        return (InListExecutor) cache.computeIfAbsent(key,
                k -> doGenerateInListExecutor(columnIndex, typeCode));
    }

    private InListExecutor doGenerateInListExecutor(int columnIndex, byte typeCode) {
        // Equivalent generated Java (simplified):
        // final class InListExecutor$GenN implements InListExecutor {
        // public Selection execute(GeneratedTable table, Object value) {
        // return interceptor.execute(table, value);
        // }
        // }
        String className = "io.memris.runtime.codegen.InListExecutor$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createInListExecutorInterceptor(columnIndex, typeCode);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(InListExecutor.class)
                    .name(className);

            builder = builder.method(named("execute"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (InListExecutor) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            return createFallbackInListExecutor(columnIndex, typeCode);
        }
    }

    private static Object createInListExecutorInterceptor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                new StringInListExecutor(columnIndex, typeCode);
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE, TypeCodes.TYPE_DOUBLE ->
                new LongInListExecutor(columnIndex, typeCode);
            case TypeCodes.TYPE_INT, TypeCodes.TYPE_BOOLEAN, TypeCodes.TYPE_BYTE, TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR, TypeCodes.TYPE_FLOAT ->
                new IntInListExecutor(columnIndex, typeCode);
            default -> new UnsupportedInListExecutor(typeCode);
        };
    }

    public static class StringInListExecutor {
        private final int columnIndex;
        private final byte typeCode;

        public StringInListExecutor(int columnIndex, byte typeCode) {
            this.columnIndex = columnIndex;
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, Object value) {
            if (value == null) {
                return SelectionImpl.EMPTY;
            }
            return SelectionImpl.fromScanIndices(table, table.scanInString(columnIndex, toStringArray(typeCode, value)));
        }
    }

    public static class LongInListExecutor {
        private final int columnIndex;
        private final byte typeCode;

        public LongInListExecutor(int columnIndex, byte typeCode) {
            this.columnIndex = columnIndex;
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, Object value) {
            if (value == null) {
                return SelectionImpl.EMPTY;
            }
            return SelectionImpl.fromScanIndices(table, table.scanInLong(columnIndex, toLongArray(typeCode, value)));
        }
    }

    public static class IntInListExecutor {
        private final int columnIndex;
        private final byte typeCode;

        public IntInListExecutor(int columnIndex, byte typeCode) {
            this.columnIndex = columnIndex;
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, Object value) {
            if (value == null) {
                return SelectionImpl.EMPTY;
            }
            return SelectionImpl.fromScanIndices(table, table.scanInInt(columnIndex, toIntArray(typeCode, value)));
        }
    }

    public static class UnsupportedInListExecutor {
        private final byte typeCode;

        public UnsupportedInListExecutor(byte typeCode) {
            this.typeCode = typeCode;
        }

        @RuntimeType
        public Selection execute(GeneratedTable table, Object value) {
            throw new UnsupportedOperationException("IN not supported for type code: " + typeCode);
        }
    }

    private static InListExecutor createFallbackInListExecutor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                (table, value) -> {
                    if (value == null) {
                        return SelectionImpl.EMPTY;
                    }
                    return SelectionImpl.fromScanIndices(table,
                            table.scanInString(columnIndex, toStringArray(typeCode, value)));
                };
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE, TypeCodes.TYPE_DOUBLE ->
                (table, value) -> {
                    if (value == null) {
                        return SelectionImpl.EMPTY;
                    }
                    return SelectionImpl.fromScanIndices(table,
                            table.scanInLong(columnIndex, toLongArray(typeCode, value)));
                };
            case TypeCodes.TYPE_INT, TypeCodes.TYPE_BOOLEAN, TypeCodes.TYPE_BYTE, TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR, TypeCodes.TYPE_FLOAT ->
                (table, value) -> {
                    if (value == null) {
                        return SelectionImpl.EMPTY;
                    }
                    return SelectionImpl.fromScanIndices(table,
                            table.scanInInt(columnIndex, toIntArray(typeCode, value)));
                };
            default ->
                (table, value) -> {
                    throw new UnsupportedOperationException("IN not supported for type code: " + typeCode);
                };
        };
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private static long[] toLongArray(byte typeCode, Object value) {
        return InArgumentDecoder.toLongArrayStrict(typeCode, value);
    }

    private static int[] toIntArray(byte typeCode, Object value) {
        return InArgumentDecoder.toIntArrayStrict(typeCode, value);
    }

    private static String[] toStringArray(byte typeCode, Object value) {
        return InArgumentDecoder.toStringArrayStrict(typeCode, value);
    }

    private static String[] toStringArray(Object value) {
        return InArgumentDecoder.toStringArrayStrict(TypeCodes.TYPE_STRING, value);
    }

    private static long convertToLong(byte typeCode, Object value) {
        if (value == null) {
            return 0L;
        }
        return InArgumentDecoder.toLongStorageValue(typeCode, value);
    }

    private static int convertToInt(byte typeCode, Object value) {
        if (value == null) {
            return 0;
        }
        return InArgumentDecoder.toIntStorageValue(typeCode, value);
    }

    private static long convertToEpochLong(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INSTANT -> ((Instant) value).toEpochMilli();
            case TypeCodes.TYPE_LOCAL_DATE -> ((LocalDate) value).toEpochDay();
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
            case TypeCodes.TYPE_DATE -> ((Date) value).getTime();
            default -> ((Number) value).longValue();
        };
    }

    // ========================================================================
    // IdLookup Generation
    // ========================================================================

    // ========================================================================
    // StorageValueReader Generation
    // ========================================================================

    /**
     * Generate a specialized StorageValueReader for the given column and type.
     * Eliminates runtime branching on type codes when reading raw storage values.
     */
    public io.memris.runtime.StorageValueReader generateStorageValueReader(int columnIndex, byte typeCode) {
        String key = "SVR_" + columnIndex + "_" + typeCode;
        return (io.memris.runtime.StorageValueReader) cache.computeIfAbsent(key,
                k -> doGenerateStorageValueReader(columnIndex, typeCode));
    }

    private io.memris.runtime.StorageValueReader doGenerateStorageValueReader(int columnIndex, byte typeCode) {
        String className = "io.memris.runtime.codegen.StorageValueReader$Gen" + classCounter.incrementAndGet();

        try {
            Object interceptor = createStorageValueReaderInterceptor(columnIndex, typeCode);

            DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(Object.class)
                    .implement(io.memris.runtime.StorageValueReader.class)
                    .name(className);

            builder = builder.method(named("read"))
                    .intercept(MethodDelegation.to(interceptor));

            try (DynamicType.Unloaded<?> unloaded = builder.make()) {
                Class<?> implClass = unloaded.load(RuntimeExecutorGenerator.class.getClassLoader()).getLoaded();
                return (io.memris.runtime.StorageValueReader) implClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate StorageValueReader for column " + columnIndex, e);
        }
    }

    private static Object createStorageValueReaderInterceptor(int columnIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING, TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER ->
                new StringStorageReader(columnIndex);
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE, TypeCodes.TYPE_DOUBLE ->
                new LongStorageReader(columnIndex);
            default ->
                new IntStorageReader(columnIndex);
        };
    }

    public static class StringStorageReader {
        private final int columnIndex;

        public StringStorageReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.readString(columnIndex, rowIndex);
        }
    }

    public static class LongStorageReader {
        private final int columnIndex;

        public LongStorageReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.readLong(columnIndex, rowIndex);
        }
    }

    public static class IntStorageReader {
        private final int columnIndex;

        public IntStorageReader(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @RuntimeType
        public Object read(GeneratedTable table, int rowIndex) {
            return table.readInt(columnIndex, rowIndex);
        }
    }

    /**
     * Clear the executor cache. Primarily for testing.
     */
    public void clearCache() {
        cache.clear();
    }
}

