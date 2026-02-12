package io.memris.storage.heap;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.kernel.RowId;
import io.memris.storage.Selection;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * MethodHandle-based implementation of GeneratedTable methods.
 * <p>
 * Uses cached MethodHandles for field access. Good performance (~5ns overhead),
 * easy to maintain, reliable.
 */
@SuppressWarnings("PMD.AvoidReassigningParameters")
public class MethodHandleImplementation implements TableImplementationStrategy {

    private static final ClassValue<GeneratedFieldCache> GENERATED_FIELD_CACHE = new ClassValue<>() {
        @Override
        protected GeneratedFieldCache computeValue(Class<?> type) {
            return new GeneratedFieldCache(resolveAccessibleField(type, "TYPE_CODES"),
                    resolveAccessibleField(type, "idIndex"));
        }
    };

    private static final Method ALLOCATED_COUNT_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "allocatedCount");
    private static final Method IS_TOMBSTONE_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "isTombstone",
            RowId.class);
    private static final Method ALLOCATE_ROW_ID_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "allocateRowId");
    private static final Method ROW_GENERATION_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "rowGeneration",
            int.class);
    private static final Method INCREMENT_ROW_COUNT_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "incrementRowCount");
    private static final Method TOMBSTONE_METHOD = resolveAccessibleMethod(
            AbstractTable.class,
            "tombstone",
            RowId.class,
            long.class);

    private static GeneratedFieldCache getGeneratedFieldCache(Object obj) {
        return GENERATED_FIELD_CACHE.get(obj.getClass());
    }

    private static Field resolveAccessibleField(Class<?> type, String fieldName) {
        try {
            var field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Generated table field missing: " + fieldName + " on " + type.getName(),
                    e);
        }
    }

    private static Method resolveAccessibleMethod(Class<?> type, String methodName,
            Class<?>... params) {
        try {
            var method = type.getDeclaredMethod(methodName, params);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Required method missing: " + methodName + " on " + type.getName(), e);
        }
    }

    private record GeneratedFieldCache(Field typeCodesField, Field idIndexField) {
    }

    /**
     * Wires GeneratedTable methods to MethodDelegation interceptors.
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * final class PersonTable extends AbstractTable implements GeneratedTable {
     *     public long readLong(int columnIndex, int rowIndex) {
     *         return (long) readInterceptor.intercept(columnIndex, rowIndex, this);
     *     }
     *
     *     public int[] scanEqualsLong(int columnIndex, long value) {
     *         return scanEqualsLongInterceptor.intercept(columnIndex, value, this);
     *     }
     * }
     * }</pre>
     */
    @Override
    public DynamicType.Builder<AbstractTable> implementMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields,
            Class<?> idIndexType) {

        // 1. Metadata methods
        builder = addMetadataMethods(builder, columnFields.size());

        // 2. Read methods
        builder = addReadMethods(builder, columnFields);

        // 3. Scan methods
        builder = addScanMethods(builder, columnFields);

        // 4. Row lifecycle methods
        builder = addLifecycleMethods(builder, columnFields);

        // 5. ID index methods
        builder = addIdIndexMethods(builder);

        return builder;
    }

    private static DynamicType.Builder<AbstractTable> addMetadataMethods(
            DynamicType.Builder<AbstractTable> builder, int columnCount) {

        // Equivalent generated Java (simplified):
        // int columnCount() { return <constant>; }
        // byte typeCodeAt(int i) { return TypeCodeInterceptor.intercept(i, this); }
        // long allocatedCount() { return super.allocatedCount(); }
        // int liveCount() { return super.rowCount(); }
        // long currentGeneration() { return super.currentGeneration(); }
        // long rowGeneration(int rowIndex) { return RowGenerationInterceptor.intercept(rowIndex, this); }

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("columnCount"))
                .intercept(FixedValue.value(columnCount));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("typeCodeAt"))
                .intercept(MethodDelegation.to(TypeCodeInterceptor.class));

        try {
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("allocatedCount"))
                    .intercept(net.bytebuddy.implementation.MethodCall.invoke(
                            AbstractTable.class.getDeclaredMethod("allocatedCount")).onSuper());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find allocatedCount method", e);
        }

        try {
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("liveCount"))
                    .intercept(net.bytebuddy.implementation.MethodCall.invoke(
                            AbstractTable.class.getDeclaredMethod("rowCount")).onSuper());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find rowCount method", e);
        }

        try {
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("currentGeneration"))
                    .intercept(net.bytebuddy.implementation.MethodCall.invoke(
                            AbstractTable.class.getDeclaredMethod("currentGeneration")).onSuper());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find currentGeneration method", e);
        }

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("rowGeneration"))
                .intercept(MethodDelegation.to(new RowGenerationInterceptor()));

        return builder;
    }

    private static DynamicType.Builder<AbstractTable> addReadMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields) {

        // Equivalent generated Java (simplified):
        // long readLong(int columnIndex, int rowIndex) {
        //     return (long) readInterceptor.intercept(columnIndex, rowIndex, this);
        // }
        // int readInt(...) and String readString(...) delegate to the same interceptor.

        ReadInterceptor interceptor = new ReadInterceptor(columnFields);

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readLong"))
                .intercept(MethodDelegation.to(interceptor));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readInt"))
                .intercept(MethodDelegation.to(interceptor));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readString"))
                .intercept(MethodDelegation.to(interceptor));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("isPresent"))
                .intercept(MethodDelegation.to(new PresentInterceptor(columnFields)));

        return builder;
    }

    private static DynamicType.Builder<AbstractTable> addScanMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields) {

        // Equivalent generated Java (simplified):
        // int[] scanEqualsLong(int columnIndex, long value) {
        //     return scanEqualsLongInterceptor.intercept(columnIndex, value, this);
        // }
        // ...same pattern for scanBetween*/scanIn*/scanAll with specialized interceptors.

        // Each scan method gets its own specialized interceptor - O(1) dispatch, no
        // runtime switch
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsLong"))
                .intercept(MethodDelegation.to(new ScanEqualsLongInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsInt"))
                .intercept(MethodDelegation.to(new ScanEqualsIntInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsString"))
                .intercept(MethodDelegation.to(new ScanEqualsStringInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsStringIgnoreCase"))
                .intercept(MethodDelegation.to(new ScanEqualsStringIgnoreCaseInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenLong"))
                .intercept(MethodDelegation.to(new ScanBetweenLongInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenInt"))
                .intercept(MethodDelegation.to(new ScanBetweenIntInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInLong"))
                .intercept(MethodDelegation.to(new ScanInLongInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInInt"))
                .intercept(MethodDelegation.to(new ScanInIntInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInString"))
                .intercept(MethodDelegation.to(new ScanInStringInterceptor(columnFields)));

        // scanAll returns all allocated row indices
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanAll"))
                .intercept(MethodDelegation.to(new ScanAllInterceptor()));

        return builder;
    }

    private static DynamicType.Builder<AbstractTable> addLifecycleMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields) {

        // Equivalent generated Java (simplified):
        // long insertFrom(Object[] values) { return insertInterceptor.intercept(values, this); }
        // void tombstone(long ref) { tombstoneInterceptor.intercept(ref, this); }
        // boolean isLive(long ref) { return isLiveInterceptor.intercept(ref, this); }

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("insertFrom"))
                .intercept(MethodDelegation.to(new InsertInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("tombstone")
                .and(net.bytebuddy.matcher.ElementMatchers.takesArguments(long.class)))
                .intercept(MethodDelegation.to(new TombstoneInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("isLive"))
                .intercept(MethodDelegation.to(new IsLiveInterceptor()));

        return builder;
    }

    private static DynamicType.Builder<AbstractTable> addIdIndexMethods(
            DynamicType.Builder<AbstractTable> builder) {

        // Equivalent generated Java (simplified):
        // long lookupById(long id) { return lookupInterceptor.intercept(id, this); }
        // long lookupByIdString(String id) { return lookupByIdStringInterceptor.intercept(id, this); }
        // void removeById(Object id) { removeInterceptor.intercept(id, this); }

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupById"))
                .intercept(MethodDelegation.to(new LookupInterceptor("long")));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupByIdString"))
                .intercept(MethodDelegation.to(new LookupInterceptor("String")));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("removeById"))
                .intercept(MethodDelegation.to(new LookupInterceptor("remove")));

        return builder;
    }

    // ====================================================================================
    // Interceptor Classes
    // ====================================================================================

    public static class TypeCodeInterceptor {
        @RuntimeType
        public static byte intercept(@Argument(0) int columnIndex, @This Object obj) throws Exception {
            byte[] typeCodes = (byte[]) getGeneratedFieldCache(obj).typeCodesField().get(obj);
            if (columnIndex < 0 || columnIndex >= typeCodes.length) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            return typeCodes[columnIndex];
        }
    }

    public static class RowGenerationInterceptor {
        @RuntimeType
        public long intercept(@Argument(0) int rowIndex, @This AbstractTable table) {
            return table.rowGenerationAt(rowIndex);
        }
    }

    public static class ReadInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ReadInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public Object intercept(@Argument(0) int columnIndex,
                @Argument(1) int rowIndex,
                @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }

            try {
                return ((AbstractTable) obj).readWithSeqLock(rowIndex, () -> {
                    try {
                        ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
                        Object column = resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);

                        byte typeCode = fieldInfo.typeCode();
                        if (typeCode == TypeCodes.TYPE_LONG
                                || typeCode == TypeCodes.TYPE_DOUBLE
                                || typeCode == TypeCodes.TYPE_INSTANT
                                || typeCode == TypeCodes.TYPE_LOCAL_DATE
                                || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                                || typeCode == TypeCodes.TYPE_DATE) {
                            return ((PageColumnLong) column).get(rowIndex);
                        } else if (typeCode == TypeCodes.TYPE_INT
                                || typeCode == TypeCodes.TYPE_FLOAT
                                || typeCode == TypeCodes.TYPE_BOOLEAN
                                || typeCode == TypeCodes.TYPE_BYTE
                                || typeCode == TypeCodes.TYPE_SHORT
                                || typeCode == TypeCodes.TYPE_CHAR) {
                            return ((PageColumnInt) column).get(rowIndex);
                        } else if (typeCode == TypeCodes.TYPE_STRING
                                || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                                || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                            return ((PageColumnString) column).get(rowIndex);
                        } else {
                            throw new IllegalStateException("Unknown type code: " + typeCode);
                        }
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() != null) {
                    throw e.getCause();
                }
                throw e;
            }
        }
    }

    public static class PresentInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public PresentInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public boolean intercept(@Argument(0) int columnIndex,
                @Argument(1) int rowIndex,
                @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }

            try {
                return ((AbstractTable) obj).readWithSeqLock(rowIndex, () -> {
                    try {
                        ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
                        Object column = resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);

                        byte typeCode = fieldInfo.typeCode();
                        if (typeCode == TypeCodes.TYPE_LONG
                                || typeCode == TypeCodes.TYPE_DOUBLE
                                || typeCode == TypeCodes.TYPE_INSTANT
                                || typeCode == TypeCodes.TYPE_LOCAL_DATE
                                || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                                || typeCode == TypeCodes.TYPE_DATE) {
                            return ((PageColumnLong) column).isPresent(rowIndex);
                        } else if (typeCode == TypeCodes.TYPE_INT
                                || typeCode == TypeCodes.TYPE_FLOAT
                                || typeCode == TypeCodes.TYPE_BOOLEAN
                                || typeCode == TypeCodes.TYPE_BYTE
                                || typeCode == TypeCodes.TYPE_SHORT
                                || typeCode == TypeCodes.TYPE_CHAR) {
                            return ((PageColumnInt) column).isPresent(rowIndex);
                        } else if (typeCode == TypeCodes.TYPE_STRING
                                || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                                || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                            return ((PageColumnString) column).isPresent(rowIndex);
                        } else {
                            throw new IllegalStateException("Unknown type code: " + typeCode);
                        }
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() != null) {
                    throw e.getCause();
                }
                throw e;
            }
        }
    }

    // Specialized scan interceptors - each handles one scan type with zero runtime
    // dispatch

    public static class ScanEqualsLongInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanEqualsLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) Long value, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnLong column = (PageColumnLong) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanEqualsIntInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanEqualsIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) Integer value, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnInt column = (PageColumnInt) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanEqualsStringInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanEqualsStringInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String value, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnString column = (PageColumnString) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanEqualsStringIgnoreCaseInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanEqualsStringIgnoreCaseInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String value, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnString column = (PageColumnString) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanEqualsIgnoreCase(value, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanBetweenLongInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanBetweenLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) Long lower, @Argument(2) Long upper,
                @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnLong column = (PageColumnLong) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanBetween(lower, upper, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanBetweenIntInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanBetweenIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) Integer lower, @Argument(2) Integer upper,
                @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnInt column = (PageColumnInt) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanBetween(lower, upper, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanInLongInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanInLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) long[] values, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnLong column = (PageColumnLong) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanInIntInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanInIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) int[] values, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnInt column = (PageColumnInt) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanInStringInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public ScanInStringInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String[] values, @This Object obj)
                throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            PageColumnString column = (PageColumnString) resolveColumn(obj, fieldInfo, columnIndex, columnFieldRefs, columnGetters, lookup);
            int limit = (int) (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, obj);
        }

        private int[] filterTombstoned(int[] rows, Object obj) throws Exception {
            if (rows.length == 0)
                return rows;
            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int rowIndex : rows) {
                RowId rowId = new RowId(rowIndex / pageSize, rowIndex % pageSize);
                if (!(boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId)) {
                    filtered[count++] = rowIndex;
                }
            }
            int[] result = new int[count];
            System.arraycopy(filtered, 0, result, 0, count);
            return result;
        }
    }

    public static class ScanAllInterceptor {
        @RuntimeType
        public int[] intercept(@This Object obj) throws Exception {
            long allocated = (long) ALLOCATED_COUNT_METHOD.invoke(obj);

            int[] temp = new int[(int) allocated];
            int count = 0;
            int pageSize = ((AbstractTable) obj).pageSize();
            for (int i = 0; i < allocated; i++) {
                int pageId = i / pageSize;
                int offset = i % pageSize;
                RowId rowId = new RowId(pageId, offset);
                boolean isTombstoned = (boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId);
                if (!isTombstoned) {
                    temp[count++] = i;
                }
            }

            // Trim to actual size
            int[] results = new int[count];
            System.arraycopy(temp, 0, results, 0, count);
            return results;
        }
    }

    public static class InsertInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public InsertInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public long intercept(@Argument(0) Object[] values, @This Object obj) throws Throwable {
            if (values.length != columnFields.size()) {
                throw new IllegalArgumentException(
                        "Value count mismatch: expected " + columnFields.size() + ", got " + values.length);
            }

            // Allocate row
            RowId rowId = (RowId) ALLOCATE_ROW_ID_METHOD.invoke(obj);
            int pageSize = ((AbstractTable) obj).pageSize();
            int rowIndex = (int) (rowId.page() * pageSize + rowId.offset());

            // Get generation for this row
            long generation = (long) ROW_GENERATION_METHOD.invoke(obj, rowIndex);

            AbstractTable table = (AbstractTable) obj;
            table.beginSeqLock(rowIndex);
            try {
                // Write values to columns using MethodHandles
                for (int i = 0; i < columnFields.size(); i++) {
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    Object column = resolveColumn(obj, fieldInfo, i, columnFieldRefs, columnGetters, lookup);
                    Object value = values[i];

                    byte typeCode = fieldInfo.typeCode();
                    if (typeCode == TypeCodes.TYPE_LONG
                            || typeCode == TypeCodes.TYPE_INSTANT
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                            || typeCode == TypeCodes.TYPE_DATE
                            || typeCode == TypeCodes.TYPE_DOUBLE) {
                        PageColumnLong col = (PageColumnLong) column;
                        if (value == null) {
                            col.setNull(rowIndex);
                            continue;
                        }
                        long longValue;
                        if (typeCode == TypeCodes.TYPE_DOUBLE) {
                            if (value instanceof Double) {
                                longValue = FloatEncoding.doubleToSortableLong((Double) value);
                            } else if (value instanceof Number) {
                                longValue = FloatEncoding
                                        .doubleToSortableLong(((Number) value).doubleValue());
                            } else {
                                throw new IllegalArgumentException("Expected Double for column " + i);
                            }
                        } else if (value instanceof Long) {
                            longValue = (Long) value;
                        } else if (value instanceof Integer) {
                            longValue = ((Integer) value).longValue();
                        } else if (value instanceof Number) {
                            longValue = ((Number) value).longValue();
                        } else {
                            throw new IllegalArgumentException("Expected Long for column " + i);
                        }
                        col.set(rowIndex, longValue);
                    } else if (typeCode == TypeCodes.TYPE_INT
                            || typeCode == TypeCodes.TYPE_FLOAT
                            || typeCode == TypeCodes.TYPE_BOOLEAN
                            || typeCode == TypeCodes.TYPE_BYTE
                            || typeCode == TypeCodes.TYPE_SHORT
                            || typeCode == TypeCodes.TYPE_CHAR) {
                        PageColumnInt col = (PageColumnInt) column;
                        if (value == null) {
                            col.setNull(rowIndex);
                            continue;
                        }
                        int intValue;
                        if (typeCode == TypeCodes.TYPE_FLOAT) {
                            if (value instanceof Float) {
                                intValue = FloatEncoding.floatToSortableInt((Float) value);
                            } else if (value instanceof Number) {
                                intValue = FloatEncoding
                                        .floatToSortableInt(((Number) value).floatValue());
                            } else {
                                throw new IllegalArgumentException("Expected Float for column " + i);
                            }
                        } else if (typeCode == TypeCodes.TYPE_BOOLEAN) {
                            if (value instanceof Boolean) {
                                intValue = (Boolean) value ? 1 : 0;
                            } else {
                                throw new IllegalArgumentException("Expected Boolean for column " + i);
                            }
                        } else if (typeCode == TypeCodes.TYPE_CHAR) {
                            if (value instanceof Character) {
                                intValue = (Character) value;
                            } else {
                                throw new IllegalArgumentException("Expected Character for column " + i);
                            }
                        } else if (value instanceof Integer) {
                            intValue = (Integer) value;
                        } else if (value instanceof Long) {
                            intValue = ((Long) value).intValue();
                        } else if (value instanceof Number) {
                            intValue = ((Number) value).intValue();
                        } else {
                            throw new IllegalArgumentException("Expected Integer for column " + i);
                        }
                        col.set(rowIndex, intValue);
                    } else if (typeCode == TypeCodes.TYPE_STRING
                            || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                            || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                        PageColumnString col = (PageColumnString) column;
                        if (value == null) {
                            col.setNull(rowIndex);
                        } else {
                            col.set(rowIndex, value.toString());
                        }
                    } else {
                        throw new IllegalArgumentException("Unsupported type code: " + typeCode);
                    }
                }

                // Publish row to make data visible to scans
                for (int i = 0; i < columnFields.size(); i++) {
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    Object column = resolveColumn(obj, fieldInfo, i, columnFieldRefs, columnGetters, lookup);

                    byte typeCode = fieldInfo.typeCode();
                    if (typeCode == TypeCodes.TYPE_LONG
                            || typeCode == TypeCodes.TYPE_DOUBLE
                            || typeCode == TypeCodes.TYPE_INSTANT
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                            || typeCode == TypeCodes.TYPE_DATE) {
                        PageColumnLong col = (PageColumnLong) column;
                        col.publish(rowIndex + 1);
                    } else if (typeCode == TypeCodes.TYPE_INT
                            || typeCode == TypeCodes.TYPE_FLOAT
                            || typeCode == TypeCodes.TYPE_BOOLEAN
                            || typeCode == TypeCodes.TYPE_BYTE
                            || typeCode == TypeCodes.TYPE_SHORT
                            || typeCode == TypeCodes.TYPE_CHAR) {
                        PageColumnInt col = (PageColumnInt) column;
                        col.publish(rowIndex + 1);
                    } else if (typeCode == TypeCodes.TYPE_STRING
                            || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                            || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                        PageColumnString col = (PageColumnString) column;
                        col.publish(rowIndex + 1);
                    }
                }
            } finally {
                table.endSeqLock(rowIndex);
            }

            // Update ID index
            Object idIndex = getGeneratedFieldCache(obj).idIndexField().get(obj);
            Object idValue = values[0];

            if (idIndex instanceof LongIdIndex longIdIndex && idValue instanceof Number) {
                longIdIndex.put(((Number) idValue).longValue(), rowId, generation);
            } else if (idIndex instanceof StringIdIndex stringIdIndex && idValue instanceof String) {
                stringIdIndex.put((String) idValue, rowId, generation);
            }

            // Increment row count
            INCREMENT_ROW_COUNT_METHOD.invoke(obj);

            return Selection.pack(rowIndex, generation);
        }
    }

    public static class TombstoneInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        private final Field[] columnFieldRefs;
        private final AtomicReferenceArray<MethodHandle> columnGetters;

        public TombstoneInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.columnFieldRefs = new Field[columnFields.size()];
            this.columnGetters = new AtomicReferenceArray<>(columnFields.size());
        }

        @RuntimeType
        public void intercept(@Argument(0) long ref, @This Object obj) throws Throwable {
            int rowIndex = Selection.index(ref);
            long generation = Selection.generation(ref);
            AbstractTable table = (AbstractTable) obj;
            int pageSize = table.pageSize();
            int pageId = rowIndex / pageSize;
            int offset = rowIndex % pageSize;
            RowId rowId = new RowId(pageId, offset);
            table.beginSeqLock(rowIndex);
            Object idValue = null;
            try {
                // Read ID column (first column) for index cleanup
                if (!columnFields.isEmpty()) {
                    ColumnFieldInfo idFieldInfo = columnFields.get(0);
                    Object idColumn = resolveColumn(obj, idFieldInfo, 0, columnFieldRefs, columnGetters, lookup);

                    byte typeCode = idFieldInfo.typeCode();
                    if (typeCode == TypeCodes.TYPE_LONG
                            || typeCode == TypeCodes.TYPE_DOUBLE
                            || typeCode == TypeCodes.TYPE_INSTANT
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                            || typeCode == TypeCodes.TYPE_DATE) {
                        idValue = ((PageColumnLong) idColumn).get(rowIndex);
                    } else if (typeCode == TypeCodes.TYPE_INT
                            || typeCode == TypeCodes.TYPE_FLOAT
                            || typeCode == TypeCodes.TYPE_BOOLEAN
                            || typeCode == TypeCodes.TYPE_BYTE
                            || typeCode == TypeCodes.TYPE_SHORT
                            || typeCode == TypeCodes.TYPE_CHAR) {
                        idValue = ((PageColumnInt) idColumn).get(rowIndex);
                    } else if (typeCode == TypeCodes.TYPE_STRING
                            || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                            || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                        idValue = ((PageColumnString) idColumn).get(rowIndex);
                    }
                }

                // Use generation-validated tombstone
                TOMBSTONE_METHOD.invoke(obj, rowId, generation);
            } finally {
                table.endSeqLock(rowIndex);
            }

            // Remove from ID index after tombstoning (eventual consistency)
            if (idValue != null) {
                Object idIndex = getGeneratedFieldCache(obj).idIndexField().get(obj);

                if (idIndex instanceof LongIdIndex longIdx && idValue instanceof Number) {
                    longIdx.remove(((Number) idValue).longValue());
                } else if (idIndex instanceof StringIdIndex stringIdx && idValue instanceof String) {
                    stringIdx.remove((String) idValue);
                }
            }
        }
    }

    public static class IsLiveInterceptor {
        @RuntimeType
        public boolean intercept(@Argument(0) long ref, @This AbstractTable table) {
            int rowIndex = Selection.index(ref);
            long generation = Selection.generation(ref);
            int pageSize = table.pageSize();
            int pageId = rowIndex / pageSize;
            int offset = rowIndex % pageSize;
            RowId rowId = new RowId(pageId, offset);

            // Check tombstone
            if (table.isTombstone(rowId)) {
                return false;
            }

            // Check generation - stale ref detection (direct field access)
            return table.rowGenerationAt(rowIndex) == generation;
        }
    }

    public static class LookupInterceptor {
        private final String operation;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();

        public LookupInterceptor(String operation) {
            this.operation = operation;
        }

        @RuntimeType
        public Object intercept(@Argument(0) Object key, @This Object obj) throws Throwable {
            Object idIndex = getGeneratedFieldCache(obj).idIndexField().get(obj);

            if ("long".equals(operation)) {
                LongIdIndex idx = (LongIdIndex) idIndex;
                LongIdIndex.RowIdAndGeneration rag = idx.getWithGeneration(((Number) key).longValue());
                if (rag == null)
                    return -1L;

                RowId rowId = rag.rowId();
                long generation = rag.generation();
                int pageSize = ((AbstractTable) obj).pageSize();
                int rowIndex = (int) (rowId.page() * pageSize + rowId.offset());

                // Validate not tombstoned
                boolean isTombstoned = (boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId);
                if (isTombstoned)
                    return -1L;

                // Validate generation matches
                long storedGen = (long) ROW_GENERATION_METHOD.invoke(obj, rowIndex);
                if (storedGen != generation)
                    return -1L;

                return Selection.pack(rowIndex, generation);
            } else if ("String".equals(operation)) {
                StringIdIndex idx = (StringIdIndex) idIndex;
                StringIdIndex.RowIdAndGeneration rag = idx.getWithGeneration((String) key);
                if (rag == null)
                    return -1L;

                RowId rowId = rag.rowId();
                long generation = rag.generation();
                int pageSize = ((AbstractTable) obj).pageSize();
                int rowIndex = (int) (rowId.page() * pageSize + rowId.offset());

                // Validate not tombstoned
                boolean isTombstoned = (boolean) IS_TOMBSTONE_METHOD.invoke(obj, rowId);
                if (isTombstoned)
                    return -1L;

                // Validate generation matches
                long storedGen = (long) ROW_GENERATION_METHOD.invoke(obj, rowIndex);
                if (storedGen != generation)
                    return -1L;

                return Selection.pack(rowIndex, generation);
            } else if ("remove".equals(operation)) {
                if (idIndex instanceof LongIdIndex longIdx) {
                    longIdx.remove(((Number) key).longValue());
                } else if (idIndex instanceof StringIdIndex stringIdx) {
                    stringIdx.remove((String) key);
                }
                return null;
            } else {
                throw new IllegalStateException("Unknown operation: " + operation);
            }
        }
    }

    private static Object resolveColumn(Object obj,
            ColumnFieldInfo fieldInfo,
            int columnIndex,
            Field[] columnFieldRefs,
            AtomicReferenceArray<MethodHandle> columnGetters,
            MethodHandles.Lookup lookup) throws Throwable {
        MethodHandle getter = columnGetters.get(columnIndex);
        if (getter != null) {
            return getter.invoke(obj);
        }
        
        // Lock-free lazy initialization using CAS
        var field = columnFieldRefs[columnIndex];
        if (field == null) {
            field = resolveAccessibleField(obj.getClass(), fieldInfo.fieldName());
            columnFieldRefs[columnIndex] = field;
        }
        
        MethodHandle newGetter = lookup.unreflectGetter(field);
        
        // Try to CAS - if we win, we return our getter; if we lose, use the winner's getter
        if (columnGetters.compareAndSet(columnIndex, null, newGetter)) {
            return newGetter.invoke(obj);
        } else {
            // Another thread won the race, use their getter
            return columnGetters.get(columnIndex).invoke(obj);
        }
    }
}
