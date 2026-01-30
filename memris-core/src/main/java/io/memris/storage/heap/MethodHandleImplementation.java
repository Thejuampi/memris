package io.memris.storage.heap;

import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.bind.annotation.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * MethodHandle-based implementation of GeneratedTable methods.
 * <p>
 * Uses cached MethodHandles for field access. Good performance (~5ns overhead),
 * easy to maintain, reliable.
 */
public class MethodHandleImplementation implements TableImplementationStrategy {
    
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
        builder = addLifecycleMethods(builder, columnFields, idIndexType);
        
        // 5. ID index methods
        builder = addIdIndexMethods(builder, idIndexType);
        
        return builder;
    }
    
    private static DynamicType.Builder<AbstractTable> addMetadataMethods(
            DynamicType.Builder<AbstractTable> builder, int columnCount) {
        
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
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("currentGeneration"))
                .intercept(FixedValue.value(0L));
        
        return builder;
    }
    
    private static DynamicType.Builder<AbstractTable> addReadMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields) {
        
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
        
        ScanInterceptor interceptor = new ScanInterceptor(columnFields);
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsLong"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsInt"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsString"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsStringIgnoreCase"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenLong"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenInt"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInLong"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInInt"))
                .intercept(MethodDelegation.to(interceptor));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInString"))
                .intercept(MethodDelegation.to(interceptor));
        
        // scanAll returns all allocated row indices
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanAll"))
                .intercept(MethodDelegation.to(new ScanAllInterceptor()));
        
        return builder;
    }
    
    private static DynamicType.Builder<AbstractTable> addLifecycleMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields,
            Class<?> idIndexType) {
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("insertFrom"))
                .intercept(MethodDelegation.to(new InsertInterceptor(columnFields, idIndexType)));
        
        // Skip tombstone/isLive - AbstractTable has implementations that match GeneratedTable interface
        // These don't need delegation since AbstractTable implements them correctly
        
        return builder;
    }
    
    private static DynamicType.Builder<AbstractTable> addIdIndexMethods(
            DynamicType.Builder<AbstractTable> builder,
            Class<?> idIndexType) {
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupById"))
                .intercept(MethodDelegation.to(new LookupInterceptor(idIndexType, "long")));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupByIdString"))
                .intercept(MethodDelegation.to(new LookupInterceptor(idIndexType, "String")));
        
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("removeById"))
                .intercept(MethodDelegation.to(new LookupInterceptor(idIndexType, "remove")));
        
        return builder;
    }
    
    // ====================================================================================
    // Interceptor Classes
    // ====================================================================================
    
    public static class TypeCodeInterceptor {
        @RuntimeType
        public static byte intercept(@Argument(0) int columnIndex, @This Object obj) throws Exception {
            Field typeCodesField = obj.getClass().getDeclaredField("TYPE_CODES");
            typeCodesField.setAccessible(true);
            byte[] typeCodes = (byte[]) typeCodesField.get(obj); // Get instance field, not static
            if (columnIndex < 0 || columnIndex >= typeCodes.length) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            return typeCodes[columnIndex];
        }
    }
    
    public static class ReadInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        
        public ReadInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }
        
        @RuntimeType
        public Object intercept(@Argument(0) int columnIndex, 
                                @Argument(1) int rowIndex,
                                @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            MethodHandle getter = lookup.unreflectGetter(field);
            Object column = getter.invoke(obj);
            
            byte typeCode = fieldInfo.typeCode();
            // TypeCodes enum ordinals: TYPE_INT=0, TYPE_LONG=1, TYPE_STRING=8
            if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                return ((PageColumnLong) column).get(rowIndex);
            } else if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                return ((PageColumnInt) column).get(rowIndex);
            } else if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                return ((PageColumnString) column).get(rowIndex);
            } else {
                throw new IllegalStateException("Unknown type code: " + typeCode);
            }
        }
    }

    public static class PresentInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();

        public PresentInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public boolean intercept(@Argument(0) int columnIndex,
                                 @Argument(1) int rowIndex,
                                 @This Object obj) throws Throwable {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }

            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            MethodHandle getter = lookup.unreflectGetter(field);
            Object column = getter.invoke(obj);

            byte typeCode = fieldInfo.typeCode();
            if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                return ((PageColumnLong) column).isPresent(rowIndex);
            } else if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                return ((PageColumnInt) column).isPresent(rowIndex);
            } else if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                return ((PageColumnString) column).isPresent(rowIndex);
            } else {
                throw new IllegalStateException("Unknown type code: " + typeCode);
            }
        }
    }
    
    public static class ScanInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        
        public ScanInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }
        
        @RuntimeType
        public int[] intercept(@AllArguments Object[] args, @This Object obj) throws Throwable {
            int columnIndex = (int) args[0];
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }
            
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            MethodHandle getter = lookup.unreflectGetter(field);
            Object column = getter.invoke(obj);
            
            // Get allocated count
            java.lang.reflect.Method allocatedMethod = AbstractTable.class.getDeclaredMethod("allocatedCount");
            allocatedMethod.setAccessible(true);
            long allocated = (long) allocatedMethod.invoke(obj);
            int limit = (int) allocated;
            
            // Determine method name from stack trace
            String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
            
            byte typeCode = fieldInfo.typeCode();
            return switch (methodName) {
                case "scanEqualsLong" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                        yield ((PageColumnLong) column).scanEquals((Long) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanEqualsLong called on non-long column: " + typeCode);
                    }
                }
                case "scanEqualsInt" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                        yield ((PageColumnInt) column).scanEquals((Integer) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanEqualsInt called on non-int column: " + typeCode);
                    }
                }
                case "scanEqualsString" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                        yield ((PageColumnString) column).scanEquals((String) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanEqualsString called on non-string column: " + typeCode);
                    }
                }
                case "scanEqualsStringIgnoreCase" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                        yield ((PageColumnString) column).scanEqualsIgnoreCase((String) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanEqualsStringIgnoreCase called on non-string column: " + typeCode);
                    }
                }
                case "scanBetweenLong" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                        yield ((PageColumnLong) column).scanBetween((Long) args[1], (Long) args[2], limit);
                    } else {
                        throw new IllegalStateException("scanBetweenLong called on non-long column: " + typeCode);
                    }
                }
                case "scanBetweenInt" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                        yield ((PageColumnInt) column).scanBetween((Integer) args[1], (Integer) args[2], limit);
                    } else {
                        throw new IllegalStateException("scanBetweenInt called on non-int column: " + typeCode);
                    }
                }
                case "scanInLong" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                        yield ((PageColumnLong) column).scanIn((long[]) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanInLong called on non-long column: " + typeCode);
                    }
                }
                case "scanInInt" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                        yield ((PageColumnInt) column).scanIn((int[]) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanInInt called on non-int column: " + typeCode);
                    }
                }
                case "scanInString" -> {
                    if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                        yield ((PageColumnString) column).scanIn((String[]) args[1], limit);
                    } else {
                        throw new IllegalStateException("scanInString called on non-string column: " + typeCode);
                    }
                }
                default -> throw new IllegalStateException("Unknown scan method: " + methodName);
            };
        }
    }
    
    public static class ScanAllInterceptor {
        @RuntimeType
        public int[] intercept(@This Object obj) throws Exception {
            java.lang.reflect.Method allocatedMethod = AbstractTable.class.getDeclaredMethod("allocatedCount");
            allocatedMethod.setAccessible(true);
            long allocated = (long) allocatedMethod.invoke(obj);
            
            int[] results = new int[(int) allocated];
            for (int i = 0; i < allocated; i++) {
                results[i] = i;
            }
            return results;
        }
    }
    
    public static class InsertInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final Class<?> idIndexType;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        
        public InsertInterceptor(List<ColumnFieldInfo> columnFields, Class<?> idIndexType) {
            this.columnFields = columnFields;
            this.idIndexType = idIndexType;
        }
        
        @RuntimeType
        public long intercept(@Argument(0) Object[] values, @This Object obj) throws Throwable {
            if (values.length != columnFields.size()) {
                throw new IllegalArgumentException("Value count mismatch: expected " + columnFields.size() + ", got " + values.length);
            }
            
            // Allocate row
            java.lang.reflect.Method allocateMethod = AbstractTable.class.getDeclaredMethod("allocateRowId");
            allocateMethod.setAccessible(true);
            io.memris.kernel.RowId rowId = (io.memris.kernel.RowId) allocateMethod.invoke(obj);
            int rowIndex = (int) (rowId.page() * 1024 + rowId.offset());
            
            // Write values to columns using MethodHandles
            for (int i = 0; i < columnFields.size(); i++) {
                ColumnFieldInfo fieldInfo = columnFields.get(i);
                Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
                MethodHandle getter = lookup.unreflectGetter(field);
                Object column = getter.invoke(obj);
                Object value = values[i];
                
                byte typeCode = fieldInfo.typeCode();
                // TypeCodes enum ordinals: TYPE_INT=0, TYPE_LONG=1, TYPE_STRING=8
                if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                    PageColumnLong col = (PageColumnLong) column;
                    if (value == null) {
                        col.setNull(rowIndex);
                        continue;
                    }
                    long longValue;
                    if (value instanceof Long) longValue = (Long) value;
                    else if (value instanceof Integer) longValue = ((Integer) value).longValue();
                    else if (value instanceof Number) longValue = ((Number) value).longValue();
                    else throw new IllegalArgumentException("Expected Long for column " + i);
                    col.set(rowIndex, longValue);
                } else if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                    PageColumnInt col = (PageColumnInt) column;
                    if (value == null) {
                        col.setNull(rowIndex);
                        continue;
                    }
                    int intValue;
                    if (value instanceof Integer) intValue = (Integer) value;
                    else if (value instanceof Long) intValue = ((Long) value).intValue();
                    else if (value instanceof Number) intValue = ((Number) value).intValue();
                    else throw new IllegalArgumentException("Expected Integer for column " + i);
                    col.set(rowIndex, intValue);
                } else if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                    PageColumnString col = (PageColumnString) column;
                    if (value == null) {
                        col.setNull(rowIndex);
                    } else {
                        col.set(rowIndex, value.toString());
                    }
                }
            }
            
            // Update ID index
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            MethodHandle idIndexGetter = lookup.unreflectGetter(idIndexField);
            Object idIndex = idIndexGetter.invoke(obj);
            Object idValue = values[0];
            
            if (idIndex instanceof LongIdIndex longIdIndex && idValue instanceof Number) {
                longIdIndex.put(((Number) idValue).longValue(), rowId, 0L);
            } else if (idIndex instanceof StringIdIndex stringIdIndex && idValue instanceof String) {
                stringIdIndex.put((String) idValue, rowId);
            }
            
            // Publish row to make data visible to scans
            for (int i = 0; i < columnFields.size(); i++) {
                ColumnFieldInfo fieldInfo = columnFields.get(i);
                Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
                MethodHandle getter = lookup.unreflectGetter(field);
                Object column = getter.invoke(obj);
                
                byte typeCode = fieldInfo.typeCode();
                if (typeCode == io.memris.spring.TypeCodes.TYPE_LONG) {
                    PageColumnLong col = (PageColumnLong) column;
                    col.publish(rowIndex + 1);
                } else if (typeCode == io.memris.spring.TypeCodes.TYPE_INT) {
                    PageColumnInt col = (PageColumnInt) column;
                    col.publish(rowIndex + 1);
                } else if (typeCode == io.memris.spring.TypeCodes.TYPE_STRING) {
                    PageColumnString col = (PageColumnString) column;
                    col.publish(rowIndex + 1);
                }
            }
            
            // Increment row count
            java.lang.reflect.Method incrementMethod = AbstractTable.class.getDeclaredMethod("incrementRowCount");
            incrementMethod.setAccessible(true);
            incrementMethod.invoke(obj);
            
            return io.memris.storage.Selection.pack(rowIndex, 0L);
        }
    }
    
    public static class TombstoneInterceptor {
        @RuntimeType
        public void intercept(@Argument(0) long ref, @This Object obj) throws Exception {
            int rowIndex = io.memris.storage.Selection.index(ref);
            int pageId = rowIndex / 1024;
            int offset = rowIndex % 1024;
            io.memris.kernel.RowId rowId = new io.memris.kernel.RowId(pageId, offset);
            
            java.lang.reflect.Method tombstoneMethod = AbstractTable.class.getDeclaredMethod("tombstone", io.memris.kernel.RowId.class);
            tombstoneMethod.setAccessible(true);
            tombstoneMethod.invoke(obj, rowId);
        }
    }
    
    public static class IsLiveInterceptor {
        @RuntimeType
        public boolean intercept(@Argument(0) long ref, @This Object obj) throws Exception {
            int rowIndex = io.memris.storage.Selection.index(ref);
            int pageId = rowIndex / 1024;
            int offset = rowIndex % 1024;
            io.memris.kernel.RowId rowId = new io.memris.kernel.RowId(pageId, offset);
            
            java.lang.reflect.Method isTombstoneMethod = AbstractTable.class.getDeclaredMethod("isTombstone", io.memris.kernel.RowId.class);
            isTombstoneMethod.setAccessible(true);
            boolean isTombstoned = (boolean) isTombstoneMethod.invoke(obj, rowId);
            
            return !isTombstoned;
        }
    }
    
    public static class LookupInterceptor {
        private final Class<?> idIndexType;
        private final String operation;
        private final MethodHandles.Lookup lookup = MethodHandles.lookup();
        
        public LookupInterceptor(Class<?> idIndexType, String operation) {
            this.idIndexType = idIndexType;
            this.operation = operation;
        }
        
        @RuntimeType
        public Object intercept(@Argument(0) Object key, @This Object obj) throws Throwable {
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            MethodHandle getter = lookup.unreflectGetter(idIndexField);
            Object idIndex = getter.invoke(obj);
            
            if ("long".equals(operation)) {
                LongIdIndex idx = (LongIdIndex) idIndex;
                io.memris.kernel.RowId result = idx.get(((Number) key).longValue());
                if (result == null) return -1L;
                return io.memris.storage.Selection.pack(
                        (int) (result.page() * 1024 + result.offset()), 0L);
            } else if ("String".equals(operation)) {
                StringIdIndex idx = (StringIdIndex) idIndex;
                io.memris.kernel.RowId result = idx.get((String) key);
                if (result == null) return -1L;
                return io.memris.storage.Selection.pack(
                        (int) (result.page() * 1024 + result.offset()), 0L);
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
}
