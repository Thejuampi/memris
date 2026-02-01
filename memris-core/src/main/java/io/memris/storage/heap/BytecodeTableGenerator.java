package io.memris.storage.heap;

import io.memris.kernel.RowId;
import io.memris.core.TypeCodes;
import io.memris.core.FloatEncoding;
import io.memris.storage.Selection;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.jar.asm.Opcodes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates table implementation classes using ByteBuddy with direct field
 * access.
 * <p>
 * This generator creates table subclasses that:
 * <ul>
 * <li>Have direct column field references (no MethodHandle fields)</li>
 * <li>Implement scan operations with direct field access</li>
 * <li>Filter tombstoned rows inline (no reflection)</li>
 * <li>Use O(1) static array for type code lookup</li>
 * </ul>
 */
public final class BytecodeTableGenerator {

    private static final int DEFAULT_ID_INDEX_CAPACITY = 16;

    private BytecodeTableGenerator() {
    }

    /**
     * Generate a table implementation class for the given metadata.
     *
     * @param metadata the table metadata
     * @return the generated table class
     */
    public static Class<? extends AbstractTable> generate(TableMetadata metadata) {
        String className = metadata.entityName() + "BytecodeTable";
        String packageName = "io.memris.storage.generated";

        ByteBuddy byteBuddy = new ByteBuddy();

        DynamicType.Builder<AbstractTable> builder = byteBuddy
                .subclass(AbstractTable.class)
                .implement(io.memris.storage.GeneratedTable.class)
                .name(packageName + "." + className)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Collect column field info
        List<ColumnFieldInfo> columnFields = new ArrayList<>();
        int idx = 0;

        // Add fields for each column
        for (FieldMetadata field : metadata.fields()) {
            String columnFieldName = "col" + idx;
            Class<?> columnType = getColumnType(field.type());
            byte typeCode = field.type();

            builder = builder.defineField(columnFieldName,
                    columnType,
                    Visibility.PRIVATE,
                    FieldManifestation.FINAL);

            columnFields.add(new ColumnFieldInfo(columnFieldName, columnType, typeCode, idx++));
        }

        // Add ID index field
        Class<?> idIndexType = getIdIndexType(metadata.idTypeCode());
        builder = builder.defineField("idIndex", idIndexType, Visibility.PRIVATE);

        // Add TYPE_CODES instance field (not static - so we can set it via reflection
        // in constructor)
        builder = builder.defineField("TYPE_CODES", byte[].class,
                Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add constructor
        builder = addConstructor(builder, columnFields, idIndexType, metadata.entityName());

        // Implement GeneratedTable methods
        builder = implementGeneratedTableMethods(builder, columnFields);

        // Load the class
        return builder.make()
                .load(BytecodeTableGenerator.class.getClassLoader())
                .getLoaded();
    }

    private static Class<?> getColumnType(byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> PageColumnLong.class;
            case TypeCodes.TYPE_INT -> PageColumnInt.class;
            case TypeCodes.TYPE_STRING -> PageColumnString.class;
            case TypeCodes.TYPE_DOUBLE -> PageColumnLong.class;
            case TypeCodes.TYPE_FLOAT -> PageColumnInt.class;
            case TypeCodes.TYPE_BOOLEAN -> PageColumnInt.class;
            case TypeCodes.TYPE_BYTE -> PageColumnInt.class;
            case TypeCodes.TYPE_SHORT -> PageColumnInt.class;
            case TypeCodes.TYPE_CHAR -> PageColumnInt.class;
            case TypeCodes.TYPE_INSTANT -> PageColumnLong.class;
            case TypeCodes.TYPE_LOCAL_DATE -> PageColumnLong.class;
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> PageColumnLong.class;
            case TypeCodes.TYPE_DATE -> PageColumnLong.class;
            case TypeCodes.TYPE_BIG_DECIMAL -> PageColumnString.class;
            case TypeCodes.TYPE_BIG_INTEGER -> PageColumnString.class;
            default -> throw new IllegalArgumentException("Unsupported type code: " + typeCode);
        };
    }

    private static Class<?> getIdIndexType(byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG, TypeCodes.TYPE_INT -> LongIdIndex.class;
            case TypeCodes.TYPE_STRING -> StringIdIndex.class;
            default -> throw new IllegalArgumentException("Unsupported ID type code: " + typeCode);
        };
    }

    private static DynamicType.Builder<AbstractTable> addConstructor(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields,
            Class<?> idIndexType,
            String entityName) {
        try {
            return builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(int.class, int.class, int.class)
                    .intercept(MethodCall
                            .invoke(AbstractTable.class.getDeclaredConstructor(String.class, int.class, int.class, int.class))
                            .with(entityName)
                            .withArgument(0)
                            .withArgument(1)
                            .withArgument(2)
                            .andThen(MethodDelegation.to(new ConstructorInterceptor(columnFields, idIndexType))));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find AbstractTable constructor", e);
        }
    }

    private static DynamicType.Builder<AbstractTable> implementGeneratedTableMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields) {

        // Metadata methods
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("columnCount"))
                .intercept(net.bytebuddy.implementation.FixedValue.value(columnFields.size()));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("typeCodeAt"))
                .intercept(new TypeCodeAtImplementation());

        try {
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("allocatedCount"))
                    .intercept(MethodCall.invoke(AbstractTable.class.getDeclaredMethod("allocatedCount")).onSuper());

            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("liveCount"))
                    .intercept(MethodCall.invoke(AbstractTable.class.getDeclaredMethod("rowCount")).onSuper());
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("rowGeneration"))
                    .intercept(MethodDelegation.to(new RowGenerationInterceptor()));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find AbstractTable methods", e);
        }

        try {
            builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("currentGeneration"))
                    .intercept(MethodCall.invoke(AbstractTable.class.getDeclaredMethod("currentGeneration")).onSuper());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find currentGeneration method", e);
        }

        // Read methods
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readLong"))
                .intercept(MethodDelegation.to(new ReadInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readInt"))
                .intercept(MethodDelegation.to(new ReadInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readString"))
                .intercept(MethodDelegation.to(new ReadInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("isPresent"))
                .intercept(MethodDelegation.to(new PresentInterceptor(columnFields)));

        // Scan methods
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

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanAll"))
                .intercept(MethodDelegation.to(new ScanAllInterceptor()));

        // ID index methods
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupById")
                .and(net.bytebuddy.matcher.ElementMatchers.takesArguments(long.class)))
                .intercept(MethodDelegation.to(new LookupInterceptor("long")));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("lookupByIdString"))
                .intercept(MethodDelegation.to(new LookupInterceptor("String")));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("removeById"))
                .intercept(MethodDelegation.to(new LookupInterceptor("remove")));

        // Lifecycle methods - use precise matchers to avoid overriding AbstractTable
        // methods
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("insertFrom"))
                .intercept(MethodDelegation.to(new InsertInterceptor(columnFields)));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("tombstone")
                .and(net.bytebuddy.matcher.ElementMatchers.takesArguments(long.class)))
                .intercept(MethodDelegation.to(new TombstoneInterceptor()));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("isLive"))
                .intercept(MethodDelegation.to(new IsLiveInterceptor()));

        return builder;
    }

    // Column field info record
    private record ColumnFieldInfo(String fieldName, Class<?> columnType, byte typeCode, int index) {
    }

    // ====================================================================================
    // Interceptor Classes - use direct field access via reflection API (not
    // MethodHandle)
    // ====================================================================================

    public static class ConstructorInterceptor {
        private final List<ColumnFieldInfo> columnFields;
        private final Class<?> idIndexType;

        public ConstructorInterceptor(List<ColumnFieldInfo> columnFields, Class<?> idIndexType) {
            this.columnFields = columnFields;
            this.idIndexType = idIndexType;
        }

        @RuntimeType
        public void intercept(@This Object obj, @AllArguments Object[] args) throws Exception {
            int pageSize = (int) args[0];
            int maxPages = (int) args[1];
            int initialPages = (int) args[2];

            // Initialize column fields
            for (ColumnFieldInfo field : columnFields) {
                Field declaredField = obj.getClass().getDeclaredField(field.fieldName());
                declaredField.setAccessible(true);
                Object columnInstance = field.columnType()
                        .getDeclaredConstructor(int.class, int.class, int.class)
                        .newInstance(pageSize, maxPages, initialPages);
                declaredField.set(obj, columnInstance);
            }

            // Initialize idIndex field
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            idIndexField.setAccessible(true);
            Object idIndexInstance = idIndexType
                    .getDeclaredConstructor(int.class)
                    .newInstance(DEFAULT_ID_INDEX_CAPACITY);
            idIndexField.set(obj, idIndexInstance);

            // Initialize TYPE_CODES field
            byte[] typeCodes = new byte[columnFields.size()];
            for (int i = 0; i < columnFields.size(); i++) {
                typeCodes[i] = columnFields.get(i).typeCode();
            }
            Field typeCodesField = obj.getClass().getDeclaredField("TYPE_CODES");
            typeCodesField.setAccessible(true);
            typeCodesField.set(obj, typeCodes);
        }
    }

    // Bytecode implementation for typeCodeAt - direct instance field access
    private static class TypeCodeAtImplementation implements Implementation {
        @Override
        public net.bytebuddy.dynamic.scaffold.InstrumentedType prepare(
                net.bytebuddy.dynamic.scaffold.InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public net.bytebuddy.implementation.bytecode.ByteCodeAppender appender(
                net.bytebuddy.implementation.Implementation.Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                // Load this (local variable 0)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

                // Load TYPE_CODES instance field
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD,
                        implementationContext.getInstrumentedType().getInternalName(),
                        "TYPE_CODES", "[B");

                // Load columnIndex parameter (local variable 1)
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);

                // Array access: TYPE_CODES[columnIndex]
                methodVisitor.visitInsn(Opcodes.BALOAD);

                // Return the byte
                methodVisitor.visitInsn(Opcodes.IRETURN);

                return new net.bytebuddy.implementation.bytecode.ByteCodeAppender.Size(3, 2);
            };
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

        public ReadInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public Object intercept(@Argument(0) int columnIndex,
                @Argument(1) int rowIndex,
                @This Object obj) throws Exception {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }

            AbstractTable table = (AbstractTable) obj;
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);

            return table.readWithSeqLock(rowIndex, () -> {
                try {
                    Object column = field.get(obj);
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
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static class PresentInterceptor {
        private final List<ColumnFieldInfo> columnFields;

        public PresentInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public boolean intercept(@Argument(0) int columnIndex,
                @Argument(1) int rowIndex,
                @This Object obj) throws Exception {
            if (columnIndex < 0 || columnIndex >= columnFields.size()) {
                throw new IndexOutOfBoundsException("Column index out of range: " + columnIndex);
            }

            AbstractTable table = (AbstractTable) obj;
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);

            return table.readWithSeqLock(rowIndex, () -> {
                try {
                    Object column = field.get(obj);
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
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static class ScanEqualsLongInterceptor {
        private final List<ColumnFieldInfo> columnFields;

        public ScanEqualsLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) long value, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnLong column = (PageColumnLong) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanEqualsIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) int value, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnInt column = (PageColumnInt) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanEqualsStringInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String value, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnString column = (PageColumnString) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanEquals(value, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanEqualsStringIgnoreCaseInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String value, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnString column = (PageColumnString) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanEqualsIgnoreCase(value, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanBetweenLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) long min, @Argument(2) long max,
                @This Object obj) throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            Object column = field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results;
            if (column instanceof PageColumnLong longCol) {
                results = longCol.scanBetween(min, max, limit);
            } else if (column instanceof PageColumnInt intCol) {
                results = intCol.scanBetween((int) min, (int) max, limit);
            } else {
                throw new IllegalStateException("Column " + columnIndex + " is not a numeric column");
            }
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanBetweenIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) int min, @Argument(2) int max,
                @This Object obj) throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnInt column = (PageColumnInt) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanBetween(min, max, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanInLongInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) long[] values, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnLong column = (PageColumnLong) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanInIntInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) int[] values, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnInt column = (PageColumnInt) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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

        public ScanInStringInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public int[] intercept(@Argument(0) int columnIndex, @Argument(1) String[] values, @This Object obj)
                throws Exception {
            ColumnFieldInfo fieldInfo = columnFields.get(columnIndex);
            Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
            field.setAccessible(true);
            PageColumnString column = (PageColumnString) field.get(obj);

            AbstractTable table = (AbstractTable) obj;
            int limit = (int) table.allocatedCount();
            int[] results = column.scanIn(values, limit);
            return filterTombstoned(results, table);
        }

        private int[] filterTombstoned(int[] rows, AbstractTable table) {
            if (rows.length == 0)
                return rows;

            int[] filtered = new int[rows.length];
            int count = 0;
            int pageSize = table.pageSize();
            for (int rowIndex : rows) {
                int pageId = rowIndex / pageSize;
                int offset = rowIndex % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
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
        public int[] intercept(@This Object obj) {
            AbstractTable table = (AbstractTable) obj;
            long allocated = table.allocatedCount();
            int pageSize = table.pageSize();

            int[] temp = new int[(int) allocated];
            int count = 0;
            for (int i = 0; i < allocated; i++) {
                int pageId = i / pageSize;
                int offset = i % pageSize;
                RowId rowId = new RowId(pageId, offset);
                if (!table.isTombstone(rowId)) {
                    temp[count++] = i;
                }
            }

            int[] results = new int[count];
            System.arraycopy(temp, 0, results, 0, count);
            return results;
        }
    }

    public static class LookupInterceptor {
        private final String operation;

        public LookupInterceptor(String operation) {
            this.operation = operation;
        }

        @RuntimeType
        public Object intercept(@Argument(0) Object key, @This Object obj) throws Exception {
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            idIndexField.setAccessible(true);
            Object idIndex = idIndexField.get(obj);
            AbstractTable table = (AbstractTable) obj;
            int pageSize = table.pageSize();

            if ("long".equals(operation)) {
                LongIdIndex idx = (LongIdIndex) idIndex;
                LongIdIndex.RowIdAndGeneration rag = idx.getWithGeneration(((Number) key).longValue());
                if (rag == null)
                    return -1L;

                RowId rowId = rag.rowId();
                long generation = rag.generation();
                int rowIndex = (int) (rowId.page() * pageSize + rowId.offset());

                if (table.isTombstone(rowId))
                    return -1L;
                if (table.rowGeneration(rowIndex) != generation)
                    return -1L;

                return Selection.pack(rowIndex, generation);
            } else if ("String".equals(operation)) {
                StringIdIndex idx = (StringIdIndex) idIndex;
                StringIdIndex.RowIdAndGeneration rag = idx.getWithGeneration((String) key);
                if (rag == null)
                    return -1L;

                RowId rowId = rag.rowId();
                long generation = rag.generation();
                int rowIndex = (int) (rowId.page() * pageSize + rowId.offset());

                if (table.isTombstone(rowId))
                    return -1L;
                if (table.rowGeneration(rowIndex) != generation)
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

    public static class InsertInterceptor {
        private final List<ColumnFieldInfo> columnFields;

        public InsertInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
        }

        @RuntimeType
        public long intercept(@Argument(0) Object[] values, @This Object obj) throws Exception {
            if (values.length != columnFields.size()) {
                throw new IllegalArgumentException(
                        "Value count mismatch: expected " + columnFields.size() + ", got " + values.length);
            }

            AbstractTable table = (AbstractTable) obj;
            RowId rowId = table.allocateRowId();
            int rowIndex = (int) (rowId.page() * table.pageSize() + rowId.offset());
            long generation = table.rowGeneration(rowIndex);
            long packed;

            // Begin seqlock - mark row as being written (odd)
            table.beginSeqLock(rowIndex);
            try {
                // Write values to columns
                for (int i = 0; i < columnFields.size(); i++) {
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
                    field.setAccessible(true);
                    Object column = field.get(obj);
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
                                longValue = FloatEncoding.doubleToSortableLong(((Number) value).doubleValue());
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
                                intValue = FloatEncoding.floatToSortableInt(((Number) value).floatValue());
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

                // Update ID index
                Field idIndexField = obj.getClass().getDeclaredField("idIndex");
                idIndexField.setAccessible(true);
                Object idIndex = idIndexField.get(obj);
                Object idValue = values[0];

                if (idIndex instanceof LongIdIndex longIdIndex && idValue instanceof Number) {
                    longIdIndex.put(((Number) idValue).longValue(), rowId, generation);
                } else if (idIndex instanceof StringIdIndex stringIdIndex && idValue instanceof String) {
                    stringIdIndex.put((String) idValue, rowId, generation);
                }

                // Publish row to make data visible
                for (int i = 0; i < columnFields.size(); i++) {
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    Field field = obj.getClass().getDeclaredField(fieldInfo.fieldName());
                    field.setAccessible(true);
                    Object column = field.get(obj);

                    byte typeCode = fieldInfo.typeCode();
                    if (typeCode == TypeCodes.TYPE_LONG
                            || typeCode == TypeCodes.TYPE_DOUBLE
                            || typeCode == TypeCodes.TYPE_INSTANT
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE
                            || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME
                            || typeCode == TypeCodes.TYPE_DATE) {
                        ((PageColumnLong) column).publish(rowIndex + 1);
                    } else if (typeCode == TypeCodes.TYPE_INT
                            || typeCode == TypeCodes.TYPE_FLOAT
                            || typeCode == TypeCodes.TYPE_BOOLEAN
                            || typeCode == TypeCodes.TYPE_BYTE
                            || typeCode == TypeCodes.TYPE_SHORT
                            || typeCode == TypeCodes.TYPE_CHAR) {
                        ((PageColumnInt) column).publish(rowIndex + 1);
                    } else if (typeCode == TypeCodes.TYPE_STRING
                            || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                            || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                        ((PageColumnString) column).publish(rowIndex + 1);
                    }
                }

                packed = Selection.pack(rowIndex, generation);
            } finally {
                // Ensure seqlock is ended even if exception occurs
                table.endSeqLock(rowIndex);
            }

            table.incrementRowCount();
            return packed;
        }
    }

    public static class TombstoneInterceptor {
        @RuntimeType
        public void intercept(@Argument(0) long ref, @This Object obj) throws Exception {
            AbstractTable table = (AbstractTable) obj;
            int rowIndex = Selection.index(ref);
            long generation = Selection.generation(ref);
            int pageSize = table.pageSize();
            int pageId = rowIndex / pageSize;
            int offset = rowIndex % pageSize;
            RowId rowId = new RowId(pageId, offset);

            // Read ID column for index cleanup
            Field idColumnField = obj.getClass().getDeclaredField("col0");
            idColumnField.setAccessible(true);
            Object idColumn = idColumnField.get(obj);

            Object idValue = null;
            table.beginSeqLock(rowIndex);
            try {
                if (idColumn instanceof PageColumnLong longCol) {
                    if (longCol.isPresent(rowIndex)) {
                        idValue = longCol.get(rowIndex);
                    }
                } else if (idColumn instanceof PageColumnInt intCol) {
                    if (intCol.isPresent(rowIndex)) {
                        idValue = intCol.get(rowIndex);
                    }
                } else if (idColumn instanceof PageColumnString stringCol) {
                    if (stringCol.isPresent(rowIndex)) {
                        idValue = stringCol.get(rowIndex);
                    }
                }

                table.tombstone(rowId, generation);
            } finally {
                table.endSeqLock(rowIndex);
            }

            // Remove from ID index
            if (idValue != null) {
                Field idIndexField = obj.getClass().getDeclaredField("idIndex");
                idIndexField.setAccessible(true);
                Object idIndex = idIndexField.get(obj);

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
        public boolean intercept(@Argument(0) long ref, @This Object obj) {
            AbstractTable table = (AbstractTable) obj;
            int rowIndex = Selection.index(ref);
            long generation = Selection.generation(ref);
            int pageSize = table.pageSize();
            int pageId = rowIndex / pageSize;
            int offset = rowIndex % pageSize;
            RowId rowId = new RowId(pageId, offset);

            if (table.isTombstone(rowId)) {
                return false;
            }

            return table.rowGeneration(rowIndex) == generation;
        }
    }
}
