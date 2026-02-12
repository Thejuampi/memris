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
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import io.memris.storage.heap.TableImplementationStrategy.ColumnFieldInfo;

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
    private static final ClassValue<GeneratedFieldCache> GENERATED_FIELD_CACHE = new ClassValue<>() {
        @Override
        protected GeneratedFieldCache computeValue(Class<?> type) {
            return new GeneratedFieldCache(resolveAccessibleField(type, "CACHED_COLUMN_FIELDS"),
                    resolveAccessibleField(type, "ID_INDEX_FIELD"));
        }
    };

    private BytecodeTableGenerator() {
    }

    /**
     * Generate a table implementation class for the given metadata.
     *
     * @param metadata the table metadata
     * @return the generated table class
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * final class PersonBytecodeTable extends AbstractTable implements GeneratedTable {
     *     private final PageColumnLong col0;
     *     private final PageColumnString col1;
     *     private final PageColumnInt col2;
     *     private final byte[] TYPE_CODES;
     *
     *     public PersonBytecodeTable(int pageSize, int maxPages, int initialPages) {
     *         super("Person", pageSize, maxPages, initialPages);
     *         // ConstructorInterceptor initializes all generated fields.
     *     }
     * }
     * }</pre>
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

            columnFields.add(new ColumnFieldInfo(
                    columnFieldName,
                    columnType,
                    typeCode,
                    idx++,
                    field.primitiveNonNull(),
                    field.isId()));
        }

        // Add ID index field
        Class<?> idIndexType = getIdIndexType(metadata.idTypeCode());
        builder = builder.defineField("idIndex", idIndexType, Visibility.PRIVATE);

        // Add TYPE_CODES instance field (not static - so we can set it via reflection
        // in constructor)
        builder = builder.defineField("TYPE_CODES", byte[].class,
                Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add cached Field arrays to avoid per-operation reflection lookups
        builder = builder.defineField("CACHED_COLUMN_FIELDS", Field[].class,
                Visibility.PRIVATE, FieldManifestation.FINAL);
        builder = builder.defineField("ID_INDEX_FIELD", Field.class,
                Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add constructor
        builder = addConstructor(builder, columnFields, idIndexType, metadata.entityName());

        // Implement GeneratedTable methods
        builder = implementMethods(builder, columnFields);

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
        // Equivalent generated Java (simplified):
        // PersonBytecodeTable(int pageSize, int maxPages, int initialPages) {
        //     super(entityName, pageSize, maxPages, initialPages);
        //     constructorInterceptor.intercept(this, new Object[] {pageSize, maxPages,
        //     initialPages});
        // }
        try {
            return builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(int.class, int.class, int.class)
                    .intercept(MethodCall
                            .invoke(AbstractTable.class.getDeclaredConstructor(String.class, int.class, int.class,
                                    int.class))
                            .with(entityName)
                            .withArgument(0)
                            .withArgument(1)
                            .withArgument(2)
                            .andThen(MethodDelegation.to(new ConstructorInterceptor(columnFields, idIndexType))));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find AbstractTable constructor", e);
        }
    }

    @SuppressWarnings("PMD.AvoidReassigningParameters")
    /**
     * Wires generated table methods to either direct bytecode implementations or
     * interceptor delegates.
     *
     * Equivalent Java code for generated method mapping (simplified):
     *
     * <pre>{@code
     * builder.readLong(...)   -> new ReadMethodImplementation(..., TYPE_LONG)
     * builder.readInt(...)    -> new ReadMethodImplementation(..., TYPE_INT)
     * builder.readString(...) -> new ReadMethodImplementation(..., TYPE_STRING)
     *
     * builder.scanEqualsLong(...)   -> new ScanMethodImplementation(..., SCAN_EQUALS_LONG)
     * builder.scanBetweenLong(...)  -> new ScanMethodImplementation(..., SCAN_BETWEEN_LONG)
     * builder.scanInString(...)     -> new ScanMethodImplementation(..., SCAN_IN_STRING)
     *
     * builder.insertFrom(...) -> new InsertInterceptor(...)
     * builder.tombstone(...)  -> new TombstoneInterceptor()
     * }</pre>
     */
    static DynamicType.Builder<AbstractTable> implementMethods(
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

        // Read methods - Direct Bytecode Generation
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readLong"))
                .intercept(new ReadMethodImplementation(columnFields, TypeCodes.TYPE_LONG));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readInt"))
                .intercept(new ReadMethodImplementation(columnFields, TypeCodes.TYPE_INT));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("readString"))
                .intercept(new ReadMethodImplementation(columnFields, TypeCodes.TYPE_STRING));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("isPresent"))
                .intercept(MethodDelegation.to(new PresentInterceptor(columnFields)));

        // Scan methods
        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsLong"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_EQUALS_LONG));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsInt"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_EQUALS_INT));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsString"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_EQUALS_STRING));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanEqualsStringIgnoreCase"))
                .intercept(new ScanMethodImplementation(columnFields,
                        ScanMethodImplementation.SCAN_EQUALS_STRING_IGNORE_CASE));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenLong"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_BETWEEN_LONG));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanBetweenInt"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_BETWEEN_INT));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInLong"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_IN_LONG));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInInt"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_IN_INT));

        builder = builder.method(net.bytebuddy.matcher.ElementMatchers.named("scanInString"))
                .intercept(new ScanMethodImplementation(columnFields, ScanMethodImplementation.SCAN_IN_STRING));

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

        // Per-column direct accessor methods (zero dispatch)
        // Equivalent generated Java (simplified):
        // readCol0Long(int rowIndex), readCol1String(int rowIndex), ...
        // scanEqualsCol0Long(long value), scanEqualsCol2Int(int value), ...
        // Note: only scanEquals* per-column methods are generated currently.
        for (ColumnFieldInfo field : columnFields) {
            String capitalizedName = capitalize(field.fieldName());
            byte typeCode = field.typeCode();

            // Generate per-column read methods
            if (isLongCompatible(typeCode)) {
                builder = builder.defineMethod("read" + capitalizedName + "Long", long.class, Visibility.PUBLIC)
                        .withParameter(int.class, "rowIndex")
                        .intercept(new PerColumnReadImplementation(field, TypeCodes.TYPE_LONG));
            }
            if (isIntCompatible(typeCode)) {
                builder = builder.defineMethod("read" + capitalizedName + "Int", int.class, Visibility.PUBLIC)
                        .withParameter(int.class, "rowIndex")
                        .intercept(new PerColumnReadImplementation(field, TypeCodes.TYPE_INT));
            }
            if (typeCode == TypeCodes.TYPE_STRING || typeCode == TypeCodes.TYPE_BIG_DECIMAL
                    || typeCode == TypeCodes.TYPE_BIG_INTEGER) {
                builder = builder.defineMethod("read" + capitalizedName + "String", String.class, Visibility.PUBLIC)
                        .withParameter(int.class, "rowIndex")
                        .intercept(new PerColumnReadImplementation(field, TypeCodes.TYPE_STRING));
            }

            // Generate per-column scan methods
            if (isLongCompatible(typeCode)) {
                builder = builder.defineMethod("scanEquals" + capitalizedName + "Long", int[].class, Visibility.PUBLIC)
                        .withParameter(long.class, "value")
                        .intercept(new PerColumnScanImplementation(field, ScanMethodImplementation.SCAN_EQUALS_LONG));
            }
            if (isIntCompatible(typeCode)) {
                builder = builder.defineMethod("scanEquals" + capitalizedName + "Int", int[].class, Visibility.PUBLIC)
                        .withParameter(int.class, "value")
                        .intercept(new PerColumnScanImplementation(field, ScanMethodImplementation.SCAN_EQUALS_INT));
            }
            // Note: BIG_DECIMAL and BIG_INTEGER are stored as strings but not yet included
            // here.
            // They would require generating scanEquals...String methods.
            if (typeCode == TypeCodes.TYPE_STRING) {
                builder = builder
                        .defineMethod("scanEquals" + capitalizedName + "String", int[].class, Visibility.PUBLIC)
                        .withParameter(String.class, "value")
                        .intercept(new PerColumnScanImplementation(field, ScanMethodImplementation.SCAN_EQUALS_STRING));
            }
        }

        return builder;
    }

    // ====================================================================================
    // Interceptor Classes - use direct field access via reflection API (not
    // MethodHandle)
    // ====================================================================================

    /**
     * Initializes generated table instance fields using constructor arguments.
     *
     * Equivalent Java code for generated constructor tail (simplified):
     *
     * <pre>{@code
     * this.col0 = new PageColumnLong(pageSize, maxPages, initialPages);
     * this.col1 = new PageColumnString(pageSize, maxPages, initialPages);
     * ...
     * this.CACHED_COLUMN_FIELDS = new Field[] { col0Field, col1Field, ... };
     *
     * this.idIndex = new LongIdIndex(DEFAULT_ID_INDEX_CAPACITY);
     * this.ID_INDEX_FIELD = idIndexField;
     * this.TYPE_CODES = new byte[] { TYPE_LONG, TYPE_STRING, ... };
     * }</pre>
     */
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

            // Pre-resolve and cache column fields for fast access in hot paths
            Field[] cachedColumnFields = new Field[columnFields.size()];

            // Initialize column fields and cache them
            for (int i = 0; i < columnFields.size(); i++) {
                ColumnFieldInfo field = columnFields.get(i);
                Field declaredField = obj.getClass().getDeclaredField(field.fieldName());
                declaredField.setAccessible(true);
                Object columnInstance = field.columnType()
                        .getDeclaredConstructor(int.class, int.class, int.class)
                        .newInstance(pageSize, maxPages, initialPages);
                declaredField.set(obj, columnInstance);
                cachedColumnFields[i] = declaredField;
            }

            // Store cached column fields
            Field cachedFieldsField = obj.getClass().getDeclaredField("CACHED_COLUMN_FIELDS");
            cachedFieldsField.setAccessible(true);
            cachedFieldsField.set(obj, cachedColumnFields);

            // Initialize idIndex field and cache it
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            idIndexField.setAccessible(true);
            Object idIndexInstance = idIndexType
                    .getDeclaredConstructor(int.class)
                    .newInstance(DEFAULT_ID_INDEX_CAPACITY);
            idIndexField.set(obj, idIndexInstance);

            // Cache the idIndex field for fast access
            Field idIndexCacheField = obj.getClass().getDeclaredField("ID_INDEX_FIELD");
            idIndexCacheField.setAccessible(true);
            idIndexCacheField.set(obj, idIndexField);

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

    /**
     * Generates O(1) `typeCodeAt` lookup via direct `TYPE_CODES[columnIndex]`
     * access.
     *
     * Equivalent Java code for PersonBytecodeTable.typeCodeAt(int columnIndex):
     *
     * <pre>{@code
     * public byte typeCodeAt(int columnIndex) {
     *     return this.TYPE_CODES[columnIndex];
     * }
     * }</pre>
     */
    private static final class TypeCodeAtImplementation implements Implementation {
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

    private static GeneratedFieldCache getGeneratedFieldCache(Object obj) {
        return GENERATED_FIELD_CACHE.get(obj.getClass());
    }

    private static Field resolveAccessibleField(Class<?> type, String fieldName) {
        try {
            var field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Generated table field missing: " + fieldName + " on " + type.getName(), e);
        }
    }

    // Helper method to get cached column fields (reduces per-operation reflection)
    private static Field[] getCachedColumnFields(Object obj) throws Exception {
        var cache = getGeneratedFieldCache(obj);
        return (Field[]) cache.cachedColumnFieldsField().get(obj);
    }

    // Helper method to get cached ID index field
    private static Field getCachedIdIndexField(Object obj) throws Exception {
        var cache = getGeneratedFieldCache(obj);
        return (Field) cache.idIndexFieldField().get(obj);
    }

    private record GeneratedFieldCache(Field cachedColumnFieldsField, Field idIndexFieldField) {
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

            // Use cached field instead of getDeclaredField
            Field[] cachedFields = getCachedColumnFields(obj);
            Field field = cachedFields[columnIndex];

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
            // Use cached field instead of getDeclaredField
            Field idIndexField = getCachedIdIndexField(obj);
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
        @FunctionalInterface
        private interface ColumnWriter {
            void write(Object column, Object value, int rowIndex);
        }

        @FunctionalInterface
        private interface ColumnPublisher {
            void publish(Object column, int rowLimit);
        }

        @FunctionalInterface
        private interface IdIndexUpdater {
            void update(Object idIndex, Object idValue, RowId rowId, long generation);
        }

        private final List<ColumnFieldInfo> columnFields;
        private final ColumnWriter[] writers;
        private final ColumnPublisher[] publishers;
        private final int idColumnIndex;
        private final IdIndexUpdater idIndexUpdater;

        public InsertInterceptor(List<ColumnFieldInfo> columnFields) {
            this.columnFields = columnFields;
            this.writers = new ColumnWriter[columnFields.size()];
            this.publishers = new ColumnPublisher[columnFields.size()];
            this.idColumnIndex = resolveIdColumnIndex(columnFields);
            for (var i = 0; i < columnFields.size(); i++) {
                var fieldInfo = columnFields.get(i);
                writers[i] = createWriter(fieldInfo, i);
                publishers[i] = createPublisher(fieldInfo.typeCode());
            }
            this.idIndexUpdater = createIdIndexUpdater(columnFields.get(idColumnIndex).typeCode());
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
                // Get cached fields once instead of per-column reflection
                Field[] cachedFields = getCachedColumnFields(obj);
                Field idIndexField = getCachedIdIndexField(obj);

                var columns = new Object[columnFields.size()];
                for (int i = 0; i < columnFields.size(); i++) {
                    Field field = cachedFields[i];
                    Object column = field.get(obj);
                    columns[i] = column;
                    writers[i].write(column, values[i], rowIndex);
                }

                // Update ID index using cached field
                Object idIndex = idIndexField.get(obj);
                Object idValue = values[idColumnIndex];
                idIndexUpdater.update(idIndex, idValue, rowId, generation);

                // Publish row to make data visible using cached fields
                for (int i = 0; i < columnFields.size(); i++) {
                    publishers[i].publish(columns[i], rowIndex + 1);
                }

                packed = Selection.pack(rowIndex, generation);
            } finally {
                // Ensure seqlock is ended even if exception occurs
                table.endSeqLock(rowIndex);
            }

            table.incrementRowCount();
            return packed;
        }

        private static int resolveIdColumnIndex(List<ColumnFieldInfo> fields) {
            for (var i = 0; i < fields.size(); i++) {
                if (fields.get(i).idColumn()) {
                    return i;
                }
            }
            return 0;
        }

        private static ColumnWriter createWriter(ColumnFieldInfo fieldInfo, int columnIndex) {
            var primitive = fieldInfo.primitiveNonNull();
            return switch (fieldInfo.typeCode()) {
                case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_INSTANT,
                        TypeCodes.TYPE_LOCAL_DATE,
                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                        TypeCodes.TYPE_DATE -> (column, value, rowIndex) -> {
                            var col = (PageColumnLong) column;
                            if (value == null) {
                                if (primitive) {
                                    throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                                }
                                col.setNull(rowIndex);
                                return;
                            }
                            col.set(rowIndex, ((Number) value).longValue());
                        };
                case TypeCodes.TYPE_DOUBLE -> (column, value, rowIndex) -> {
                    var col = (PageColumnLong) column;
                    if (value == null) {
                        if (primitive) {
                            throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                        }
                        col.setNull(rowIndex);
                        return;
                    }
                    col.set(rowIndex, FloatEncoding.doubleToSortableLong(((Number) value).doubleValue()));
                };
                case TypeCodes.TYPE_INT,
                        TypeCodes.TYPE_BYTE,
                        TypeCodes.TYPE_SHORT -> (column, value, rowIndex) -> {
                            var col = (PageColumnInt) column;
                            if (value == null) {
                                if (primitive) {
                                    throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                                }
                                col.setNull(rowIndex);
                                return;
                            }
                            col.set(rowIndex, ((Number) value).intValue());
                        };
                case TypeCodes.TYPE_FLOAT -> (column, value, rowIndex) -> {
                    var col = (PageColumnInt) column;
                    if (value == null) {
                        if (primitive) {
                            throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                        }
                        col.setNull(rowIndex);
                        return;
                    }
                    col.set(rowIndex, FloatEncoding.floatToSortableInt(((Number) value).floatValue()));
                };
                case TypeCodes.TYPE_BOOLEAN -> (column, value, rowIndex) -> {
                    var col = (PageColumnInt) column;
                    if (value == null) {
                        if (primitive) {
                            throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                        }
                        col.setNull(rowIndex);
                        return;
                    }
                    col.set(rowIndex, (Boolean) value ? 1 : 0);
                };
                case TypeCodes.TYPE_CHAR -> (column, value, rowIndex) -> {
                    var col = (PageColumnInt) column;
                    if (value == null) {
                        if (primitive) {
                            throw new IllegalArgumentException("Null assigned to primitive column: " + fieldInfo.fieldName());
                        }
                        col.setNull(rowIndex);
                        return;
                    }
                    col.set(rowIndex, (Character) value);
                };
                case TypeCodes.TYPE_STRING,
                        TypeCodes.TYPE_BIG_DECIMAL,
                        TypeCodes.TYPE_BIG_INTEGER -> (column, value, rowIndex) -> {
                            var col = (PageColumnString) column;
                            if (value == null) {
                                col.setNull(rowIndex);
                            } else {
                                col.set(rowIndex, value.toString());
                            }
                        };
                default -> throw new IllegalArgumentException(
                        "Unsupported type code: " + fieldInfo.typeCode() + " at column " + columnIndex);
            };
        }

        private static ColumnPublisher createPublisher(byte typeCode) {
            return switch (typeCode) {
                case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_DOUBLE,
                        TypeCodes.TYPE_INSTANT,
                        TypeCodes.TYPE_LOCAL_DATE,
                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                        TypeCodes.TYPE_DATE -> (column, rowLimit) -> ((PageColumnLong) column).publish(rowLimit);
                case TypeCodes.TYPE_INT,
                        TypeCodes.TYPE_FLOAT,
                        TypeCodes.TYPE_BOOLEAN,
                        TypeCodes.TYPE_BYTE,
                        TypeCodes.TYPE_SHORT,
                        TypeCodes.TYPE_CHAR -> (column, rowLimit) -> ((PageColumnInt) column).publish(rowLimit);
                case TypeCodes.TYPE_STRING,
                        TypeCodes.TYPE_BIG_DECIMAL,
                        TypeCodes.TYPE_BIG_INTEGER -> (column, rowLimit) -> ((PageColumnString) column).publish(rowLimit);
                default -> (column, rowLimit) -> {
                };
            };
        }

        private static IdIndexUpdater createIdIndexUpdater(byte idTypeCode) {
            return switch (idTypeCode) {
                case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_INT -> (idIndex, idValue, rowId, generation) -> {
                            if (idIndex instanceof LongIdIndex longIdIndex && idValue instanceof Number number) {
                                longIdIndex.put(number.longValue(), rowId, generation);
                            }
                        };
                case TypeCodes.TYPE_STRING -> (idIndex, idValue, rowId, generation) -> {
                    if (idIndex instanceof StringIdIndex stringIdIndex && idValue instanceof String stringValue) {
                        stringIdIndex.put(stringValue, rowId, generation);
                    }
                };
                default -> (idIndex, idValue, rowId, generation) -> {
                };
            };
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

            // Use cached fields instead of getDeclaredField
            Field[] cachedFields = getCachedColumnFields(obj);
            Field idIndexField = getCachedIdIndexField(obj);
            Object idColumn = cachedFields[0].get(obj); // col0 is always the ID column

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

            // Remove from ID index using cached field
            if (idValue != null) {
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

    // ====================================================================================
    // Phase 2: Direct Bytecode Generation Implementations
    // ====================================================================================

    /**
     * Generates O(1) read operations with seqlock retry path.
     *
     * Equivalent Java code for PersonBytecodeTable.readLong(int columnIndex, int
     * rowIndex):
     * 
     * <pre>{@code
     * public long readLong(int columnIndex, int rowIndex) {
     *     while (true) {
     *         long version = this.getSeqLock(rowIndex);
     *         if ((version & 1) != 0) { // odd means write in progress
     *             AbstractTable.backoff(1);
     *             continue;
     *         }
     *         long result;
     *         switch (columnIndex) {
     *             case 0: // compatible long-family column
     *                 result = this.col0.get(rowIndex);
     *                 break;
     *             case 1: // in-range but incompatible for readLong
     *                 throw new IllegalArgumentException("Column 1 type mismatch for requested read operation");
     *             case 3: // compatible long-family column
     *                 result = this.col3.get(rowIndex);
     *                 break;
     *             default:
     *                 throw new IndexOutOfBoundsException("Column index out of bounds");
     *         }
     *         if (this.getSeqLock(rowIndex) == version) {
     *             return result;
     *         }
     *     }
     * }
     * }</pre>
     */
    private static class ReadMethodImplementation implements Implementation {
        private final List<ColumnFieldInfo> columnFields;
        private final int targetTypeCode; // TYPE_LONG, TYPE_INT, or TYPE_STRING

        public ReadMethodImplementation(List<ColumnFieldInfo> columnFields, int targetTypeCode) {
            this.columnFields = columnFields;
            this.targetTypeCode = targetTypeCode;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new Appender(columnFields, targetTypeCode);
        }

        private static class Appender implements ByteCodeAppender {
            private final List<ColumnFieldInfo> columnFields;
            private final int targetTypeCode;

            public Appender(List<ColumnFieldInfo> columnFields, int targetTypeCode) {
                this.columnFields = columnFields;
                this.targetTypeCode = targetTypeCode;
            }

            @Override
            public Size apply(MethodVisitor mv, Context implementationContext, MethodDescription instrumentedMethod) {
                // Locals: 0=this, 1=columnIndex, 2=rowIndex, 3=version(long),
                // 5=result(long/int/obj)
                int resultVar = 5;

                mv.visitCode();

                // Initialize version (3) to 0 to satisfy verifier at loopStart merge
                mv.visitInsn(Opcodes.LCONST_0);
                mv.visitVarInsn(Opcodes.LSTORE, 3);

                Label loopStart = new Label();
                Label retry = new Label();
                Label checkVersion = new Label();

                mv.visitLabel(loopStart);
                mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.LONG }, 0, null);

                // 1. long version = this.getSeqLock(rowIndex);
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitVarInsn(Opcodes.ILOAD, 2);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "getSeqLock", "(I)J", false);
                mv.visitVarInsn(Opcodes.LSTORE, 3);

                // 2. if ((version & 1) != 0) goto retry;
                mv.visitVarInsn(Opcodes.LLOAD, 3);
                mv.visitInsn(Opcodes.LCONST_1);
                mv.visitInsn(Opcodes.LAND);
                mv.visitInsn(Opcodes.LCONST_0);
                mv.visitInsn(Opcodes.LCMP);
                mv.visitJumpInsn(Opcodes.IFNE, retry);

                // 3. Switch for READ
                mv.visitVarInsn(Opcodes.ILOAD, 1); // columnIndex

                Label defaultLabel = new Label();
                Label[] columnLabels = new Label[columnFields.size()];
                for (int i = 0; i < columnFields.size(); i++) {
                    columnLabels[i] = new Label();
                }

                mv.visitTableSwitchInsn(0, columnFields.size() - 1, defaultLabel, columnLabels);

                for (int i = 0; i < columnFields.size(); i++) {
                    mv.visitLabel(columnLabels[i]);
                    mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    byte typeCode = fieldInfo.typeCode();

                    if (isCompatible(typeCode, targetTypeCode)) {
                        mv.visitVarInsn(Opcodes.ALOAD, 0);
                        String owner = implementationContext.getInstrumentedType().getInternalName();
                        String fieldName = fieldInfo.fieldName();
                        String fieldDesc = net.bytebuddy.jar.asm.Type.getDescriptor(fieldInfo.columnType());
                        mv.visitFieldInsn(Opcodes.GETFIELD, owner, fieldName, fieldDesc);

                        mv.visitVarInsn(Opcodes.ILOAD, 2); // rowIndex

                        String colTypeInternal = net.bytebuddy.jar.asm.Type.getInternalName(fieldInfo.columnType());
                        if (targetTypeCode == TypeCodes.TYPE_LONG) {
                            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)J", false);
                            mv.visitVarInsn(Opcodes.LSTORE, resultVar);
                        } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)I", false);
                            mv.visitVarInsn(Opcodes.ISTORE, resultVar);
                        } else {
                            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)Ljava/lang/String;",
                                    false);
                            mv.visitVarInsn(Opcodes.ASTORE, resultVar);
                        }
                        mv.visitJumpInsn(Opcodes.GOTO, checkVersion);
                    } else {
                        generateThrowException(mv, "java/lang/IllegalArgumentException",
                                "Column " + i + " type mismatch for requested read operation");
                    }
                }

                mv.visitLabel(defaultLabel);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                generateThrowException(mv, "java/lang/IndexOutOfBoundsException", "Column index out of bounds");

                // 4. Check version
                mv.visitLabel(checkVersion);
                if (targetTypeCode == TypeCodes.TYPE_LONG) {
                    mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.LONG }, 0, null);
                } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                    mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
                } else {
                    mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { "java/lang/String" }, 0, null);
                }
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitVarInsn(Opcodes.ILOAD, 2);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "getSeqLock", "(I)J", false);
                mv.visitVarInsn(Opcodes.LLOAD, 3);
                mv.visitInsn(Opcodes.LCMP);
                mv.visitJumpInsn(Opcodes.IFNE, retry); // if (current != version) retry

                // 5. Return result
                if (targetTypeCode == TypeCodes.TYPE_LONG) {
                    mv.visitVarInsn(Opcodes.LLOAD, resultVar);
                    mv.visitInsn(Opcodes.LRETURN);
                } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                    mv.visitVarInsn(Opcodes.ILOAD, resultVar);
                    mv.visitInsn(Opcodes.IRETURN);
                } else {
                    mv.visitVarInsn(Opcodes.ALOAD, resultVar);
                    mv.visitInsn(Opcodes.ARETURN);
                }

                // 6. Retry logic
                mv.visitLabel(retry);
                mv.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
                mv.visitIntInsn(Opcodes.BIPUSH, 1); // backoff(1)
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        net.bytebuddy.jar.asm.Type.getInternalName(AbstractTable.class),
                        "backoff", "(I)V", false);
                mv.visitJumpInsn(Opcodes.GOTO, loopStart);

                mv.visitMaxs(6, 8);
                return new Size(6, 8);
            }

            private boolean isCompatible(byte actualType, int requestType) {
                if (requestType == TypeCodes.TYPE_LONG) {
                    return actualType == TypeCodes.TYPE_LONG || actualType == TypeCodes.TYPE_DOUBLE
                            || actualType == TypeCodes.TYPE_INSTANT || actualType == TypeCodes.TYPE_LOCAL_DATE
                            || actualType == TypeCodes.TYPE_LOCAL_DATE_TIME || actualType == TypeCodes.TYPE_DATE;
                } else if (requestType == TypeCodes.TYPE_INT) {
                    return actualType == TypeCodes.TYPE_INT || actualType == TypeCodes.TYPE_FLOAT
                            || actualType == TypeCodes.TYPE_BOOLEAN || actualType == TypeCodes.TYPE_BYTE
                            || actualType == TypeCodes.TYPE_SHORT || actualType == TypeCodes.TYPE_CHAR;
                } else { // String
                    return actualType == TypeCodes.TYPE_STRING || actualType == TypeCodes.TYPE_BIG_DECIMAL
                            || actualType == TypeCodes.TYPE_BIG_INTEGER;
                }
            }

            private void generateThrowException(MethodVisitor mv, String exceptionType, String message) {
                mv.visitTypeInsn(Opcodes.NEW, exceptionType);
                mv.visitInsn(Opcodes.DUP);
                mv.visitLdcInsn(message);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, exceptionType, "<init>", "(Ljava/lang/String;)V", false);
                mv.visitInsn(Opcodes.ATHROW);
            }
        }

    }

    /**
     * Generates O(1) scan operations by dispatching to the correct column field.
     *
     * Equivalent Java code for PersonBytecodeTable.scanEqualsLong(int columnIndex,
     * long value):
     * 
     * <pre>{@code
     * public int[] scanEqualsLong(int columnIndex, long value) {
     *     int limit = (int) this.allocatedCount();
     *     int[] results;
     *     switch (columnIndex) {
     *         case 0: // id
     *             results = this.col0.scanEquals(value, limit);
     *             break;
     *         case 1: // in-range but incompatible for scanEqualsLong
     *             throw new IllegalArgumentException("Column 1 type mismatch for requested scan operation");
     *         case 3: // salary
     *             results = this.col3.scanEquals(value, limit);
     *             break;
     *         default:
     *             throw new IndexOutOfBoundsException("Column index out of bounds");
     *     }
     *     return filterTombstoned(results);
     * }
     * }</pre>
     */
    private static class ScanMethodImplementation implements Implementation {
        private final List<ColumnFieldInfo> columnFields;
        private final int scanType;

        static final int SCAN_EQUALS_LONG = 0;
        static final int SCAN_EQUALS_INT = 1;
        static final int SCAN_EQUALS_STRING = 2;
        static final int SCAN_EQUALS_STRING_IGNORE_CASE = 3;
        static final int SCAN_BETWEEN_LONG = 4;
        static final int SCAN_BETWEEN_INT = 5;
        static final int SCAN_IN_LONG = 6;
        static final int SCAN_IN_INT = 7;
        static final int SCAN_IN_STRING = 8;

        public ScanMethodImplementation(List<ColumnFieldInfo> columnFields, int scanType) {
            this.columnFields = columnFields;
            this.scanType = scanType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new Appender(columnFields, scanType);
        }

        private static class Appender implements ByteCodeAppender {
            private final List<ColumnFieldInfo> columnFields;
            private final int scanType;

            public Appender(List<ColumnFieldInfo> columnFields, int scanType) {
                this.columnFields = columnFields;
                this.scanType = scanType;
            }

            @Override
            public Size apply(MethodVisitor mv, Context implementationContext, MethodDescription instrumentedMethod) {
                mv.visitCode();

                Label defaultLabel = new Label();
                Label[] columnLabels = new Label[columnFields.size()];
                for (int i = 0; i < columnFields.size(); i++) {
                    columnLabels[i] = new Label();
                }

                // Arg 0 = this, Arg 1 = columnIndex.
                mv.visitVarInsn(Opcodes.ILOAD, 1);
                mv.visitTableSwitchInsn(0, columnFields.size() - 1, defaultLabel, columnLabels);

                for (int i = 0; i < columnFields.size(); i++) {
                    mv.visitLabel(columnLabels[i]);
                    mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                    ColumnFieldInfo fieldInfo = columnFields.get(i);
                    byte typeCode = fieldInfo.typeCode();

                    if (isCompatible(typeCode, scanType)) {
                        mv.visitVarInsn(Opcodes.ALOAD, 0);
                        String owner = implementationContext.getInstrumentedType().getInternalName();
                        String fieldName = fieldInfo.fieldName();
                        String fieldDesc = net.bytebuddy.jar.asm.Type.getDescriptor(fieldInfo.columnType());
                        mv.visitFieldInsn(Opcodes.GETFIELD, owner, fieldName, fieldDesc);

                        // Load arguments and invoke scan
                        generateScanInvocation(mv, fieldInfo, implementationContext);

                        // Filter tombstones
                        mv.visitVarInsn(Opcodes.ALOAD, 0); // table
                        mv.visitInsn(Opcodes.SWAP); // table, rows
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                implementationContext.getInstrumentedType().getSuperClass().asErasure()
                                        .getInternalName(),
                                "filterTombstoned", "([I)[I", false);

                        mv.visitInsn(Opcodes.ARETURN);
                    } else {
                        generateThrowException(mv, "java/lang/IllegalArgumentException",
                                "Column " + i + " type mismatch for requested scan operation");
                    }
                }

                mv.visitLabel(defaultLabel);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                generateThrowException(mv, "java/lang/IndexOutOfBoundsException", "Column index out of bounds");

                mv.visitMaxs(8, 8); // Conservative maxs
                return new Size(8, 8);
            }

            private void generateScanInvocation(MethodVisitor mv, ColumnFieldInfo fieldInfo, Context context) {
                String colTypeInternal = net.bytebuddy.jar.asm.Type.getInternalName(fieldInfo.columnType());
                boolean isIntColumn = isIntCompatible(fieldInfo.typeCode());

                // 1. Load scan arguments
                if (scanType == SCAN_EQUALS_LONG) {
                    mv.visitVarInsn(Opcodes.LLOAD, 2); // value
                } else if (scanType == SCAN_EQUALS_INT) {
                    mv.visitVarInsn(Opcodes.ILOAD, 2); // value
                } else if (scanType == SCAN_EQUALS_STRING || scanType == SCAN_EQUALS_STRING_IGNORE_CASE) {
                    mv.visitVarInsn(Opcodes.ALOAD, 2); // value
                } else if (scanType == SCAN_BETWEEN_LONG) {
                    if (isIntColumn) {
                        // Cast long args to int for INT columns
                        mv.visitVarInsn(Opcodes.LLOAD, 2); // min (long)
                        mv.visitInsn(Opcodes.L2I); // min (int)
                        mv.visitVarInsn(Opcodes.LLOAD, 4); // max (long)
                        mv.visitInsn(Opcodes.L2I); // max (int)
                    } else {
                        mv.visitVarInsn(Opcodes.LLOAD, 2); // min
                        mv.visitVarInsn(Opcodes.LLOAD, 4); // max
                    }
                } else if (scanType == SCAN_BETWEEN_INT) {
                    mv.visitVarInsn(Opcodes.ILOAD, 2); // min
                    mv.visitVarInsn(Opcodes.ILOAD, 3); // max
                } else if (scanType == SCAN_IN_LONG || scanType == SCAN_IN_INT || scanType == SCAN_IN_STRING) {
                    mv.visitVarInsn(Opcodes.ALOAD, 2); // array/set
                }

                // 2. Load limit (allocatedCount)
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        context.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "allocatedCount", "()J", false);
                mv.visitInsn(Opcodes.L2I); // allocatedCount returns long, but scan expects int limit

                // 3. Invoke scan
                if (scanType == SCAN_EQUALS_LONG) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals", "(JI)[I", false);
                } else if (scanType == SCAN_EQUALS_INT) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals", "(II)[I", false);
                } else if (scanType == SCAN_EQUALS_STRING) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals", "(Ljava/lang/String;I)[I",
                            false);
                } else if (scanType == SCAN_EQUALS_STRING_IGNORE_CASE) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEqualsIgnoreCase",
                            "(Ljava/lang/String;I)[I", false);
                } else if (scanType == SCAN_BETWEEN_LONG) {
                    if (isIntColumn) {
                        // Use int version for INT columns
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanBetween", "(III)[I", false);
                    } else {
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanBetween", "(JJI)[I", false);
                    }
                } else if (scanType == SCAN_BETWEEN_INT) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanBetween", "(III)[I", false);
                } else if (scanType == SCAN_IN_LONG) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanIn", "([JI)[I", false);
                } else if (scanType == SCAN_IN_INT) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanIn", "([II)[I", false);
                } else if (scanType == SCAN_IN_STRING) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanIn", "([Ljava/lang/String;I)[I",
                            false);
                }
            }

            private boolean isCompatible(byte actualType, int scanType) {
                switch (scanType) {
                    case SCAN_EQUALS_LONG:
                    case SCAN_IN_LONG:
                        // scanInLong only supports long-family (can't easily convert long[] to int[])
                        return actualType == TypeCodes.TYPE_LONG || actualType == TypeCodes.TYPE_DOUBLE
                                || actualType == TypeCodes.TYPE_INSTANT || actualType == TypeCodes.TYPE_LOCAL_DATE
                                || actualType == TypeCodes.TYPE_LOCAL_DATE_TIME || actualType == TypeCodes.TYPE_DATE;
                    case SCAN_BETWEEN_LONG:
                        // scanBetweenLong supports both int/long columns (scalar casts work)
                        return actualType == TypeCodes.TYPE_LONG || actualType == TypeCodes.TYPE_DOUBLE
                                || actualType == TypeCodes.TYPE_INSTANT || actualType == TypeCodes.TYPE_LOCAL_DATE
                                || actualType == TypeCodes.TYPE_LOCAL_DATE_TIME || actualType == TypeCodes.TYPE_DATE
                                || actualType == TypeCodes.TYPE_INT || actualType == TypeCodes.TYPE_FLOAT
                                || actualType == TypeCodes.TYPE_BOOLEAN || actualType == TypeCodes.TYPE_BYTE
                                || actualType == TypeCodes.TYPE_SHORT || actualType == TypeCodes.TYPE_CHAR;
                    case SCAN_EQUALS_INT:
                    case SCAN_BETWEEN_INT:
                    case SCAN_IN_INT:
                        return actualType == TypeCodes.TYPE_INT || actualType == TypeCodes.TYPE_FLOAT
                                || actualType == TypeCodes.TYPE_BOOLEAN || actualType == TypeCodes.TYPE_BYTE
                                || actualType == TypeCodes.TYPE_SHORT || actualType == TypeCodes.TYPE_CHAR;
                    case SCAN_EQUALS_STRING:
                    case SCAN_EQUALS_STRING_IGNORE_CASE:
                    case SCAN_IN_STRING:
                        return actualType == TypeCodes.TYPE_STRING || actualType == TypeCodes.TYPE_BIG_DECIMAL
                                || actualType == TypeCodes.TYPE_BIG_INTEGER;
                    default:
                        return false;
                }
            }

            private void generateThrowException(MethodVisitor mv, String exceptionType, String message) {
                mv.visitTypeInsn(Opcodes.NEW, exceptionType);
                mv.visitInsn(Opcodes.DUP);
                mv.visitLdcInsn(message);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, exceptionType, "<init>", "(Ljava/lang/String;)V", false);
                mv.visitInsn(Opcodes.ATHROW);
            }
        }
    }

    // ====================================================================================
    // Helper Methods for Per-Column Generation
    // ====================================================================================

    private static String capitalize(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return fieldName;
        }
        // Convert col0 -> Col0, colId -> ColId
        return Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
    }

    private static boolean isLongCompatible(byte typeCode) {
        return typeCode == TypeCodes.TYPE_LONG || typeCode == TypeCodes.TYPE_DOUBLE
                || typeCode == TypeCodes.TYPE_INSTANT || typeCode == TypeCodes.TYPE_LOCAL_DATE
                || typeCode == TypeCodes.TYPE_LOCAL_DATE_TIME || typeCode == TypeCodes.TYPE_DATE;
    }

    private static boolean isIntCompatible(byte typeCode) {
        return typeCode == TypeCodes.TYPE_INT || typeCode == TypeCodes.TYPE_FLOAT
                || typeCode == TypeCodes.TYPE_BOOLEAN || typeCode == TypeCodes.TYPE_BYTE
                || typeCode == TypeCodes.TYPE_SHORT || typeCode == TypeCodes.TYPE_CHAR;
    }

    // ====================================================================================
    // Per-Column Direct Accessor Implementations
    // ====================================================================================

    /**
     * Generates per-column read method with zero dispatch.
     *
     * Equivalent Java code for PersonBytecodeTable.readCol0Long(int rowIndex):
     * 
     * <pre>{@code
     * public long readCol0Long(int rowIndex) {
     *     while (true) {
     *         long version = this.getSeqLock(rowIndex);
     *         if ((version & 1) != 0) {
     *             AbstractTable.backoff(1);
     *             continue;
     *         }
     *         long result = this.col0.get(rowIndex);
     *         if (this.getSeqLock(rowIndex) == version) {
     *             return result;
     *         }
     *     }
     * }
     * }</pre>
     */
    private static class PerColumnReadImplementation implements Implementation {
        private final ColumnFieldInfo field;
        private final int targetTypeCode;

        public PerColumnReadImplementation(ColumnFieldInfo field, int targetTypeCode) {
            this.field = field;
            this.targetTypeCode = targetTypeCode;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new Appender(field, targetTypeCode);
        }

        private static class Appender implements ByteCodeAppender {
            private final ColumnFieldInfo field;
            private final int targetTypeCode;

            public Appender(ColumnFieldInfo field, int targetTypeCode) {
                this.field = field;
                this.targetTypeCode = targetTypeCode;
            }

            @Override
            public Size apply(MethodVisitor mv, Context implementationContext, MethodDescription instrumentedMethod) {
                // Locals: 0=this, 1=rowIndex, 2=version(long), 4=result(long/int/obj)
                Label loopStart = new Label();
                Label retry = new Label();
                Label checkVersion = new Label();

                int resultVar = (targetTypeCode == TypeCodes.TYPE_LONG) ? 4 : 4;

                mv.visitCode();

                // 1. Loop start
                mv.visitLabel(loopStart);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                // 2. version = getSeqLock(rowIndex)
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitVarInsn(Opcodes.ILOAD, 1);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "getSeqLock", "(I)J", false);
                mv.visitVarInsn(Opcodes.LSTORE, 2);

                // 3. if ((version & 1) != 0) goto retry
                mv.visitVarInsn(Opcodes.LLOAD, 2);
                mv.visitInsn(Opcodes.LCONST_1);
                mv.visitInsn(Opcodes.LAND);
                mv.visitInsn(Opcodes.LCONST_0);
                mv.visitInsn(Opcodes.LCMP);
                mv.visitJumpInsn(Opcodes.IFNE, retry);

                // 4. Direct field access - no switch needed
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                String owner = implementationContext.getInstrumentedType().getInternalName();
                String fieldName = field.fieldName();
                String fieldDesc = net.bytebuddy.jar.asm.Type.getDescriptor(field.columnType());
                mv.visitFieldInsn(Opcodes.GETFIELD, owner, fieldName, fieldDesc);

                mv.visitVarInsn(Opcodes.ILOAD, 1); // rowIndex

                String colTypeInternal = net.bytebuddy.jar.asm.Type.getInternalName(field.columnType());
                if (targetTypeCode == TypeCodes.TYPE_LONG) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)J", false);
                    mv.visitVarInsn(Opcodes.LSTORE, resultVar);
                } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)I", false);
                    mv.visitVarInsn(Opcodes.ISTORE, resultVar);
                } else {
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "get", "(I)Ljava/lang/String;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, resultVar);
                }
                mv.visitJumpInsn(Opcodes.GOTO, checkVersion);

                // 5. Check version - use F_FULL to explicitly list all locals
                mv.visitLabel(checkVersion);
                String ownerInternal = implementationContext.getInstrumentedType().getInternalName();
                if (targetTypeCode == TypeCodes.TYPE_LONG) {
                    // locals: this, rowIndex(int), version(long), result(long)
                    // Note: long counts as 1 in stackmap frame (second slot is implicit)
                    mv.visitFrame(Opcodes.F_FULL, 4,
                            new Object[] { ownerInternal, Opcodes.INTEGER, Opcodes.LONG, Opcodes.LONG }, 0, null);
                } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                    // locals: this, rowIndex(int), version(long), result(int)
                    mv.visitFrame(Opcodes.F_FULL, 4,
                            new Object[] { ownerInternal, Opcodes.INTEGER, Opcodes.LONG, Opcodes.INTEGER }, 0, null);
                } else {
                    // locals: this, rowIndex(int), version(long), result(String)
                    mv.visitFrame(Opcodes.F_FULL, 4,
                            new Object[] { ownerInternal, Opcodes.INTEGER, Opcodes.LONG, "java/lang/String" }, 0, null);
                }
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitVarInsn(Opcodes.ILOAD, 1);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "getSeqLock", "(I)J", false);
                mv.visitVarInsn(Opcodes.LLOAD, 2);
                mv.visitInsn(Opcodes.LCMP);
                mv.visitJumpInsn(Opcodes.IFNE, retry);

                // 6. Return result
                if (targetTypeCode == TypeCodes.TYPE_LONG) {
                    mv.visitVarInsn(Opcodes.LLOAD, resultVar);
                    mv.visitInsn(Opcodes.LRETURN);
                } else if (targetTypeCode == TypeCodes.TYPE_INT) {
                    mv.visitVarInsn(Opcodes.ILOAD, resultVar);
                    mv.visitInsn(Opcodes.IRETURN);
                } else {
                    mv.visitVarInsn(Opcodes.ALOAD, resultVar);
                    mv.visitInsn(Opcodes.ARETURN);
                }

                // 7. Retry logic - use F_FULL to match loop start state (only this, rowIndex)
                mv.visitLabel(retry);
                mv.visitFrame(Opcodes.F_FULL, 2,
                        new Object[] { ownerInternal, Opcodes.INTEGER }, 0, null);
                mv.visitIntInsn(Opcodes.BIPUSH, 1);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        net.bytebuddy.jar.asm.Type.getInternalName(AbstractTable.class),
                        "backoff", "(I)V", false);
                mv.visitJumpInsn(Opcodes.GOTO, loopStart);

                mv.visitMaxs(6, 8);
                return new Size(6, 8);
            }
        }
    }

    /**
     * Generates per-column scan method with zero dispatch.
     *
     * Equivalent Java code for PersonBytecodeTable.scanEqualsCol0Long(long value):
     * 
     * <pre>{@code
     * public int[] scanEqualsCol0Long(long value) {
     *     int limit = (int) this.allocatedCount();
     *     int[] results = this.col0.scanEquals(value, limit);
     *     return filterTombstoned(results);
     * }
     * }</pre>
     */
    private static class PerColumnScanImplementation implements Implementation {
        private final ColumnFieldInfo field;
        private final int scanType;

        public PerColumnScanImplementation(ColumnFieldInfo field, int scanType) {
            this.field = field;
            this.scanType = scanType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return new Appender(field, scanType);
        }

        private static class Appender implements ByteCodeAppender {
            private final ColumnFieldInfo field;
            private final int scanType;

            public Appender(ColumnFieldInfo field, int scanType) {
                this.field = field;
                this.scanType = scanType;
            }

            @Override
            public Size apply(MethodVisitor mv, Context implementationContext, MethodDescription instrumentedMethod) {
                mv.visitCode();

                // 1. Get limit = (int) allocatedCount()
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "allocatedCount", "()J", false);
                mv.visitInsn(Opcodes.L2I);
                int limitVar = (scanType == ScanMethodImplementation.SCAN_EQUALS_LONG
                        || scanType == ScanMethodImplementation.SCAN_BETWEEN_LONG
                        || scanType == ScanMethodImplementation.SCAN_IN_LONG) ? 3 : 2;
                mv.visitVarInsn(Opcodes.ISTORE, limitVar);

                // 2. Load column field
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                String owner = implementationContext.getInstrumentedType().getInternalName();
                String colTypeInternal = net.bytebuddy.jar.asm.Type.getInternalName(field.columnType());
                mv.visitFieldInsn(Opcodes.GETFIELD, owner, field.fieldName(),
                        net.bytebuddy.jar.asm.Type.getDescriptor(field.columnType()));

                // 3. Load value argument and limit, invoke scan method
                if (scanType == ScanMethodImplementation.SCAN_EQUALS_LONG) {
                    mv.visitVarInsn(Opcodes.LLOAD, 1); // value (long)
                    mv.visitVarInsn(Opcodes.ILOAD, limitVar);
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals", "(JI)[I", false);
                } else if (scanType == ScanMethodImplementation.SCAN_EQUALS_INT) {
                    mv.visitVarInsn(Opcodes.ILOAD, 1); // value (int)
                    mv.visitVarInsn(Opcodes.ILOAD, limitVar);
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals", "(II)[I", false);
                } else if (scanType == ScanMethodImplementation.SCAN_EQUALS_STRING) {
                    mv.visitVarInsn(Opcodes.ALOAD, 1); // value (String)
                    mv.visitVarInsn(Opcodes.ILOAD, limitVar);
                    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, colTypeInternal, "scanEquals",
                            "(Ljava/lang/String;I)[I", false);
                }

                // 4. Call filterTombstoned(results)
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitInsn(Opcodes.SWAP);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        implementationContext.getInstrumentedType().getSuperClass().asErasure().getInternalName(),
                        "filterTombstoned", "([I)[I", false);

                // 5. Return
                mv.visitInsn(Opcodes.ARETURN);

                mv.visitMaxs(5, 5);
                return new Size(5, 5);
            }
        }
    }
}
