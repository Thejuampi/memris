package io.memris.storage.heap;

import io.memris.core.MemrisConfiguration;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.List;

public final class TableGenerator {

    private static final int DEFAULT_ID_INDEX_CAPACITY = 16;

    /**
     * Generate a table class from entity metadata using default configuration.
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends AbstractTable> generate(TableMetadata metadata) {
        return generate(metadata, MemrisConfiguration.builder().build());
    }

    /**
     * Generate a table class from entity metadata with specified configuration.
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * final class PersonTable extends AbstractTable implements GeneratedTable {
     *     public final PageColumnLong idColumn;
     *     public final PageColumnString nameColumn;
     *     public final PageColumnInt ageColumn;
     *     public LongIdIndex idIndex; // or StringIdIndex depending on id type
     *     private final byte[] TYPE_CODES;
     *
     *     public PersonTable(int pageSize, int maxPages, int initialPages) {
     *         super("Person", pageSize, maxPages, initialPages);
     *         // ConstructorInterceptor initializes column fields, idIndex and
     *         // TYPE_CODES.
     *     }
     * }
     * }</pre>
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends AbstractTable> generate(TableMetadata metadata, MemrisConfiguration configuration) {
        var className = metadata.entityName() + "Table";
        var packageName = "io.memris.storage.generated";

        var byteBuddy = new ByteBuddy();

        var builder = byteBuddy
                .subclass(AbstractTable.class)
                .implement(io.memris.storage.GeneratedTable.class)
                .name(packageName + "." + className)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Collect column field info
        List<TableImplementationStrategy.ColumnFieldInfo> columnFields = new ArrayList<>();
        var idx = 0;
        
        // Add fields for each column
        for (FieldMetadata field : metadata.fields()) {
            var columnFieldName = field.name() + "Column";
            var columnType = getColumnType(field.type());
            var typeCode = field.type();

            builder = builder.defineField(columnFieldName,
                    columnType,
                    Visibility.PUBLIC,
                    FieldManifestation.FINAL);

            columnFields.add(new TableImplementationStrategy.ColumnFieldInfo(
                columnFieldName, columnType, typeCode, idx++));
        }

        // Add ID index field
        var idIndexType = getIdIndexType(metadata.idTypeCode());
        builder = builder.defineField("idIndex", idIndexType, Visibility.PUBLIC);

        // Add TYPE_CODES field (instance final)
        builder = builder.defineField("TYPE_CODES", byte[].class, Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add cached Field arrays for bytecode implementations
        builder = builder.defineField("CACHED_COLUMN_FIELDS", Field[].class, Visibility.PRIVATE, FieldManifestation.FINAL);
        builder = builder.defineField("ID_INDEX_FIELD", Field.class, Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add constructor using MethodCall to super constructor and then field initialization
        try {
            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(int.class, int.class, int.class)
                    .intercept(MethodCall.invoke(AbstractTable.class.getDeclaredConstructor(String.class, int.class, int.class, int.class))
                            .with(metadata.entityName())
                            .withArgument(0)
                            .withArgument(1)
                            .withArgument(2)
                            .andThen(MethodDelegation.to(new ConstructorInterceptor(columnFields, idIndexType))));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find AbstractTable constructor", e);
        }

        // Implement all GeneratedTable interface methods using pluggable strategy
        var strategy = configuration.tableImplementation() == MemrisConfiguration.TableImplementation.BYTECODE
                ? new BytecodeImplementation()
                : new MethodHandleImplementation();
        builder = strategy.implementMethods(builder, columnFields, idIndexType);

        // Load the class
        return builder.make()
                .load(TableGenerator.class.getClassLoader())
                .getLoaded();
    }

    /**
     * Interceptor for constructor initialization with TYPE_CODES.
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * this.idColumn = new PageColumnLong(pageSize, maxPages, initialPages);
     * this.nameColumn = new PageColumnString(pageSize, maxPages, initialPages);
     * this.ageColumn = new PageColumnInt(pageSize, maxPages, initialPages);
     * this.idIndex = new LongIdIndex(DEFAULT_ID_INDEX_CAPACITY);
     * this.TYPE_CODES = new byte[] { TYPE_LONG, TYPE_STRING, TYPE_INT };
     * }</pre>
     */
    public static class ConstructorInterceptor {
        private final List<TableImplementationStrategy.ColumnFieldInfo> columnFields;
        private final Class<?> idIndexType;

        public ConstructorInterceptor(List<TableImplementationStrategy.ColumnFieldInfo> columnFields, Class<?> idIndexType) {
            this.columnFields = columnFields;
            this.idIndexType = idIndexType;
        }

        @RuntimeType
        public void intercept(@This Object obj, @AllArguments Object[] args) throws Exception {
            var pageSize = (int) args[0];
            var maxPages = (int) args[1];
            var initialPages = (int) args[2];

            // Initialize column fields
            var cachedColumnFields = new Field[columnFields.size()];
            for (TableImplementationStrategy.ColumnFieldInfo field : columnFields) {
                var declaredField = obj.getClass().getDeclaredField(field.fieldName());
                declaredField.setAccessible(true);
                var columnInstance = field.columnType()
                        .getDeclaredConstructor(int.class, int.class, int.class)
                        .newInstance(pageSize, maxPages, initialPages);
                declaredField.set(obj, columnInstance);
                cachedColumnFields[field.index()] = declaredField;
            }

            // Initialize idIndex field
            var idIndexField = obj.getClass().getDeclaredField("idIndex");
            idIndexField.setAccessible(true);
            var idIndexInstance = idIndexType
                    .getDeclaredConstructor(int.class)
                    .newInstance(DEFAULT_ID_INDEX_CAPACITY);
            idIndexField.set(obj, idIndexInstance);

            var cachedFieldsField = obj.getClass().getDeclaredField("CACHED_COLUMN_FIELDS");
            cachedFieldsField.setAccessible(true);
            cachedFieldsField.set(obj, cachedColumnFields);

            var cachedIdIndexField = obj.getClass().getDeclaredField("ID_INDEX_FIELD");
            cachedIdIndexField.setAccessible(true);
            cachedIdIndexField.set(obj, idIndexField);

            // Initialize static TYPE_CODES field
            var typeCodes = new byte[columnFields.size()];
            for (int i = 0; i < columnFields.size(); i++) {
                typeCodes[i] = columnFields.get(i).typeCode();
            }
            
            var typeCodesField = obj.getClass().getDeclaredField("TYPE_CODES");
            typeCodesField.setAccessible(true);
            typeCodesField.set(obj, typeCodes);

            // Pages are allocated lazily by columns on demand.
        }
    }

    /**
     * Get column type for field type code.
     */
    private static Class<?> getColumnType(byte typeCode) {
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_LONG -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_INT -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_STRING -> PageColumnString.class;
            case io.memris.core.TypeCodes.TYPE_DOUBLE -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_FLOAT -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_BOOLEAN -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_BYTE -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_SHORT -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_CHAR -> PageColumnInt.class;
            case io.memris.core.TypeCodes.TYPE_INSTANT -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE_TIME -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_DATE -> PageColumnLong.class;
            case io.memris.core.TypeCodes.TYPE_BIG_DECIMAL -> PageColumnString.class;
            case io.memris.core.TypeCodes.TYPE_BIG_INTEGER -> PageColumnString.class;
            default -> throw new IllegalArgumentException("Unsupported type code: " + typeCode);
        };
    }

    /**
     * Get ID index type for ID type code.
     */
    private static Class<?> getIdIndexType(byte typeCode) {
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_LONG -> LongIdIndex.class;
            case io.memris.core.TypeCodes.TYPE_INT -> LongIdIndex.class;
            case io.memris.core.TypeCodes.TYPE_STRING -> StringIdIndex.class;
            default -> throw new IllegalArgumentException("Unsupported ID type code: " + typeCode);
        };
    }
}
