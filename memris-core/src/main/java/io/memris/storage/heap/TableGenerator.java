package io.memris.storage.heap;

import io.memris.core.MemrisConfiguration;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import java.lang.reflect.Field;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import io.memris.core.TypeCodes;

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
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends AbstractTable> generate(TableMetadata metadata, MemrisConfiguration configuration) {
        String className = metadata.entityName() + "Table";
        String packageName = "io.memris.storage.generated";

        ByteBuddy byteBuddy = new ByteBuddy();

        DynamicType.Builder<AbstractTable> builder = byteBuddy
                .subclass(AbstractTable.class)
                .implement(io.memris.storage.GeneratedTable.class)
                .name(packageName + "." + className)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Collect column field info
        List<TableImplementationStrategy.ColumnFieldInfo> columnFields = new ArrayList<>();
        int idx = 0;
        
        // Add fields for each column
        for (FieldMetadata field : metadata.fields()) {
            String columnFieldName = field.name() + "Column";
            Class<?> columnType = getColumnType(field.type());
            byte typeCode = field.type();

            builder = builder.defineField(columnFieldName,
                    columnType,
                    Visibility.PUBLIC,
                    FieldManifestation.FINAL);

            columnFields.add(new TableImplementationStrategy.ColumnFieldInfo(
                columnFieldName, columnType, typeCode, idx++));
        }

        // Add ID index field
        Class<?> idIndexType = getIdIndexType(metadata.idTypeCode());
        builder = builder.defineField("idIndex", idIndexType, Visibility.PUBLIC);

        // Add TYPE_CODES field (instance final)
        builder = builder.defineField("TYPE_CODES", byte[].class, Visibility.PRIVATE, FieldManifestation.FINAL);

        // Add constructor using MethodCall to super constructor and then field initialization
        try {
            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(int.class, int.class)
                    .intercept(MethodCall.invoke(AbstractTable.class.getDeclaredConstructor(String.class, int.class, int.class))
                            .with(metadata.entityName())
                            .withArgument(0)
                            .withArgument(1)
                            .andThen(MethodDelegation.to(new ConstructorInterceptor(columnFields, idIndexType))));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find AbstractTable constructor", e);
        }

        // Implement all GeneratedTable interface methods using pluggable strategy
        TableImplementationStrategy strategy = configuration.tableImplementation() == MemrisConfiguration.TableImplementation.BYTECODE
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
            int pageSize = (int) args[0];
            int maxPages = (int) args[1];
            int capacity = pageSize * maxPages;

            // Initialize column fields
            for (TableImplementationStrategy.ColumnFieldInfo field : columnFields) {
                Field declaredField = obj.getClass().getDeclaredField(field.fieldName());
                declaredField.setAccessible(true);
                Object columnInstance = field.columnType()
                        .getDeclaredConstructor(int.class)
                        .newInstance(capacity);
                declaredField.set(obj, columnInstance);
            }

            // Initialize idIndex field
            Field idIndexField = obj.getClass().getDeclaredField("idIndex");
            idIndexField.setAccessible(true);
            Object idIndexInstance = idIndexType
                    .getDeclaredConstructor(int.class)
                    .newInstance(DEFAULT_ID_INDEX_CAPACITY);
            idIndexField.set(obj, idIndexInstance);

            // Initialize static TYPE_CODES field
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
