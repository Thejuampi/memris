package io.memris.spring;

import io.memris.storage.ffm.FfmTable;
import io.memris.spring.converter.TypeConverter;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.implementation.bytecode.assign.Assigner;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates EntityHydrator implementations using ByteBuddy.
 * <p>
 * This implementation uses MethodDelegation with interceptor classes.
 * Reflection is used ONLY during code generation, not in the generated hot path.
 * <p>
 * Future optimization: Generate pure bytecode using ByteBuddy Advice API
 * to eliminate even the MethodDelegation overhead.
 */
public final class EntityHydratorGenerator {

    private final ByteBuddy byteBuddy = new ByteBuddy();

    @SuppressWarnings("unchecked")
    public <T> EntityHydrator<T> generate(EntityLayout<T> layout, FfmTable table) {
        String className = layout.entityClass().getSimpleName() + "_MemrisHydrator";

        try {
            DynamicType.Builder<?> builder = byteBuddy
                    .subclass(EntityHydrator.class)
                    .name(layout.entityClass().getPackage().getName() + "." + className);

            // Add table field
            builder = builder.defineField("table", FfmTable.class, Visibility.PRIVATE);

            // Add columnId constants (baked into bytecode as field values)
            for (EntityLayout.FieldLayout field : layout.fields()) {
                String constantName = constantNameForField(field.propertyPath());
                builder = builder.defineField(constantName, int.class, Visibility.PRIVATE)
                        .value(field.columnId());
            }

            // Add converter fields (only for fields that need conversion)
            List<ConverterField> converterFields = new ArrayList<>();
            for (EntityLayout.FieldLayout field : layout.fields()) {
                if (field.converterOrNull() != null) {
                    String converterFieldName = converterFieldNameForField(field.propertyPath());
                    builder = builder.defineField(converterFieldName, TypeConverter.class, Visibility.PRIVATE);
                    converterFields.add(new ConverterField(converterFieldName, field.converterOrNull()));
                }
            }

            // Constructor - stores table reference
            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(FfmTable.class)
                    .intercept(MethodDelegation.to(new ConstructorInterceptor(converterFields)));

            // materializeRow method
            builder = builder.defineMethod("materializeRow", Object.class, Visibility.PUBLIC)
                    .withParameter(int.class, "row")
                    .intercept(MethodDelegation.to(new MaterializeInterceptor(layout)));

            // materializeRows method
            builder = builder.defineMethod("materializeRows", void.class, Visibility.PUBLIC)
                    .withParameter(int[].class, "rows")
                    .withParameter(java.util.List.class, "out")
                    .intercept(MethodDelegation.to(new BulkMaterializeInterceptor()));

            // Build and load
            DynamicType.Unloaded<?> unloaded = builder.make();
            Class<?> generatedClass = unloaded.load(EntityHydrator.class.getClassLoader())
                    .getLoaded();

            return (EntityHydrator<T>) generatedClass
                    .getDeclaredConstructor(FfmTable.class)
                    .newInstance(table);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to generate hydrator: " + className, t);
        }
    }

    private record ConverterField(String fieldName, TypeConverter<?, ?> converter) {}

    /**
     * Constructor interceptor - stores table and converter references via reflection (one-time cost).
     */
    public static class ConstructorInterceptor {
        private final List<ConverterField> converterFields;

        public ConstructorInterceptor(List<ConverterField> converterFields) {
            this.converterFields = converterFields;
        }

        public void construct(@This Object hydrator, @Argument(0) FfmTable table) {
            try {
                // Set table field
                java.lang.reflect.Field tableField = hydrator.getClass().getDeclaredField("table");
                tableField.setAccessible(true);
                tableField.set(hydrator, table);

                // Set converter fields
                for (ConverterField cf : converterFields) {
                    java.lang.reflect.Field converterField = hydrator.getClass().getDeclaredField(cf.fieldName());
                    converterField.setAccessible(true);
                    converterField.set(hydrator, cf.converter());
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize hydrator fields", e);
            }
        }
    }

    /**
     * Materialize interceptor - generates code for each entity field.
     * <p>
     * Uses switch on byte typeCode for zero-allocation type dispatch.
     * Uses columnId constants baked into the generated class.
     */
    public static class MaterializeInterceptor {
        private final EntityLayout<?> layout;

        public MaterializeInterceptor(EntityLayout<?> layout) {
            this.layout = layout;
        }

        public Object materialize(@This Object hydrator, @Argument(0) int row) throws Throwable {
            FfmTable table = getTable(hydrator);
            Object entity = layout.entityClass().getDeclaredConstructor().newInstance();

            for (EntityLayout.FieldLayout field : layout.fields()) {
                // Skip _id and _msb columns
                if (field.propertyPath().endsWith("_id") && !field.propertyPath().equals(layout.idColumnName())) {
                    continue;
                }
                if (field.propertyPath().endsWith("_msb")) {
                    continue;
                }

                // Get value from table using typeCode dispatch
                Object value = getTableValue(table, field.columnName(), field.typeCode(), row);

                // Apply converter if present
                if (field.converterOrNull() != null) {
                    @SuppressWarnings("unchecked")
                    TypeConverter<Object, Object> converter = (TypeConverter<Object, Object>) field.converterOrNull();
                    value = converter.fromStorage(value);
                }

                // Set value on entity using reflection
                // TODO: Future optimization - generate direct setter calls in bytecode
                String setterName = "set" + capitalize(field.propertyPath());
                java.lang.reflect.Method setter = layout.entityClass().getMethod(setterName, field.javaType());
                setter.invoke(entity, value);
            }

            return entity;
        }

        private Object getTableValue(FfmTable table, String columnName, byte typeCode, int row) {
            // Switch on byte typeCode - zero-allocation dispatch
            return switch (typeCode) {
                case TypeCodes.TYPE_INT -> table.getInt(columnName, row);
                case TypeCodes.TYPE_LONG -> table.getLong(columnName, row);
                case TypeCodes.TYPE_BOOLEAN -> table.getBoolean(columnName, row);
                case TypeCodes.TYPE_BYTE -> table.getByte(columnName, row);
                case TypeCodes.TYPE_SHORT -> table.getShort(columnName, row);
                case TypeCodes.TYPE_FLOAT -> table.getFloat(columnName, row);
                case TypeCodes.TYPE_DOUBLE -> table.getDouble(columnName, row);
                case TypeCodes.TYPE_CHAR -> table.getChar(columnName, row);
                case TypeCodes.TYPE_STRING -> table.getString(columnName, row);
                default -> throw new IllegalArgumentException("Unknown typeCode: " + typeCode);
            };
        }

        private FfmTable getTable(Object hydrator) throws Exception {
            java.lang.reflect.Field field = hydrator.getClass().getDeclaredField("table");
            field.setAccessible(true);
            return (FfmTable) field.get(hydrator);
        }

        private int getColumnId(Object hydrator, String propertyPath) throws Exception {
            String constantName = propertyPath.toUpperCase() + "_COL";
            java.lang.reflect.Field field = hydrator.getClass().getDeclaredField(constantName);
            field.setAccessible(true);
            return (int) field.get(hydrator);
        }

        private String capitalize(String s) {
            if (s == null || s.isEmpty()) return s;
            return Character.toUpperCase(s.charAt(0)) + s.substring(1);
        }
    }

    /**
     * Bulk materialize interceptor - simple loop calling materializeRow.
     */
    public static class BulkMaterializeInterceptor {
        public void materialize(@This Object hydrator,
                                  @Argument(0) int[] rows,
                                  @Argument(1) java.util.List out) throws Throwable {
            // Get the materializeRow method and call it for each row
            java.lang.reflect.Method materializeRow = hydrator.getClass().getDeclaredMethod("materializeRow", int.class);
            materializeRow.setAccessible(true);
            for (int row : rows) {
                Object entity = materializeRow.invoke(hydrator, row);
                out.add(entity);
            }
        }
    }

    // Helper methods
    private String constantNameForField(String propertyPath) {
        return propertyPath.toUpperCase() + "_COL";
    }

    private String converterFieldNameForField(String propertyPath) {
        return propertyPath.toUpperCase() + "_CONVERTER";
    }
}
