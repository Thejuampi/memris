package io.memris.spring;

import io.memris.storage.ffm.FfmTable;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.spring.QueryMethodParser;
import io.memris.kernel.Predicate;
import io.memris.spring.converter.TypeConverterRegistry;
import io.memris.spring.converter.TypeConverter;

import jakarta.persistence.Entity;
import jakarta.persistence.Enumerated;
import jakarta.persistence.EnumType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Transient;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PreUpdate;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Extracts metadata from entity classes for repository generation.
 * All reflection work happens once during metadata extraction.
 */
public final class MetadataExtractor {
    // Type code constants for zero-overhead type switching
    private static final int TYPE_INT = 0;
    private static final int TYPE_LONG = 1;
    private static final int TYPE_BOOLEAN = 2;
    private static final int TYPE_BYTE = 3;
    private static final int TYPE_SHORT = 4;
    private static final int TYPE_FLOAT = 5;
    private static final int TYPE_DOUBLE = 6;
    private static final int TYPE_CHAR = 7;
    private static final int TYPE_STRING = 8;

    /**
     * Get type code for a storage type.
     * Uses switch on class literal (JIT-optimized to jump table, no boxing).
     * Called once during metadata extraction.
     */
    private static int getTypeCode(Class<?> type) {
        if (type == int.class || type == Integer.class) return TYPE_INT;
        if (type == long.class || type == Long.class) return TYPE_LONG;
        if (type == boolean.class || type == Boolean.class) return TYPE_BOOLEAN;
        if (type == byte.class || type == Byte.class) return TYPE_BYTE;
        if (type == short.class || type == Short.class) return TYPE_SHORT;
        if (type == float.class || type == Float.class) return TYPE_FLOAT;
        if (type == double.class || type == Double.class) return TYPE_DOUBLE;
        if (type == char.class || type == Character.class) return TYPE_CHAR;
        if (type == String.class) return TYPE_STRING;
        return -1;
    }
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts metadata from entity classes and repository interfaces.
 * All reflection work happens here, once at repository creation.
 *
 * Field accessor detection strategy:
 * 1. For records: Use record accessor methods (fieldName())
 * 2. For non-records: Try JavaBean style (get/set) first, then Kotlin-style (property name)
 */
public final class MetadataExtractor {

    public static <T> EntityMetadata<T> extractEntityMetadata(Class<T> entityClass, FfmTable table) {
        try {
            // Check if entity is a record
            boolean isRecord = entityClass.isRecord();

            // Get ID column name
            String idColumnName = findIdColumnName(entityClass);

            // Get entity constructor (for records, use the canonical constructor)
            Constructor<T> constructor = isRecord
                ? findCanonicalConstructor(entityClass)
                : entityClass.getDeclaredConstructor();

            // Extract field mappings
            List<EntityMetadata.FieldMapping> fields = new ArrayList<>();
            Set<String> foreignKeyColumns = new HashSet<>();

            // Extract TypeConverters for each field
            Map<String, TypeConverter<?, ?>> converters = new HashMap<>();

            // Extract field accessor MethodHandles
            Map<String, MethodHandle> fieldGetters = new HashMap<>();
            Map<String, MethodHandle> fieldSetters = new HashMap<>();

            MethodHandles.Lookup lookup = MethodHandles.lookup();

            if (isRecord) {
                // For records, use record components
                for (RecordComponent component : entityClass.getRecordComponents()) {
                    String fieldName = component.getName();
                    Class<?> fieldType = component.getType();

                    // Skip transient fields
                    if (isTransientField(entityClass, fieldName)) {
                        continue;
                    }

                    String columnName = fieldName;
                    Class<?> javaType = fieldType;
                    Class<?> storageType = javaType;

                    // Handle TypeConverter
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(javaType);
                    if (converter != null) {
                        storageType = converter.getStorageType();
                        converters.put(fieldName, converter);
                    }

                    // Get column position
                    int columnPosition = findColumnPosition(table, columnName);

                    // Pre-compute type code once (no runtime overhead)
                    int typeCode = getTypeCode(storageType);

                    EntityMetadata.FieldMapping mapping = new EntityMetadata.FieldMapping(
                            fieldName,
                            columnName,
                            javaType,
                            storageType,
                            columnPosition,
                            typeCode
                    );
                    fields.add(mapping);

                    // For records: accessor is fieldName() (no "get" prefix)
                    MethodHandle getter = lookup.unreflect(component.getAccessor());
                    fieldGetters.put(fieldName, getter);

                    // Records are immutable - no setters
                    fieldSetters.put(fieldName, null);
                }
            } else {
                // For non-records: scan declared fields
                for (Field field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Transient.class)) {
                        continue;
                    }

                    String fieldName = field.getName();
                    String columnName = field.getName();
                    Class<?> javaType = field.getType();
                    Class<?> storageType = javaType;

                    // Handle TypeConverter
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(javaType);
                    if (converter != null) {
                        storageType = converter.getStorageType();
                        converters.put(fieldName, converter);
                    }

                    // Get column position
                    int columnPosition = findColumnPosition(table, columnName);

                    // Pre-compute type code once (no runtime overhead)
                    int typeCode = getTypeCode(storageType);

                    EntityMetadata.FieldMapping mapping = new EntityMetadata.FieldMapping(
                            fieldName,
                            columnName,
                            javaType,
                            storageType,
                            columnPosition,
                            typeCode
                    );
                    fields.add(mapping);

                    // Extract getter and setter MethodHandles
                    // Try JavaBean style first: getFieldName() / setFieldName(value)
                    // Then try Kotlin-style: fieldName() / fieldName(value)
                    MethodHandle getter = findGetter(lookup, entityClass, fieldName, javaType);
                    MethodHandle setter = findSetter(lookup, entityClass, fieldName, javaType);

                    fieldGetters.put(fieldName, getter);
                    fieldSetters.put(fieldName, setter);
                }

                // Track foreign key columns
                for (EntityMetadata.FieldMapping field : fields) {
                    if (field.columnName().endsWith("_id") && !field.columnName().equals(idColumnName)) {
                        foreignKeyColumns.add(field.columnName());
                    }
                }
            }

            // Extract lifecycle callback MethodHandles
            MethodHandle prePersistHandle = null;
            MethodHandle postLoadHandle = null;
            MethodHandle preUpdateHandle = null;

            for (Method m : entityClass.getDeclaredMethods()) {
                if (m.getParameterCount() == 0 && m.getReturnType() == void.class) {
                    m.setAccessible(true);
                    if (m.isAnnotationPresent(PrePersist.class)) {
                        prePersistHandle = lookup.unreflect(m);
                    } else if (m.isAnnotationPresent(PostLoad.class)) {
                        postLoadHandle = lookup.unreflect(m);
                    } else if (m.isAnnotationPresent(PreUpdate.class)) {
                        preUpdateHandle = lookup.unreflect(m);
                    }
                }
            }

            return new EntityMetadata<>(entityClass, constructor, idColumnName,
                fields, foreignKeyColumns, converters,
                prePersistHandle, postLoadHandle, preUpdateHandle,
                fieldGetters, fieldSetters, isRecord);

        } catch (Exception e) {
            throw new RuntimeException("Failed to extract metadata for entity: " + entityClass.getName(), e);
        }
    }

    /**
     * Finds a getter method for a field, trying multiple conventions.
     *
     * Conventions tried (in order):
     * 1. JavaBean: getField()
     * 2. JavaBean (boolean): isField()
     * 3. Kotlin/Immutable: field()
     * 4. Fallback: direct field access via MethodHandles
     */
    private static MethodHandle findGetter(MethodHandles.Lookup lookup, Class<?> entityClass,
            String fieldName, Class<?> fieldType) throws Exception {
        String capitalizedFieldName = capitalize(fieldName);

        // Try JavaBean style: getField()
        try {
            Method getter = entityClass.getDeclaredMethod("get" + capitalizedFieldName);
            return lookup.unreflect(getter);
        } catch (NoSuchMethodException e) {
            // Continue
        }

        // Try JavaBean style for boolean: isField()
        if (fieldType == boolean.class || fieldType == Boolean.class) {
            try {
                Method getter = entityClass.getDeclaredMethod("is" + capitalizedFieldName);
                return lookup.unreflect(getter);
            } catch (NoSuchMethodException e) {
                // Continue
            }
        }

        // Try Kotlin-style/immutable: field()
        try {
            Method getter = entityClass.getDeclaredMethod(fieldName);
            if (getter.getParameterCount() == 0 &&
                getter.getReturnType().equals(fieldType)) {
                return lookup.unreflect(getter);
            }
        } catch (NoSuchMethodException e) {
            // Continue
        }

        // Fallback: direct field access via MethodHandles
        // This handles private fields without getters
        try {
            return lookup.findGetter(entityClass, fieldName, fieldType);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(
                "Cannot find getter for field '" + fieldName + "' in " + entityClass.getName() +
                ". Tried: get" + capitalizedFieldName + "(), " + fieldName + "(), and direct field access.", e);
        }
    }

    /**
     * Finds a setter method for a field, trying multiple conventions.
     *
     * Conventions tried (in order):
     * 1. JavaBean: setField(value)
     * 2. Kotlin/Builder: field(value)
     * 3. Fallback: direct field access via MethodHandles
     */
    private static MethodHandle findSetter(MethodHandles.Lookup lookup, Class<?> entityClass,
            String fieldName, Class<?> fieldType) throws Exception {
        String capitalizedFieldName = capitalize(fieldName);

        // Try JavaBean style: setField(value)
        try {
            Method setter = entityClass.getDeclaredMethod("set" + capitalizedFieldName, fieldType);
            return lookup.unreflect(setter);
        } catch (NoSuchMethodException e) {
            // Continue
        }

        // Try Kotlin-style/builder: field(value)
        try {
            Method setter = entityClass.getDeclaredMethod(fieldName, fieldType);
            if (setter.getParameterCount() == 1 && setter.getReturnType() == void.class) {
                return lookup.unreflect(setter);
            }
        } catch (NoSuchMethodException e) {
            // Continue
        }

        // Fallback: direct field access via MethodHandles
        try {
            return lookup.findSetter(entityClass, fieldName, fieldType);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(
                "Cannot find setter for field '" + fieldName + "' in " + entityClass.getName() +
                ". Tried: set" + capitalizedFieldName + "(" + fieldType.getSimpleName() + "), " +
                fieldName + "(" + fieldType.getSimpleName() + "), and direct field access.", e);
        }
    }

    /**
     * Finds the canonical constructor for a record class.
     * The canonical constructor has parameters matching all record components.
     */
    @SuppressWarnings("unchecked")
    private static <T> Constructor<T> findCanonicalConstructor(Class<T> recordClass) throws Exception {
        RecordComponent[] components = recordClass.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            paramTypes[i] = components[i].getType();
        }
        return (Constructor<T>) recordClass.getDeclaredConstructor(paramTypes);
    }

    /**
     * Checks if a field in a record class should be treated as transient.
     * Records don't support @Transient on fields, so we check for a static method.
     */
    private static boolean isTransientField(Class<?> recordClass, String fieldName) {
        try {
            // Check for static isFieldNameTransient() method
            java.lang.reflect.Method method = recordClass.getDeclaredMethod("is" + capitalize(fieldName) + "Transient");
            return java.lang.reflect.Modifier.isStatic(method.getModifiers()) &&
                   method.getReturnType() == boolean.class &&
                   method.getParameterCount() == 0 &&
                   (boolean) method.invoke(null);
        } catch (Exception e) {
            return false;
        }
    }

    private static int findColumnPosition(FfmTable table, String columnName) {
        int colIndex = 0;
        for (var col : table.columns()) {
            if (col.name().equals(columnName)) {
                return colIndex;
            }
            colIndex++;
        }
        return -1;
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    public static List<QueryMetadata> extractQueryMethods(Class<?> repositoryInterface) {
        List<QueryMetadata> queries = new ArrayList<>();

        for (Method method : repositoryInterface.getDeclaredMethods()) {
            if (QueryMethodParser.isQueryMethod(method)) {
                QueryMetadata metadata = parseQueryMethod(method);
                queries.add(metadata);
            }
        }

        return queries;
    }

    private static QueryMetadata parseQueryMethod(Method method) {
        QueryMethodParser.ParsedQueryResult parsed = QueryMethodParser.parseQuery(method);

        // Extract conditions
        List<QueryMetadata.Condition> conditions = new ArrayList<>();
        Parameter[] parameters = method.getParameters();

        int paramIndex = 0;
        for (int i = 0; i < parsed.conditions().size(); i++) {
            String columnName = parsed.conditions().get(i);
            Predicate.Operator operator = parsed.operators().get(i);

            // Handle BETWEEN (uses 2 parameters)
            if (operator == Predicate.Operator.BETWEEN) {
                conditions.add(new QueryMetadata.Condition(columnName, operator, paramIndex));
                conditions.add(new QueryMetadata.Condition(columnName, operator, paramIndex + 1));
                paramIndex += 2;
            } else if (operator == Predicate.Operator.IN || operator == Predicate.Operator.NOT_IN) {
                // Collection parameter
                conditions.add(new QueryMetadata.Condition(columnName, operator, paramIndex));
                paramIndex += 1;
            } else {
                // Single parameter
                conditions.add(new QueryMetadata.Condition(columnName, operator, paramIndex));
                paramIndex += 1;
            }
        }

        return new QueryMetadata(
                method,
                method.getName(),
                method.getReturnType(),
                parsed.queryType(),
                parsed.limit(),
                parsed.isDistinct(),
                conditions,
                parsed.orders()
        );
    }

    private static String findIdColumnName(Class<?> entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                return field.getName();
            }
        }
        throw new IllegalArgumentException("No ID field found for entity: " + entityClass.getName());
    }

    private MetadataExtractor() {}
}
