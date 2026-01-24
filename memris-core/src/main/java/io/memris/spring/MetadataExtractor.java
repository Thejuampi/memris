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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts metadata from entity classes and repository interfaces.
 * All reflection work happens here, once at repository creation.
 */
public final class MetadataExtractor {

    public static <T> EntityMetadata<T> extractEntityMetadata(Class<T> entityClass, FfmTable table) {
        try {
            // Get ID column name
            String idColumnName = findIdColumnName(entityClass);

            // Get entity constructor
            Constructor<T> constructor = entityClass.getDeclaredConstructor();

            // Extract field mappings
            List<EntityMetadata.FieldMapping> fields = new ArrayList<>();
            Set<String> foreignKeyColumns = new HashSet<>();

            // NEW: Extract TypeConverters for each field
            Map<String, TypeConverter<?, ?>> converters = new HashMap<>();

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(Transient.class)) {
                    continue;
                }

                String columnName = field.getName();
                Class<?> javaType = field.getType();
                Class<?> storageType = javaType;

                // Handle TypeConverter
                TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(javaType);
                if (converter != null) {
                    storageType = converter.getStorageType();
                    converters.put(field.getName(), converter);
                }

                // Get column position (for potential future optimizations)
                int columnPosition = -1;
                int colIndex = 0;
                for (var col : table.columns()) {
                    if (col.name().equals(columnName)) {
                        columnPosition = colIndex;
                        break;
                    }
                    colIndex++;
                }

                EntityMetadata.FieldMapping mapping = new EntityMetadata.FieldMapping(
                        field.getName(),
                        columnName,
                        javaType,
                        storageType,
                        columnPosition
                );
                fields.add(mapping);

                // Track foreign key columns
                if (columnName.endsWith("_id") && !columnName.equals(idColumnName)) {
                    foreignKeyColumns.add(columnName);
                }
            }

            // NEW: Extract lifecycle callback MethodHandles
            MethodHandles.Lookup lookup = MethodHandles.lookup();
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
                prePersistHandle, postLoadHandle, preUpdateHandle);

        } catch (Exception e) {
            throw new RuntimeException("Failed to extract metadata for entity: " + entityClass.getName(), e);
        }
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
