package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.core.converter.TypeConverter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compatibility wrapper for path-aware saving.
 * <p>
 * Prefer {@link io.memris.repository.EntitySaverGenerator} in runtime code paths.
 */
@Deprecated(forRemoval = false)
public final class ReflectionEntitySaver<T> implements EntitySaver<T, Object> {

    private final PropertyPathAccessor idAccessor;
    private final Entry[] entries;
    private final Map<String, PropertyPathAccessor> relationshipIdAccessors;

    public ReflectionEntitySaver(EntityMetadata<T> metadata) {
        this.idAccessor = PropertyPathAccessor.compile(metadata.entityClass(), metadata.idColumnName());
        this.entries = buildEntries(metadata);
        this.relationshipIdAccessors = buildRelationshipIdAccessors(metadata);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T save(T entity, io.memris.storage.GeneratedTable table, Object id) {
        setId(entity, id);
        Object[] values = new Object[entries.length];
        for (Entry entry : entries) {
            if (entry == null) {
                continue;
            }
            Object value = entry.accessor.get(entity);
            if (entry.relationshipAccessor != null) {
                value = entry.relationshipAccessor.get(value);
            }
            if (entry.converter != null && value != null) {
                value = ((TypeConverter<Object, Object>) entry.converter).toStorage(value);
            }
            values[entry.columnIndex] = value;
        }
        table.insertFrom(values);
        return entity;
    }

    @Override
    public Object extractId(T entity) {
        return idAccessor.get(entity);
    }

    @Override
    public void setId(T entity, Object id) {
        idAccessor.set(entity, id);
    }

    @Override
    public Object resolveRelationshipId(String fieldName, Object relatedEntity) {
        if (fieldName == null || relatedEntity == null) {
            return null;
        }
        PropertyPathAccessor accessor = relationshipIdAccessors.get(fieldName);
        return accessor == null ? null : accessor.get(relatedEntity);
    }

    private static <T> Entry[] buildEntries(EntityMetadata<T> metadata) {
        int columnCount = metadata.fields().stream()
                .filter(mapping -> mapping.columnPosition() >= 0)
                .mapToInt(EntityMetadata.FieldMapping::columnPosition)
                .max()
                .orElse(-1) + 1;

        List<Entry> sorted = new ArrayList<>();
        for (var mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0) {
                continue;
            }
            var accessor = PropertyPathAccessor.compile(metadata.entityClass(), mapping.name());
            PropertyPathAccessor relationshipAccessor = null;
            if (mapping.isRelationship() && !mapping.isCollection() && mapping.targetEntity() != null) {
                String idFieldName = resolveIdFieldName(mapping.targetEntity());
                relationshipAccessor = PropertyPathAccessor.compile(mapping.targetEntity(), idFieldName);
            }
            sorted.add(new Entry(
                    mapping.columnPosition(),
                    accessor,
                    metadata.converters().get(mapping.name()),
                    relationshipAccessor));
        }
        sorted.sort(Comparator.comparingInt(Entry::columnIndex));

        Entry[] byColumn = new Entry[columnCount];
        for (Entry entry : sorted) {
            byColumn[entry.columnIndex] = entry;
        }
        return byColumn;
    }

    private static <T> Map<String, PropertyPathAccessor> buildRelationshipIdAccessors(EntityMetadata<T> metadata) {
        Map<String, PropertyPathAccessor> accessors = new HashMap<>();
        for (var mapping : metadata.fields()) {
            if (!mapping.isRelationship() || mapping.isCollection() || mapping.targetEntity() == null) {
                continue;
            }
            String idFieldName = resolveIdFieldName(mapping.targetEntity());
            accessors.put(mapping.name(), PropertyPathAccessor.compile(mapping.targetEntity(), idFieldName));
        }
        return Map.copyOf(accessors);
    }

    private static String resolveIdFieldName(Class<?> entityClass) {
        Field resolved = null;
        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }
            if (resolved != null) {
                throw new IllegalArgumentException("Multiple @Id fields on " + entityClass.getName());
            }
            resolved = field;
        }
        if (resolved == null) {
            throw new IllegalArgumentException("Missing @Id field on " + entityClass.getName());
        }
        return resolved.getName();
    }

    private record Entry(
            int columnIndex,
            PropertyPathAccessor accessor,
            TypeConverter<?, ?> converter,
            PropertyPathAccessor relationshipAccessor) {
    }
}
