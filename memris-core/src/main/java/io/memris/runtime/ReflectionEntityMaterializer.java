package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.storage.GeneratedTable;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reflection-based materializer used for entities with flattened embedded paths.
 */
public final class ReflectionEntityMaterializer<T> implements EntityMaterializer<T> {

    private final Constructor<T> constructor;
    private final Entry[] entries;

    public ReflectionEntityMaterializer(EntityMetadata<T> metadata) {
        this.constructor = metadata.entityConstructor();
        if (this.constructor != null) {
            this.constructor.setAccessible(true);
        }
        this.entries = buildEntries(metadata);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T materialize(GeneratedTable table, int rowIndex) {
        try {
            T entity = constructor.newInstance();
            for (Entry entry : entries) {
                int columnIndex = entry.mapping.columnPosition();
                if (!entry.mapping.javaType().isPrimitive() && !table.isPresent(columnIndex, rowIndex)) {
                    continue;
                }
                Object value = readValue(table, columnIndex, rowIndex, entry.mapping.typeCode());
                if (entry.converter != null && value != null) {
                    value = ((TypeConverter<Object, Object>) entry.converter).fromStorage(value);
                }
                entry.accessor.set(entity, value);
            }
            return entity;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to materialize entity", e);
        }
    }

    private static Object readValue(GeneratedTable table, int columnIndex, int rowIndex, byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> table.readLong(columnIndex, rowIndex);
            case TypeCodes.TYPE_INT -> table.readInt(columnIndex, rowIndex);
            case TypeCodes.TYPE_STRING -> table.readString(columnIndex, rowIndex);
            case TypeCodes.TYPE_BOOLEAN -> table.readInt(columnIndex, rowIndex) != 0;
            case TypeCodes.TYPE_BYTE -> (byte) table.readInt(columnIndex, rowIndex);
            case TypeCodes.TYPE_SHORT -> (short) table.readInt(columnIndex, rowIndex);
            case TypeCodes.TYPE_FLOAT -> FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, rowIndex));
            case TypeCodes.TYPE_DOUBLE -> FloatEncoding.sortableLongToDouble(table.readLong(columnIndex, rowIndex));
            case TypeCodes.TYPE_CHAR -> (char) table.readInt(columnIndex, rowIndex);
            case TypeCodes.TYPE_INSTANT, TypeCodes.TYPE_LOCAL_DATE, TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.TYPE_DATE ->
                table.readLong(columnIndex, rowIndex);
            case TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.TYPE_BIG_INTEGER -> table.readString(columnIndex, rowIndex);
            default -> throw new IllegalStateException("Unknown type code: " + typeCode);
        };
    }

    private Entry[] buildEntries(EntityMetadata<T> metadata) {
        List<Entry> flattened = new ArrayList<>();
        for (var mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0 || mapping.isRelationship()) {
                continue;
            }
            flattened.add(new Entry(
                    mapping,
                    PropertyPathAccessor.compile(metadata.entityClass(), mapping.name()),
                    metadata.converters().get(mapping.name())));
        }
        flattened.sort(Comparator.comparingInt(entry -> entry.mapping.columnPosition()));
        return flattened.toArray(new Entry[0]);
    }

    private record Entry(
            EntityMetadata.FieldMapping mapping,
            PropertyPathAccessor accessor,
            TypeConverter<?, ?> converter) {
    }
}
