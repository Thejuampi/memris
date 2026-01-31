package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.storage.GeneratedTable;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

/**
 * Implementation of EntityMaterializer that constructs entity instances from table row data.
 * <p>
 * This implementation:
 * <ul>
 *   <li>Uses pre-compiled MethodHandles for zero-reflection field access</li>
 *   <li>Reads directly from GeneratedTable using typed methods (readLong, readInt, readString)</li>
 *   <li>Applies TypeConverters when converting from storage to Java types</li>
 *   <li>Uses type code switching for O(1) dispatch</li>
 * </ul>
 *
 * @param <T> the entity type
 */
public class EntityMaterializerImpl<T> implements EntityMaterializer<T> {

    private final EntityMetadata<T> metadata;
    private final MethodHandle constructor;
    private final Map<String, MethodHandle> fieldSetters;
    private final List<FieldMapping> fields;
    private final Map<String, TypeConverter<?, ?>> converters;

    public EntityMaterializerImpl(EntityMetadata<T> metadata) {
        this.metadata = metadata;
        this.constructor = metadata.entityConstructor() != null 
            ? toMethodHandle(metadata.entityConstructor()) 
            : null;
        this.fieldSetters = metadata.fieldSetters();
        this.fields = metadata.fields();
        this.converters = metadata.converters();
    }

    private MethodHandle toMethodHandle(java.lang.reflect.Constructor<T> ctor) {
        try {
            return java.lang.invoke.MethodHandles.lookup().unreflectConstructor(ctor);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create MethodHandle for constructor", e);
        }
    }

    @Override
    public T materialize(HeapRuntimeKernel kernel, int rowIndex) {
        try {
            // Create entity instance
            T entity;
            if (constructor != null) {
                entity = (T) constructor.invoke();
            } else {
                // Fallback to reflection if no constructor handle available
                entity = metadata.entityConstructor().newInstance();
            }

            GeneratedTable table = kernel.table();

            // Read and set each field
            for (FieldMapping field : fields) {
                int colIdx = field.columnPosition();
                if (field.isRelationship()) {
                    continue;
                }
                if (!field.javaType().isPrimitive() && !table.isPresent(colIdx, rowIndex)) {
                    MethodHandle setter = fieldSetters.get(field.name());
                    if (setter != null) {
                        setter.invoke(entity, (Object) null);
                    }
                    continue;
                }
                byte typeCode = field.typeCode();
                Object value;

                // Read value based on type code
                value = switch (typeCode) {
                    case TypeCodes.TYPE_LONG -> table.readLong(colIdx, rowIndex);
                    case TypeCodes.TYPE_INT -> table.readInt(colIdx, rowIndex);
                    case TypeCodes.TYPE_STRING -> table.readString(colIdx, rowIndex);
                    case TypeCodes.TYPE_BOOLEAN -> table.readInt(colIdx, rowIndex) != 0; // stored as int
                    case TypeCodes.TYPE_BYTE -> (byte) table.readInt(colIdx, rowIndex);
                    case TypeCodes.TYPE_SHORT -> (short) table.readInt(colIdx, rowIndex);
                    case TypeCodes.TYPE_FLOAT -> Float.intBitsToFloat(table.readInt(colIdx, rowIndex));
                    case TypeCodes.TYPE_DOUBLE -> Double.longBitsToDouble(table.readLong(colIdx, rowIndex));
                    case TypeCodes.TYPE_CHAR -> (char) table.readInt(colIdx, rowIndex);
                    case TypeCodes.TYPE_INSTANT -> table.readLong(colIdx, rowIndex);
                    case TypeCodes.TYPE_LOCAL_DATE -> table.readLong(colIdx, rowIndex);
                    case TypeCodes.TYPE_LOCAL_DATE_TIME -> table.readLong(colIdx, rowIndex);
                    case TypeCodes.TYPE_DATE -> table.readLong(colIdx, rowIndex);
                    case TypeCodes.TYPE_BIG_DECIMAL -> table.readString(colIdx, rowIndex);
                    case TypeCodes.TYPE_BIG_INTEGER -> table.readString(colIdx, rowIndex);
                    default -> throw new IllegalStateException("Unknown type code: " + typeCode);
                };

                // Apply converter if present
                TypeConverter<?, ?> converter = converters.get(field.name());
                if (converter != null && value != null) {
                    @SuppressWarnings("unchecked")
                    TypeConverter<Object, Object> typedConverter = (TypeConverter<Object, Object>) converter;
                    value = typedConverter.fromStorage(value);
                }

                // Set field via MethodHandle
                MethodHandle setter = fieldSetters.get(field.name());
                if (setter != null) {
                    setter.invoke(entity, value);
                }
            }

            return entity;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to materialize entity of type " + metadata.entityClass().getName(), e);
        }
    }
}
