package io.memris.spring.converter;

import io.memris.storage.ffm.FfmTable;

/**
 * Interface for converting Java entity field types to/from storage types.
 * Similar to JPA's AttributeConverter, this allows extensible type handling.
 *
 * @param <J> The Java type of the entity field
 * @param <S> The storage type used in FfmTable
 */
public interface TypeConverter<J, S> {

    /**
     * Get the Java type this converter handles.
     */
    Class<J> getJavaType();

    /**
     * Get the storage type this converter uses.
     */
    Class<S> getStorageType();

    /**
     * Convert Java entity value to storage value for persistence.
     */
    S toStorage(J javaValue);

    /**
     * Convert storage value back to Java entity value.
     */
    J fromStorage(S storageValue);

    /**
     * Get the storage column name for this field.
     */
    default String getColumnName(String fieldName, FfmTable table) {
        return fieldName;
    }
}
