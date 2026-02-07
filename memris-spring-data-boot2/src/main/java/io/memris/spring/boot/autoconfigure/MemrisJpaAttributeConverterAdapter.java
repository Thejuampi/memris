package io.memris.spring.boot.autoconfigure;

import io.memris.core.converter.TypeConverter;

import java.lang.reflect.ParameterizedType;

/**
 * Adapts a JPA {@code AttributeConverter} into a Memris {@link TypeConverter}.
 *
 * @param <J> Java-side type
 * @param <S> storage-side type
 */
public final class MemrisJpaAttributeConverterAdapter<J, S> implements TypeConverter<J, S> {
    private final javax.persistence.AttributeConverter<J, S> delegate;
    private final Class<J> javaType;
    private final Class<S> storageType;

    /**
     * Creates an adapter for the provided converter instance.
     *
     * @param delegate JPA attribute converter
     */
    @SuppressWarnings("unchecked")
    public MemrisJpaAttributeConverterAdapter(Object delegate) {
        this.delegate = (javax.persistence.AttributeConverter<J, S>) delegate;
        var type = resolveGenericType(delegate.getClass());
        this.javaType = (Class<J>) type[0];
        this.storageType = (Class<S>) type[1];
    }

    @Override
    public Class<J> javaType() {
        return javaType;
    }

    @Override
    public Class<S> storageType() {
        return storageType;
    }

    @Override
    public S toStorage(J javaValue) {
        return delegate.convertToDatabaseColumn(javaValue);
    }

    @Override
    public J fromStorage(S storageValue) {
        return delegate.convertToEntityAttribute(storageValue);
    }

    private static Class<?>[] resolveGenericType(Class<?> converterClass) {
        var type = converterClass.getGenericInterfaces();
        for (var iface : type) {
            if (iface instanceof ParameterizedType pt) {
                if (pt.getRawType() instanceof Class<?> raw
                        && javax.persistence.AttributeConverter.class.isAssignableFrom(raw)) {
                    var args = pt.getActualTypeArguments();
                    return new Class<?>[] { (Class<?>) args[0], (Class<?>) args[1] };
                }
            }
        }
        throw new IllegalArgumentException("Cannot resolve AttributeConverter generic types: "
                + converterClass.getName());
    }
}
