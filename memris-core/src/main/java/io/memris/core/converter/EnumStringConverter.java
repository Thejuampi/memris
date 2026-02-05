package io.memris.core.converter;

public final class EnumStringConverter<E extends Enum<E>> implements TypeConverter<E, String> {
    private final Class<E> enumType;

    public EnumStringConverter(Class<?> enumType) {
        @SuppressWarnings("unchecked")
        var typed = (Class<E>) enumType;
        this.enumType = typed;
    }

    @Override
    public Class<E> javaType() {
        return enumType;
    }

    @Override
    public Class<String> storageType() {
        return String.class;
    }

    @Override
    public String toStorage(E javaValue) {
        return javaValue == null ? null : javaValue.name();
    }

    @Override
    public E fromStorage(String storageValue) {
        return storageValue == null ? null : Enum.valueOf(enumType, storageValue);
    }
}
