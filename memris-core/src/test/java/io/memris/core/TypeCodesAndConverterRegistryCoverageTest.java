package io.memris.core;

import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeCodesAndConverterRegistryCoverageTest {

    @Test
    void typeCodesForClassOrDefaultCoversAllBranches() {
        assertThat(TypeCodes.forClassOrDefault(Integer.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_INT);
        assertThat(TypeCodes.forClassOrDefault(Long.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_LONG);
        assertThat(TypeCodes.forClassOrDefault(Boolean.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_BOOLEAN);
        assertThat(TypeCodes.forClassOrDefault(Byte.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_BYTE);
        assertThat(TypeCodes.forClassOrDefault(Short.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_SHORT);
        assertThat(TypeCodes.forClassOrDefault(Float.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_FLOAT);
        assertThat(TypeCodes.forClassOrDefault(Double.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_DOUBLE);
        assertThat(TypeCodes.forClassOrDefault(Character.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_CHAR);
        assertThat(TypeCodes.forClassOrDefault(String.class, (byte) 99)).isEqualTo(TypeCodes.TYPE_STRING);
        assertThat(TypeCodes.forClassOrDefault(java.util.concurrent.atomic.AtomicInteger.class, (byte) 99))
                .isEqualTo((byte) 99);

        assertThatThrownBy(() -> TypeCodes.forClass(java.util.concurrent.atomic.AtomicInteger.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported type");
    }

    @Test
    void converterRegistryClassLookupRegistrationAndPresenceChecks() {
        var registry = TypeConverterRegistry.getInstance();

        // Boxed fallback path in getConverter()
        assertThat(registry.getConverter(Integer.class)).isNotNull();
        assertThat(registry.getConverter(Long.class)).isNotNull();

        // Low-level converter methods on default converters
        assertThat(registry.getConverter(UUID.class).storageType()).isEqualTo(String.class);
        assertThat(registry.getConverter(LocalTime.class).storageType()).isEqualTo(String.class);

        // registerByClass + getConverterByClass + hasConverter
        registry.registerByClass(CustomIdConverter.class);
        assertThat(registry.hasConverter(CustomId.class)).isTrue();
        assertThat(registry.getConverterByClass(CustomIdConverter.class)).isNotNull();
        assertThat(registry.hasConverter(CustomUnsupported.class)).isFalse();

        assertThatThrownBy(() -> registry.registerByClass(BadConverter.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to instantiate converter");
    }

    static final class CustomId {
        private final String value;

        CustomId(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }

    static final class CustomUnsupported {
    }

    public static final class CustomIdConverter implements TypeConverter<CustomId, String> {
        @Override
        public Class<CustomId> javaType() {
            return CustomId.class;
        }

        @Override
        public Class<String> storageType() {
            return String.class;
        }

        @Override
        public String toStorage(CustomId javaValue) {
            return javaValue == null ? null : javaValue.value();
        }

        @Override
        public CustomId fromStorage(String storageValue) {
            return storageValue == null ? null : new CustomId(storageValue);
        }
    }

    public static final class BadConverter implements TypeConverter<CustomUnsupported, String> {
        private final String config;

        public BadConverter(String config) {
            this.config = config;
        }

        @Override
        public Class<CustomUnsupported> javaType() {
            return CustomUnsupported.class;
        }

        @Override
        public Class<String> storageType() {
            return String.class;
        }

        @Override
        public String toStorage(CustomUnsupported javaValue) {
            return config;
        }

        @Override
        public CustomUnsupported fromStorage(String storageValue) {
            return new CustomUnsupported();
        }
    }
}
