package io.memris.core.converter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

/**
 * Registry for TypeConverters with support for client-registered custom
 * converters.
 * Uses method handles for efficient reflection-free conversion.
 */
public final class TypeConverterRegistry {
    private static final TypeConverterRegistry INSTANCE = new TypeConverterRegistry();

    @SuppressWarnings("rawtypes")
    private final java.util.Map<Class, TypeConverter> converters = new java.util.HashMap<>();

    private TypeConverterRegistry() {
        registerDefaults();
    }

    public static TypeConverterRegistry getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    private void registerDefaults() {
        // Primitive and boxed types - direct mapping
        register(new IdentityConverter<>(int.class, int.class));
        register(new IdentityConverter<>(long.class, long.class));
        register(new IdentityConverter<>(boolean.class, boolean.class));
        register(new IdentityConverter<>(byte.class, byte.class));
        register(new IdentityConverter<>(short.class, short.class));
        register(new IdentityConverter<>(float.class, float.class));
        register(new IdentityConverter<>(double.class, double.class));
        register(new IdentityConverter<>(char.class, char.class));

        // Boxed types map to primitives
        register(new BoxedToPrimitiveConverter<>(Integer.class, int.class));
        register(new BoxedToPrimitiveConverter<>(Long.class, long.class));
        register(new BoxedToPrimitiveConverter<>(Boolean.class, boolean.class));
        register(new BoxedToPrimitiveConverter<>(Byte.class, byte.class));
        register(new BoxedToPrimitiveConverter<>(Short.class, short.class));
        register(new BoxedToPrimitiveConverter<>(Float.class, float.class));
        register(new BoxedToPrimitiveConverter<>(Double.class, double.class));
        register(new BoxedToPrimitiveConverter<>(Character.class, char.class));

        // String type
        register(new IdentityConverter<>(String.class, String.class));

        // UUID - stored as String
        register(new UUIDConverter());

        // BigDecimal and BigInteger - stored as String
        register(new BigDecimalConverter());
        register(new BigIntegerConverter());

        // Date/Time types - stored as epoch-based long values
        register(new LocalDateLongConverter());
        register(new LocalDateTimeLongConverter());
        register(new InstantLongConverter());
        register(new DateLongConverter());

        // LocalTime remains stored as String
        register(new LocalTimeConverter());
        register(new SqlDateConverter());
        register(new SqlTimestampConverter());
    }

    @SuppressWarnings("unchecked")
    public <J, S> void register(TypeConverter<J, S> converter) {
        converters.put(converter.getJavaType(), converter);
    }

    @SuppressWarnings("unchecked")
    public <J> TypeConverter<J, ?> getConverter(Class<J> javaType) {
        // Try exact match first
        TypeConverter<?, ?> converter = converters.get(javaType);
        if (converter != null) {
            return (TypeConverter<J, ?>) converter;
        }

        // Try boxed to primitive lookup
        if (javaType == Integer.class)
            return (TypeConverter<J, ?>) converters.get(int.class);
        if (javaType == Long.class)
            return (TypeConverter<J, ?>) converters.get(long.class);
        if (javaType == Boolean.class)
            return (TypeConverter<J, ?>) converters.get(boolean.class);
        if (javaType == Byte.class)
            return (TypeConverter<J, ?>) converters.get(byte.class);
        if (javaType == Short.class)
            return (TypeConverter<J, ?>) converters.get(short.class);
        if (javaType == Float.class)
            return (TypeConverter<J, ?>) converters.get(float.class);
        if (javaType == Double.class)
            return (TypeConverter<J, ?>) converters.get(double.class);
        if (javaType == Character.class)
            return (TypeConverter<J, ?>) converters.get(char.class);

        return null;
    }

    public boolean hasConverter(Class<?> javaType) {
        return getConverter(javaType) != null;
    }

    // Default converters

    private static final class IdentityConverter<J, S> implements TypeConverter<J, S> {
        private final Class<J> javaType;
        private final Class<S> storageType;

        IdentityConverter(Class<J> javaType, Class<S> storageType) {
            this.javaType = javaType;
            this.storageType = storageType;
        }

        @Override
        public Class<J> getJavaType() {
            return javaType;
        }

        @Override
        public Class<S> getStorageType() {
            return storageType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public S toStorage(J javaValue) {
            return (S) javaValue;
        }

        @Override
        @SuppressWarnings("unchecked")
        public J fromStorage(S storageValue) {
            return (J) storageValue;
        }
    }

    private static final class BoxedToPrimitiveConverter<J, S> implements TypeConverter<J, S> {
        private final Class<J> javaType;
        private final Class<S> storageType;

        BoxedToPrimitiveConverter(Class<J> javaType, Class<S> storageType) {
            this.javaType = javaType;
            this.storageType = storageType;
        }

        @Override
        public Class<J> getJavaType() {
            return javaType;
        }

        @Override
        public Class<S> getStorageType() {
            return storageType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public S toStorage(J javaValue) {
            return (S) javaValue;
        }

        @Override
        @SuppressWarnings("unchecked")
        public J fromStorage(S storageValue) {
            return (J) storageValue;
        }
    }

    private static final class UUIDConverter implements TypeConverter<UUID, String> {
        @Override
        public Class<UUID> getJavaType() {
            return UUID.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(UUID javaValue) {
            return javaValue == null ? null : javaValue.toString();
        }

        @Override
        public UUID fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null : UUID.fromString(storageValue);
        }
    }

    private static final class BigDecimalConverter implements TypeConverter<BigDecimal, String> {
        @Override
        public Class<BigDecimal> getJavaType() {
            return BigDecimal.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(BigDecimal javaValue) {
            return javaValue == null ? null : javaValue.toString();
        }

        @Override
        public BigDecimal fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null : new BigDecimal(storageValue);
        }
    }

    private static final class BigIntegerConverter implements TypeConverter<BigInteger, String> {
        @Override
        public Class<BigInteger> getJavaType() {
            return BigInteger.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(BigInteger javaValue) {
            return javaValue == null ? null : javaValue.toString();
        }

        @Override
        public BigInteger fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null : new BigInteger(storageValue);
        }
    }

    private static final class LocalDateLongConverter implements TypeConverter<LocalDate, Long> {
        @Override
        public Class<LocalDate> getJavaType() {
            return LocalDate.class;
        }

        @Override
        public Class<Long> getStorageType() {
            return long.class;
        }

        @Override
        public Long toStorage(LocalDate javaValue) {
            return javaValue == null ? null : javaValue.toEpochDay();
        }

        @Override
        public LocalDate fromStorage(Long storageValue) {
            return storageValue == null ? null : LocalDate.ofEpochDay(storageValue);
        }
    }

    private static final class LocalDateTimeLongConverter implements TypeConverter<LocalDateTime, Long> {
        @Override
        public Class<LocalDateTime> getJavaType() {
            return LocalDateTime.class;
        }

        @Override
        public Class<Long> getStorageType() {
            return long.class;
        }

        @Override
        public Long toStorage(LocalDateTime javaValue) {
            return javaValue == null ? null : javaValue.toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        @Override
        public LocalDateTime fromStorage(Long storageValue) {
            return storageValue == null ? null
                    : LocalDateTime.ofInstant(Instant.ofEpochMilli(storageValue), ZoneOffset.UTC);
        }
    }

    private static final class LocalTimeConverter implements TypeConverter<LocalTime, String> {
        @Override
        public Class<LocalTime> getJavaType() {
            return LocalTime.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(LocalTime javaValue) {
            return javaValue == null ? null : javaValue.toString();
        }

        @Override
        public LocalTime fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null : LocalTime.parse(storageValue);
        }
    }

    private static final class InstantLongConverter implements TypeConverter<Instant, Long> {
        @Override
        public Class<Instant> getJavaType() {
            return Instant.class;
        }

        @Override
        public Class<Long> getStorageType() {
            return long.class;
        }

        @Override
        public Long toStorage(Instant javaValue) {
            return javaValue == null ? null : javaValue.toEpochMilli();
        }

        @Override
        public Instant fromStorage(Long storageValue) {
            return storageValue == null ? null : Instant.ofEpochMilli(storageValue);
        }
    }

    private static final class DateLongConverter implements TypeConverter<Date, Long> {
        @Override
        public Class<Date> getJavaType() {
            return Date.class;
        }

        @Override
        public Class<Long> getStorageType() {
            return long.class;
        }

        @Override
        public Long toStorage(Date javaValue) {
            return javaValue == null ? null : javaValue.getTime();
        }

        @Override
        public Date fromStorage(Long storageValue) {
            return storageValue == null ? null : new Date(storageValue);
        }
    }

    private static final class SqlDateConverter implements TypeConverter<java.sql.Date, String> {
        @Override
        public Class<java.sql.Date> getJavaType() {
            return java.sql.Date.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(java.sql.Date javaValue) {
            return javaValue == null ? null : javaValue.toLocalDate().toString();
        }

        @Override
        public java.sql.Date fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null
                    : java.sql.Date.valueOf(LocalDate.parse(storageValue));
        }
    }

    private static final class SqlTimestampConverter implements TypeConverter<java.sql.Timestamp, String> {
        @Override
        public Class<java.sql.Timestamp> getJavaType() {
            return java.sql.Timestamp.class;
        }

        @Override
        public Class<String> getStorageType() {
            return String.class;
        }

        @Override
        public String toStorage(java.sql.Timestamp javaValue) {
            return javaValue == null ? null : javaValue.toLocalDateTime().toString();
        }

        @Override
        public java.sql.Timestamp fromStorage(String storageValue) {
            return storageValue == null || storageValue.isEmpty() ? null
                    : java.sql.Timestamp.valueOf(LocalDateTime.parse(storageValue));
        }
    }
}
