package io.memris.spring.converter;

import io.memris.spring.MemrisException;
import io.memris.storage.ffm.FfmTable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.lang.reflect.Method;
import java.util.UUID;

/**
 * Registry for TypeConverters with support for client-registered custom converters.
 * Uses method handles for efficient reflection-free conversion.
 */
public final class TypeConverterRegistry {
    private static final TypeConverterRegistry INSTANCE = new TypeConverterRegistry();

    @SuppressWarnings("rawtypes")
    private final java.util.Map<Class, TypeConverter> converters = new java.util.HashMap<>();
    private final java.util.Map<Class, Method> getterCache = new java.util.HashMap<>();

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

        // Date/Time types - stored as String in ISO format
        register(new LocalDateConverter());
        register(new LocalDateTimeConverter());
        register(new LocalTimeConverter());
        register(new InstantConverter());
        register(new SqlDateConverter());
        register(new SqlTimestampConverter());

        // Cache getter methods for FfmTable
        cacheGetters();
    }

    private void cacheGetters() {
        try {
            getterCache.put(int.class, FfmTable.class.getMethod("getInt", String.class, int.class));
            getterCache.put(long.class, FfmTable.class.getMethod("getLong", String.class, int.class));
            getterCache.put(boolean.class, FfmTable.class.getMethod("getBoolean", String.class, int.class));
            getterCache.put(byte.class, FfmTable.class.getMethod("getByte", String.class, int.class));
            getterCache.put(short.class, FfmTable.class.getMethod("getShort", String.class, int.class));
            getterCache.put(float.class, FfmTable.class.getMethod("getFloat", String.class, int.class));
            getterCache.put(double.class, FfmTable.class.getMethod("getDouble", String.class, int.class));
            getterCache.put(char.class, FfmTable.class.getMethod("getChar", String.class, int.class));
            getterCache.put(String.class, FfmTable.class.getMethod("getString", String.class, int.class));
        } catch (NoSuchMethodException e) {
            throw new MemrisException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <J, S> void register(TypeConverter<J, S> converter) {
        converters.put(converter.getJavaType(), converter);
    }

    /**
       * Read a storage value from FfmTable and convert to Java type.
       */
    @SuppressWarnings("unchecked")
    public <J> J readStorageValue(FfmTable table, String columnName, Class<J> javaType, int rowIndex) {
        // Use type-based switch (Java 21 pattern matching)
        // Note: In pattern switches, we can't use generic J directly, so we cast the result
        return (J) readStorageValueRaw(table, columnName, javaType, rowIndex);
    }

    @SuppressWarnings("unchecked")
    private Object readStorageValueRaw(FfmTable table, String columnName, Class<?> javaType, int rowIndex) {
        // Direct switch on Class objects - each case returns Object
        if (javaType == int.class) {
            return table.getInt(columnName, rowIndex);
        } else if (javaType == long.class) {
            return table.getLong(columnName, rowIndex);
        } else if (javaType == boolean.class) {
            return table.getBoolean(columnName, rowIndex);
        } else if (javaType == byte.class) {
            return table.getByte(columnName, rowIndex);
        } else if (javaType == short.class) {
            return table.getShort(columnName, rowIndex);
        } else if (javaType == float.class) {
            return table.getFloat(columnName, rowIndex);
        } else if (javaType == double.class) {
            return table.getDouble(columnName, rowIndex);
        } else if (javaType == char.class) {
            return table.getChar(columnName, rowIndex);
        } else if (javaType == Integer.class) {
            return table.getInt(columnName, rowIndex);
        } else if (javaType == Long.class) {
            return table.getLong(columnName, rowIndex);
        } else if (javaType == Boolean.class) {
            return table.getBoolean(columnName, rowIndex);
        } else if (javaType == Byte.class) {
            return table.getByte(columnName, rowIndex);
        } else if (javaType == Short.class) {
            return table.getShort(columnName, rowIndex);
        } else if (javaType == Float.class) {
            return table.getFloat(columnName, rowIndex);
        } else if (javaType == Double.class) {
            return table.getDouble(columnName, rowIndex);
        } else if (javaType == Character.class) {
            return table.getChar(columnName, rowIndex);
        } else if (javaType == String.class) {
            return table.getString(columnName, rowIndex);
        } else if (javaType == BigDecimal.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : new BigDecimal(s);
        } else if (javaType == BigInteger.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : new BigInteger(s);
        } else if (javaType == LocalDate.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : LocalDate.parse(s);
        } else if (javaType == LocalDateTime.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : LocalDateTime.parse(s);
        } else if (javaType == LocalTime.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : LocalTime.parse(s);
        } else if (javaType == Instant.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : Instant.parse(s);
        } else if (javaType == java.sql.Date.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : java.sql.Date.valueOf(LocalDate.parse(s));
        } else if (javaType == java.sql.Timestamp.class) {
            String s = table.getString(columnName, rowIndex);
            return s == null || s.isEmpty() ? null : java.sql.Timestamp.valueOf(LocalDateTime.parse(s));
        }
        throw new IllegalArgumentException("Unsupported type: " + javaType);
    }

    @SuppressWarnings("unchecked")
    public <J> TypeConverter<J, ?> getConverter(Class<J> javaType) {
        // Try exact match first
        TypeConverter<?, ?> converter = converters.get(javaType);
        if (converter != null) {
            return (TypeConverter<J, ?>) converter;
        }

        // Try boxed to primitive lookup
        if (javaType == Integer.class) return (TypeConverter<J, ?>) converters.get(int.class);
        if (javaType == Long.class) return (TypeConverter<J, ?>) converters.get(long.class);
        if (javaType == Boolean.class) return (TypeConverter<J, ?>) converters.get(boolean.class);
        if (javaType == Byte.class) return (TypeConverter<J, ?>) converters.get(byte.class);
        if (javaType == Short.class) return (TypeConverter<J, ?>) converters.get(short.class);
        if (javaType == Float.class) return (TypeConverter<J, ?>) converters.get(float.class);
        if (javaType == Double.class) return (TypeConverter<J, ?>) converters.get(double.class);
        if (javaType == Character.class) return (TypeConverter<J, ?>) converters.get(char.class);

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

        @Override public Class<J> getJavaType() { return javaType; }
        @Override public Class<S> getStorageType() { return storageType; }
        @Override public S toStorage(J value) { return (S) value; }
        @Override public J fromStorage(S value) { return (J) value; }
    }

    private static final class BoxedToPrimitiveConverter<J, S> implements TypeConverter<J, S> {
        private final Class<J> javaType;
        private final Class<S> storageType;

        BoxedToPrimitiveConverter(Class<J> javaType, Class<S> storageType) {
            this.javaType = javaType;
            this.storageType = storageType;
        }

        @Override public Class<J> getJavaType() { return javaType; }
        @Override public Class<S> getStorageType() { return storageType; }
        @Override public S toStorage(J value) { return value == null ? null : (S) convertToPrimitive(value); }
        @Override public J fromStorage(S value) { return value == null ? null : (J) convertToBoxed(value); }

        @SuppressWarnings("unchecked")
        private Object convertToPrimitive(Object boxed) {
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            return switch (boxed) {
                case Number n -> {
                    // Guarded patterns needed because javaType is generic Class<J>
                    yield switch (javaType) {
                        case Class<?> c when c == Integer.class -> n.intValue();
                        case Class<?> c when c == Long.class -> n.longValue();
                        case Class<?> c when c == Short.class -> n.shortValue();
                        case Class<?> c when c == Byte.class -> n.byteValue();
                        case Class<?> c when c == Float.class -> n.floatValue();
                        case Class<?> c when c == Double.class -> n.doubleValue();
                        default -> boxed;
                    };
                }
                default -> boxed;
            };
        }

        @SuppressWarnings("unchecked")
        private Object convertToBoxed(Object primitive) {
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            return switch (primitive) {
                case Number n -> {
                    // Guarded patterns needed because storageType is generic Class<S>
                    yield switch (storageType) {
                        case Class<?> c when c == int.class -> n.intValue();
                        case Class<?> c when c == long.class -> n.longValue();
                        case Class<?> c when c == short.class -> n.shortValue();
                        case Class<?> c when c == byte.class -> n.byteValue();
                        case Class<?> c when c == float.class -> n.floatValue();
                        case Class<?> c when c == double.class -> n.doubleValue();
                        default -> primitive;
                    };
                }
                default -> primitive;
            };
        }
    }

    private static final class BigDecimalConverter implements TypeConverter<BigDecimal, String> {
        @Override public Class<BigDecimal> getJavaType() { return BigDecimal.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(BigDecimal value) { return value == null ? "" : value.toPlainString(); }
        @Override public BigDecimal fromStorage(String value) { return value == null || value.isEmpty() ? null : new BigDecimal(value); }
    }

    private static final class BigIntegerConverter implements TypeConverter<BigInteger, String> {
        @Override public Class<BigInteger> getJavaType() { return BigInteger.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(BigInteger value) { return value == null ? "" : value.toString(); }
        @Override public BigInteger fromStorage(String value) { return value == null || value.isEmpty() ? null : new BigInteger(value); }
    }

    private static final class LocalDateConverter implements TypeConverter<LocalDate, String> {
        @Override public Class<LocalDate> getJavaType() { return LocalDate.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(LocalDate value) { return value == null ? "" : value.toString(); }
        @Override public LocalDate fromStorage(String value) { return value == null || value.isEmpty() ? null : LocalDate.parse(value); }
    }

    private static final class LocalDateTimeConverter implements TypeConverter<LocalDateTime, String> {
        @Override public Class<LocalDateTime> getJavaType() { return LocalDateTime.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(LocalDateTime value) { return value == null ? "" : value.toString(); }
        @Override public LocalDateTime fromStorage(String value) { return value == null || value.isEmpty() ? null : LocalDateTime.parse(value); }
    }

    private static final class LocalTimeConverter implements TypeConverter<LocalTime, String> {
        @Override public Class<LocalTime> getJavaType() { return LocalTime.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(LocalTime value) { return value == null ? "" : value.toString(); }
        @Override public LocalTime fromStorage(String value) { return value == null || value.isEmpty() ? null : LocalTime.parse(value); }
    }

    private static final class InstantConverter implements TypeConverter<Instant, String> {
        @Override public Class<Instant> getJavaType() { return Instant.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(Instant value) { return value == null ? "" : value.toString(); }
        @Override public Instant fromStorage(String value) { return value == null || value.isEmpty() ? null : Instant.parse(value); }
    }

    private static final class SqlDateConverter implements TypeConverter<java.sql.Date, String> {
        @Override public Class<java.sql.Date> getJavaType() { return java.sql.Date.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(java.sql.Date value) { return value == null ? "" : value.toLocalDate().toString(); }
        @Override public java.sql.Date fromStorage(String value) { return value == null || value.isEmpty() ? null : java.sql.Date.valueOf(LocalDate.parse(value)); }
    }

    private static final class UUIDConverter implements TypeConverter<UUID, String> {
        @Override public Class<UUID> getJavaType() { return UUID.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(UUID value) { return value == null ? "" : value.toString(); }
        @Override public UUID fromStorage(String value) { return value == null || value.isEmpty() ? null : UUID.fromString(value); }
    }

    private static final class SqlTimestampConverter implements TypeConverter<java.sql.Timestamp, String> {
        @Override public Class<java.sql.Timestamp> getJavaType() { return java.sql.Timestamp.class; }
        @Override public Class<String> getStorageType() { return String.class; }
        @Override public String toStorage(java.sql.Timestamp value) { return value == null ? "" : value.toLocalDateTime().toString(); }
        @Override public java.sql.Timestamp fromStorage(String value) { return value == null || value.isEmpty() ? null : java.sql.Timestamp.valueOf(LocalDateTime.parse(value)); }
    }
}
