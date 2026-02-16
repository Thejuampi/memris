package io.memris.runtime;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

/**
 * Shared IN/NOT_IN argument decoder.
 *
 * <p>
 * Strict semantics:
 * <ul>
 * <li>null top-level value -> empty set</li>
 * <li>null element inside list/array -> IllegalArgumentException</li>
 * <li>incompatible element type -> IllegalArgumentException</li>
 * </ul>
 */
public final class InArgumentDecoder {

    private static final Object[] EMPTY_OBJECTS = new Object[0];

    private InArgumentDecoder() {
    }

    public static long toLongStorageValue(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INSTANT -> ((Instant) value).toEpochMilli();
            case TypeCodes.TYPE_LOCAL_DATE -> ((LocalDate) value).toEpochDay();
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
            case TypeCodes.TYPE_DATE -> ((Date) value).getTime();
            case TypeCodes.TYPE_DOUBLE -> FloatEncoding.doubleToSortableLong(((Number) value).doubleValue());
            default -> ((Number) value).longValue();
        };
    }

    public static int toIntStorageValue(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_BOOLEAN -> (Boolean) value ? 1 : 0;
            case TypeCodes.TYPE_CHAR -> (value instanceof Character character)
                    ? character
                    : value.toString().charAt(0);
            case TypeCodes.TYPE_FLOAT -> FloatEncoding.floatToSortableInt(((Number) value).floatValue());
            default -> ((Number) value).intValue();
        };
    }

    public static long[] toLongArrayStrict(byte typeCode, Object value) {
        if (value == null) {
            return new long[0];
        }
        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> decodeLongValues(value, typeCode);
            case TypeCodes.TYPE_DOUBLE -> decodeDoubleValues(value, typeCode);
            case TypeCodes.TYPE_INSTANT -> decodeInstantValues(value, typeCode);
            case TypeCodes.TYPE_LOCAL_DATE -> decodeLocalDateValues(value, typeCode);
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> decodeLocalDateTimeValues(value, typeCode);
            case TypeCodes.TYPE_DATE -> decodeDateValues(value, typeCode);
            default -> throw new IllegalArgumentException("IN strict long decoding not supported for type "
                    + typeName(typeCode));
        };
    }

    public static int[] toIntArrayStrict(byte typeCode, Object value) {
        if (value == null) {
            return new int[0];
        }
        return switch (typeCode) {
            case TypeCodes.TYPE_INT -> decodeIntValues(value, typeCode);
            case TypeCodes.TYPE_BYTE -> decodeByteValues(value, typeCode);
            case TypeCodes.TYPE_SHORT -> decodeShortValues(value, typeCode);
            case TypeCodes.TYPE_CHAR -> decodeCharValues(value, typeCode);
            case TypeCodes.TYPE_BOOLEAN -> decodeBooleanValues(value, typeCode);
            case TypeCodes.TYPE_FLOAT -> decodeFloatValues(value, typeCode);
            default -> throw new IllegalArgumentException("IN strict int decoding not supported for type "
                    + typeName(typeCode));
        };
    }

    public static String[] toStringArrayStrict(byte typeCode, Object value) {
        if (value == null) {
            return new String[0];
        }
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING -> decodeStringValues(value, typeCode);
            case TypeCodes.TYPE_BIG_DECIMAL -> decodeBigDecimalValues(value, typeCode);
            case TypeCodes.TYPE_BIG_INTEGER -> decodeBigIntegerValues(value, typeCode);
            default -> throw new IllegalArgumentException("IN strict string decoding not supported for type "
                    + typeName(typeCode));
        };
    }

    public static Object[] toIndexValuesStrict(byte typeCode, Object value) {
        if (value == null) {
            return EMPTY_OBJECTS;
        }
        return switch (typeCode) {
            case TypeCodes.TYPE_INT -> decodeIndexValues(value, typeCode, Integer.class);
            case TypeCodes.TYPE_LONG -> decodeIndexValues(value, typeCode, Long.class);
            case TypeCodes.TYPE_BOOLEAN -> decodeIndexValues(value, typeCode, Boolean.class);
            case TypeCodes.TYPE_BYTE -> decodeIndexValues(value, typeCode, Byte.class);
            case TypeCodes.TYPE_SHORT -> decodeIndexValues(value, typeCode, Short.class);
            case TypeCodes.TYPE_FLOAT -> decodeIndexValues(value, typeCode, Float.class);
            case TypeCodes.TYPE_DOUBLE -> decodeIndexValues(value, typeCode, Double.class);
            case TypeCodes.TYPE_CHAR -> decodeIndexValues(value, typeCode, Character.class);
            case TypeCodes.TYPE_STRING -> decodeIndexValues(value, typeCode, String.class);
            case TypeCodes.TYPE_INSTANT -> decodeIndexValues(value, typeCode, Instant.class);
            case TypeCodes.TYPE_LOCAL_DATE -> decodeIndexValues(value, typeCode, LocalDate.class);
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> decodeIndexValues(value, typeCode, LocalDateTime.class);
            case TypeCodes.TYPE_DATE -> decodeIndexValues(value, typeCode, Date.class);
            case TypeCodes.TYPE_BIG_DECIMAL -> decodeIndexValues(value, typeCode, BigDecimal.class);
            case TypeCodes.TYPE_BIG_INTEGER -> decodeIndexValues(value, typeCode, BigInteger.class);
            default -> throw new IllegalArgumentException(
                    "IN strict index decoding not supported for type " + typeName(typeCode));
        };
    }

    private static long[] decodeLongValues(Object value, byte typeCode) {
        if (value instanceof long[] longs) {
            return longs;
        }
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Long.class, typeCode, i);
        }
        return result;
    }

    private static long[] decodeDoubleValues(Object value, byte typeCode) {
        if (value instanceof double[] doubles) {
            var result = new long[doubles.length];
            for (var i = 0; i < doubles.length; i++) {
                result[i] = FloatEncoding.doubleToSortableLong(doubles[i]);
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            var element = requireType(elements[i], Double.class, typeCode, i);
            result[i] = FloatEncoding.doubleToSortableLong(element);
        }
        return result;
    }

    private static long[] decodeInstantValues(Object value, byte typeCode) {
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Instant.class, typeCode, i).toEpochMilli();
        }
        return result;
    }

    private static long[] decodeLocalDateValues(Object value, byte typeCode) {
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], LocalDate.class, typeCode, i).toEpochDay();
        }
        return result;
    }

    private static long[] decodeLocalDateTimeValues(Object value, byte typeCode) {
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], LocalDateTime.class, typeCode, i)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        }
        return result;
    }

    private static long[] decodeDateValues(Object value, byte typeCode) {
        var elements = toObjectElements(value);
        var result = new long[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Date.class, typeCode, i).getTime();
        }
        return result;
    }

    private static int[] decodeIntValues(Object value, byte typeCode) {
        if (value instanceof int[] ints) {
            return ints;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Integer.class, typeCode, i);
        }
        return result;
    }

    private static int[] decodeByteValues(Object value, byte typeCode) {
        if (value instanceof byte[] bytes) {
            var result = new int[bytes.length];
            for (var i = 0; i < bytes.length; i++) {
                result[i] = bytes[i];
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Byte.class, typeCode, i);
        }
        return result;
    }

    private static int[] decodeShortValues(Object value, byte typeCode) {
        if (value instanceof short[] shorts) {
            var result = new int[shorts.length];
            for (var i = 0; i < shorts.length; i++) {
                result[i] = shorts[i];
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Short.class, typeCode, i);
        }
        return result;
    }

    private static int[] decodeCharValues(Object value, byte typeCode) {
        if (value instanceof char[] chars) {
            var result = new int[chars.length];
            for (var i = 0; i < chars.length; i++) {
                result[i] = chars[i];
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Character.class, typeCode, i);
        }
        return result;
    }

    private static int[] decodeBooleanValues(Object value, byte typeCode) {
        if (value instanceof boolean[] booleans) {
            var result = new int[booleans.length];
            for (var i = 0; i < booleans.length; i++) {
                result[i] = booleans[i] ? 1 : 0;
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], Boolean.class, typeCode, i) ? 1 : 0;
        }
        return result;
    }

    private static int[] decodeFloatValues(Object value, byte typeCode) {
        if (value instanceof float[] floats) {
            var result = new int[floats.length];
            for (var i = 0; i < floats.length; i++) {
                result[i] = FloatEncoding.floatToSortableInt(floats[i]);
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new int[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = FloatEncoding.floatToSortableInt(requireType(elements[i], Float.class, typeCode, i));
        }
        return result;
    }

    private static String[] decodeStringValues(Object value, byte typeCode) {
        if (value instanceof String[] strings) {
            return strings;
        }
        var elements = toObjectElements(value);
        var result = new String[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], String.class, typeCode, i);
        }
        return result;
    }

    private static String[] decodeBigDecimalValues(Object value, byte typeCode) {
        if (value instanceof BigDecimal[] decimals) {
            var result = new String[decimals.length];
            for (var i = 0; i < decimals.length; i++) {
                result[i] = decimals[i].toString();
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new String[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], BigDecimal.class, typeCode, i).toString();
        }
        return result;
    }

    private static String[] decodeBigIntegerValues(Object value, byte typeCode) {
        if (value instanceof BigInteger[] integers) {
            var result = new String[integers.length];
            for (var i = 0; i < integers.length; i++) {
                result[i] = integers[i].toString();
            }
            return result;
        }
        var elements = toObjectElements(value);
        var result = new String[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], BigInteger.class, typeCode, i).toString();
        }
        return result;
    }

    private static <T> Object[] decodeIndexValues(Object value, byte typeCode, Class<T> elementType) {
        if (value instanceof int[] ints) {
            if (elementType != Integer.class) {
                throw incompatibleType(typeCode, 0, elementType, int[].class);
            }
            var result = new Object[ints.length];
            for (var i = 0; i < ints.length; i++) {
                result[i] = ints[i];
            }
            return result;
        }
        if (value instanceof long[] longs) {
            if (elementType != Long.class) {
                throw incompatibleType(typeCode, 0, elementType, long[].class);
            }
            var result = new Object[longs.length];
            for (var i = 0; i < longs.length; i++) {
                result[i] = longs[i];
            }
            return result;
        }
        if (value instanceof byte[] bytes) {
            if (elementType != Byte.class) {
                throw incompatibleType(typeCode, 0, elementType, byte[].class);
            }
            var result = new Object[bytes.length];
            for (var i = 0; i < bytes.length; i++) {
                result[i] = bytes[i];
            }
            return result;
        }
        if (value instanceof short[] shorts) {
            if (elementType != Short.class) {
                throw incompatibleType(typeCode, 0, elementType, short[].class);
            }
            var result = new Object[shorts.length];
            for (var i = 0; i < shorts.length; i++) {
                result[i] = shorts[i];
            }
            return result;
        }
        if (value instanceof float[] floats) {
            if (elementType != Float.class) {
                throw incompatibleType(typeCode, 0, elementType, float[].class);
            }
            var result = new Object[floats.length];
            for (var i = 0; i < floats.length; i++) {
                result[i] = floats[i];
            }
            return result;
        }
        if (value instanceof double[] doubles) {
            if (elementType != Double.class) {
                throw incompatibleType(typeCode, 0, elementType, double[].class);
            }
            var result = new Object[doubles.length];
            for (var i = 0; i < doubles.length; i++) {
                result[i] = doubles[i];
            }
            return result;
        }
        if (value instanceof char[] chars) {
            if (elementType != Character.class) {
                throw incompatibleType(typeCode, 0, elementType, char[].class);
            }
            var result = new Object[chars.length];
            for (var i = 0; i < chars.length; i++) {
                result[i] = chars[i];
            }
            return result;
        }
        if (value instanceof boolean[] booleans) {
            if (elementType != Boolean.class) {
                throw incompatibleType(typeCode, 0, elementType, boolean[].class);
            }
            var result = new Object[booleans.length];
            for (var i = 0; i < booleans.length; i++) {
                result[i] = booleans[i];
            }
            return result;
        }

        var elements = toObjectElements(value);
        var result = new Object[elements.length];
        for (var i = 0; i < elements.length; i++) {
            result[i] = requireType(elements[i], elementType, typeCode, i);
        }
        return result;
    }

    private static Object[] toObjectElements(Object value) {
        if (value == null) {
            return EMPTY_OBJECTS;
        }
        if (value instanceof Object[] objects) {
            return objects;
        }
        if (value instanceof Collection<?> collection) {
            return collection.toArray();
        }
        if (value instanceof Iterable<?> iterable) {
            var list = new ArrayList<>();
            for (var item : iterable) {
                list.add(item);
            }
            return list.toArray();
        }
        return new Object[] { value };
    }

    private static <T> T requireType(Object value, Class<T> expectedType, byte typeCode, int index) {
        if (value == null) {
            throw new IllegalArgumentException("IN values cannot contain nulls (type " + typeName(typeCode)
                    + ", index " + index + ")");
        }
        if (!expectedType.isInstance(value)) {
            throw incompatibleType(typeCode, index, expectedType, value.getClass());
        }
        return expectedType.cast(value);
    }

    private static IllegalArgumentException incompatibleType(byte typeCode,
            int index,
            Class<?> expectedType,
            Class<?> actualType) {
        return new IllegalArgumentException("IN value at index " + index + " for type " + typeName(typeCode)
                + " must be " + expectedType.getName() + " but was " + actualType.getName());
    }

    private static String typeName(byte typeCode) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INT -> "INT";
            case TypeCodes.TYPE_LONG -> "LONG";
            case TypeCodes.TYPE_BOOLEAN -> "BOOLEAN";
            case TypeCodes.TYPE_BYTE -> "BYTE";
            case TypeCodes.TYPE_SHORT -> "SHORT";
            case TypeCodes.TYPE_FLOAT -> "FLOAT";
            case TypeCodes.TYPE_DOUBLE -> "DOUBLE";
            case TypeCodes.TYPE_CHAR -> "CHAR";
            case TypeCodes.TYPE_STRING -> "STRING";
            case TypeCodes.TYPE_INSTANT -> "INSTANT";
            case TypeCodes.TYPE_LOCAL_DATE -> "LOCAL_DATE";
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> "LOCAL_DATE_TIME";
            case TypeCodes.TYPE_DATE -> "DATE";
            case TypeCodes.TYPE_BIG_DECIMAL -> "BIG_DECIMAL";
            case TypeCodes.TYPE_BIG_INTEGER -> "BIG_INTEGER";
            default -> "UNKNOWN(" + typeCode + ")";
        };
    }
}
