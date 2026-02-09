package io.memris.runtime.dispatch;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;

final class ConditionArgDecoders {

    private ConditionArgDecoders() {
    }

    static Object argAt(Object[] args, int index) {
        if (index < 0 || index >= args.length) {
            throw new IllegalArgumentException("Condition argument index out of range: " + index);
        }
        return args[index];
    }

    static long toLong(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INSTANT -> ((Instant) value).toEpochMilli();
            case TypeCodes.TYPE_LOCAL_DATE -> ((LocalDate) value).toEpochDay();
            case TypeCodes.TYPE_LOCAL_DATE_TIME -> ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
            case TypeCodes.TYPE_DATE -> ((Date) value).getTime();
            case TypeCodes.TYPE_DOUBLE -> FloatEncoding.doubleToSortableLong(((Number) value).doubleValue());
            default -> ((Number) value).longValue();
        };
    }

    static int toInt(byte typeCode, Object value) {
        return switch (typeCode) {
            case TypeCodes.TYPE_BOOLEAN -> (Boolean) value ? 1 : 0;
            case TypeCodes.TYPE_CHAR -> (value instanceof Character character)
                    ? character
                    : value.toString().charAt(0);
            case TypeCodes.TYPE_FLOAT -> FloatEncoding.floatToSortableInt(((Number) value).floatValue());
            default -> ((Number) value).intValue();
        };
    }

    static String toStringValue(Object value) {
        return value == null ? null : value.toString();
    }

    static long[] toLongArray(byte typeCode, Object value) {
        if (value == null) {
            return new long[0];
        }
        if (value instanceof long[] longs) {
            return longs;
        }
        if (value instanceof int[] ints) {
            var result = new long[ints.length];
            for (var i = 0; i < ints.length; i++) {
                result[i] = ints[i];
            }
            return result;
        }
        if (value instanceof short[] shorts) {
            var result = new long[shorts.length];
            for (var i = 0; i < shorts.length; i++) {
                result[i] = shorts[i];
            }
            return result;
        }
        if (value instanceof byte[] bytes) {
            var result = new long[bytes.length];
            for (var i = 0; i < bytes.length; i++) {
                result[i] = bytes[i];
            }
            return result;
        }
        if (value instanceof double[] doubles) {
            var result = new long[doubles.length];
            for (var i = 0; i < doubles.length; i++) {
                result[i] = toLong(typeCode, doubles[i]);
            }
            return result;
        }
        if (value instanceof float[] floats) {
            var result = new long[floats.length];
            for (var i = 0; i < floats.length; i++) {
                result[i] = toLong(typeCode, floats[i]);
            }
            return result;
        }
        if (value instanceof Object[] objects) {
            var result = new long[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = toLong(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var result = new long[iterableSize(iterable)];
            var i = 0;
            for (var item : iterable) {
                result[i++] = toLong(typeCode, item);
            }
            return result;
        }
        return new long[] { toLong(typeCode, value) };
    }

    static int[] toIntArray(byte typeCode, Object value) {
        if (value == null) {
            return new int[0];
        }
        if (value instanceof int[] ints) {
            return ints;
        }
        if (value instanceof byte[] bytes) {
            var result = new int[bytes.length];
            for (var i = 0; i < bytes.length; i++) {
                result[i] = bytes[i];
            }
            return result;
        }
        if (value instanceof short[] shorts) {
            var result = new int[shorts.length];
            for (var i = 0; i < shorts.length; i++) {
                result[i] = shorts[i];
            }
            return result;
        }
        if (value instanceof char[] chars) {
            var result = new int[chars.length];
            for (var i = 0; i < chars.length; i++) {
                result[i] = chars[i];
            }
            return result;
        }
        if (value instanceof boolean[] booleans) {
            var result = new int[booleans.length];
            for (var i = 0; i < booleans.length; i++) {
                result[i] = booleans[i] ? 1 : 0;
            }
            return result;
        }
        if (value instanceof float[] floats) {
            var result = new int[floats.length];
            for (var i = 0; i < floats.length; i++) {
                result[i] = toInt(typeCode, floats[i]);
            }
            return result;
        }
        if (value instanceof long[] longs) {
            var result = new int[longs.length];
            for (var i = 0; i < longs.length; i++) {
                result[i] = toInt(typeCode, longs[i]);
            }
            return result;
        }
        if (value instanceof Object[] objects) {
            var result = new int[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = toInt(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var result = new int[iterableSize(iterable)];
            var i = 0;
            for (var item : iterable) {
                result[i++] = toInt(typeCode, item);
            }
            return result;
        }
        return new int[] { toInt(typeCode, value) };
    }

    static String[] toStringArray(Object value) {
        if (value == null) {
            return new String[0];
        }
        if (value instanceof String[] strings) {
            return strings;
        }
        if (value instanceof Object[] objects) {
            var result = new String[objects.length];
            for (var i = 0; i < objects.length; i++) {
                result[i] = toStringValue(objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            var result = new String[iterableSize(iterable)];
            var i = 0;
            for (var item : iterable) {
                result[i++] = toStringValue(item);
            }
            return result;
        }
        return new String[] { toStringValue(value) };
    }

    private static int iterableSize(Iterable<?> iterable) {
        if (iterable instanceof Collection<?> collection) {
            return collection.size();
        }
        var size = 0;
        for (var ignored : iterable) {
            size++;
        }
        return size;
    }
}
