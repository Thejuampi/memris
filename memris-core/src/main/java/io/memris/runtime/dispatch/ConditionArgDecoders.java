package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.runtime.InArgumentDecoder;

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
        return InArgumentDecoder.toLongStorageValue(typeCode, value);
    }

    static int toInt(byte typeCode, Object value) {
        return InArgumentDecoder.toIntStorageValue(typeCode, value);
    }

    static String toStringValue(Object value) {
        return value == null ? null : value.toString();
    }

    static long[] toLongArray(byte typeCode, Object value) {
        return InArgumentDecoder.toLongArrayStrict(typeCode, value);
    }

    static int[] toIntArray(byte typeCode, Object value) {
        return InArgumentDecoder.toIntArrayStrict(typeCode, value);
    }

    static String[] toStringArray(byte typeCode, Object value) {
        return InArgumentDecoder.toStringArrayStrict(typeCode, value);
    }

    static String[] toStringArray(Object value) {
        return InArgumentDecoder.toStringArrayStrict(TypeCodes.TYPE_STRING, value);
    }
}
