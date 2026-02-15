package io.memris.runtime.dispatch;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionArgDecodersTest {

    @Test
    void argAtShouldReturnValueAndRejectOutOfRange() {
        Object[] args = { "a", 2 };

        assertThat(ConditionArgDecoders.argAt(args, 1)).isEqualTo(2);
        assertThatThrownBy(() -> ConditionArgDecoders.argAt(args, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConditionArgDecoders.argAt(args, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toLongShouldDecodeTemporalAndNumericTypes() {
        Instant instant = Instant.parse("2024-01-02T03:04:05Z");
        LocalDate date = LocalDate.of(2024, 1, 2);
        LocalDateTime dateTime = LocalDateTime.of(2024, 1, 2, 3, 4, 5);
        Date legacyDate = new Date(1_700_000_000_000L);

        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_INSTANT, instant)).isEqualTo(instant.toEpochMilli());
        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_LOCAL_DATE, date)).isEqualTo(date.toEpochDay());
        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_LOCAL_DATE_TIME, dateTime))
                .isEqualTo(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_DATE, legacyDate)).isEqualTo(legacyDate.getTime());
        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_DOUBLE, 12.5d))
                .isEqualTo(FloatEncoding.doubleToSortableLong(12.5d));
        assertThat(ConditionArgDecoders.toLong(TypeCodes.TYPE_LONG, 77)).isEqualTo(77L);
    }

    @Test
    void toIntShouldDecodeBooleanCharFloatAndNumericTypes() {
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_BOOLEAN, true)).isEqualTo(1);
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_BOOLEAN, false)).isEqualTo(0);
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_CHAR, 'Z')).isEqualTo((int) 'Z');
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_CHAR, "K")).isEqualTo((int) 'K');
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_FLOAT, 9.5f))
                .isEqualTo(FloatEncoding.floatToSortableInt(9.5f));
        assertThat(ConditionArgDecoders.toInt(TypeCodes.TYPE_INT, 42L)).isEqualTo(42);
    }

    @Test
    void toLongArrayShouldHandlePrimitiveArraysObjectArraysIterableAndScalar() {
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, null)).isEmpty();
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, new long[] { 1L, 2L }))
                .containsExactly(1L, 2L);
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, new int[] { 3, 4 }))
                .containsExactly(3L, 4L);
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, new short[] { 5, 6 }))
                .containsExactly(5L, 6L);
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, new byte[] { 7, 8 }))
                .containsExactly(7L, 8L);

        var doubles = ConditionArgDecoders.toLongArray(TypeCodes.TYPE_DOUBLE, new double[] { 1.25d, 2.5d });
        assertThat(doubles).containsExactly(
                FloatEncoding.doubleToSortableLong(1.25d),
                FloatEncoding.doubleToSortableLong(2.5d));

        var floats = ConditionArgDecoders.toLongArray(TypeCodes.TYPE_DOUBLE, new float[] { 1.5f });
        assertThat(floats).containsExactly(FloatEncoding.doubleToSortableLong(1.5d));

        var objects = ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, new Object[] { 9, 10L });
        assertThat(objects).containsExactly(9L, 10L);

        Iterable<Object> iterable = List.of((Object) 11, 12L);
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, iterable)).containsExactly(11L, 12L);
        assertThat(ConditionArgDecoders.toLongArray(TypeCodes.TYPE_LONG, 13)).containsExactly(13L);
    }

    @Test
    void toIntArrayShouldHandlePrimitiveArraysObjectArraysIterableAndScalar() {
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, null)).isEmpty();
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new int[] { 1, 2 }))
                .containsExactly(1, 2);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new byte[] { 3, 4 }))
                .containsExactly(3, 4);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new short[] { 5, 6 }))
                .containsExactly(5, 6);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new char[] { 'A', 'B' }))
                .containsExactly((int) 'A', (int) 'B');
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_BOOLEAN, new boolean[] { true, false, true }))
                .containsExactly(1, 0, 1);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_FLOAT, new float[] { 1.5f }))
                .containsExactly(FloatEncoding.floatToSortableInt(1.5f));
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new long[] { 7L, 8L }))
                .containsExactly(7, 8);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, new Object[] { 9, 10L }))
                .containsExactly(9, 10);

        Iterable<Object> iterable = List.of((Object) 11, 12);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, iterable)).containsExactly(11, 12);
        assertThat(ConditionArgDecoders.toIntArray(TypeCodes.TYPE_INT, 13L)).containsExactly(13);
    }

    @Test
    void toStringArrayShouldHandleNullArrayObjectArrayIterableAndScalar() {
        assertThat(ConditionArgDecoders.toStringArray(null)).isEmpty();
        assertThat(ConditionArgDecoders.toStringArray(new String[] { "a", "b" })).containsExactly("a", "b");
        assertThat(ConditionArgDecoders.toStringArray(new Object[] { "x", 1, null })).containsExactly("x", "1", null);

        Iterable<Object> iterable = Arrays.asList((Object) "y", 2, null);
        assertThat(ConditionArgDecoders.toStringArray(iterable)).containsExactly("y", "2", null);
        assertThat(ConditionArgDecoders.toStringArray(99)).containsExactly("99");
    }
}
