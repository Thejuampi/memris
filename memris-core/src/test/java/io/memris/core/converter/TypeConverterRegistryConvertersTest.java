package io.memris.core.converter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TypeConverterRegistry converters.
 */
class TypeConverterRegistryConvertersTest {

    private final TypeConverterRegistry registry = TypeConverterRegistry.getInstance();

    private record ConversionSnapshot<T, S>(boolean converterPresent, S storage, T restored) {
    }

    @SuppressWarnings("unchecked")
    private <T, S> TypeConverter<T, S> converterFor(Class<T> type) {
        return (TypeConverter<T, S>) (TypeConverter<?, ?>) registry.getConverter(type);
    }

    @Test
    @DisplayName("Should convert UUID to String and back")
    void shouldConvertUuidToStringAndBack() {
        var converter = converterFor(UUID.class);
        var original = UUID.randomUUID();
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, original.toString(), original));
    }

    @Test
    @DisplayName("Should handle null UUID")
    void shouldHandleNullUuid() {
        var converter = converterFor(UUID.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should convert BigDecimal to String and back")
    void shouldConvertBigDecimalToStringAndBack() {
        var converter = converterFor(BigDecimal.class);
        var original = new BigDecimal("123456789.0123456789");
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, "123456789.0123456789", original));
    }

    @Test
    @DisplayName("Should handle null BigDecimal")
    void shouldHandleNullBigDecimal() {
        var converter = converterFor(BigDecimal.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should convert BigInteger to String and back")
    void shouldConvertBigIntegerToStringAndBack() {
        var converter = converterFor(BigInteger.class);
        var original = new BigInteger("123456789012345678901234567890");
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, "123456789012345678901234567890", original));
    }

    @Test
    @DisplayName("Should handle null BigInteger")
    void shouldHandleNullBigInteger() {
        var converter = converterFor(BigInteger.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should convert LocalDate to epoch day and back")
    void shouldConvertLocalDateToEpochDayAndBack() {
        var converter = converterFor(LocalDate.class);
        var original = LocalDate.of(2024, 1, 15);
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, original.toEpochDay(), original));
    }

    @Test
    @DisplayName("Should handle null LocalDate")
    void shouldHandleNullLocalDate() {
        var converter = converterFor(LocalDate.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null)))
                .containsExactly(null, null);
    }

    @Test
    @DisplayName("Should convert LocalDateTime to epoch millis and back")
    void shouldConvertLocalDateTimeToEpochMillisAndBack() {
        var converter = converterFor(LocalDateTime.class);
        var original = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage != null, restored))
                .isEqualTo(new ConversionSnapshot<>(true, true, original));
    }

    @Test
    @DisplayName("Should handle null LocalDateTime")
    void shouldHandleNullLocalDateTime() {
        var converter = converterFor(LocalDateTime.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null)))
                .containsExactly(null, null);
    }

    @Test
    @DisplayName("Should convert LocalTime to String and back")
    void shouldConvertLocalTimeToStringAndBack() {
        var converter = converterFor(LocalTime.class);
        var original = LocalTime.of(14, 30, 45);
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, "14:30:45", original));
    }

    @Test
    @DisplayName("Should handle null LocalTime")
    void shouldHandleNullLocalTime() {
        var converter = converterFor(LocalTime.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should convert Instant to epoch millis and back")
    void shouldConvertInstantToEpochMillisAndBack() {
        var converter = converterFor(Instant.class);
        var original = Instant.parse("2024-01-15T10:30:45Z");
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, original.toEpochMilli(), original));
    }

    @Test
    @DisplayName("Should handle null Instant")
    void shouldHandleNullInstant() {
        var converter = converterFor(Instant.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null)))
                .containsExactly(null, null);
    }

    @Test
    @DisplayName("Should convert Date to epoch millis and back")
    void shouldConvertDateToEpochMillisAndBack() {
        var converter = converterFor(Date.class);
        var original = new Date(1705315845000L);
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, original.getTime(), original));
    }

    @Test
    @DisplayName("Should handle null Date")
    void shouldHandleNullDate() {
        var converter = converterFor(Date.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null)))
                .containsExactly(null, null);
    }

    @Test
    @DisplayName("Should convert java.sql.Date to String and back")
    void shouldConvertSqlDateToStringAndBack() {
        var converter = converterFor(java.sql.Date.class);
        var original = java.sql.Date.valueOf("2024-01-15");
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, "2024-01-15", original));
    }

    @Test
    @DisplayName("Should handle null java.sql.Date")
    void shouldHandleNullSqlDate() {
        var converter = converterFor(java.sql.Date.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should convert java.sql.Timestamp to String and back")
    void shouldConvertSqlTimestampToStringAndBack() {
        var converter = converterFor(java.sql.Timestamp.class);
        var original = java.sql.Timestamp.valueOf("2024-01-15 10:30:45");
        var storage = converter.toStorage(original);
        var restored = converter.fromStorage(storage);

        assertThat(new ConversionSnapshot<>(converter != null, storage, restored))
                .isEqualTo(new ConversionSnapshot<>(true, "2024-01-15T10:30:45", original));
    }

    @Test
    @DisplayName("Should handle null java.sql.Timestamp")
    void shouldHandleNullSqlTimestamp() {
        var converter = converterFor(java.sql.Timestamp.class);

        assertThat(Arrays.asList(converter.toStorage(null), converter.fromStorage(null), converter.fromStorage("")))
                .containsExactly(null, null, null);
    }

    @Test
    @DisplayName("Should handle edge case dates")
    void shouldHandleEdgeCaseDates() {
        var dateConverter = converterFor(LocalDate.class);
        var minDate = LocalDate.MIN;
        var maxDate = LocalDate.MAX;
        var epoch = LocalDate.ofEpochDay(0);

        assertThat(List.of(
                dateConverter.fromStorage(dateConverter.toStorage(minDate)),
                dateConverter.fromStorage(dateConverter.toStorage(maxDate)),
                dateConverter.fromStorage(dateConverter.toStorage(epoch))
        )).containsExactly(minDate, maxDate, epoch);
    }

    @Test
    @DisplayName("Should handle edge case BigDecimal values")
    void shouldHandleEdgeCaseBigDecimalValues() {
        var converter = converterFor(BigDecimal.class);
        var zero = BigDecimal.ZERO;
        var negative = new BigDecimal("-999999999999999999.999999999");
        var verySmall = new BigDecimal("0.000000000000000001");

        assertThat(List.of(
                converter.fromStorage(converter.toStorage(zero)),
                converter.fromStorage(converter.toStorage(negative)),
                converter.fromStorage(converter.toStorage(verySmall))
        )).containsExactly(zero, negative, verySmall);
    }

    @Test
    @DisplayName("Should handle edge case BigInteger values")
    void shouldHandleEdgeCaseBigIntegerValues() {
        var converter = converterFor(BigInteger.class);
        var zero = BigInteger.ZERO;
        var negative = new BigInteger("-999999999999999999999999999999");
        var veryLarge = new BigInteger("999999999999999999999999999999");

        assertThat(List.of(
                converter.fromStorage(converter.toStorage(zero)),
                converter.fromStorage(converter.toStorage(negative)),
                converter.fromStorage(converter.toStorage(veryLarge))
        )).containsExactly(zero, negative, veryLarge);
    }
}
