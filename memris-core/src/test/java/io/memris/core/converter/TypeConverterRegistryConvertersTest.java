package io.memris.core.converter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TypeConverterRegistry converters.
 */
class TypeConverterRegistryConvertersTest {

    private final TypeConverterRegistry registry = TypeConverterRegistry.getInstance();

    @Test
    @DisplayName("Should convert UUID to String and back")
    void shouldConvertUuidToStringAndBack() {
        TypeConverter<UUID, String> converter = (TypeConverter<UUID, String>) (TypeConverter<?, ?>) registry.getConverter(UUID.class);
        assertThat(converter).isNotNull();

        UUID original = UUID.randomUUID();
        String storage = converter.toStorage(original);
        UUID restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo(original.toString());
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null UUID")
    void shouldHandleNullUuid() {
        TypeConverter<UUID, String> converter = (TypeConverter<UUID, String>) (TypeConverter<?, ?>) registry.getConverter(UUID.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should convert BigDecimal to String and back")
    void shouldConvertBigDecimalToStringAndBack() {
        TypeConverter<BigDecimal, String> converter = (TypeConverter<BigDecimal, String>) (TypeConverter<?, ?>) registry.getConverter(BigDecimal.class);
        assertThat(converter).isNotNull();

        BigDecimal original = new BigDecimal("123456789.0123456789");
        String storage = converter.toStorage(original);
        BigDecimal restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo("123456789.0123456789");
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null BigDecimal")
    void shouldHandleNullBigDecimal() {
        TypeConverter<BigDecimal, String> converter = (TypeConverter<BigDecimal, String>) (TypeConverter<?, ?>) registry.getConverter(BigDecimal.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should convert BigInteger to String and back")
    void shouldConvertBigIntegerToStringAndBack() {
        TypeConverter<BigInteger, String> converter = (TypeConverter<BigInteger, String>) (TypeConverter<?, ?>) registry.getConverter(BigInteger.class);
        assertThat(converter).isNotNull();

        BigInteger original = new BigInteger("123456789012345678901234567890");
        String storage = converter.toStorage(original);
        BigInteger restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo("123456789012345678901234567890");
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null BigInteger")
    void shouldHandleNullBigInteger() {
        TypeConverter<BigInteger, String> converter = (TypeConverter<BigInteger, String>) (TypeConverter<?, ?>) registry.getConverter(BigInteger.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should convert LocalDate to epoch day and back")
    void shouldConvertLocalDateToEpochDayAndBack() {
        TypeConverter<LocalDate, Long> converter = (TypeConverter<LocalDate, Long>) (TypeConverter<?, ?>) registry.getConverter(LocalDate.class);
        assertThat(converter).isNotNull();

        LocalDate original = LocalDate.of(2024, 1, 15);
        Long storage = converter.toStorage(original);
        LocalDate restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo(original.toEpochDay());
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null LocalDate")
    void shouldHandleNullLocalDate() {
        TypeConverter<LocalDate, Long> converter = (TypeConverter<LocalDate, Long>) (TypeConverter<?, ?>) registry.getConverter(LocalDate.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
    }

    @Test
    @DisplayName("Should convert LocalDateTime to epoch millis and back")
    void shouldConvertLocalDateTimeToEpochMillisAndBack() {
        TypeConverter<LocalDateTime, Long> converter = (TypeConverter<LocalDateTime, Long>) (TypeConverter<?, ?>) registry.getConverter(LocalDateTime.class);
        assertThat(converter).isNotNull();

        LocalDateTime original = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
        Long storage = converter.toStorage(original);
        LocalDateTime restored = converter.fromStorage(storage);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null LocalDateTime")
    void shouldHandleNullLocalDateTime() {
        TypeConverter<LocalDateTime, Long> converter = (TypeConverter<LocalDateTime, Long>) (TypeConverter<?, ?>) registry.getConverter(LocalDateTime.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
    }

    @Test
    @DisplayName("Should convert LocalTime to String and back")
    void shouldConvertLocalTimeToStringAndBack() {
        TypeConverter<LocalTime, String> converter = (TypeConverter<LocalTime, String>) (TypeConverter<?, ?>) registry.getConverter(LocalTime.class);
        assertThat(converter).isNotNull();

        LocalTime original = LocalTime.of(14, 30, 45);
        String storage = converter.toStorage(original);
        LocalTime restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo("14:30:45");
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null LocalTime")
    void shouldHandleNullLocalTime() {
        TypeConverter<LocalTime, String> converter = (TypeConverter<LocalTime, String>) (TypeConverter<?, ?>) registry.getConverter(LocalTime.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should convert Instant to epoch millis and back")
    void shouldConvertInstantToEpochMillisAndBack() {
        TypeConverter<Instant, Long> converter = (TypeConverter<Instant, Long>) (TypeConverter<?, ?>) registry.getConverter(Instant.class);
        assertThat(converter).isNotNull();

        Instant original = Instant.parse("2024-01-15T10:30:45Z");
        Long storage = converter.toStorage(original);
        Instant restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo(original.toEpochMilli());
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null Instant")
    void shouldHandleNullInstant() {
        TypeConverter<Instant, Long> converter = (TypeConverter<Instant, Long>) (TypeConverter<?, ?>) registry.getConverter(Instant.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
    }

    @Test
    @DisplayName("Should convert Date to epoch millis and back")
    void shouldConvertDateToEpochMillisAndBack() {
        TypeConverter<Date, Long> converter = (TypeConverter<Date, Long>) (TypeConverter<?, ?>) registry.getConverter(Date.class);
        assertThat(converter).isNotNull();

        Date original = new Date(1705315845000L);
        Long storage = converter.toStorage(original);
        Date restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo(original.getTime());
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null Date")
    void shouldHandleNullDate() {
        TypeConverter<Date, Long> converter = (TypeConverter<Date, Long>) (TypeConverter<?, ?>) registry.getConverter(Date.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
    }

    @Test
    @DisplayName("Should convert java.sql.Date to String and back")
    void shouldConvertSqlDateToStringAndBack() {
        TypeConverter<java.sql.Date, String> converter = (TypeConverter<java.sql.Date, String>) (TypeConverter<?, ?>) registry.getConverter(java.sql.Date.class);
        assertThat(converter).isNotNull();

        java.sql.Date original = java.sql.Date.valueOf("2024-01-15");
        String storage = converter.toStorage(original);
        java.sql.Date restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo("2024-01-15");
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null java.sql.Date")
    void shouldHandleNullSqlDate() {
        TypeConverter<java.sql.Date, String> converter = (TypeConverter<java.sql.Date, String>) (TypeConverter<?, ?>) registry.getConverter(java.sql.Date.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should convert java.sql.Timestamp to String and back")
    void shouldConvertSqlTimestampToStringAndBack() {
        TypeConverter<java.sql.Timestamp, String> converter = (TypeConverter<java.sql.Timestamp, String>) (TypeConverter<?, ?>) registry.getConverter(java.sql.Timestamp.class);
        assertThat(converter).isNotNull();

        java.sql.Timestamp original = java.sql.Timestamp.valueOf("2024-01-15 10:30:45");
        String storage = converter.toStorage(original);
        java.sql.Timestamp restored = converter.fromStorage(storage);

        assertThat(storage).isEqualTo("2024-01-15T10:30:45");
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("Should handle null java.sql.Timestamp")
    void shouldHandleNullSqlTimestamp() {
        TypeConverter<java.sql.Timestamp, String> converter = (TypeConverter<java.sql.Timestamp, String>) (TypeConverter<?, ?>) registry.getConverter(java.sql.Timestamp.class);

        assertThat(converter.toStorage(null)).isNull();
        assertThat(converter.fromStorage(null)).isNull();
        assertThat(converter.fromStorage("")).isNull();
    }

    @Test
    @DisplayName("Should handle edge case dates")
    void shouldHandleEdgeCaseDates() {
        TypeConverter<LocalDate, Long> dateConverter = (TypeConverter<LocalDate, Long>) (TypeConverter<?, ?>) registry.getConverter(LocalDate.class);

        // Min/Max dates
        LocalDate minDate = LocalDate.MIN;
        LocalDate maxDate = LocalDate.MAX;

        assertThat(dateConverter.fromStorage(dateConverter.toStorage(minDate))).isEqualTo(minDate);
        assertThat(dateConverter.fromStorage(dateConverter.toStorage(maxDate))).isEqualTo(maxDate);

        // Epoch
        LocalDate epoch = LocalDate.ofEpochDay(0);
        assertThat(dateConverter.fromStorage(dateConverter.toStorage(epoch))).isEqualTo(epoch);
    }

    @Test
    @DisplayName("Should handle edge case BigDecimal values")
    void shouldHandleEdgeCaseBigDecimalValues() {
        TypeConverter<BigDecimal, String> converter = (TypeConverter<BigDecimal, String>) (TypeConverter<?, ?>) registry.getConverter(BigDecimal.class);

        BigDecimal zero = BigDecimal.ZERO;
        BigDecimal negative = new BigDecimal("-999999999999999999.999999999");
        BigDecimal verySmall = new BigDecimal("0.000000000000000001");

        assertThat(converter.fromStorage(converter.toStorage(zero))).isEqualTo(zero);
        assertThat(converter.fromStorage(converter.toStorage(negative))).isEqualTo(negative);
        assertThat(converter.fromStorage(converter.toStorage(verySmall))).isEqualTo(verySmall);
    }

    @Test
    @DisplayName("Should handle edge case BigInteger values")
    void shouldHandleEdgeCaseBigIntegerValues() {
        TypeConverter<BigInteger, String> converter = (TypeConverter<BigInteger, String>) (TypeConverter<?, ?>) registry.getConverter(BigInteger.class);

        BigInteger zero = BigInteger.ZERO;
        BigInteger negative = new BigInteger("-999999999999999999999999999999");
        BigInteger veryLarge = new BigInteger("999999999999999999999999999999");

        assertThat(converter.fromStorage(converter.toStorage(zero))).isEqualTo(zero);
        assertThat(converter.fromStorage(converter.toStorage(negative))).isEqualTo(negative);
        assertThat(converter.fromStorage(converter.toStorage(veryLarge))).isEqualTo(veryLarge);
    }
}
