package io.memris.core;

import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class TypeConverterRegistryTest {

    @Test
    void timeConvertersUseEpochLongs() {
        var registry = TypeConverterRegistry.getInstance();
        var actual = new TimeSnapshot(
                registry.getConverter(Instant.class).toStorage(Instant.ofEpochMilli(1234L)),
                fromStorage(registry.getConverter(Instant.class), 1234L),
                registry.getConverter(LocalDate.class).toStorage(LocalDate.ofEpochDay(5)),
                fromStorage(registry.getConverter(LocalDate.class), 5L),
                registry.getConverter(LocalDateTime.class).toStorage(LocalDateTime.of(1970, 1, 1, 0, 0, 1)),
                fromStorage(registry.getConverter(LocalDateTime.class), 1000L),
                registry.getConverter(Date.class).toStorage(new Date(9876L)),
                fromStorage(registry.getConverter(Date.class), 9876L)
        );
        var expected = new TimeSnapshot(
                1234L,
                Instant.ofEpochMilli(1234L),
                5L,
                LocalDate.ofEpochDay(5),
                1000L,
                LocalDateTime.of(1970, 1, 1, 0, 0, 1),
                9876L,
                new Date(9876L)
        );
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void bigConvertersUseStrings() {
        var registry = TypeConverterRegistry.getInstance();
        var actual = new BigSnapshot(
                registry.getConverter(BigDecimal.class).toStorage(new BigDecimal("123.45")),
                fromStorage(registry.getConverter(BigDecimal.class), "123.45"),
                registry.getConverter(BigInteger.class).toStorage(new BigInteger("987654321")),
                fromStorage(registry.getConverter(BigInteger.class), "987654321")
        );
        var expected = new BigSnapshot(
                "123.45",
                new BigDecimal("123.45"),
                "987654321",
                new BigInteger("987654321")
        );
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    @SuppressWarnings("unchecked")
    private static <T, S> T fromStorage(TypeConverter<T, ?> converter, S value) {
        return ((TypeConverter<T, S>) converter).fromStorage(value);
    }

    private record TimeSnapshot(
            Object instantStorage,
            Object instantFromStorage,
            Object dateStorage,
            Object dateFromStorage,
            Object dateTimeStorage,
            Object dateTimeFromStorage,
            Object utilDateStorage,
            Object utilDateFromStorage
    ) {
    }

    private record BigSnapshot(
            Object decimalStorage,
            Object decimalFromStorage,
            Object integerStorage,
            Object integerFromStorage
    ) {
    }
}
