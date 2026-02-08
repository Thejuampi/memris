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
        var instantConverter = registry.getConverter(Instant.class);
        var localDateConverter = registry.getConverter(LocalDate.class);
        var localDateTimeConverter = registry.getConverter(LocalDateTime.class);
        var dateConverter = registry.getConverter(Date.class);
        assertThat(instantConverter).isNotNull();
        assertThat(localDateConverter).isNotNull();
        assertThat(localDateTimeConverter).isNotNull();
        assertThat(dateConverter).isNotNull();
        var actual = new TimeSnapshot(
                instantConverter.toStorage(Instant.ofEpochMilli(1234L)),
                fromStorage(instantConverter, 1234L),
                localDateConverter.toStorage(LocalDate.ofEpochDay(5)),
                fromStorage(localDateConverter, 5L),
                localDateTimeConverter.toStorage(LocalDateTime.of(1970, 1, 1, 0, 0, 1)),
                fromStorage(localDateTimeConverter, 1000L),
                dateConverter.toStorage(new Date(9876L)),
                fromStorage(dateConverter, 9876L)
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
        var bigDecimalConverter = registry.getConverter(BigDecimal.class);
        var bigIntegerConverter = registry.getConverter(BigInteger.class);
        assertThat(bigDecimalConverter).isNotNull();
        assertThat(bigIntegerConverter).isNotNull();
        var actual = new BigSnapshot(
                bigDecimalConverter.toStorage(new BigDecimal("123.45")),
                fromStorage(bigDecimalConverter, "123.45"),
                bigIntegerConverter.toStorage(new BigInteger("987654321")),
                fromStorage(bigIntegerConverter, "987654321")
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
