package io.memris.spring;

import io.memris.spring.converter.TypeConverter;
import io.memris.spring.converter.TypeConverterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TypeConverterRegistryTest {

    @Test
    void timeConvertersUseEpochLongs() {
        TypeConverterRegistry registry = TypeConverterRegistry.getInstance();

        TypeConverter<Instant, ?> instantConverter = registry.getConverter(Instant.class);
        assertNotNull(instantConverter);
        Object instantStorage = instantConverter.toStorage(Instant.ofEpochMilli(1234L));
        assertEquals(1234L, instantStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<Instant, Long> typedInstant = (TypeConverter<Instant, Long>) instantConverter;
        assertEquals(Instant.ofEpochMilli(1234L), typedInstant.fromStorage(1234L));

        TypeConverter<LocalDate, ?> dateConverter = registry.getConverter(LocalDate.class);
        assertNotNull(dateConverter);
        Object dateStorage = dateConverter.toStorage(LocalDate.ofEpochDay(5));
        assertEquals(5L, dateStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<LocalDate, Long> typedDate = (TypeConverter<LocalDate, Long>) dateConverter;
        assertEquals(LocalDate.ofEpochDay(5), typedDate.fromStorage(5L));

        TypeConverter<LocalDateTime, ?> dateTimeConverter = registry.getConverter(LocalDateTime.class);
        assertNotNull(dateTimeConverter);
        Object dateTimeStorage = dateTimeConverter.toStorage(LocalDateTime.of(1970, 1, 1, 0, 0, 1));
        assertEquals(1000L, dateTimeStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<LocalDateTime, Long> typedDateTime = (TypeConverter<LocalDateTime, Long>) dateTimeConverter;
        assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0, 1), typedDateTime.fromStorage(1000L));

        TypeConverter<Date, ?> utilDateConverter = registry.getConverter(Date.class);
        assertNotNull(utilDateConverter);
        Object utilDateStorage = utilDateConverter.toStorage(new Date(9876L));
        assertEquals(9876L, utilDateStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<Date, Long> typedUtilDate = (TypeConverter<Date, Long>) utilDateConverter;
        assertEquals(new Date(9876L), typedUtilDate.fromStorage(9876L));
    }

    @Test
    void bigConvertersUseStrings() {
        TypeConverterRegistry registry = TypeConverterRegistry.getInstance();

        TypeConverter<BigDecimal, ?> decimalConverter = registry.getConverter(BigDecimal.class);
        assertNotNull(decimalConverter);
        Object decimalStorage = decimalConverter.toStorage(new BigDecimal("123.45"));
        assertEquals("123.45", decimalStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<BigDecimal, String> typedDecimal = (TypeConverter<BigDecimal, String>) decimalConverter;
        assertEquals(new BigDecimal("123.45"), typedDecimal.fromStorage("123.45"));

        TypeConverter<BigInteger, ?> integerConverter = registry.getConverter(BigInteger.class);
        assertNotNull(integerConverter);
        Object integerStorage = integerConverter.toStorage(new BigInteger("987654321"));
        assertEquals("987654321", integerStorage);

        @SuppressWarnings("unchecked")
        TypeConverter<BigInteger, String> typedInteger = (TypeConverter<BigInteger, String>) integerConverter;
        assertEquals(new BigInteger("987654321"), typedInteger.fromStorage("987654321"));
    }
}
