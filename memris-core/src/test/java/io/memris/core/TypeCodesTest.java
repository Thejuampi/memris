package io.memris.core;

import io.memris.core.TypeCodes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TypeCodesTest {

    @Test
    void resolvesCommonTypes() {
        var actual = Map.of(
                Instant.class, TypeCodes.forClass(Instant.class),
                LocalDate.class, TypeCodes.forClass(LocalDate.class),
                LocalDateTime.class, TypeCodes.forClass(LocalDateTime.class),
                Date.class, TypeCodes.forClass(Date.class),
                BigDecimal.class, TypeCodes.forClass(BigDecimal.class),
                BigInteger.class, TypeCodes.forClass(BigInteger.class)
        );
        var expected = Map.of(
                Instant.class, TypeCodes.TYPE_INSTANT,
                LocalDate.class, TypeCodes.TYPE_LOCAL_DATE,
                LocalDateTime.class, TypeCodes.TYPE_LOCAL_DATE_TIME,
                Date.class, TypeCodes.TYPE_DATE,
                BigDecimal.class, TypeCodes.TYPE_BIG_DECIMAL,
                BigInteger.class, TypeCodes.TYPE_BIG_INTEGER
        );
        assertThat(actual).isEqualTo(expected);
    }
}
