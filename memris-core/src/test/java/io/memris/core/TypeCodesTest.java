package io.memris.core;

import io.memris.core.TypeCodes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TypeCodesTest {

    @Test
    void resolvesCommonTypes() {
        assertEquals(TypeCodes.TYPE_INSTANT, TypeCodes.forClass(Instant.class));
        assertEquals(TypeCodes.TYPE_LOCAL_DATE, TypeCodes.forClass(LocalDate.class));
        assertEquals(TypeCodes.TYPE_LOCAL_DATE_TIME, TypeCodes.forClass(LocalDateTime.class));
        assertEquals(TypeCodes.TYPE_DATE, TypeCodes.forClass(Date.class));
        assertEquals(TypeCodes.TYPE_BIG_DECIMAL, TypeCodes.forClass(BigDecimal.class));
        assertEquals(TypeCodes.TYPE_BIG_INTEGER, TypeCodes.forClass(BigInteger.class));
    }
}
