package io.memris.runtime.handler;

import io.memris.runtime.TypeHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Invalid direct-handler inputs cannot be expressed by a correctly typed repository interface. */
class TypeHandlerEdgeCaseTest {
    @ParameterizedTest(name = "reject incompatible conversion: {0}")
    @MethodSource("handlers")
    void shouldRejectIncompatibleConversionInputs(TypeHandler<?> handler) {
        assertThatThrownBy(() -> handler.convertValue(new Object())).isInstanceOf(IllegalArgumentException.class);
    }

    private static Stream<TypeHandler<?>> handlers() {
        return Stream.of(new ByteTypeHandler(), new ShortTypeHandler(), new IntTypeHandler(), new LongTypeHandler(),
                new FloatTypeHandler(), new DoubleTypeHandler(), new BooleanTypeHandler(), new CharTypeHandler(),
                new StringTypeHandler(), new BigDecimalTypeHandler(), new BigIntegerTypeHandler(),
                new DateTypeHandler(), new InstantTypeHandler(), new LocalDateTypeHandler(), new LocalDateTimeTypeHandler());
    }

    @ParameterizedTest
    @ValueSource(strings = { "128", "-129", "invalid" })
    void shouldRejectUnrepresentableByteText(String value) {
        assertThatThrownBy(() -> new ByteTypeHandler().convertValue(value))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Cannot convert");
    }

    @Test
    void shouldRejectNullAndInvalidNumericTextInsteadOfCoercingIt() {
        assertThatThrownBy(() -> new ByteTypeHandler().convertValue(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleTypeHandler().convertValue(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShortTypeHandler().convertValue("32768")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DoubleTypeHandler().convertValue("invalid")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectHeterogeneousOrNullInElementsBeforeScanning() {
        var bytes = new ByteTypeHandler();
        var doubles = new DoubleTypeHandler();
        assertThatThrownBy(() -> bytes.executeIn(null, 0, List.of("1", (byte) 2)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be java.lang.Byte");
        assertThatThrownBy(() -> bytes.executeIn(null, 0, Arrays.asList((byte) 1, null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot contain nulls");
        assertThatThrownBy(() -> doubles.executeIn(null, 0, List.of("1.0", 2.0d)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must be java.lang.Double");
        assertThatThrownBy(() -> doubles.executeIn(null, 0, Arrays.asList(1.0d, null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot contain nulls");
    }

    @Test
    void shouldValidateCharacterAndBooleanCoercionsForUntypedCallers() {
        var characters = new CharTypeHandler();
        assertThat(characters.convertValue(65)).isEqualTo('A');
        assertThat(characters.convertValue("あ")).isEqualTo('あ');
        assertThatThrownBy(() -> characters.convertValue("AB")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> characters.convertValue("")).isInstanceOf(IllegalArgumentException.class);
        assertThat(new BooleanTypeHandler().convertValue(-5)).isTrue();
        assertThat(new BooleanTypeHandler().convertValue(0)).isFalse();
    }

    @Test
    void shouldRejectUnsupportedOrderingAndAmbiguousSingleValueRanges() {
        assertThatThrownBy(() -> new StringTypeHandler().executeGreaterThan(null, 0, "value"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new BooleanTypeHandler().executeLessThan(null, 0, true))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new BigDecimalTypeHandler().executeBetween(null, 0, java.math.BigDecimal.ONE))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new BigIntegerTypeHandler().executeGreaterThan(null, 0, java.math.BigInteger.ONE))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
