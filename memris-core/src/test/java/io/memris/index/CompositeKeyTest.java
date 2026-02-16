package io.memris.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeKeyTest {

    @Test
    void compareToHandlesSentinelNullAndFallbackComparison() {
        assertThat(CompositeKey.of(new Object[] { CompositeKey.minSentinel() })
                .compareTo(CompositeKey.of(new Object[] { 10 })))
                .isNegative();
        assertThat(CompositeKey.of(new Object[] { CompositeKey.maxSentinel() })
                .compareTo(CompositeKey.of(new Object[] { 10 })))
                .isPositive();
        assertThat(CompositeKey.of(new Object[] { null }).compareTo(CompositeKey.of(new Object[] { 10 })))
                .isNegative();
        assertThat(CompositeKey.of(new Object[] { 10 }).compareTo(CompositeKey.of(new Object[] { null })))
                .isPositive();

        Object left = new NonComparable("a");
        Object right = new NonComparable("b");
        assertThat(CompositeKey.of(new Object[] { left }).compareTo(CompositeKey.of(new Object[] { right })))
                .isNegative();
    }

    @Test
    void equalsHandlesTypeMismatch() {
        var key = CompositeKey.of(new Object[] { 1, "x" });
        assertThat(key.equals("x")).isFalse();
        assertThat(key.equals(CompositeKey.of(new Object[] { 1, "x" }))).isTrue();
    }

    private record NonComparable(String value) {
        @Override
        public String toString() {
            return value;
        }
    }
}
