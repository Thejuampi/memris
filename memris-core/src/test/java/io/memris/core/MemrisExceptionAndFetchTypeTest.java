package io.memris.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MemrisExceptionAndFetchTypeTest {

    @Test
    void shouldConstructExceptionWithCauseOnly() {
        var cause = new IllegalStateException("root");

        var exception = new MemrisException(cause);

        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    void shouldConstructExceptionWithMessageAndCause() {
        var cause = new IllegalArgumentException("bad");

        var exception = new MemrisException("boom", cause);

        assertThat(exception.getMessage()).isEqualTo("boom");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    void shouldConstructExceptionWithMessageOnly() {
        var exception = new MemrisException("only-message");

        assertThat(exception.getMessage()).isEqualTo("only-message");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void shouldExposeSingleSupportedFetchType() {
        assertThat(FetchType.values()).containsExactly(FetchType.EAGER);
        assertThat(FetchType.valueOf("EAGER")).isEqualTo(FetchType.EAGER);
        assertThat(FetchType.EAGER.name()).isEqualTo("EAGER");
    }
}
