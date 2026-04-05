package io.memris.bugfix;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TurkishLocaleTest {

    @Test
    void toLowerCase_withoutRoot_shouldNotThrowForTurkishI() {
        assertThatCode(() -> "FINDBy".toLowerCase(Locale.ROOT)).doesNotThrowAnyException();

        var turkishResult = "FINDBy".toLowerCase(Locale.forLanguageTag("tr"));
        var rootResult = "FINDBy".toLowerCase(Locale.ROOT);

        assertThat(rootResult).isEqualTo("findby");
        assertThat(turkishResult).isNotEqualTo(rootResult);
    }
}
