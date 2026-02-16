package io.memris.query;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IdParamTest {

    @Test
    void isValidIdType_checks() {
        assertThat(IdParam.isValidIdType(int.class)).isFalse();
        assertThat(IdParam.isValidIdType(Integer.class)).isTrue();
        assertThat(IdParam.isValidIdType(Long.class)).isTrue();
        assertThat(IdParam.isValidIdType(String.class)).isTrue();
        assertThat(IdParam.isValidIdType(String[].class)).isFalse();
        assertThat(IdParam.isValidIdType(List.class)).isFalse();
    }
}
