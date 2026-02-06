package io.memris.spring.data.repository.support;

import org.junit.jupiter.api.Test;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemrisEntityInformationTest {

    @Test
    void shouldReadEntityIdFromExplicitIdField() {
        var info = new MemrisEntityInformation<AnnotatedIdEntity, Long>(AnnotatedIdEntity.class);
        var entity = new AnnotatedIdEntity();
        entity.pk = 42L;

        assertThat(info.getId(entity)).isEqualTo(42L);
    }

    @Test
    void shouldRejectGeneratedValueWithoutIdAnnotation() {
        assertThatThrownBy(() -> new MemrisEntityInformation<>(GeneratedOnlyEntity.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing explicit jakarta.persistence.Id field");
    }

    static class AnnotatedIdEntity {
        @Id
        Long pk;
    }

    static class GeneratedOnlyEntity {
        @GeneratedValue
        Long id;
    }
}
