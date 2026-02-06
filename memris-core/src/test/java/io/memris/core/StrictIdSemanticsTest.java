package io.memris.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrictIdSemanticsTest {

    @Test
    void shouldRequireExplicitIdAnnotation() {
        assertThatThrownBy(() -> MetadataExtractor.extractEntityMetadata(MissingIdEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing explicit ID field");
    }

    @Test
    void shouldAllowGeneratedValueOnNonIdField() {
        var metadata = MetadataExtractor.extractEntityMetadata(GeneratedNonIdEntity.class);

        assertThat(metadata.idColumnName()).isEqualTo("pk");
    }

    @Test
    void shouldRejectMultipleIdFields() {
        assertThatThrownBy(() -> MetadataExtractor.extractEntityMetadata(MultipleIdEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Multiple ID fields found");
    }

    @Entity
    static class MissingIdEntity {
        public Long id;
    }

    @Entity
    static class GeneratedNonIdEntity {
        @Id
        public Long pk;

        @GeneratedValue
        public Long sequence;
    }

    @Entity
    static class MultipleIdEntity {
        @Id
        public Long id;

        @Id
        public Long legacyId;
    }
}
