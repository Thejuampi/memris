package io.memris.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeIndexMetadataExtractorTest {

    @Test
    void shouldExtractClassLevelCompositeIndexes() {
        var metadata = MetadataExtractor.extractEntityMetadata(CompositeIndexedEntity.class);

        assertThat(metadata.indexDefinitions())
                .extracting(def -> def.name() + ":" + String.join(",", def.fieldNames()) + ":" + def.indexType())
                .containsExactlyInAnyOrder(
                        "idx_region_code:region,code:HASH",
                        "idx_region_score:region,score:BTREE");
    }

    @Test
    void shouldRejectCompositePrefixIndex() {
        assertThatThrownBy(() -> MetadataExtractor.extractEntityMetadata(InvalidCompositePrefixEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Composite indexes support HASH/BTREE only");
    }

    @Entity
    @Indexes({
            @Index(name = "idx_region_code", fields = { "region", "code" }, type = Index.IndexType.HASH),
            @Index(name = "idx_region_score", fields = { "region", "score" }, type = Index.IndexType.BTREE)
    })
    static class CompositeIndexedEntity {
        @GeneratedValue
        public Long id;
        public String region;
        public int code;
        public int score;
    }

    @Entity
    @Index(name = "idx_bad", fields = { "region", "code" }, type = Index.IndexType.PREFIX)
    static class InvalidCompositePrefixEntity {
        @GeneratedValue
        public Long id;
        public String region;
        public int code;
    }
}
