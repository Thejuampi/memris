package io.memris.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnAccessPlanMetadataTest {

    @Test
    void metadataShouldBuildPlansForAllPersistedColumns() {
        var metadata = MetadataExtractor.extractEntityMetadata(ProfileOwner.class);
        var plansByColumn = metadata.columnAccessPlansByColumn();

        for (var mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0) {
                continue;
            }
            assertThat(plansByColumn[mapping.columnPosition()]).isNotNull();
            assertThat(plansByColumn[mapping.columnPosition()].propertyPath()).isEqualTo(mapping.name());
        }

        var deepPlan = metadata.columnAccessPlan("profile.address.city");
        assertThat(deepPlan).isNotNull();
        assertThat(deepPlan.columnIndex()).isEqualTo(metadata.resolvePropertyPosition("profile.address.city"));
        assertThat(deepPlan.multiSegment()).isTrue();
    }

    @Test
    void shouldFailForUnknownPathSegment() {
        assertThatThrownBy(() -> ColumnAccessPlan.compile(ProfileOwner.class, "profile.unknown.city"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field path not found");
    }

    @Test
    void shouldFailWhenIntermediateHasNoNoArgConstructor() {
        assertThatThrownBy(() -> ColumnAccessPlan.compile(NoDefaultRoot.class, "intermediate.value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no-arg constructor");
    }

    @Entity
    static class ProfileOwner {
        @Id
        public Long id;
        public Profile profile;
    }

    static class Profile {
        public String email;
        public Address address;
    }

    static class Address {
        public String city;
    }

    static class NoDefaultRoot {
        NoDefaultIntermediate intermediate;
    }

    static class NoDefaultIntermediate {
        final String value;

        NoDefaultIntermediate(String value) {
            this.value = value;
        }
    }
}
