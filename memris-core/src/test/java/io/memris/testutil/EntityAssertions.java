package io.memris.testutil;

import static org.assertj.core.api.Assertions.assertThat;

public final class EntityAssertions {

    private EntityAssertions() {
    }

    public static <T> void assertEntityMatches(T actual, T expected, String... ignoredFields) {
        assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFields(ignoredFields)
                .isEqualTo(expected);
    }

    public static <T> void assertEntitiesMatchExactOrder(Iterable<T> actual, Iterable<T> expected, String... ignoredFields) {
        assertThat(actual)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields(ignoredFields)
                .containsExactlyElementsOf(expected);
    }

    public static <T> void assertEntitiesMatchAnyOrder(Iterable<T> actual, Iterable<T> expected, String... ignoredFields) {
        assertThat(actual)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields(ignoredFields)
                .containsExactlyInAnyOrderElementsOf(expected);
    }
}
