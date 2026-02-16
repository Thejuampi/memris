package io.memris.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SelectionTest {

    @Test
    void packedReferenceHelpers() {
        long ref = Selection.pack(123, 45L);

        assertThat(Selection.index(ref)).isEqualTo(123);
        assertThat(Selection.generation(ref)).isEqualTo(45L);
        assertThat(Selection.isSameGen(ref, 45L)).isTrue();
        assertThat(Selection.isSameGen(ref, 44L)).isFalse();
    }
}
