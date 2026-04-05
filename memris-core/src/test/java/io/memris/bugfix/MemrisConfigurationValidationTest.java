package io.memris.bugfix;

import io.memris.core.MemrisConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemrisConfigurationValidationTest {

    @Test
    void build_zeroPageSize_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().pageSize(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pageSize");
    }

    @Test
    void build_negativePageSize_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().pageSize(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_zeroMaxPages_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().maxPages(0).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPages");
    }

    @Test
    void build_negativeMaxPages_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().maxPages(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_initialPagesExceedsMaxPages_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().maxPages(2).initialPages(5).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialPages");
    }

    @Test
    void build_negativeParallelSortThreshold_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().parallelSortThreshold(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelSortThreshold");
    }

    @Test
    void build_negativeInitialPages_throws() {
        assertThatThrownBy(() -> MemrisConfiguration.builder().initialPages(-1).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialPages");
    }

    @Test
    void build_validDefaults_succeeds() {
        assertThatCode(() -> MemrisConfiguration.builder().build()).doesNotThrowAnyException();
    }
}
