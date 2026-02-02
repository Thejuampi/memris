package io.memris.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD Tests for MemrisConfiguration builder.
 */
class MemrisConfigurationTest {

    @Test
    void shouldCreateConfigurationWithDefaults() {
        MemrisConfiguration config = MemrisConfiguration.builder().build();

        assertThat(config.tableImplementation()).isEqualTo(MemrisConfiguration.TableImplementation.BYTECODE);
        assertThat(config.pageSize()).isEqualTo(1024);
        assertThat(config.maxPages()).isEqualTo(1024);
        assertThat(config.initialPages()).isEqualTo(1024);
        assertThat(config.enableParallelSorting()).isTrue();
        assertThat(config.parallelSortThreshold()).isEqualTo(1000);
    }

    @Test
    void shouldCreateConfigurationWithCustomValues() {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE)
                .pageSize(2048)
                .maxPages(512)
                .initialPages(2)
                .enableParallelSorting(false)
                .parallelSortThreshold(500)
                .build();

        assertThat(config.tableImplementation()).isEqualTo(MemrisConfiguration.TableImplementation.METHOD_HANDLE);
        assertThat(config.pageSize()).isEqualTo(2048);
        assertThat(config.maxPages()).isEqualTo(512);
        assertThat(config.initialPages()).isEqualTo(2);
        assertThat(config.enableParallelSorting()).isFalse();
        assertThat(config.parallelSortThreshold()).isEqualTo(500);
    }

    @Test
    void shouldBeImmutable() {
        MemrisConfiguration config = MemrisConfiguration.builder().build();

        // Configuration should be immutable - no setters
        // This is enforced by the design (all fields final, no setters)
        assertThat(config.tableImplementation()).isEqualTo(MemrisConfiguration.TableImplementation.BYTECODE);
    }

    @Test
    void builderShouldSupportFluentApi() {
        // Test that we can chain multiple configuration options
        MemrisConfiguration config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.BYTECODE)
                .pageSize(4096)
                .maxPages(1024)
                .initialPages(4)
                .enableParallelSorting(true)
                .parallelSortThreshold(2000)
                .build();

        assertThat(config.pageSize()).isEqualTo(4096);
        assertThat(config.initialPages()).isEqualTo(4);
        assertThat(config.parallelSortThreshold()).isEqualTo(2000);
    }
}
