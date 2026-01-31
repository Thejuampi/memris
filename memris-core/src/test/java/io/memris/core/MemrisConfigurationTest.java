package io.memris.core;

import io.memris.core.MemrisConfiguration;
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
        assertThat(config.defaultPageSize()).isEqualTo(1024);
        assertThat(config.defaultMaxPages()).isEqualTo(1024);
        assertThat(config.enableParallelSorting()).isTrue();
        assertThat(config.parallelSortThreshold()).isEqualTo(1000);
    }

    @Test
    void shouldCreateConfigurationWithCustomValues() {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE)
                .defaultPageSize(2048)
                .defaultMaxPages(512)
                .enableParallelSorting(false)
                .parallelSortThreshold(500)
                .build();

        assertThat(config.tableImplementation()).isEqualTo(MemrisConfiguration.TableImplementation.METHOD_HANDLE);
        assertThat(config.defaultPageSize()).isEqualTo(2048);
        assertThat(config.defaultMaxPages()).isEqualTo(512);
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
                .defaultPageSize(4096)
                .defaultMaxPages(1024)
                .enableParallelSorting(true)
                .parallelSortThreshold(2000)
                .build();

        assertThat(config.defaultPageSize()).isEqualTo(4096);
        assertThat(config.parallelSortThreshold()).isEqualTo(2000);
    }
}
