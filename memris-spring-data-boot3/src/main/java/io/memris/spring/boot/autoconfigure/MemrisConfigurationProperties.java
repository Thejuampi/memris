package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisConfiguration;
import lombok.Data;

/**
 * Per-arena Memris tuning properties.
 */
@Data
public class MemrisConfigurationProperties {
    private int pageSize = 1024;
    private int maxPages = 1024;
    private int initialPages = 1024;
    private boolean enableParallelSorting = true;
    private int parallelSortThreshold = 1000;
    private boolean codegenEnabled = true;
    private boolean enablePrefixIndex = true;
    private boolean enableSuffixIndex = true;

    /**
     * Creates property values with defaults suitable for local development.
     */
    public MemrisConfigurationProperties() {
    }

    /**
     * Creates the immutable Memris configuration from these bound properties.
     *
     * @return Memris configuration
     */
    public MemrisConfiguration toConfiguration() {
        return MemrisConfiguration.builder()
                .pageSize(pageSize)
                .maxPages(maxPages)
                .initialPages(initialPages)
                .enableParallelSorting(enableParallelSorting)
                .parallelSortThreshold(parallelSortThreshold)
                .codegenEnabled(codegenEnabled)
                .enablePrefixIndex(enablePrefixIndex)
                .enableSuffixIndex(enableSuffixIndex)
                .build();
    }
}
