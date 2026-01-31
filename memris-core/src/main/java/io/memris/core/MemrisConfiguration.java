package io.memris.core;

/**
 * Immutable configuration for Memris repository factory.
 * <p>
 * Use the builder pattern to create custom configurations:
 * <pre>
 * MemrisConfiguration config = MemrisConfiguration.builder()
 *     .tableImplementation(TableImplementation.BYTECODE)
 *     .defaultPageSize(2048)
 *     .build();
 * </pre>
 * <p>
 * All configuration is immutable once built.
 *
 * @see io.memris.repository.MemrisRepositoryFactory
 */
public final class MemrisConfiguration {

    // Table implementation strategy
    private final TableImplementation tableImplementation;

    // Table sizing defaults
    private final int defaultPageSize;
    private final int defaultMaxPages;

    // Sorting configuration
    private final boolean enableParallelSorting;
    private final int parallelSortThreshold;

    // Audit configuration
    private final AuditProvider auditProvider;

    private MemrisConfiguration(Builder builder) {
        this.tableImplementation = builder.tableImplementation;
        this.defaultPageSize = builder.defaultPageSize;
        this.defaultMaxPages = builder.defaultMaxPages;
        this.enableParallelSorting = builder.enableParallelSorting;
        this.parallelSortThreshold = builder.parallelSortThreshold;
        this.auditProvider = builder.auditProvider;
    }

    /**
     * Create a new builder for MemrisConfiguration.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get the table implementation strategy.
     *
     * @return the table implementation (BYTECODE or METHOD_HANDLE)
     */
    public TableImplementation tableImplementation() {
        return tableImplementation;
    }

    /**
     * Get the default page size for new tables.
     *
     * @return default page size (number of rows per page)
     */
    public int defaultPageSize() {
        return defaultPageSize;
    }

    /**
     * Get the default maximum number of pages for new tables.
     *
     * @return default max pages
     */
    public int defaultMaxPages() {
        return defaultMaxPages;
    }

    /**
     * Check if parallel sorting is enabled.
     *
     * @return true if parallel sorting is enabled
     */
    public boolean enableParallelSorting() {
        return enableParallelSorting;
    }

    /**
     * Get the threshold for parallel sorting.
     * Sorting will use parallel streams when result size exceeds this threshold.
     *
     * @return parallel sort threshold
     */
    public int parallelSortThreshold() {
        return parallelSortThreshold;
    }

    /**
     * Get the audit provider (used for @CreatedBy/@LastModifiedBy).
     */
    public AuditProvider auditProvider() {
        return auditProvider;
    }

    /**
     * Table implementation strategy enum.
     */
    public enum TableImplementation {
        /**
         * Use bytecode-generated table implementation.
         * Fastest performance, zero reflection.
         */
        BYTECODE,

        /**
         * Use MethodHandle-based table implementation.
         * Good performance, uses reflection (~5ns overhead per call).
         */
        METHOD_HANDLE
    }

    /**
     * Builder for MemrisConfiguration.
     * <p>
     * Provides a fluent API for building configuration instances.
     */
    public static class Builder {
        private TableImplementation tableImplementation = TableImplementation.BYTECODE;
        private int defaultPageSize = 1024;
        private int defaultMaxPages = 1024;
        private boolean enableParallelSorting = true;
        private int parallelSortThreshold = 1000;
        private AuditProvider auditProvider;

        private Builder() {
        }

        /**
         * Set the table implementation strategy.
         *
         * @param tableImplementation the implementation strategy
         * @return this builder for method chaining
         */
        public Builder tableImplementation(TableImplementation tableImplementation) {
            this.tableImplementation = tableImplementation;
            return this;
        }

        /**
         * Set the default page size for new tables.
         *
         * @param defaultPageSize the page size (number of rows per page)
         * @return this builder for method chaining
         */
        public Builder defaultPageSize(int defaultPageSize) {
            this.defaultPageSize = defaultPageSize;
            return this;
        }

        /**
         * Set the default maximum number of pages for new tables.
         *
         * @param defaultMaxPages the maximum number of pages
         * @return this builder for method chaining
         */
        public Builder defaultMaxPages(int defaultMaxPages) {
            this.defaultMaxPages = defaultMaxPages;
            return this;
        }

        /**
         * Enable or disable parallel sorting.
         *
         * @param enableParallelSorting true to enable parallel sorting
         * @return this builder for method chaining
         */
        public Builder enableParallelSorting(boolean enableParallelSorting) {
            this.enableParallelSorting = enableParallelSorting;
            return this;
        }

        /**
         * Set the threshold for using parallel sorting.
         *
         * @param parallelSortThreshold the threshold in number of rows
         * @return this builder for method chaining
         */
        public Builder parallelSortThreshold(int parallelSortThreshold) {
            this.parallelSortThreshold = parallelSortThreshold;
            return this;
        }

        /**
         * Set the audit provider for @CreatedBy/@LastModifiedBy.
         */
        public Builder auditProvider(AuditProvider auditProvider) {
            this.auditProvider = auditProvider;
            return this;
        }

        /**
         * Build the immutable MemrisConfiguration.
         *
         * @return a new MemrisConfiguration instance
         */
        public MemrisConfiguration build() {
            return new MemrisConfiguration(this);
        }
    }
}
