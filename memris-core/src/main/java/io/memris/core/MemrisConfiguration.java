package io.memris.core;

/**
 * Immutable configuration for Memris repository factory.
 * <p>
 * Use the builder pattern to create custom configurations:
 * <pre>
 * MemrisConfiguration config = MemrisConfiguration.builder()
 *     .tableImplementation(TableImplementation.BYTECODE)
 *     .pageSize(2048)
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

    // Table sizing
    private final int pageSize;
    private final int maxPages;
    private final int initialPages;

    // Sorting configuration
    private final boolean enableParallelSorting;
    private final int parallelSortThreshold;

    // Audit configuration
    private final AuditProvider auditProvider;

    // Code generation configuration
    private final boolean codegenEnabled;

    // String pattern matching optimizations
    private final boolean enablePrefixIndex;
    private final boolean enableSuffixIndex;

    // Metadata provider
    private final EntityMetadataProvider entityMetadataProvider;

    private MemrisConfiguration(Builder builder) {
        this.tableImplementation = builder.tableImplementation;
        this.pageSize = builder.pageSize;
        this.maxPages = builder.maxPages;
        this.initialPages = builder.initialPages;
        this.enableParallelSorting = builder.enableParallelSorting;
        this.parallelSortThreshold = builder.parallelSortThreshold;
        this.auditProvider = builder.auditProvider;
        this.codegenEnabled = builder.codegenEnabled;
        this.enablePrefixIndex = builder.enablePrefixIndex;
        this.enableSuffixIndex = builder.enableSuffixIndex;
        this.entityMetadataProvider = builder.entityMetadataProvider != null
                ? builder.entityMetadataProvider
                : MetadataExtractor::extractEntityMetadata;
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
     * Get the page size for new tables.
     *
     * @return page size (number of rows per page)
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Get the maximum number of pages for new tables.
     *
     * @return max pages
     */
    public int maxPages() {
        return maxPages;
    }

    /**
     * Get the initial number of pages for new tables.
     *
     * @return initial pages
     */
    public int initialPages() {
        return initialPages;
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
     * Check if code generation is enabled for runtime executors.
     * 
     * @return true if code generation is enabled (default: true)
     */
    public boolean codegenEnabled() {
        return codegenEnabled;
    }

    /**
     * Check if prefix index optimization is enabled for STARTING_WITH queries.
     * 
     * @return true if prefix index is enabled (default: true)
     */
    public boolean enablePrefixIndex() {
        return enablePrefixIndex;
    }

    /**
     * Check if suffix index optimization is enabled for ENDING_WITH queries.
     * 
     * @return true if suffix index is enabled (default: true)
     */
    public boolean enableSuffixIndex() {
        return enableSuffixIndex;
    }

    public EntityMetadataProvider entityMetadataProvider() {
        return entityMetadataProvider;
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
        private int pageSize = 1024;
        private int maxPages = 1024;
        private int initialPages = 1024;
        private boolean enableParallelSorting = true;
        private int parallelSortThreshold = 1000;
        private AuditProvider auditProvider;
        private boolean codegenEnabled = true;
        private boolean enablePrefixIndex = true;
        private boolean enableSuffixIndex = true;
        private EntityMetadataProvider entityMetadataProvider;

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
         * Set the page size for new tables.
         *
         * @param pageSize the page size (number of rows per page)
         * @return this builder for method chaining
         */
        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Set the maximum number of pages for new tables.
         *
         * @param maxPages the maximum number of pages
         * @return this builder for method chaining
         */
        public Builder maxPages(int maxPages) {
            this.maxPages = maxPages;
            return this;
        }

        /**
         * Set the initial number of pages for new tables.
         *
         * @param initialPages the initial number of pages
         * @return this builder for method chaining
         */
        public Builder initialPages(int initialPages) {
            this.initialPages = initialPages;
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
         * Enable or disable code generation for runtime executors.
         * When disabled, fallback implementations using runtime branching are used.
         *
         * @param codegenEnabled true to enable code generation (default: true)
         * @return this builder for method chaining
         */
        public Builder codegenEnabled(boolean codegenEnabled) {
            this.codegenEnabled = codegenEnabled;
            return this;
        }

        /**
         * Enable or disable prefix index optimization for STARTING_WITH queries.
         * When enabled, creates trie-based indexes for String fields annotated with
         * @Index(type = IndexType.PREFIX), providing O(k) lookup instead of O(n) scans.
         *
         * @param enablePrefixIndex true to enable prefix index (default: true)
         * @return this builder for method chaining
         */
        public Builder enablePrefixIndex(boolean enablePrefixIndex) {
            this.enablePrefixIndex = enablePrefixIndex;
            return this;
        }

        /**
         * Enable or disable suffix index optimization for ENDING_WITH queries.
         * When enabled, creates reverse-string indexes for String fields annotated with
         * @Index(type = IndexType.SUFFIX), providing O(k) lookup instead of O(n) scans.
         *
         * @param enableSuffixIndex true to enable suffix index (default: true)
         * @return this builder for method chaining
         */
        public Builder enableSuffixIndex(boolean enableSuffixIndex) {
            this.enableSuffixIndex = enableSuffixIndex;
            return this;
        }

        public Builder entityMetadataProvider(EntityMetadataProvider entityMetadataProvider) {
            this.entityMetadataProvider = entityMetadataProvider;
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
