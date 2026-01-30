package io.memris.spring.runtime;

import io.memris.spring.plan.CompiledQuery;
import io.memris.storage.GeneratedTable;

import java.lang.invoke.MethodHandle;

/**
 * RepositoryPlan is the single compiled artifact created ONCE at repository creation time.
 * <p>
 * This class enforces the "compile once, reuse forever" principle by holding all
 * pre-compiled data needed for runtime execution.
 *
 * @param <T> the entity type
 */
public final class RepositoryPlan<T> {

    private final Class<T> entityClass;
    private final String idColumnName;
    private final CompiledQuery[] queries;
    private final GeneratedTable table;
    private final HeapRuntimeKernel kernel;
    private final String[] columnNames;
    private final byte[] typeCodes;
    private final java.util.Map<Class<?>, GeneratedTable> tablesByEntity;
    private final java.util.Map<Class<?>, HeapRuntimeKernel> kernelsByEntity;
    private final java.util.Map<Class<?>, EntityMaterializer<?>> materializersByEntity;

    private RepositoryPlan(Builder<T> builder) {
        this.entityClass = builder.entityClass;
        this.idColumnName = builder.idColumnName;
        this.queries = builder.queries;
        this.table = builder.table;
        this.kernel = builder.kernel;
        this.columnNames = builder.columnNames;
        this.typeCodes = builder.typeCodes;
        this.tablesByEntity = builder.tablesByEntity;
        this.kernelsByEntity = builder.kernelsByEntity;
        this.materializersByEntity = builder.materializersByEntity;
    }

    public Class<T> entityClass() { return entityClass; }
    public String idColumnName() { return idColumnName; }
    public CompiledQuery[] queries() { return queries; }
    public GeneratedTable table() { return table; }
    public HeapRuntimeKernel kernel() { return kernel; }
    public String[] columnNames() { return columnNames; }
    public byte[] typeCodes() { return typeCodes; }
    public java.util.Map<Class<?>, GeneratedTable> tablesByEntity() { return tablesByEntity; }
    public java.util.Map<Class<?>, HeapRuntimeKernel> kernelsByEntity() { return kernelsByEntity; }
    public java.util.Map<Class<?>, EntityMaterializer<?>> materializersByEntity() { return materializersByEntity; }

    /**
     * Create a RepositoryPlan from a GeneratedTable.
     */
    public static <T> RepositoryPlan<T> fromGeneratedTable(
            GeneratedTable table,
            Class<T> entityClass,
            String idColumnName,
            CompiledQuery[] queries,
            MethodHandle entityConstructor,
            String[] columnNames,
            byte[] typeCodes,
            io.memris.spring.converter.TypeConverter<?, ?>[] converters,
            MethodHandle[] setters,
            java.util.Map<Class<?>, GeneratedTable> tablesByEntity,
            java.util.Map<Class<?>, HeapRuntimeKernel> kernelsByEntity,
            java.util.Map<Class<?>, EntityMaterializer<?>> materializersByEntity) {
        
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        return new Builder<T>()
                .entityClass(entityClass)
                .idColumnName(idColumnName)
                .queries(queries)
                .table(table)
                .kernel(kernel)
                .columnNames(columnNames)
                .typeCodes(typeCodes)
                .tablesByEntity(tablesByEntity)
                .kernelsByEntity(kernelsByEntity)
                .materializersByEntity(materializersByEntity)
                .build();
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private Class<T> entityClass;
        private String idColumnName;
        private CompiledQuery[] queries;
        private GeneratedTable table;
        private HeapRuntimeKernel kernel;
        private String[] columnNames;
        private byte[] typeCodes;
        private java.util.Map<Class<?>, GeneratedTable> tablesByEntity = java.util.Map.of();
        private java.util.Map<Class<?>, HeapRuntimeKernel> kernelsByEntity = java.util.Map.of();
        private java.util.Map<Class<?>, EntityMaterializer<?>> materializersByEntity = java.util.Map.of();

        public Builder<T> entityClass(Class<T> entityClass) {
            this.entityClass = entityClass;
            return this;
        }

        public Builder<T> idColumnName(String idColumnName) {
            this.idColumnName = idColumnName;
            return this;
        }

        public Builder<T> queries(CompiledQuery[] queries) {
            this.queries = queries;
            return this;
        }

        public Builder<T> table(GeneratedTable table) {
            this.table = table;
            return this;
        }

        public Builder<T> kernel(HeapRuntimeKernel kernel) {
            this.kernel = kernel;
            return this;
        }

        public Builder<T> columnNames(String[] columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder<T> typeCodes(byte[] typeCodes) {
            this.typeCodes = typeCodes;
            return this;
        }

        public Builder<T> tablesByEntity(java.util.Map<Class<?>, GeneratedTable> tablesByEntity) {
            this.tablesByEntity = tablesByEntity;
            return this;
        }

        public Builder<T> kernelsByEntity(java.util.Map<Class<?>, HeapRuntimeKernel> kernelsByEntity) {
            this.kernelsByEntity = kernelsByEntity;
            return this;
        }

        public Builder<T> materializersByEntity(java.util.Map<Class<?>, EntityMaterializer<?>> materializersByEntity) {
            this.materializersByEntity = materializersByEntity;
            return this;
        }

        public RepositoryPlan<T> build() {
            return new RepositoryPlan<>(this);
        }
    }
}
