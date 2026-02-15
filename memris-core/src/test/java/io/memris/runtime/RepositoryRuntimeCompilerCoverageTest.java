package io.memris.runtime;

import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.query.OpCode;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeCompilerCoverageTest {

    @Test
    void shouldReturnEmptyStorageReadersWhenTableHasNoColumns() throws Exception {
        var plan = RepositoryPlan.<Object>builder()
                .table(new FixedColumnTable(0, TypeCodes.TYPE_INT))
                .typeCodes(new byte[0])
                .build();
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());

        StorageValueReader[] readers = invokeCompileStorageReaders(plan, generator);

        assertThat(readers).isEmpty();
    }

    @Test
    void shouldCompileStorageReadersFromTypeCodesWhenTableIsMissing() throws Exception {
        var plan = RepositoryPlan.<Object>builder()
                .table(null)
                .typeCodes(new byte[] { TypeCodes.TYPE_INT, TypeCodes.TYPE_LONG })
                .build();
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());

        StorageValueReader[] readers = invokeCompileStorageReaders(plan, generator);

        assertThat(readers).hasSize(2);
        assertThat(readers[0]).isNotNull();
        assertThat(readers[1]).isNotNull();
    }

    @Test
    void shouldFallbackToDefaultMetadataProviderWhenProjectionProviderIsNull() throws Exception {
        var query = CompiledQuery.of(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                new CompiledQuery.CompiledCondition[0]);
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());

        ProjectionExecutor[] executors = invokeCompileProjectionExecutors(
                new CompiledQuery[] { query },
                null,
                generator);

        assertThat(executors).hasSize(1);
        assertThat(executors[0]).isNull();
    }

    private static StorageValueReader[] invokeCompileStorageReaders(
            RepositoryPlan<?> plan,
            RuntimeExecutorGenerator runtimeExecutorGenerator) throws Exception {
        Method method = RepositoryRuntimeCompiler.class.getDeclaredMethod(
                "compileStorageReaders",
                RepositoryPlan.class,
                RuntimeExecutorGenerator.class);
        method.setAccessible(true);
        return (StorageValueReader[]) method.invoke(null, plan, runtimeExecutorGenerator);
    }

    private static ProjectionExecutor[] invokeCompileProjectionExecutors(
            CompiledQuery[] queries,
            io.memris.core.EntityMetadataProvider metadataProvider,
            RuntimeExecutorGenerator runtimeExecutorGenerator) throws Exception {
        Method method = RepositoryRuntimeCompiler.class.getDeclaredMethod(
                "compileProjectionExecutors",
                CompiledQuery[].class,
                io.memris.core.EntityMetadataProvider.class,
                RuntimeExecutorGenerator.class);
        method.setAccessible(true);
        return (ProjectionExecutor[]) method.invoke(null, queries, metadataProvider, runtimeExecutorGenerator);
    }

    private static final class FixedColumnTable implements GeneratedTable {
        private final int columnCount;
        private final byte typeCode;

        private FixedColumnTable(int columnCount, byte typeCode) {
            this.columnCount = columnCount;
            this.typeCode = typeCode;
        }

        @Override
        public int columnCount() {
            return columnCount;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return typeCode;
        }

        @Override
        public long allocatedCount() {
            return 0;
        }

        @Override
        public long liveCount() {
            return 0;
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, Supplier<T> reader) {
            return reader.get();
        }

        @Override
        public long lookupById(long id) {
            return -1;
        }

        @Override
        public long lookupByIdString(String id) {
            return -1;
        }

        @Override
        public void removeById(long id) {
        }

        @Override
        public long insertFrom(Object[] values) {
            return -1;
        }

        @Override
        public void tombstone(long ref) {
        }

        @Override
        public boolean isLive(long ref) {
            return false;
        }

        @Override
        public long currentGeneration() {
            return 0;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return 0;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return new int[0];
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return new int[0];
        }

        @Override
        public int[] scanAll() {
            return new int[0];
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return null;
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return false;
        }
    }
}
