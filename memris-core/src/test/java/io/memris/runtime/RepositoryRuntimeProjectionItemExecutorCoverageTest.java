package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeProjectionItemExecutorCoverageTest {

    @Test
    void projectionItemExecutorReturnsNullWhenSourceTableIsMissing() throws Exception {
        GeneratedTable table = new StubTable();
        RepositoryPlan<Object> plan = RepositoryPlan.<Object>builder()
                .entityClass(Object.class)
                .idColumnName("id")
                .queries(new io.memris.query.CompiledQuery[0])
                .bindings(new RepositoryMethodBinding[0])
                .executors(new RepositoryMethodExecutor[0])
                .conditionExecutors(new ConditionExecutor[0][])
                .orderExecutors(new OrderExecutor[0])
                .projectionExecutors(new ProjectionExecutor[0])
                .table(table)
                .kernel(new HeapRuntimeKernel(table, new RuntimeExecutorGenerator()))
                .columnNames(new String[] { "id" })
                .typeCodes(new byte[] { TypeCodes.TYPE_LONG })
                .tablesByEntity(Map.of()) // source table intentionally missing
                .kernelsByEntity(Map.of())
                .materializersByEntity(Map.of())
                .joinTables(Map.of())
                .joinRuntimeByQuery(new JoinRuntimeBinding[0][])
                .idLookup(IdLookup.forTypeCode(TypeCodes.TYPE_LONG))
                .build();
        RepositoryRuntime<Object> runtime = new RepositoryRuntime<>(plan, null, (io.memris.core.EntityMetadata<Object>) null);

        Class<?> fkReaderType = Class.forName("io.memris.runtime.RepositoryRuntime$FkReader");
        Class<?> resolverType = Class.forName("io.memris.runtime.RepositoryRuntime$TargetRowResolver");
        Class<?> fieldReaderType = Class.forName("io.memris.runtime.RepositoryRuntime$FieldValueReader");
        Class<?> stepType = Class.forName("io.memris.runtime.RepositoryRuntime$ProjectionStepExecutor");
        Class<?> itemType = Class.forName("io.memris.runtime.RepositoryRuntime$ProjectionItemExecutor");

        Object fkReader = Proxy.newProxyInstance(
                fkReaderType.getClassLoader(),
                new Class<?>[] { fkReaderType },
                (proxy, method, args) -> 1L);
        Object resolver = Proxy.newProxyInstance(
                resolverType.getClassLoader(),
                new Class<?>[] { resolverType },
                (proxy, method, args) -> 0);
        Object fieldReader = Proxy.newProxyInstance(
                fieldReaderType.getClassLoader(),
                new Class<?>[] { fieldReaderType },
                (proxy, method, args) -> "value");

        Constructor<?> stepCtor = stepType.getDeclaredConstructor(Class.class, Class.class, fkReaderType, resolverType);
        stepCtor.setAccessible(true);
        Object step = stepCtor.newInstance(Object.class, String.class, fkReader, resolver);
        Object steps = Array.newInstance(stepType, 1);
        Array.set(steps, 0, step);

        Constructor<?> itemCtor = itemType.getDeclaredConstructor(Class.class, steps.getClass(), fieldReaderType);
        itemCtor.setAccessible(true);
        Object item = itemCtor.newInstance(String.class, steps, fieldReader);

        Method resolve = itemType.getDeclaredMethod("resolve", RepositoryRuntime.class, int.class);
        resolve.setAccessible(true);
        Object result = resolve.invoke(item, runtime, 0);

        assertThat(result).isNull();
    }

    static class StubTable implements GeneratedTable {
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_LONG; }
        @Override public long allocatedCount() { return 0; }
        @Override public long liveCount() { return 0; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) { }
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[0]; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return false; }
    }
}
