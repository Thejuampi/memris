package io.memris.runtime.dispatch;

import io.memris.core.MemrisArena;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.HeapRuntimeKernel;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionProgramCompilerTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    @DisplayName("should return empty for primitive non-null IS_NULL")
    void shouldReturnEmptyForPrimitiveNonNullIsNull() {
        var repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 10));
        var table = arena.getTable(TestEntity.class);
        var ageColumn = MetadataExtractor.extractEntityMetadata(TestEntity.class).resolvePropertyPosition("age");

        var condition = CompiledQuery.CompiledCondition.of(ageColumn, TypeCodes.TYPE_INT, LogicalQuery.Operator.IS_NULL, 0);
        var selection = ConditionProgramCompiler.compile(condition, true)
                .execute(table, new HeapRuntimeKernel(table), new Object[0]);

        assertThat(selection.size()).isZero();
    }

    @Test
    @DisplayName("should subtract equals for NE")
    void shouldSubtractEqualsForNe() {
        var repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 10));
        repo.save(new TestEntity(null, "Bob", 10));
        repo.save(new TestEntity(null, "Cara", 10));
        var table = arena.getTable(TestEntity.class);
        var nameColumn = MetadataExtractor.extractEntityMetadata(TestEntity.class).resolvePropertyPosition("name");

        var condition = CompiledQuery.CompiledCondition.of(nameColumn, TypeCodes.TYPE_STRING, LogicalQuery.Operator.NE, 0);
        var selection = ConditionProgramCompiler.compile(condition, false)
                .execute(table, new HeapRuntimeKernel(table), new Object[] { "Bob" });
        var actual = java.util.Arrays.stream(selection.toIntArray()).mapToObj(row -> table.readString(nameColumn, row)).sorted().toList();

        assertThat(actual).containsExactly("Alice", "Cara");
    }

    @Test
    @DisplayName("should delegate unsupported operator to kernel fallback")
    void shouldDelegateUnsupportedOperatorToKernelFallback() {
        var repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "alpha", 10));
        repo.save(new TestEntity(null, "beta", 20));
        var table = arena.getTable(TestEntity.class);
        var nameColumn = MetadataExtractor.extractEntityMetadata(TestEntity.class).resolvePropertyPosition("name");

        var condition = CompiledQuery.CompiledCondition.of(nameColumn,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Operator.CONTAINING,
                0);
        var selection = ConditionProgramCompiler.compile(condition, false)
                .execute(table, new HeapRuntimeKernel(table), new Object[] { "alp" });
        var actual = java.util.Arrays.stream(selection.toIntArray()).mapToObj(row -> table.readString(nameColumn, row)).toList();

        assertThat(actual).containsExactly("alpha");
    }
}
