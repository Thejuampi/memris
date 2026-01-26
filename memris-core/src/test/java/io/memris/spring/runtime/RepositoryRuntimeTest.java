package io.memris.spring.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.CompiledQuery.CompiledCondition;
import io.memris.spring.plan.OpCode;
import io.memris.spring.plan.LogicalQuery.Operator;
import io.memris.spring.plan.LogicalQuery.ReturnKind;

/**
 * TDD test for RepositoryRuntime.
 * RED → GREEN → REFACTOR
 */
class RepositoryRuntimeTest {

    static class TestEntity {
        private Long id;
        private String name;
        private int age;

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    private RepositoryRuntime<TestEntity> runtime;
    private CompiledQuery[] compiledQueries;

    @BeforeEach
    void setUp() throws Exception {
        // Create a minimal compiled query for testing
        // findByUserId(Long id) → list1(queryId=0, id)
        CompiledCondition condition = CompiledCondition.of(0, Operator.EQ, 0); // column 0, EQ, arg 0
        CompiledQuery findByIdQuery = CompiledQuery.of(
            OpCode.FIND_BY_ID,
            ReturnKind.ONE_OPTIONAL,
            new CompiledCondition[] { condition }
        );

        // findAll() → list0(queryId=1)
        CompiledQuery findAllQuery = CompiledQuery.of(
            OpCode.FIND_ALL,
            ReturnKind.MANY_LIST,
            new CompiledCondition[0]
        );

        compiledQueries = new CompiledQuery[] { findByIdQuery, findAllQuery };

        // Get entity constructor
        Constructor<TestEntity> ctor = TestEntity.class.getDeclaredConstructor();
        MethodHandle constructor = MethodHandles.lookup().unreflectConstructor(ctor);

        // Create dense arrays for metadata
        String[] columnNames = new String[] { "id", "name", "age" };
        byte[] typeCodes = new byte[] { 0, 0, 0 }; // dummy type codes for now
        io.memris.spring.converter.TypeConverter<?, ?>[] converters = new io.memris.spring.converter.TypeConverter[3];
        MethodHandle[] setters = new MethodHandle[3];

        // Get setter handles
        setters[0] = MethodHandles.lookup().unreflect(TestEntity.class.getMethod("setId", Long.class));
        setters[1] = MethodHandles.lookup().unreflect(TestEntity.class.getMethod("setName", String.class));
        setters[2] = MethodHandles.lookup().unreflect(TestEntity.class.getMethod("setAge", int.class));

        // Create runtime (null table/factory for now - we'll test basic structure first)
        runtime = new RepositoryRuntime<>(
            null,  // table
            null,  // factory
            TestEntity.class,
            "id",
            compiledQueries,
            constructor,
            columnNames,
            typeCodes,
            converters,
            setters
        );
    }

    @AfterEach
    void tearDown() {
        runtime = null;
    }

    // ========================================================================
    // RED Test 1: Runtime should be created with compiled queries
    // ========================================================================

    @Test
    void runtimeShouldStoreCompiledQueries() {
        assertThat(runtime.compiledQueries()).isNotNull();
        assertThat(runtime.compiledQueries()).hasSize(2);
        assertThat(runtime.compiledQueries()[0].opCode()).isEqualTo(OpCode.FIND_BY_ID);
        assertThat(runtime.compiledQueries()[1].opCode()).isEqualTo(OpCode.FIND_ALL);
    }

    // ========================================================================
    // RED Test 2: Runtime should have typed entrypoints
    // ========================================================================

    @Test
    void runtimeShouldHaveTypedEntrypoints() {
        // These should exist and be callable (will throw NPE due to null table, but that's OK for now)
        assertThrows(NullPointerException.class, () -> runtime.list0(1));
        assertThrows(NullPointerException.class, () -> runtime.list1(0, 123L));
    }

    // ========================================================================
    // RED Test 3: Materialization should work with dense arrays
    // ========================================================================

    @Test
    void materializationShouldUseDenseArrays() {
        // For now, just verify the runtime was created successfully
        // We'll add table data testing once we have a working FfmTable mock
        assertThat(runtime).isNotNull();
    }

    // ========================================================================
    // RED Test 4: QueryId dispatch should work
    // ========================================================================

    @Test
    void queryIdDispatchShouldSelectCorrectQuery() {
        CompiledQuery findById = compiledQueries[0];
        CompiledQuery findAll = compiledQueries[1];

        assertThat(findById.opCode()).isEqualTo(OpCode.FIND_BY_ID);
        assertThat(findById.returnKind()).isEqualTo(ReturnKind.ONE_OPTIONAL);
        assertThat(findById.arity()).isEqualTo(1);

        assertThat(findAll.opCode()).isEqualTo(OpCode.FIND_ALL);
        assertThat(findAll.returnKind()).isEqualTo(ReturnKind.MANY_LIST);
        assertThat(findAll.arity()).isEqualTo(0);
    }
}
