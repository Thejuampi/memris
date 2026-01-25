package io.memris.spring.scaffold;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.CompiledQuery.CompiledCondition;
import io.memris.spring.plan.LogicalQuery.Operator;
import io.memris.spring.plan.LogicalQuery.ReturnKind;

/**
 * TDD test for RepositoryScaffolder.
 * RED → GREEN → REFACTOR
 */
class RepositoryScaffolderTest {

    static class TestEntity {
        private Long id;
        private String name;

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    interface TestRepository {
        Optional<TestEntity> findById(Long id);
        List<TestEntity> findAll();
        List<TestEntity> findByName(String name);
    }

    // ========================================================================
    // RED Test 1: QueryPlanner should parse method names correctly
    // ========================================================================

    @Test
    void queryPlannerShouldParseFindById() throws Exception {
        var method = TestRepository.class.getMethod("findById", Long.class);
        var query = io.memris.spring.plan.QueryPlanner.parse(method, "id");

        assertEquals("findById", query.methodName());
        assertEquals(ReturnKind.ONE_OPTIONAL, query.returnKind());
        assertEquals(1, query.arity());

        var conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("id", conditions[0].propertyPath());
        assertEquals(Operator.EQ, conditions[0].operator());
        assertEquals(0, conditions[0].argumentIndex());
    }

    @Test
    void queryPlannerShouldParseFindAll() throws Exception {
        var method = TestRepository.class.getMethod("findAll");
        var query = io.memris.spring.plan.QueryPlanner.parse(method, "id");

        assertEquals("findAll", query.methodName());
        assertEquals(ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(0, query.arity());
        assertEquals(0, query.conditions().length);
    }

    @Test
    void queryPlannerShouldParseFindByName() throws Exception {
        var method = TestRepository.class.getMethod("findByName", String.class);
        var query = io.memris.spring.plan.QueryPlanner.parse(method, "id");

        assertEquals("findByName", query.methodName());
        assertEquals(ReturnKind.MANY_LIST, query.returnKind());

        var conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("name", conditions[0].propertyPath());
        assertEquals(Operator.EQ, conditions[0].operator());
    }

    // ========================================================================
    // RED Test 2: CompiledQuery should have resolved column indices
    // ========================================================================

    @Test
    void compiledQueryShouldHaveResolvedColumnIndices() {
        // Verify CompiledCondition structure
        var condition = CompiledCondition.of(0, Operator.EQ, 0);
        assertEquals(0, condition.columnIndex());
        assertEquals(Operator.EQ, condition.operator());
        assertEquals(0, condition.argumentIndex());
        assertFalse(condition.ignoreCase());
    }

    @Test
    void compiledQueryShouldSupportIgnoreCase() {
        var condition = CompiledCondition.of(0, Operator.IGNORE_CASE_EQ, 0, true);
        assertEquals(Operator.IGNORE_CASE_EQ, condition.operator());
        assertTrue(condition.ignoreCase());
    }
}
