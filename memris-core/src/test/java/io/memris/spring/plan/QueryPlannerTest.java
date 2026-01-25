package io.memris.spring.plan;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.memris.spring.plan.LogicalQuery.Condition;

/**
 * Tests for QueryPlanner.
 */
public class QueryPlannerTest {

    interface TestRepository {
        Optional<TestEntity> findById(Long id);
        boolean existsById(Long id);
        java.util.List<TestEntity> findAll();
        java.util.List<TestEntity> findByName(String name);
        java.util.List<TestEntity> findByAgeGreaterThan(int age);
        java.util.List<TestEntity> findByNameAndAge(String name, int age);
        long count();
        long countByAge(int age);
        java.util.List<TestEntity> findByPriceIn(java.util.List<Long> prices);
    }

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

    @Test
    public void testParseFindById() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findById", Long.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findById", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.ONE_OPTIONAL, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("id", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[0].operator());
        assertEquals(0, conditions[0].argumentIndex());
    }

    @Test
    public void testParseExistsById() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("existsById", Long.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("existsById", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.EXISTS_BOOL, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("id", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[0].operator());
    }

    @Test
    public void testParseFindAll() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findAll");
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findAll", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(0, query.arity());
        assertEquals(0, query.conditions().length);
    }

    @Test
    public void testParseFindByName() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findByName", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("name", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[0].operator());
    }

    @Test
    public void testParseFindByAgeGreaterThan() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findByAgeGreaterThan", int.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findByAgeGreaterThan", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("age", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.GT, conditions[0].operator());
    }

    @Test
    public void testParseFindByNameAndAge() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findByNameAndAge", String.class, int.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findByNameAndAge", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(2, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(2, conditions.length);

        assertEquals("name", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[0].operator());
        assertEquals(0, conditions[0].argumentIndex());

        assertEquals("age", conditions[1].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[1].operator());
        assertEquals(1, conditions[1].argumentIndex());
    }

    @Test
    public void testParseCount() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("count");
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("count", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.COUNT_LONG, query.returnKind());
        assertEquals(0, query.arity());
    }

    @Test
    public void testParseCountByAge() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("countByAge", int.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("countByAge", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.COUNT_LONG, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("age", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.EQ, conditions[0].operator());
    }

    @Test
    public void testFindByPriceIn_parsesCorrectly() throws NoSuchMethodException {
        Method method = TestRepository.class.getMethod("findByPriceIn", java.util.List.class);
        LogicalQuery query = QueryPlanner.parse(method, "id");

        assertEquals("findByPriceIn", query.methodName());
        assertEquals(LogicalQuery.ReturnKind.MANY_LIST, query.returnKind());
        assertEquals(1, query.arity());

        Condition[] conditions = query.conditions();
        assertEquals(1, conditions.length);
        assertEquals("price", conditions[0].propertyPath());
        assertEquals(LogicalQuery.Operator.IN, conditions[0].operator());
        assertEquals(0, conditions[0].argumentIndex());
    }
}
