package io.memris.spring.plan.entities;

import io.memris.spring.plan.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Test suite for QueryPlanner with context-aware lexer integration.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryPlannerIntegrationTest {

    @Test
    @Order(1)
    void parseSimpleFindBy_UsesEntityContext() throws Exception {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.methodName()).isEqualTo("findByName");
        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.MANY_LIST);
        assertThat(query.conditions()).hasSize(1);
        assertThat(query.conditions()[0].propertyPath()).isEqualTo("name");
        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.EQ);
        assertThat(query.conditions()[0].argumentIndex()).isEqualTo(0);
        assertThat(query.conditions()[0].ignoreCase()).isFalse();
    }

    @Test
    @Order(2)
    void parseNestedProperty_ResolvesDotNotation() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentName", String.class);
        LogicalQuery query = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(query.conditions()).hasSize(1);
        assertThat(query.conditions()[0].propertyPath()).isEqualTo("department.name");
        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.EQ);
    }

    @Test
    @Order(3)
    void parseWithOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeGreaterThan", int.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.GT);
    }

    @Test
    @Order(4)
    void parseWithLikeIgnoreCase_CombinesModifier() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameLikeIgnoreCase", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IGNORE_CASE_LIKE);
        assertThat(query.conditions()[0].ignoreCase()).isTrue();
    }

    @Test
    @Order(5)
    void parseWithAnd_CreatesMultipleConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndAge", String.class, int.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()).hasSize(2);
        assertThat(query.conditions()[0].propertyPath()).isEqualTo("name");
        assertThat(query.conditions()[0].argumentIndex()).isEqualTo(0);
        assertThat(query.conditions()[1].propertyPath()).isEqualTo("age");
        assertThat(query.conditions()[1].argumentIndex()).isEqualTo(1);
    }

    @Test
    @Order(6)
    void parseWithOrderBy_CreatesOrderBy() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrderByAgeDesc", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()).hasSize(1);
        assertThat(query.orderBy()).isNotNull();
        assertThat(query.orderBy().propertyPath()).isEqualTo("age");
        assertThat(query.orderBy().ascending()).isFalse();
    }

    @Test
    @Order(7)
    void parseWithInOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeIn", List.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IN);
    }

    @Test
    @Order(8)
    void parseWithNullOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentIdIsNull");
        LogicalQuery query = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IS_NULL);
    }

    @Test
    @Order(9)
    void parseWithBooleanOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByActiveTrue");
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IS_TRUE);
    }

    @Test
    @Order(10)
    void parseWithCountPrefix_DeterminesReturnKind() throws Exception {
        Method method = TestRepository.class.getMethod("countByName", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.COUNT_LONG);
        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @Order(11)
    void parseWithExistsPrefix_DeterminesReturnKind() throws Exception {
        Method method = TestRepository.class.getMethod("existsByName", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.EXISTS_BOOL);
    }

    @Test
    @Order(12)
    void parseDeepNesting_ResolvesCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentAddressCity", String.class);
        LogicalQuery query = QueryPlanner.parse(method, DeepNestedEntity.class, "id");

        assertThat(query.conditions()[0].propertyPath()).isEqualTo("department.address.city");
    }

    @Test
    @Order(13)
    void parseContainingOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameContaining", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.CONTAINING);
    }

    @Test
    @Order(14)
    void parseStartingWithOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameStartingWith", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.STARTING_WITH);
    }

    @Test
    @Order(15)
    void parseEndingWithOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameEndingWith", String.class);
        LogicalQuery query = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.ENDING_WITH);
    }

    interface TestRepository {
        List<SimpleEntity> findByName(String name);
        List<NestedEntity> findByDepartmentName(String departmentName);
        List<SimpleEntity> findByAgeGreaterThan(int age);
        List<SimpleEntity> findByNameLikeIgnoreCase(String pattern);
        List<SimpleEntity> findByNameAndAge(String name, int age);
        List<SimpleEntity> findByNameOrderByAgeDesc(String name);
        List<SimpleEntity> findByAgeIn(List<Integer> ages);
        List<NestedEntity> findByDepartmentIdIsNull();
        List<SimpleEntity> findByActiveTrue();
        Long countByName(String name);
        boolean existsByName(String name);
        List<DeepNestedEntity> findByDepartmentAddressCity(String city);
        List<SimpleEntity> findByNameContaining(String keyword);
        List<SimpleEntity> findByNameStartingWith(String prefix);
        List<SimpleEntity> findByNameEndingWith(String suffix);
    }
}
