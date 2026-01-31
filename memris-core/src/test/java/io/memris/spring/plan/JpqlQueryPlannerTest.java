package io.memris.query;

import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.query.entities.NestedEntity;
import io.memris.query.entities.SimpleEntity;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JpqlQueryPlannerTest {

    @Test
    void parseSelectWithNamedParam() throws Exception {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.opCode()).isEqualTo(OpCode.FIND);
        assertThat(actual.returnKind()).isEqualTo(LogicalQuery.ReturnKind.MANY_LIST);
        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 0)
        );
        assertThat(actual.boundValues()).allMatch(Objects::isNull);
        assertThat(actual.parameterIndices()).containsExactly(0);
        assertThat(actual.arity()).isEqualTo(1);
    }

    @Test
    void parseSelectWithNestedPropertyAndIlike() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentNameAndCity", String.class, String.class);
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual.opCode()).isEqualTo(OpCode.FIND);
        assertThat(actual.returnKind()).isEqualTo(LogicalQuery.ReturnKind.MANY_LIST);
        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("department.name", LogicalQuery.Operator.LIKE, 0, true, LogicalQuery.Combinator.AND),
            LogicalQuery.Condition.of("address.city", LogicalQuery.Operator.EQ, 1, false, LogicalQuery.Combinator.AND)
        );
        assertThat(actual.boundValues()).allMatch(Objects::isNull);
        assertThat(actual.parameterIndices()).containsExactly(0, 1);
        assertThat(actual.arity()).isEqualTo(2);
    }

    @Test
    void parseBetweenUsesFirstParameterIndex() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeBetween", int.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("age", LogicalQuery.Operator.BETWEEN, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(0, 1);
        assertThat(actual.arity()).isEqualTo(2);
    }

    @Test
    void parseInWithPositionalParam() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeIn", List.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("age", LogicalQuery.Operator.IN, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(0);
    }

    @Test
    void parseIsNull() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentIsNull");
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("department", LogicalQuery.Operator.IS_NULL, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(-1);
        assertThat(actual.boundValues()).containsExactly((Object) null);
        assertThat(actual.arity()).isEqualTo(1);
    }

    @Test
    void parseIsNotNull() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentIsNotNull");
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("department", LogicalQuery.Operator.NOT_NULL, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(-1);
        assertThat(actual.boundValues()).containsExactly((Object) null);
        assertThat(actual.arity()).isEqualTo(1);
    }

    @Test
    void parseBooleanLiteralCreatesBoundValue() throws Exception {
        Method method = TestRepository.class.getMethod("findActive");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("active", LogicalQuery.Operator.EQ, 0)
        );
        assertThat(actual.boundValues()).containsExactly(true);
        assertThat(actual.parameterIndices()).containsExactly(-1);
        assertThat(actual.arity()).isEqualTo(1);
    }

    @Test
    void parseStringAndNumberLiteralsCreateBoundValues() throws Exception {
        Method method = TestRepository.class.getMethod("findByLiteralNameAndAge");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 0),
            LogicalQuery.Condition.of("age", LogicalQuery.Operator.GT, 1)
        );
        assertThat(actual.boundValues()).hasSize(2);
        assertThat(actual.boundValues()[0]).isEqualTo("Alice");
        assertThat(((Number) actual.boundValues()[1]).longValue()).isEqualTo(18L);
        assertThat(actual.parameterIndices()).containsExactly(-1, -1);
        assertThat(actual.arity()).isEqualTo(2);
    }

    @Test
    void parseParenthesesAndOrBuildsDnf() throws Exception {
        Method method = TestRepository.class.getMethod("findAdultActiveOrNamed", int.class, String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("age", LogicalQuery.Operator.GT, 0),
            LogicalQuery.Condition.of("active", LogicalQuery.Operator.EQ, 1, false, LogicalQuery.Combinator.OR),
            LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 2)
        );
        assertThat(actual.boundValues()).containsExactly(null, true, null);
        assertThat(actual.parameterIndices()).containsExactly(0, -1, 1);
        assertThat(actual.arity()).isEqualTo(3);
    }

    @Test
    void parseNotKeywordNegatesOperator() throws Exception {
        Method method = TestRepository.class.getMethod("findNotByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("name", LogicalQuery.Operator.NE, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(0);
    }

    @Test
    void parseOrderByDesc() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrderByAgeDesc", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.orderBy()).containsExactly(LogicalQuery.OrderBy.desc("age"));
    }

    @Test
    void parseCountQueryUsesCountOpcode() throws Exception {
        Method method = TestRepository.class.getMethod("countActive");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.opCode()).isEqualTo(OpCode.COUNT);
        assertThat(actual.returnKind()).isEqualTo(LogicalQuery.ReturnKind.COUNT_LONG);
        assertThat(actual.boundValues()).containsExactly(true);
        assertThat(actual.parameterIndices()).containsExactly(-1);
    }

    @Test
    void parseCountAllQueryUsesCountAllOpcode() throws Exception {
        Method method = TestRepository.class.getMethod("countAll");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.opCode()).isEqualTo(OpCode.COUNT_ALL);
        assertThat(actual.returnKind()).isEqualTo(LogicalQuery.ReturnKind.COUNT_LONG);
        assertThat(actual.conditions()).isEmpty();
        assertThat(actual.parameterIndices()).isEmpty();
        assertThat(actual.boundValues()).isEmpty();
    }

    @Test
    void parseExistsReturnTypeUsesExistsOpcode() throws Exception {
        Method method = TestRepository.class.getMethod("existsByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual.opCode()).isEqualTo(OpCode.EXISTS);
        assertThat(actual.returnKind()).isEqualTo(LogicalQuery.ReturnKind.EXISTS_BOOL);
        assertThat(actual.parameterIndices()).containsExactly(0);
    }

    @Test
    void parseJoinAliasRewritesPropertyPath() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentNameUsingJoin", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual.conditions()).containsExactly(
            LogicalQuery.Condition.of("department.name", LogicalQuery.Operator.EQ, 0)
        );
        assertThat(actual.parameterIndices()).containsExactly(0);
    }

    @Test
    void parseMissingNamedParamThrows() throws Exception {
        Method method = TestRepository.class.getMethod("findMissingParam", String.class);

        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("param");
    }

    interface TestRepository {
        @Query("select s from SimpleEntity s where s.name = :name")
        List<SimpleEntity> findByName(@Param("name") String name);

        @Query("select n from NestedEntity n where n.department.name ilike :name and n.address.city = :city")
        List<NestedEntity> findByDepartmentNameAndCity(@Param("name") String name, @Param("city") String city);

        @Query("select s from SimpleEntity s where s.age between :min and :max")
        List<SimpleEntity> findByAgeBetween(@Param("min") int min, @Param("max") int max);

        @Query("select s from SimpleEntity s where s.age in ?1")
        List<SimpleEntity> findByAgeIn(List<Integer> ages);

        @Query("select n from NestedEntity n where n.department is null")
        List<NestedEntity> findByDepartmentIsNull();

        @Query("select n from NestedEntity n where n.department is not null")
        List<NestedEntity> findByDepartmentIsNotNull();

        @Query("select s from SimpleEntity s where s.active = true")
        List<SimpleEntity> findActive();

        @Query("select s from SimpleEntity s where s.name = 'Alice' and s.age > 18")
        List<SimpleEntity> findByLiteralNameAndAge();

        @Query("select s from SimpleEntity s where (s.age > :age and s.active = true) or s.name = :name")
        List<SimpleEntity> findAdultActiveOrNamed(@Param("age") int age, @Param("name") String name);

        @Query("select s from SimpleEntity s where not s.name = :name")
        List<SimpleEntity> findNotByName(@Param("name") String name);

        @Query("select s from SimpleEntity s where s.name = :name order by s.age desc")
        List<SimpleEntity> findByNameOrderByAgeDesc(@Param("name") String name);

        @Query("select count(s) from SimpleEntity s where s.active = true")
        long countActive();

        @Query("select count(s) from SimpleEntity s")
        long countAll();

        @Query("select s from SimpleEntity s where s.name = :name")
        boolean existsByName(@Param("name") String name);

        @Query("select n from NestedEntity n join n.department d where d.name = :name")
        List<NestedEntity> findByDepartmentNameUsingJoin(@Param("name") String name);

        @Query("select s from SimpleEntity s where s.name = :missing")
        List<SimpleEntity> findMissingParam(@Param("name") String name);
    }
}
