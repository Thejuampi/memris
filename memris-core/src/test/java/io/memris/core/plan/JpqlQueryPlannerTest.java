package io.memris.core.plan;

import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.query.LogicalQuery;
import io.memris.query.OpCode;
import io.memris.query.QueryPlanner;
import io.memris.core.plan.entities.NestedEntity;
import io.memris.core.plan.entities.SimpleEntity;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JpqlQueryPlannerTest {

    @Test
    void parseSelectWithNamedParam() throws Exception {
        var method = TestRepository.class.getMethod("findByName", String.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                actual.opCode(),
                actual.returnKind(),
                Arrays.asList(actual.conditions()),
                Arrays.asList(actual.boundValues()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                actual.arity()
        }).containsExactly(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                List.of(LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 0)),
                Arrays.asList((Object) null),
                List.of(0),
                1
        );
    }

    @Test
    void parseSelectProjectionWithAliases() throws Exception {
        var method = TestRepository.class.getMethod("findProjection");
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                actual.opCode(),
                actual.returnKind(),
                actual.projection().projectionType(),
                Arrays.asList(actual.projection().items())
        }).containsExactly(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                SimpleProjection.class,
                List.of(
                        new LogicalQuery.ProjectionItem("name", "name"),
                        new LogicalQuery.ProjectionItem("age", "age")
                )
        );
    }

    @Test
    void parseSelectProjectionWithNestedPath() throws Exception {
        var method = TestRepository.class.getMethod("findNestedProjection");
        var actual = QueryPlanner.parse(method, NestedEntity.class);

        assertThat(new Object[] {actual.projection().projectionType(), Arrays.asList(actual.projection().items())})
                .containsExactly(
                        NestedProjection.class,
                        List.of(
                                new LogicalQuery.ProjectionItem("departmentName", "department.name"),
                                new LogicalQuery.ProjectionItem("city", "address.city")
                        )
                );
    }

    @Test
    void parseSelectProjectionMissingAliasShouldFail() throws Exception {
        Method method = TestRepository.class.getMethod("findProjectionMissingAlias");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aliases");
    }

    @Test
    void parseSelectWithNestedPropertyAndIlike() throws Exception {
        var method = TestRepository.class.getMethod("findByDepartmentNameAndCity", String.class, String.class);
        var actual = QueryPlanner.parse(method, NestedEntity.class);

        assertThat(new Object[] {
                actual.opCode(),
                actual.returnKind(),
                Arrays.asList(actual.conditions()),
                Arrays.asList(actual.boundValues()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                actual.arity()
        }).containsExactly(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                List.of(
                        LogicalQuery.Condition.of("department.name", LogicalQuery.Operator.LIKE, 0, true,
                                LogicalQuery.Combinator.AND),
                        LogicalQuery.Condition.of("address.city", LogicalQuery.Operator.EQ, 1, false,
                                LogicalQuery.Combinator.AND)
                ),
                Arrays.asList((Object) null, null),
                List.of(0, 1),
                2
        );
    }

    @Test
    void parseBetweenUsesFirstParameterIndex() throws Exception {
        var method = TestRepository.class.getMethod("findByAgeBetween", int.class, int.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {Arrays.asList(actual.conditions()), Arrays.stream(actual.parameterIndices()).boxed().toList(), actual.arity()})
                .containsExactly(
                        List.of(LogicalQuery.Condition.of("age", LogicalQuery.Operator.BETWEEN, 0)),
                        List.of(0, 1),
                        2
                );
    }

    @Test
    void parseInWithPositionalParam() throws Exception {
        var method = TestRepository.class.getMethod("findByAgeIn", List.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {Arrays.asList(actual.conditions()), Arrays.stream(actual.parameterIndices()).boxed().toList()})
                .containsExactly(
                        List.of(LogicalQuery.Condition.of("age", LogicalQuery.Operator.IN, 0)),
                        List.of(0)
                );
    }

    @Test
    void parseIsNull() throws Exception {
        var method = TestRepository.class.getMethod("findByDepartmentIsNull");
        var actual = QueryPlanner.parse(method, NestedEntity.class);

        assertThat(new Object[] {
                Arrays.asList(actual.conditions()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                Arrays.asList(actual.boundValues()),
                actual.arity()
        }).containsExactly(
                List.of(LogicalQuery.Condition.of("department", LogicalQuery.Operator.IS_NULL, 0)),
                List.of(-1),
                Arrays.asList((Object) null),
                1
        );
    }

    @Test
    void parseIsNotNull() throws Exception {
        var method = TestRepository.class.getMethod("findByDepartmentIsNotNull");
        var actual = QueryPlanner.parse(method, NestedEntity.class);

        assertThat(new Object[] {
                Arrays.asList(actual.conditions()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                Arrays.asList(actual.boundValues()),
                actual.arity()
        }).containsExactly(
                List.of(LogicalQuery.Condition.of("department", LogicalQuery.Operator.NOT_NULL, 0)),
                List.of(-1),
                Arrays.asList((Object) null),
                1
        );
    }

    @Test
    void parseBooleanLiteralCreatesBoundValue() throws Exception {
        var method = TestRepository.class.getMethod("findActive");
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                Arrays.asList(actual.conditions()),
                Arrays.asList(actual.boundValues()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                actual.arity()
        }).containsExactly(
                List.of(LogicalQuery.Condition.of("active", LogicalQuery.Operator.EQ, 0)),
                List.of(true),
                List.of(-1),
                1
        );
    }

    @Test
    void parseStringAndNumberLiteralsCreateBoundValues() throws Exception {
        var method = TestRepository.class.getMethod("findByLiteralNameAndAge");
        var actual = QueryPlanner.parse(method, SimpleEntity.class);
        var boundValues = Arrays.asList(actual.boundValues());

        assertThat(new Object[] {
                Arrays.asList(actual.conditions()),
                boundValues.size(),
                boundValues.get(0),
                ((Number) boundValues.get(1)).longValue(),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                actual.arity()
        }).containsExactly(
                List.of(
                        LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 0),
                        LogicalQuery.Condition.of("age", LogicalQuery.Operator.GT, 1)
                ),
                2,
                "Alice",
                18L,
                List.of(-1, -1),
                2
        );
    }

    @Test
    void parseParenthesesAndOrBuildsDnf() throws Exception {
        var method = TestRepository.class.getMethod("findAdultActiveOrNamed", int.class, String.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                Arrays.asList(actual.conditions()),
                Arrays.asList(actual.boundValues()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                actual.arity()
        }).containsExactly(
                List.of(
                        LogicalQuery.Condition.of("age", LogicalQuery.Operator.GT, 0),
                        LogicalQuery.Condition.of("active", LogicalQuery.Operator.EQ, 1, false, LogicalQuery.Combinator.OR),
                        LogicalQuery.Condition.of("name", LogicalQuery.Operator.EQ, 2)
                ),
                Arrays.asList(null, true, null),
                List.of(0, -1, 1),
                3
        );
    }

    @Test
    void parseNotKeywordNegatesOperator() throws Exception {
        var method = TestRepository.class.getMethod("findNotByName", String.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {Arrays.asList(actual.conditions()), Arrays.stream(actual.parameterIndices()).boxed().toList()})
                .containsExactly(
                        List.of(LogicalQuery.Condition.of("name", LogicalQuery.Operator.NE, 0)),
                        List.of(0)
                );
    }

    @Test
    void parseOrderByDesc() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrderByAgeDesc", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(actual.orderBy()).containsExactly(LogicalQuery.OrderBy.desc("age"));
    }

    @Test
    void parseCountQueryUsesCountOpcode() throws Exception {
        var method = TestRepository.class.getMethod("countActive");
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                actual.opCode(),
                actual.returnKind(),
                Arrays.asList(actual.boundValues()),
                Arrays.stream(actual.parameterIndices()).boxed().toList()
        }).containsExactly(
                OpCode.COUNT,
                LogicalQuery.ReturnKind.COUNT_LONG,
                List.of(true),
                List.of(-1)
        );
    }

    @Test
    void parseCountAllQueryUsesCountAllOpcode() throws Exception {
        var method = TestRepository.class.getMethod("countAll");
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {
                actual.opCode(),
                actual.returnKind(),
                Arrays.asList(actual.conditions()),
                Arrays.stream(actual.parameterIndices()).boxed().toList(),
                Arrays.asList(actual.boundValues())
        }).containsExactly(
                OpCode.COUNT_ALL,
                LogicalQuery.ReturnKind.COUNT_LONG,
                List.of(),
                List.of(),
                List.of()
        );
    }

    @Test
    void parseExistsReturnTypeUsesExistsOpcode() throws Exception {
        var method = TestRepository.class.getMethod("existsByName", String.class);
        var actual = QueryPlanner.parse(method, SimpleEntity.class);

        assertThat(new Object[] {actual.opCode(), actual.returnKind(), Arrays.stream(actual.parameterIndices()).boxed().toList()})
                .containsExactly(OpCode.EXISTS, LogicalQuery.ReturnKind.EXISTS_BOOL, List.of(0));
    }

    @Test
    void parseJoinAliasRewritesPropertyPath() throws Exception {
        var method = TestRepository.class.getMethod("findByDepartmentNameUsingJoin", String.class);
        var actual = QueryPlanner.parse(method, NestedEntity.class);

        assertThat(new Object[] {Arrays.asList(actual.conditions()), Arrays.stream(actual.parameterIndices()).boxed().toList()})
                .containsExactly(
                        List.of(LogicalQuery.Condition.of("department.name", LogicalQuery.Operator.EQ, 0)),
                        List.of(0)
                );
    }

    @Test
    void parseMissingNamedParamThrows() throws Exception {
        Method method = TestRepository.class.getMethod("findMissingParam", String.class);

        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("param");
    }

    interface TestRepository {
        @Query("select s from SimpleEntity s where s.name = :name")
        List<SimpleEntity> findByName(@Param("name") String name);

        @Query("select s.name as name, s.age as age from SimpleEntity s")
        List<SimpleProjection> findProjection();

        @Query("select n.department.name as departmentName, n.address.city as city from NestedEntity n")
        List<NestedProjection> findNestedProjection();

        @Query("select s.name, s.age from SimpleEntity s")
        List<SimpleProjection> findProjectionMissingAlias();

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

    record SimpleProjection(String name, int age) {
    }

    record NestedProjection(String departmentName, String city) {
    }
}
