package io.memris.query.entities;

import io.memris.query.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.memris.query.LogicalQuery.*;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Test suite for QueryPlanner with context-aware lexer integration.
 */
class QueryPlannerIntegrationTest {

    // ========== COMPARISON OPERATORS ==========

    @Test
    void parseSimpleFindBy_UsesEntityContext() throws Exception {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.EQ, 0));
    }

    @Test
    void parseWithGreaterThan_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeGreaterThan", int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.GT, 0));
    }

    @Test
    void parseWithLessThan_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeLessThan", int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.LT, 0));
    }

    @Test
    void parseWithGreaterThanOrEqual_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeGreaterThanEqual", int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.GTE, 0));
    }

    @Test
    void parseWithLessThanOrEqual_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeLessThanEqual", int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.LTE, 0));
    }

    @Test
    void parseWithNotEquals_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNot", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.NE, 0));
    }

    // ========== STRING OPERATORS ==========

    @Test
    void parseWithLike_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameLike", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.LIKE, 0));
    }

    @Test
    void parseWithNotLike_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotLike", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.NOT_LIKE, 0));
    }

    @Test
    void parseWithLikeIgnoreCase_CombinesModifier() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameLikeIgnoreCase", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.IGNORE_CASE_LIKE, 0, true));
    }

    @Test
    void parseWithStartingWith_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameStartingWith", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.STARTING_WITH, 0));
    }

    @Test
    void parseWithNotStartingWith_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotStartingWith", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.NOT_STARTING_WITH, 0));
    }

    @Test
    void parseWithEndingWith_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameEndingWith", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.ENDING_WITH, 0));
    }

    @Test
    void parseWithNotEndingWith_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotEndingWith", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.NOT_ENDING_WITH, 0));
    }

    @Test
    void parseWithContaining_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameContaining", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.CONTAINING, 0));
    }

    @Test
    void parseWithNotContaining_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotContaining", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("name", Operator.NOT_CONTAINING, 0));
    }

    // ========== NULL CHECKS ==========

    @Test
    void parseWithNullOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentIdIsNull");
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("department.id", Operator.IS_NULL, 0)),
                0
        ));
    }

    @Test
    void parseWithNotNullOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentIdNotNull");
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("department.id", Operator.NOT_NULL, 0)),
                0
        ));
    }

    // ========== BOOLEAN CHECKS ==========

    @Test
    void parseWithTrueOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByActiveTrue");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("active", Operator.IS_TRUE, 0)),
                0
        ));
    }

    @Test
    void parseWithFalseOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByActiveFalse");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("active", Operator.IS_FALSE, 0)),
                0
        ));
    }

    // ========== COLLECTION OPERATORS ==========

    @Test
    void parseWithInOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeIn", List.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.IN, 0));
    }

    @Test
    void parseWithNotInOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeNotIn", List.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("age", Operator.NOT_IN, 0));
    }

    // ========== RANGE OPERATORS ==========

    @Test
    void parseWithBetweenOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeBetween", int.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("age", Operator.BETWEEN, 0)),
                2
        ));
    }

    // ========== DATE OPERATORS ==========

    @Test
    void parseWithBeforeOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByCreatedAtBefore", Date.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("createdat", Operator.BEFORE, 0));
    }

    @Test
    void parseWithAfterOperator_MapsCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByCreatedAtAfter", Date.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("createdat", Operator.AFTER, 0));
    }

    // ========== COMBINATION CONDITIONS ==========

    @Test
    void parseWithAnd_CreatesMultipleConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndAge", String.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("name", Operator.EQ, 0), cond("age", Operator.EQ, 1)),
                2
        ));
    }

    @Test
    void parseWithOr_CreatesMultipleConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrAge", String.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("name", Operator.EQ, 0, LogicalQuery.Combinator.OR), cond("age", Operator.EQ, 1)),
                2
        ));
    }

    @Test
    void parseWithAndOperator_CombinesWithCondition() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndAgeGreaterThan", String.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("name", Operator.EQ, 0), cond("age", Operator.GT, 1)),
                2
        ));
    }

    @Test
    void parseWithOrOperator_CombinesWithCondition() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrAgeLessThan", String.class, int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(
                conds(cond("name", Operator.EQ, 0, LogicalQuery.Combinator.OR), cond("age", Operator.LT, 1)),
                2
        ));
    }

    // ========== ORDER BY ==========

    @Test
    void parseWithOrderBy_CreatesOrderBy() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrderByAgeDesc", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind(cond("name", Operator.EQ, 0), new OrderBy[]{OrderBy.desc("age")}));
    }

    @Test
    void parseWithTopPrefix_ExtractsLimit() throws Exception {
        Method method = TestRepository.class.getMethod("findTop5ByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFindWithLimit(
                conds(cond("name", Operator.EQ, 0)),
                1,
                5
        ));
    }

    @Test
    void parseWithFirstPrefix_ExtractsLimit() throws Exception {
        Method method = TestRepository.class.getMethod("findFirstByAge", int.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFindWithLimit(
                conds(cond("age", Operator.EQ, 0)),
                1,
                1
        ));
    }

    // ========== COUNT PREFIX ==========

    @Test
    void parseWithCountPrefix_DeterminesReturnKind() throws Exception {
        Method method = TestRepository.class.getMethod("countByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedCount("name", Operator.EQ, 0));
    }

    @Test
    void parseCount_ReturnsLong() throws Exception {
        Method method = TestRepository.class.getMethod("count");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedCountAll());
    }

    // ========== EXISTS PREFIX ==========

    @Test
    void parseWithExistsPrefix_DeterminesReturnKind() throws Exception {
        Method method = TestRepository.class.getMethod("existsByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedExists("name", Operator.EQ, 0));
    }

    // ========== NESTED PROPERTIES ==========

    @Test
    void parseNestedProperty_ResolvesDotNotation() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, NestedEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("department.name", Operator.EQ, 0));
    }

    @Test
    void parseDeepNesting_ResolvesCorrectly() throws Exception {
        Method method = TestRepository.class.getMethod("findByDepartmentAddressCity", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, DeepNestedEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFind("department.address.city", Operator.EQ, 0));
    }

    // ========== BUILT-IN METHODS ==========

    @Test
    void parseFindById_ReturnsOptional() throws Exception {
        Method method = TestRepository.class.getMethod("findById", Long.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFindById(0));
    }

    @Test
    void parseFindAll_ReturnsList() throws Exception {
        Method method = TestRepository.class.getMethod("findAll");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedFindAll());
    }

    @Test
    void parseExistsById_ReturnsBoolean() throws Exception {
        Method method = TestRepository.class.getMethod("existsById", Long.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedExistsById(0));
    }

    // ========== CRUD OPERATIONS ==========

    @Test
    void parseSave_ReturnsCrudQuery() throws Exception {
        Method method = TestRepository.class.getMethod("save", SimpleEntity.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedSave(1));
    }

    @Test
    void parseSaveAll_ReturnsCrudQuery() throws Exception {
        Method method = TestRepository.class.getMethod("saveAll", List.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedSaveAll(1));
    }

    @Test
    void parseDelete_ReturnsCrudQuery() throws Exception {
        Method method = TestRepository.class.getMethod("delete", SimpleEntity.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedDelete(1));
    }

    @Test
    void parseDeleteAll_ReturnsCrudQuery() throws Exception {
        Method method = TestRepository.class.getMethod("deleteAll");
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedDeleteAll());
    }

    @Test
    void parseDeleteById_ReturnsCrudQuery() throws Exception {
        Method method = TestRepository.class.getMethod("deleteById", Long.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedDeleteById(0));
    }

    @Test
    void parseDeleteByReturnsQuery() throws Exception {
        Method method = TestRepository.class.getMethod("deleteByName", String.class);
        LogicalQuery actual = QueryPlanner.parse(method, SimpleEntity.class, "id");

        assertThat(actual).isEqualTo(expectedDeleteBy("name", Operator.EQ, 0));
    }

    // ========== ERROR CASES ==========

    @Test
    void parseWithNoPropertyAfterBy_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findBy");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("property");
    }

    @Test
    void parseEndingWithCombinator_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAnd");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("property");
    }

    @Test
    void parseWithConsecutiveCombinators_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndAndAge");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("combinator");
    }

    @Test
    void parseWithConsecutiveOperators_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameEqualsEquals");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("operator");
    }

    @Test
    void parseWithPropertyAfterOperatorWithoutOperator_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameEqualsAge");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("operator");
    }

    @Test
    void parseOrderByWithoutProperty_ThrowsException() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrderBy");
        assertThatThrownBy(() -> QueryPlanner.parse(method, SimpleEntity.class, "id"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("property");
    }

    // ========== Test repository interface ==========

    interface TestRepository {
        // Simple find
        List<SimpleEntity> findByName(String name);
        Optional<SimpleEntity> findById(Long id);
        List<SimpleEntity> findAll();

        // Comparison operators
        List<SimpleEntity> findByAgeGreaterThan(int age);
        List<SimpleEntity> findByAgeLessThan(int age);
        List<SimpleEntity> findByAgeGreaterThanEqual(int age);
        List<SimpleEntity> findByAgeLessThanEqual(int age);
        List<SimpleEntity> findByNameNot(String name);

        // String operators
        List<SimpleEntity> findByNameLike(String pattern);
        List<SimpleEntity> findByNameNotLike(String pattern);
        List<SimpleEntity> findByNameLikeIgnoreCase(String pattern);
        List<SimpleEntity> findByNameStartingWith(String prefix);
        List<SimpleEntity> findByNameNotStartingWith(String prefix);
        List<SimpleEntity> findByNameEndingWith(String suffix);
        List<SimpleEntity> findByNameNotEndingWith(String suffix);
        List<SimpleEntity> findByNameContaining(String keyword);
        List<SimpleEntity> findByNameNotContaining(String keyword);

        // Null checks
        List<NestedEntity> findByDepartmentIdIsNull();
        List<NestedEntity> findByDepartmentIdNotNull();

        // Boolean checks
        List<SimpleEntity> findByActiveTrue();
        List<SimpleEntity> findByActiveFalse();

        // Collection operators
        List<SimpleEntity> findByAgeIn(List<Integer> ages);
        List<SimpleEntity> findByAgeNotIn(List<Integer> ages);

        // Range operators
        List<SimpleEntity> findByAgeBetween(int min, int max);

        // Date operators
        List<SimpleEntity> findByCreatedAtBefore(Date date);
        List<SimpleEntity> findByCreatedAtAfter(Date date);

        // Combinations
        List<SimpleEntity> findByNameAndAge(String name, int age);
        List<SimpleEntity> findByNameOrAge(String name, int age);
        List<SimpleEntity> findByNameAndAgeGreaterThan(String name, int age);
        List<SimpleEntity> findByNameOrAgeLessThan(String name, int age);

        // Order by
        List<SimpleEntity> findByNameOrderByAgeDesc(String name);

        // Limit modifiers
        List<SimpleEntity> findTop5ByName(String name);
        List<SimpleEntity> findFirstByAge(int age);

        // Count
        Long countByName(String name);
        Long count();

        // Exists
        boolean existsByName(String name);
        boolean existsById(Long id);

        // Nested properties
        List<NestedEntity> findByDepartmentName(String departmentName);
        List<DeepNestedEntity> findByDepartmentAddressCity(String city);

        // CRUD
        SimpleEntity save(SimpleEntity entity);
        List<SimpleEntity> saveAll(List<SimpleEntity> entities);
        void delete(SimpleEntity entity);
        void deleteAll();
        void deleteById(Long id);
        void deleteByName(String name);

        // Invalid methods for error testing
        List<SimpleEntity> findBy();
        List<SimpleEntity> findByNameAnd();
        List<SimpleEntity> findByNameAndAndAge();
        List<SimpleEntity> findByNameEqualsEquals();
        List<SimpleEntity> findByNameEqualsAge();
        List<SimpleEntity> findByNameOrderBy();
    }

    // ========== Helper methods ==========

    private static LogicalQuery expectedFind(String property, Operator op, int argIndex) {
        return LogicalQuery.of(
                OpCode.FIND,
                ReturnKind.MANY_LIST,
                conds(Condition.of(property, op, argIndex)),
                null
        );
    }

    private static LogicalQuery expectedFind(String property, Operator op, int argIndex, boolean ignoreCase) {
        return LogicalQuery.of(
                OpCode.FIND,
                ReturnKind.MANY_LIST,
                conds(Condition.of(property, op, argIndex, ignoreCase)),
                null
        );
    }

    private static LogicalQuery expectedFind(Condition[] conditions, int paramCount) {
        return LogicalQuery.of(
                OpCode.FIND,
                ReturnKind.MANY_LIST,
                conditions,
                null,
                paramCount
        );
    }

    private static LogicalQuery expectedFindWithLimit(Condition[] conditions, int paramCount, int limit) {
        return LogicalQuery.of(
                OpCode.FIND,
                ReturnKind.MANY_LIST,
                conditions,
                new LogicalQuery.Join[0],
                null,
                limit,
                paramCount
        );
    }

    private static LogicalQuery expectedFind(Condition condition, OrderBy[] orderBy) {
        return LogicalQuery.of(
                OpCode.FIND,
                ReturnKind.MANY_LIST,
                conds(condition),
                orderBy
        );
    }

    private static LogicalQuery expectedFindById(int argIndex) {
        return LogicalQuery.of(
                OpCode.FIND_BY_ID,
                ReturnKind.ONE_OPTIONAL,
                conds(Condition.idCondition(argIndex)),
                null,
                1
        );
    }

    private static LogicalQuery expectedFindAll() {
        return LogicalQuery.of(
                OpCode.FIND_ALL,
                ReturnKind.MANY_LIST,
                new Condition[0],
                null,
                0
        );
    }

    private static LogicalQuery expectedCount(String property, Operator op, int argIndex) {
        return LogicalQuery.of(
                OpCode.COUNT,
                ReturnKind.COUNT_LONG,
                conds(Condition.of(property, op, argIndex)),
                null
        );
    }

    private static LogicalQuery expectedCountAll() {
        return LogicalQuery.of(
                OpCode.COUNT_ALL,
                ReturnKind.COUNT_LONG,
                new Condition[0],
                null,
                0
        );
    }

    private static LogicalQuery expectedExists(String property, Operator op, int argIndex) {
        return LogicalQuery.of(
                OpCode.EXISTS,
                ReturnKind.EXISTS_BOOL,
                conds(Condition.of(property, op, argIndex)),
                null
        );
    }

    private static LogicalQuery expectedExistsById(int argIndex) {
        return LogicalQuery.of(
                OpCode.EXISTS_BY_ID,
                ReturnKind.EXISTS_BOOL,
                conds(Condition.idCondition(argIndex)),
                null,
                1
        );
    }

    private static LogicalQuery expectedSave(int paramCount) {
        return LogicalQuery.crud(OpCode.SAVE_ONE, ReturnKind.SAVE, paramCount);
    }

    private static LogicalQuery expectedSaveAll(int paramCount) {
        return LogicalQuery.crud(OpCode.SAVE_ALL, ReturnKind.SAVE_ALL, paramCount);
    }

    private static LogicalQuery expectedDelete(int paramCount) {
        return LogicalQuery.crud(OpCode.DELETE_ONE, ReturnKind.DELETE, paramCount);
    }

    private static LogicalQuery expectedDeleteAll() {
        return LogicalQuery.crud(OpCode.DELETE_ALL, ReturnKind.DELETE_ALL, 0);
    }

    private static LogicalQuery expectedDeleteById(int argIndex) {
        return LogicalQuery.of(
                OpCode.DELETE_BY_ID,
                ReturnKind.DELETE_BY_ID,
                conds(Condition.idCondition(argIndex)),
                null,
                1
        );
    }

    private static LogicalQuery expectedDeleteBy(String property, Operator op, int argIndex) {
        return LogicalQuery.of(
                OpCode.DELETE_QUERY,
                ReturnKind.MANY_LIST,
                conds(Condition.of(property, op, argIndex)),
                null,
                1
        );
    }

    private static Condition[] conds(Condition... conditions) {
        return conditions;
    }

    private static Condition cond(String property, Operator op, int argIndex) {
        return Condition.of(property, op, argIndex);
    }

    private static Condition cond(String property, Operator op, int argIndex, LogicalQuery.Combinator combinator) {
        return Condition.of(property, op, argIndex, false, combinator);
    }
}
