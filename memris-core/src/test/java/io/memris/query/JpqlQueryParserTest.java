package io.memris.query;

import io.memris.core.Modifying;
import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.core.plan.entities.SimpleEntity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for JPQL Query Parser.
 */
class JpqlQueryParserTest {

    interface TestRepository {
        // Basic queries
        @Query("SELECT e FROM SimpleEntity e")
        List<SimpleEntity> findAll();

        @Query("SELECT e FROM SimpleEntity e WHERE e.name = :name")
        List<SimpleEntity> findByName(@Param("name") String name);

        @Query("SELECT e FROM SimpleEntity e WHERE e.age > :minAge")
        List<SimpleEntity> findByAgeGreaterThan(@Param("minAge") int minAge);

        // Comparison operators
        @Query("SELECT e FROM SimpleEntity e WHERE e.age >= :min AND e.age <= :max")
        List<SimpleEntity> findByAgeBetween(@Param("min") int min, @Param("max") int max);

        @Query("SELECT e FROM SimpleEntity e WHERE e.name != :name")
        List<SimpleEntity> findByNameNotEqual(@Param("name") String name);

        @Query("SELECT e FROM SimpleEntity e WHERE e.name <> :name")
        List<SimpleEntity> findByNameNotEqualAlt(@Param("name") String name);

        // LIKE operators
        @Query("SELECT e FROM SimpleEntity e WHERE e.name LIKE :pattern")
        List<SimpleEntity> findByNameLike(@Param("pattern") String pattern);

        @Query("SELECT e FROM SimpleEntity e WHERE e.name ILIKE :pattern")
        List<SimpleEntity> findByNameILike(@Param("pattern") String pattern);

        // IS NULL / IS NOT NULL
        @Query("SELECT e FROM SimpleEntity e WHERE e.name IS NULL")
        List<SimpleEntity> findByNameIsNull();

        @Query("SELECT e FROM SimpleEntity e WHERE e.name IS NOT NULL")
        List<SimpleEntity> findByNameIsNotNull();

        // BETWEEN
        @Query("SELECT e FROM SimpleEntity e WHERE e.age BETWEEN :min AND :max")
        List<SimpleEntity> findByAgeInRange(@Param("min") int min, @Param("max") int max);

        // IN clause
        @Query("SELECT e FROM SimpleEntity e WHERE e.name IN :names")
        List<SimpleEntity> findByNames(@Param("names") List<String> names);

        // ORDER BY
        @Query("SELECT e FROM SimpleEntity e ORDER BY e.name ASC")
        List<SimpleEntity> findAllOrderedByNameAsc();

        @Query("SELECT e FROM SimpleEntity e ORDER BY e.age DESC")
        List<SimpleEntity> findAllOrderedByAgeDesc();

        @Query("SELECT e FROM SimpleEntity e ORDER BY e.name ASC, e.age DESC")
        List<SimpleEntity> findAllOrderedByMultiple();

        // DISTINCT
        @Query("SELECT DISTINCT e FROM SimpleEntity e")
        List<SimpleEntity> findAllDistinct();

        // COUNT
        @Query("SELECT COUNT(e) FROM SimpleEntity e")
        long countAll();





        // Complex WHERE with AND/OR
        @Query("SELECT e FROM SimpleEntity e WHERE e.active = true AND e.age > 18")
        List<SimpleEntity> findActiveAdults();

        @Query("SELECT e FROM SimpleEntity e WHERE e.name = :name OR e.age = :age")
        List<SimpleEntity> findByNameOrAge(@Param("name") String name, @Param("age") int age);

        @Query("SELECT e FROM SimpleEntity e WHERE NOT e.active = false")
        List<SimpleEntity> findNotInactive();

        // Modifying queries
        @Modifying
        @Query("UPDATE SimpleEntity e SET e.name = :newName WHERE e.id = :id")
        int updateName(@Param("id") Long id, @Param("newName") String newName);

        @Modifying
        @Query("DELETE FROM SimpleEntity e WHERE e.active = false")
        int deleteInactive();

        @Modifying
        @Query("DELETE SimpleEntity e WHERE e.id = :id")
        int deleteById(@Param("id") Long id);

        // Single result queries
        @Query("SELECT e FROM SimpleEntity e WHERE e.id = :id")
        Optional<SimpleEntity> findById(@Param("id") Long id);

        // Error cases
        @Query("SELECT e FROM WrongEntity e")
        List<SimpleEntity> findFromWrongEntity();

        @Query(value = "SELECT * FROM simple_entity", nativeQuery = true)
        List<SimpleEntity> nativeQuery();

        @Query("SELECT COUNT(e) FROM SimpleEntity e")
        List<SimpleEntity> countWithWrongReturnType();

        @Query("SELECT e FROM SimpleEntity e")
        long selectWithCountReturnType();
    }

    @Test
    @DisplayName("Should parse simple SELECT all query")
    void shouldParseSimpleSelectAllQuery() throws Exception {
        Method method = TestRepository.class.getMethod("findAll");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.FIND);
        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.MANY_LIST);
        assertThat(query.conditions()).isEmpty();
    }

    @Test
    @DisplayName("Should parse query with named parameter")
    void shouldParseQueryWithNamedParameter() throws Exception {
        Method method = TestRepository.class.getMethod("findByName", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.FIND);
        assertThat(query.conditions()).hasSize(1);
        assertThat(query.conditions()[0].propertyPath()).isEqualTo("name");
        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.EQ);
        assertThat(query.parameterIndices()).containsExactly(0);
    }

    @Test
    @DisplayName("Should parse query with comparison operators")
    void shouldParseQueryWithComparisonOperators() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeGreaterThan", int.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.GT);
    }

    @Test
    @DisplayName("Should parse query with AND conditions")
    void shouldParseQueryWithAndConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeBetween", int.class, int.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(2);
        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.GTE);
        assertThat(query.conditions()[1].operator()).isEqualTo(LogicalQuery.Operator.LTE);
        assertThat(query.conditions()[0].nextCombinator()).isEqualTo(LogicalQuery.Combinator.AND);
    }

    @Test
    @DisplayName("Should parse query with NOT EQUAL operators")
    void shouldParseQueryWithNotEqualOperators() throws Exception {
        Method method1 = TestRepository.class.getMethod("findByNameNotEqual", String.class);
        Method method2 = TestRepository.class.getMethod("findByNameNotEqualAlt", String.class);

        LogicalQuery query1 = JpqlQueryParser.parse(method1, SimpleEntity.class);
        LogicalQuery query2 = JpqlQueryParser.parse(method2, SimpleEntity.class);

        assertThat(query1.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NE);
        assertThat(query2.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NE);
    }

    @Test
    @DisplayName("Should parse query with LIKE operators")
    void shouldParseQueryWithLikeOperators() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameLike", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.LIKE);
    }

    @Test
    @DisplayName("Should parse query with ILIKE operators")
    void shouldParseQueryWithILikeOperators() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameILike", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.LIKE);
        assertThat(query.conditions()[0].ignoreCase()).isTrue();
    }

    @Test
    @DisplayName("Should parse query with IS NULL")
    void shouldParseQueryWithIsNull() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameIsNull");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IS_NULL);
    }

    @Test
    @DisplayName("Should parse query with IS NOT NULL")
    void shouldParseQueryWithIsNotNull() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameIsNotNull");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NOT_NULL);
    }

    @Test
    @DisplayName("Should parse query with BETWEEN")
    void shouldParseQueryWithBetween() throws Exception {
        Method method = TestRepository.class.getMethod("findByAgeInRange", int.class, int.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.BETWEEN);
    }

    @Test
    @DisplayName("Should parse query with IN clause")
    void shouldParseQueryWithInClause() throws Exception {
        Method method = TestRepository.class.getMethod("findByNames", List.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.IN);
    }

    @Test
    @DisplayName("Should parse query with ORDER BY ASC")
    void shouldParseQueryWithOrderByAsc() throws Exception {
        Method method = TestRepository.class.getMethod("findAllOrderedByNameAsc");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.orderBy()).hasSize(1);
        assertThat(query.orderBy()[0].propertyPath()).isEqualTo("name");
        assertThat(query.orderBy()[0].ascending()).isTrue();
    }

    @Test
    @DisplayName("Should parse query with ORDER BY DESC")
    void shouldParseQueryWithOrderByDesc() throws Exception {
        Method method = TestRepository.class.getMethod("findAllOrderedByAgeDesc");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.orderBy()[0].ascending()).isFalse();
    }

    @Test
    @DisplayName("Should parse query with multiple ORDER BY")
    void shouldParseQueryWithMultipleOrderBy() throws Exception {
        Method method = TestRepository.class.getMethod("findAllOrderedByMultiple");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.orderBy()).hasSize(2);
        assertThat(query.orderBy()[0].ascending()).isTrue();
        assertThat(query.orderBy()[1].ascending()).isFalse();
    }

    @Test
    @DisplayName("Should parse DISTINCT query")
    void shouldParseDistinctQuery() throws Exception {
        Method method = TestRepository.class.getMethod("findAllDistinct");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.distinct()).isTrue();
    }

    @Test
    @DisplayName("Should parse COUNT query")
    void shouldParseCountQuery() throws Exception {
        Method method = TestRepository.class.getMethod("countAll");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.COUNT_ALL);
        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.COUNT_LONG);
    }

    @Test
    @DisplayName("Should parse query with OR conditions")
    void shouldParseQueryWithOrConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameOrAge", String.class, int.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(2);
        assertThat(query.conditions()[0].nextCombinator()).isEqualTo(LogicalQuery.Combinator.OR);
    }

    @Test
    @DisplayName("Should parse UPDATE query")
    void shouldParseUpdateQuery() throws Exception {
        Method method = TestRepository.class.getMethod("updateName", Long.class, String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.UPDATE_QUERY);
        assertThat(query.updateAssignments()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse DELETE query with FROM")
    void shouldParseDeleteQueryWithFrom() throws Exception {
        Method method = TestRepository.class.getMethod("deleteInactive");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.DELETE_QUERY);
    }

    @Test
    @DisplayName("Should parse DELETE query without FROM")
    void shouldParseDeleteQueryWithoutFrom() throws Exception {
        Method method = TestRepository.class.getMethod("deleteById", Long.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.DELETE_QUERY);
    }

    @Test
    @DisplayName("Should parse query returning Optional")
    void shouldParseQueryReturningOptional() throws Exception {
        Method method = TestRepository.class.getMethod("findById", Long.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.returnKind()).isEqualTo(LogicalQuery.ReturnKind.ONE_OPTIONAL);
    }

    @Test
    @DisplayName("Should reject query with wrong entity name")
    void shouldRejectQueryWithWrongEntityName() throws Exception {
        Method method = TestRepository.class.getMethod("findFromWrongEntity");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown entity");
    }

    @Test
    @DisplayName("Should reject native query")
    void shouldRejectNativeQuery() throws Exception {
        Method method = TestRepository.class.getMethod("nativeQuery");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("nativeQuery is not supported");
    }

    @Test
    @DisplayName("Should reject method without @Query annotation")
    void shouldRejectMethodWithoutQueryAnnotation() throws Exception {
        Method method = Object.class.getMethod("toString");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Query annotation required");
    }

    @Test
    @DisplayName("Should reject COUNT query with wrong return type")
    void shouldRejectCountQueryWithWrongReturnType() throws Exception {
        Method method = TestRepository.class.getMethod("countWithWrongReturnType");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Query count must return long");
    }

    @Test
    @DisplayName("Should reject SELECT query with long return type")
    void shouldRejectSelectQueryWithLongReturnType() throws Exception {
        Method method = TestRepository.class.getMethod("selectWithCountReturnType");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Query return type long requires count");
    }
}
