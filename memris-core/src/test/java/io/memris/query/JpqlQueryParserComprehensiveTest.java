package io.memris.query;

import io.memris.core.Modifying;
import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.core.plan.entities.SimpleEntity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for JpqlQueryParser.Parser inner class.
 * Tests parsing of complex JPQL constructs and edge cases.
 */
class JpqlQueryParserComprehensiveTest {

    interface TestRepository {
        // Complex WHERE with nested expressions
        @Query("SELECT e FROM SimpleEntity e WHERE (e.age > 18 AND e.active = true) OR e.name = 'admin'")
        List<SimpleEntity> findWithNestedConditions();

        @Query("SELECT e FROM SimpleEntity e WHERE NOT (e.age < 18 OR e.name IS NULL)")
        List<SimpleEntity> findWithNotExpression();

        // NOT LIKE / NOT ILIKE
        @Query("SELECT e FROM SimpleEntity e WHERE e.name NOT LIKE :pattern")
        List<SimpleEntity> findByNameNotLike(@Param("pattern") String pattern);

        @Query("SELECT e FROM SimpleEntity e WHERE e.name NOT ILIKE :pattern")
        List<SimpleEntity> findByNameNotILike(@Param("pattern") String pattern);

        // NOT IN
        @Query("SELECT e FROM SimpleEntity e WHERE e.name NOT IN :names")
        List<SimpleEntity> findByNameNotIn(@Param("names") List<String> names);

        // Multiple parameters
        @Query("SELECT e FROM SimpleEntity e WHERE e.name = :name AND e.age = :age AND e.active = :active")
        List<SimpleEntity> findByMultipleParams(@Param("name") String name, @Param("age") int age, @Param("active") boolean active);

        // Positional parameters
        @Query("SELECT e FROM SimpleEntity e WHERE e.name = ?1 AND e.age = ?2")
        List<SimpleEntity> findByPositionalParams(String name, int age);

        // Mixed literal and parameter
        @Query("SELECT e FROM SimpleEntity e WHERE e.name = :name AND e.active = true")
        List<SimpleEntity> findByNameAndActiveTrue(@Param("name") String name);

        // UPDATE with multiple SET clauses
        @Modifying
        @Query("UPDATE SimpleEntity e SET e.name = :name, e.age = :age, e.active = :active WHERE e.id = :id")
        int updateMultipleFields(@Param("id") Long id, @Param("name") String name, @Param("age") int age, @Param("active") boolean active);

        // UPDATE without WHERE
        @Modifying
        @Query("UPDATE SimpleEntity e SET e.active = false")
        int deactivateAll();

        // Alias variations
        @Query("SELECT e FROM SimpleEntity AS e WHERE e.name = :name")
        List<SimpleEntity> findWithExplicitAlias(@Param("name") String name);

        @Query("SELECT e FROM SimpleEntity e WHERE e.name = 'literal'")
        List<SimpleEntity> findWithStringLiteral();

        @Query("SELECT e FROM SimpleEntity e WHERE e.age = 42")
        List<SimpleEntity> findWithNumberLiteral();

        @Query("SELECT e FROM SimpleEntity e WHERE e.active = TRUE")
        List<SimpleEntity> findWithTrueLiteral();

        @Query("SELECT e FROM SimpleEntity e WHERE e.active = FALSE")
        List<SimpleEntity> findWithFalseLiteral();

        @Query("SELECT e FROM SimpleEntity e WHERE e.name IS NULL")
        List<SimpleEntity> findWithNullLiteral();

        // Complex ORDER BY
        @Query("SELECT e FROM SimpleEntity e ORDER BY e.age DESC, e.name ASC")
        List<SimpleEntity> findWithComplexOrderBy();

        // Error cases
        @Query("SELECT e FROM SimpleEntity e WHERE e.age BETWEEN 18")
        List<SimpleEntity> invalidBetween();

        @Query("SELECT e FROM SimpleEntity e WHERE e")
        List<SimpleEntity> invalidPath();

        @Query("SELECT e FROM SimpleEntity e WHERE")
        List<SimpleEntity> incompleteWhere();

        @Query("SELECT e FROM")
        List<SimpleEntity> missingEntity();

        @Query("SELECT FROM SimpleEntity e")
        List<SimpleEntity> missingSelectItem();
    }

    @Test
    @DisplayName("Should parse nested conditions with parentheses")
    void shouldParseNestedConditions() throws Exception {
        Method method = TestRepository.class.getMethod("findWithNestedConditions");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(3);
    }

    @Test
    @DisplayName("Should parse NOT with parentheses expression")
    void shouldParseNotExpression() throws Exception {
        Method method = TestRepository.class.getMethod("findWithNotExpression");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).isNotEmpty();
    }

    @Test
    @DisplayName("Should parse NOT LIKE operator")
    void shouldParseNotLikeOperator() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotLike", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NOT_LIKE);
    }

    @Test
    @DisplayName("Should parse NOT ILIKE operator")
    void shouldParseNotILikeOperator() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotILike", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NOT_LIKE);
        assertThat(query.conditions()[0].ignoreCase()).isTrue();
    }

    @Test
    @DisplayName("Should parse NOT IN operator")
    void shouldParseNotInOperator() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameNotIn", List.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()[0].operator()).isEqualTo(LogicalQuery.Operator.NOT_IN);
    }

    @Test
    @DisplayName("Should parse query with multiple named parameters")
    void shouldParseMultipleNamedParameters() throws Exception {
        Method method = TestRepository.class.getMethod("findByMultipleParams", String.class, int.class, boolean.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(3);
        assertThat(query.parameterIndices()).hasSize(3);
    }

    @Test
    @DisplayName("Should parse query with positional parameters")
    void shouldParsePositionalParameters() throws Exception {
        Method method = TestRepository.class.getMethod("findByPositionalParams", String.class, int.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(2);
    }

    @Test
    @DisplayName("Should parse query with mixed literal and parameter")
    void shouldParseMixedLiteralAndParameter() throws Exception {
        Method method = TestRepository.class.getMethod("findByNameAndActiveTrue", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(2);
    }

    @Test
    @DisplayName("Should parse UPDATE with multiple SET clauses")
    void shouldParseUpdateWithMultipleSetClauses() throws Exception {
        Method method = TestRepository.class.getMethod("updateMultipleFields", Long.class, String.class, int.class, boolean.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.UPDATE_QUERY);
        assertThat(query.updateAssignments()).hasSize(3);
    }

    @Test
    @DisplayName("Should parse UPDATE without WHERE clause")
    void shouldParseUpdateWithoutWhere() throws Exception {
        Method method = TestRepository.class.getMethod("deactivateAll");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.opCode()).isEqualTo(OpCode.UPDATE_QUERY);
        assertThat(query.conditions()).isEmpty();
    }

    @Test
    @DisplayName("Should parse query with explicit AS keyword for alias")
    void shouldParseExplicitAlias() throws Exception {
        Method method = TestRepository.class.getMethod("findWithExplicitAlias", String.class);
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse query with string literal")
    void shouldParseStringLiteral() throws Exception {
        Method method = TestRepository.class.getMethod("findWithStringLiteral");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse query with number literal")
    void shouldParseNumberLiteral() throws Exception {
        Method method = TestRepository.class.getMethod("findWithNumberLiteral");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse query with TRUE literal")
    void shouldParseTrueLiteral() throws Exception {
        Method method = TestRepository.class.getMethod("findWithTrueLiteral");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse query with FALSE literal")
    void shouldParseFalseLiteral() throws Exception {
        Method method = TestRepository.class.getMethod("findWithFalseLiteral");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse query with NULL literal")
    void shouldParseNullLiteral() throws Exception {
        Method method = TestRepository.class.getMethod("findWithNullLiteral");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.conditions()).hasSize(1);
    }

    @Test
    @DisplayName("Should parse complex ORDER BY with multiple fields")
    void shouldParseComplexOrderBy() throws Exception {
        Method method = TestRepository.class.getMethod("findWithComplexOrderBy");
        LogicalQuery query = JpqlQueryParser.parse(method, SimpleEntity.class);

        assertThat(query.orderBy()).hasSize(2);
        assertThat(query.orderBy()[0].ascending()).isFalse();
        assertThat(query.orderBy()[1].ascending()).isTrue();
    }

    @Test
    @DisplayName("Should reject BETWEEN with missing AND")
    void shouldRejectIncompleteBetween() throws Exception {
        Method method = TestRepository.class.getMethod("invalidBetween");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should reject invalid path expression")
    void shouldRejectInvalidPath() throws Exception {
        Method method = TestRepository.class.getMethod("invalidPath");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should reject incomplete WHERE clause")
    void shouldRejectIncompleteWhere() throws Exception {
        Method method = TestRepository.class.getMethod("incompleteWhere");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should reject missing entity name")
    void shouldRejectMissingEntity() throws Exception {
        Method method = TestRepository.class.getMethod("missingEntity");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should reject missing SELECT item")
    void shouldRejectMissingSelectItem() throws Exception {
        Method method = TestRepository.class.getMethod("missingSelectItem");

        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
