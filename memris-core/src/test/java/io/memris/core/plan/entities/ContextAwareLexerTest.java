package io.memris.core.plan.entities;

import io.memris.query.*;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for context-aware QueryMethodLexer with entity class support.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ContextAwareLexerTest {

    private record TokenView(QueryMethodTokenType type, String value, boolean ignoreCase) {
    }

    private static List<TokenView> snapshot(List<QueryMethodToken> tokens) {
        return tokens.stream()
                .map(token -> new TokenView(token.type(), token.value(), token.ignoreCase()))
                .toList();
    }

    // ==================== Simple Property Tests ====================

    @Test
    @Order(1)
    void tokenizeSimpleProperty_ResolvesFromEntityFields() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "name", false)
        );
    }

    @Test
    @Order(2)
    void tokenizeSimpleProperty_LowercasesPropertyName() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAge");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false)
        );
    }

    @Test
    @Order(3)
    void tokenizeSimpleProperty_NoValidationWithoutEntityClass() {
        // Without entity class, lexer just lowercases property names
        // Returns only property tokens, no prefix (backward compatible mode)
        var tokens = QueryMethodLexer.tokenize("findByNonExistentField");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "nonexistentfield", false)
        );
    }

    // ==================== Nested/Joined Property Tests ====================

    @Test
    @Order(4)
    void tokenizeNestedProperty_ResolvesToDotNotation() {
        var entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false)
        );
    }

    @Test
    @Order(5)
    void tokenizeNestedProperty_UsesEntityMetadata() {
        var entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false)
        );
    }

    // ==================== Comparison Operator Tests ====================

    @Test
    @Order(6)
    void tokenizeGreaterThan_ProducesOperatorToken() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeGreaterThan");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "GreaterThan", false)
        );
    }

    @Test
    @Order(7)
    void tokenizeBetween_ProducesOperatorToken() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeBetween");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Between", false)
        );
    }

    // ==================== String Operator Tests ====================

    @Test
    @Order(8)
    void tokenizeLike_ProducesOperatorToken() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameLike");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false)
        );
    }

    // ==================== Boolean/Null Operator Tests ====================

    @Test
    @Order(9)
    void tokenizeTrue_ProducesOperatorToken() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByActiveTrue");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "active", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "True", false)
        );
    }

    @Test
    @Order(10)
    void tokenizeIsNull_ProducesOperatorToken() {
        var entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentIdIsNull");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.id", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "IsNull", false)
        );
    }

    // ==================== IgnoreCase Modifier Tests ====================

    @Test
    @Order(11)
    void tokenizeLikeIgnoreCase_ProducesOperatorWithIgnoreCaseFlag() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameLikeIgnoreCase");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "IgnoreCase", true)
        );
    }

    // ==================== AND/OR Combinator Tests ====================

    @Test
    @Order(12)
    void tokenizeAndCombination_ProducesTwoConditions() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameAndAge");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "name", false),
                new TokenView(QueryMethodTokenType.AND, "And", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false)
        );
    }

    @Test
    @Order(13)
    void tokenizeOrCombination_ProducesTwoConditions() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameOrAge");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "name", false),
                new TokenView(QueryMethodTokenType.OR, "Or", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false)
        );
    }

    // ==================== OrderBy Clause Tests ====================

    @Test
    @Order(14)
    void tokenizeOrderBySingleProperty_ProducesCorrectTokens() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeOrderByPrice");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "OrderBy", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "price", false)
        );
    }

    @Test
    @Order(15)
    void tokenizeOrderByWithDirection_ProducesDirectionToken() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeOrderByPriceDesc");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "OrderBy", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "price", false),
                new TokenView(QueryMethodTokenType.DESC, "Desc", false)
        );
    }

    // ==================== CRUD Operation Tests ====================
    // NOTE: Built-in operations are now recognized by QueryPlanner using MethodKey,
    // not by the lexer. The lexer correctly falls back to derived query parsing
    // for these method names since it only has the method name (not the full signature).

    @Test
    @Order(16)
    void tokenizeSave_FallsBackToDerivedParsing() {
        // Lexer only has method name, not full signature, so "save" goes through
        // derived query parsing. The planner will detect it as a built-in via MethodKey.
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "save");

        // The lexer attempts to parse it as a derived query
        // (planner will detect the built-in via MethodKey matching)
        assertThat(tokens).isNotEmpty();
    }

    @Test
    @Order(17)
    void tokenizeSaveAll_FallsBackToDerivedParsing() {
        // Same as save() - lexer doesn't have signature, uses derived parsing
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "saveAll");

        assertThat(tokens).isNotEmpty();
    }

    @Test
    @Order(18)
    void tokenizeDelete_FallsBackToDerivedParsing() {
        // Same as save() - lexer doesn't have signature, uses derived parsing
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "delete");

        assertThat(tokens).isNotEmpty();
    }

    @Test
    @Order(19)
    void tokenizeDeleteAll_FallsBackToDerivedParsing() {
        // deleteAll is now recognized as a built-in operation by the planner
        // The lexer falls back to derived parsing
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "deleteAll");

        assertThat(tokens).isNotEmpty();
    }

    @Test
    @Order(20)
    void tokenizeCount_FallsBackToDerivedParsing() {
        // count() is a built-in, detected by planner via MethodKey
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "count");

        assertThat(tokens).isNotEmpty();
    }

    @Test
    @Order(21)
    void tokenizeCountAll_GoesThroughDerivedParsing() {
        // "countAll" is not a standard Spring Data method
        // It goes through derived query parsing (not a built-in)
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "countAll");

        assertThat(snapshot(tokens).getFirst().type()).isEqualTo(QueryMethodTokenType.COUNT_BY);
    }
}
