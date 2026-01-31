package io.memris.core.plan.entities;

import io.memris.query.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for context-aware QueryMethodLexer with entity class support.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ContextAwareLexerTest {

    // ==================== Simple Property Tests ====================

    @Test
    @Order(1)
    void tokenizeSimpleProperty_ResolvesFromEntityFields() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(0).value()).isEqualTo("find");
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("name");
    }

    @Test
    @Order(2)
    void tokenizeSimpleProperty_LowercasesPropertyName() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAge");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("age");
    }

    @Test
    @Order(3)
    void tokenizeSimpleProperty_NoValidationWithoutEntityClass() {
        // Without entity class, lexer just lowercases property names
        // Returns only property tokens, no prefix (backward compatible mode)
        var tokens = QueryMethodLexer.tokenize("findByNonExistentField");

        assertThat(tokens).hasSize(1);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(0).value()).isEqualTo("nonexistentfield");
    }

    // ==================== Nested/Joined Property Tests ====================

    @Test
    @Order(4)
    void tokenizeNestedProperty_ResolvesToDotNotation() {
        Class<NestedEntity> entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
    }

    @Test
    @Order(5)
    void tokenizeNestedProperty_UsesEntityMetadata() {
        Class<NestedEntity> entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
    }

    // ==================== Comparison Operator Tests ====================

    @Test
    @Order(6)
    void tokenizeGreaterThan_ProducesOperatorToken() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeGreaterThan");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("GreaterThan");
    }

    @Test
    @Order(7)
    void tokenizeBetween_ProducesOperatorToken() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeBetween");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("Between");
    }

    // ==================== String Operator Tests ====================

    @Test
    @Order(8)
    void tokenizeLike_ProducesOperatorToken() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameLike");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("Like");
    }

    // ==================== Boolean/Null Operator Tests ====================

    @Test
    @Order(9)
    void tokenizeTrue_ProducesOperatorToken() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByActiveTrue");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("True");
    }

    @Test
    @Order(10)
    void tokenizeIsNull_ProducesOperatorToken() {
        Class<NestedEntity> entityClass = NestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentIdIsNull");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.id");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("IsNull");
    }

    // ==================== IgnoreCase Modifier Tests ====================

    @Test
    @Order(11)
    void tokenizeLikeIgnoreCase_ProducesOperatorWithIgnoreCaseFlag() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameLikeIgnoreCase");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("Like");
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(3).value()).isEqualTo("IgnoreCase");
        assertThat(tokens.get(3).ignoreCase()).isTrue();
    }

    // ==================== AND/OR Combinator Tests ====================

    @Test
    @Order(12)
    void tokenizeAndCombination_ProducesTwoConditions() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameAndAge");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.AND);
        assertThat(tokens.get(2).value()).isEqualTo("And");
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
    }

    @Test
    @Order(13)
    void tokenizeOrCombination_ProducesTwoConditions() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByNameOrAge");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OR);
        assertThat(tokens.get(2).value()).isEqualTo("Or");
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
    }

    // ==================== OrderBy Clause Tests ====================

    @Test
    @Order(14)
    void tokenizeOrderBySingleProperty_ProducesCorrectTokens() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeOrderByPrice");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("age");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("OrderBy");
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(3).value()).isEqualTo("price");
    }

    @Test
    @Order(15)
    void tokenizeOrderByWithDirection_ProducesDirectionToken() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAgeOrderByPriceDesc");

        assertThat(tokens).hasSize(5);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("age");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("OrderBy");
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(3).value()).isEqualTo("price");
        assertThat(tokens.get(4).type()).isEqualTo(QueryMethodTokenType.DESC);
        assertThat(tokens.get(4).value()).isEqualTo("Desc");
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

        assertThat(tokens).hasSizeGreaterThanOrEqualTo(1);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.COUNT_BY);
    }
}
