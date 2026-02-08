package io.memris.core.plan.entities;

import io.memris.query.*;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for complex nesting scenarios in QueryMethodLexer.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ComplexNestingTest {

    private record TokenView(QueryMethodTokenType type, String value, boolean ignoreCase) {
    }

    private static List<TokenView> snapshot(List<QueryMethodToken> tokens) {
        return tokens.stream()
                .map(token -> new TokenView(token.type(), token.value(), token.ignoreCase()))
                .toList();
    }

    // ==================== Deep Nesting Tests ====================

    @Test
    @Order(1)
    void tokenizeDeepNesting_TwoLevelPath() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false)
        );
    }

    @Test
    @Order(2)
    void tokenizeDeepNesting_ThreeLevelPath() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentAddressCity");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.address.city", false)
        );
    }

    @Test
    @Order(3)
    void tokenizeDeepNesting_FourLevelPath() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentAddressState");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.address.state", false)
        );
    }

    @Test
    @Order(4)
    void tokenizeDeepNesting_WithOperator() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLike");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false)
        );
    }

    // ==================== Self-Referential Tests ====================

    @Test
    @Order(5)
    void tokenizeSelfReferential_SingleLevel() {
        var entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "parent.name", false)
        );
    }

    @Test
    @Order(6)
    void tokenizeSelfReferential_TwoLevels() {
        var entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentParentName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "parent.parent.name", false)
        );
    }

    @Test
    @Order(7)
    void tokenizeSelfReferential_WithComparison() {
        var entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentIdGreaterThan");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "parent.id", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "GreaterThan", false)
        );
    }

    // ==================== Embedded Value Object Tests ====================

    @Test
    @Order(8)
    void tokenizeEmbeddedProperty_SimpleAccess() {
        var entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileFirstName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "profile.firstName", false)
        );
    }

    @Test
    @Order(9)
    void tokenizeEmbeddedProperty_MultipleFields() {
        var entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileLastName");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "profile.lastName", false)
        );
    }

    @Test
    @Order(10)
    void tokenizeEmbeddedProperty_WithOperator() {
        var entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileEmailLike");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "profile.email", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false)
        );
    }

    // ==================== Complex Combination Tests ====================

    @Test
    @Order(11)
    void tokenizeComplex_DeepNestingAndOr() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrAccountEmail");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OR, "Or", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "account.email", false)
        );
    }

    @Test
    @Order(12)
    void tokenizeComplex_DeepNestingWithOperators() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLikeAndAccountEmailLike");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false),
                new TokenView(QueryMethodTokenType.AND, "And", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "account.email", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false)
        );
    }

    @Test
    @Order(13)
    void tokenizeComplex_DeepNestingOrderBy() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrderByAccountEmailDesc");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "OrderBy", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "account.email", false),
                new TokenView(QueryMethodTokenType.DESC, "Desc", false)
        );
    }

    // ==================== Special Characters and Case Tests ====================

    @Test
    @Order(14)
    void tokenizePropertyWithUnderscore_UnderscoreNotSupported() {
        // Underscores are not supported in property names
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findBySome_Field");

        // Lexer should treat underscore as a word boundary
        assertThat(tokens.get(1).value()).isEqualTo("some.field");
    }

    @Test
    @Order(15)
    void tokenizePropertyWithNumbers_NumberCreatesSegment() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAge2");

        // Numbers after capital letters are part of the property name
        // "Age2" is treated as a single property "age2"
        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "age2", false)
        );
    }

    // ==================== Edge Case Tests ====================

    @Test
    @Order(16)
    void tokenizeWithAllPrefix_NoPredicate() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findAll");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.OPERATION, OpCode.FIND_ALL.name(), false)
        );
    }

    @Test
    @Order(17)
    void tokenizeWithCountPrefix() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "countByName");

        assertThat(List.of(tokens.get(0).type(), tokens.get(1).type(), tokens.get(1).value()))
                .containsExactly(QueryMethodTokenType.COUNT_BY, QueryMethodTokenType.PROPERTY_PATH, "name");
    }

    @Test
    @Order(18)
    void tokenizeWithExistsPrefix() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "existsByName");

        assertThat(List.of(tokens.get(0).type(), tokens.get(1).type(), tokens.get(1).value()))
                .containsExactly(QueryMethodTokenType.EXISTS_BY, QueryMethodTokenType.PROPERTY_PATH, "name");
    }

    @Test
    @Order(19)
    void tokenizeWithDeletePrefix() {
        var entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "deleteByName");

        assertThat(List.of(tokens.get(0).type(), tokens.get(1).type(), tokens.get(1).value()))
                .containsExactly(QueryMethodTokenType.DELETE_BY, QueryMethodTokenType.PROPERTY_PATH, "name");
    }

    // ==================== IgnoreCase with Deep Nesting ====================

    @Test
    @Order(20)
    void tokenizeIgnoreCase_DeepNesting() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLikeIgnoreCase");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "IgnoreCase", true)
        );
    }

    @Test
    @Order(21)
    void tokenizeIgnoreCase_EmbeddedProperty() {
        var entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileEmailLikeIgnoreCase");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "profile.email", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "Like", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "IgnoreCase", true)
        );
    }

    // ==================== OrderBy with Complex Nesting ====================

    @Test
    @Order(22)
    void tokenizeOrderBy_DeepNestingMultiProperty() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrderByDepartmentAddressCityAsc");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "OrderBy", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.address.city", false),
                new TokenView(QueryMethodTokenType.ASC, "Asc", false)
        );
    }

    @Test
    @Order(23)
    void tokenizeOrderBy_MultipleDeepNesting() {
        var entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass,
                "findByDepartmentNameOrderByDepartmentAddressCityAscAndAccountEmailDesc");

        assertThat(snapshot(tokens)).containsExactly(
                new TokenView(QueryMethodTokenType.FIND_BY, "find", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.name", false),
                new TokenView(QueryMethodTokenType.OPERATOR, "OrderBy", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "department.address.city", false),
                new TokenView(QueryMethodTokenType.ASC, "Asc", false),
                new TokenView(QueryMethodTokenType.AND, "And", false),
                new TokenView(QueryMethodTokenType.PROPERTY_PATH, "account.email", false),
                new TokenView(QueryMethodTokenType.DESC, "Desc", false)
        );
    }
}
