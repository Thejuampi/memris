package io.memris.spring.plan.entities;

import io.memris.spring.plan.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for complex nesting scenarios in QueryMethodLexer.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ComplexNestingTest {

    // ==================== Deep Nesting Tests ====================

    @Test
    @Order(1)
    void tokenizeDeepNesting_TwoLevelPath() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
    }

    @Test
    @Order(2)
    void tokenizeDeepNesting_ThreeLevelPath() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentAddressCity");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.address.city");
    }

    @Test
    @Order(3)
    void tokenizeDeepNesting_FourLevelPath() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentAddressState");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.address.state");
    }

    @Test
    @Order(4)
    void tokenizeDeepNesting_WithOperator() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLike");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("Like");
    }

    // ==================== Self-Referential Tests ====================

    @Test
    @Order(5)
    void tokenizeSelfReferential_SingleLevel() {
        Class<SelfReferentialEntity> entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("parent.name");
    }

    @Test
    @Order(6)
    void tokenizeSelfReferential_TwoLevels() {
        Class<SelfReferentialEntity> entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentParentName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("parent.parent.name");
    }

    @Test
    @Order(7)
    void tokenizeSelfReferential_WithComparison() {
        Class<SelfReferentialEntity> entityClass = SelfReferentialEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByParentIdGreaterThan");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("parent.id");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("GreaterThan");
    }

    // ==================== Embedded Value Object Tests ====================

    @Test
    @Order(8)
    void tokenizeEmbeddedProperty_SimpleAccess() {
        Class<EmbeddedEntity> entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileFirstName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("profile.firstname");
    }

    @Test
    @Order(9)
    void tokenizeEmbeddedProperty_MultipleFields() {
        Class<EmbeddedEntity> entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileLastName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("profile.lastname");
    }

    @Test
    @Order(10)
    void tokenizeEmbeddedProperty_WithOperator() {
        Class<EmbeddedEntity> entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileEmailLike");

        assertThat(tokens).hasSize(3);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("profile.email");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OPERATOR);
        assertThat(tokens.get(2).value()).isEqualTo("Like");
    }

    // ==================== Complex Combination Tests ====================

    @Test
    @Order(11)
    void tokenizeComplex_DeepNestingAndOr() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrAccountEmail");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).type()).isEqualTo(QueryMethodTokenType.OR);
        assertThat(tokens.get(3).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(3).value()).isEqualTo("account.email");
    }

    @Test
    @Order(12)
    void tokenizeComplex_DeepNestingWithOperators() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLikeAndAccountEmailLike");

        assertThat(tokens).hasSize(6);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).value()).isEqualTo("Like");
        assertThat(tokens.get(3).value()).isEqualTo("And");
        assertThat(tokens.get(4).value()).isEqualTo("account.email");
        assertThat(tokens.get(5).value()).isEqualTo("Like");
    }

    @Test
    @Order(13)
    void tokenizeComplex_DeepNestingOrderBy() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrderByAccountEmailDesc");

        assertThat(tokens).hasSize(5);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).value()).isEqualTo("OrderBy");
        assertThat(tokens.get(3).value()).isEqualTo("account.email");
        assertThat(tokens.get(4).type()).isEqualTo(QueryMethodTokenType.DESC);
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
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByAge2");

        // Numbers after capital letters are part of the property name
        // "Age2" is treated as a single property "age2"
        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("age2");
    }

    // ==================== Edge Case Tests ====================

    @Test
    @Order(16)
    void tokenizeWithAllPrefix_NoPredicate() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findAll");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.FIND_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.FIND_ALL);
    }

    @Test
    @Order(17)
    void tokenizeWithCountPrefix() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "countByName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.COUNT);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("name");
    }

    @Test
    @Order(18)
    void tokenizeWithExistsPrefix() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "existsByName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.EXISTS_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("name");
    }

    @Test
    @Order(19)
    void tokenizeWithDeletePrefix() {
        Class<SimpleEntity> entityClass = SimpleEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "deleteByName");

        assertThat(tokens).hasSize(2);
        assertThat(tokens.get(0).type()).isEqualTo(QueryMethodTokenType.DELETE_BY);
        assertThat(tokens.get(1).type()).isEqualTo(QueryMethodTokenType.PROPERTY_PATH);
        assertThat(tokens.get(1).value()).isEqualTo("name");
    }

    // ==================== IgnoreCase with Deep Nesting ====================

    @Test
    @Order(20)
    void tokenizeIgnoreCase_DeepNesting() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameLikeIgnoreCase");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).value()).isEqualTo("Like");
        assertThat(tokens.get(3).value()).isEqualTo("IgnoreCase");
        assertThat(tokens.get(3).ignoreCase()).isTrue();
    }

    @Test
    @Order(21)
    void tokenizeIgnoreCase_EmbeddedProperty() {
        Class<EmbeddedEntity> entityClass = EmbeddedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByProfileEmailLikeIgnoreCase");

        assertThat(tokens).hasSize(4);
        assertThat(tokens.get(1).value()).isEqualTo("profile.email");
        assertThat(tokens.get(2).value()).isEqualTo("Like");
        assertThat(tokens.get(3).ignoreCase()).isTrue();
    }

    // ==================== OrderBy with Complex Nesting ====================

    @Test
    @Order(22)
    void tokenizeOrderBy_DeepNestingMultiProperty() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrderByDepartmentAddressCityAsc");

        assertThat(tokens).hasSize(5);
        assertThat(tokens.get(1).value()).isEqualTo("department.name");
        assertThat(tokens.get(2).value()).isEqualTo("OrderBy");
        assertThat(tokens.get(3).value()).isEqualTo("department.address.city");
        assertThat(tokens.get(4).type()).isEqualTo(QueryMethodTokenType.ASC);
    }

    @Test
    @Order(23)
    void tokenizeOrderBy_MultipleDeepNesting() {
        Class<DeepNestedEntity> entityClass = DeepNestedEntity.class;
        var tokens = QueryMethodLexer.tokenize(entityClass, "findByDepartmentNameOrderByDepartmentAddressCityAscAndAccountEmailDesc");

        assertThat(tokens).hasSize(8);
        assertThat(tokens.get(3).value()).isEqualTo("department.address.city");
        assertThat(tokens.get(4).type()).isEqualTo(QueryMethodTokenType.ASC);
        assertThat(tokens.get(5).value()).isEqualTo("And");
        assertThat(tokens.get(6).value()).isEqualTo("account.email");
        assertThat(tokens.get(7).type()).isEqualTo(QueryMethodTokenType.DESC);
    }
}
