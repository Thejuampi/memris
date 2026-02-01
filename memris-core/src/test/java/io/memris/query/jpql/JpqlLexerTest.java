package io.memris.query.jpql;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for JPQL Lexer covering all token types and edge cases.
 */
class JpqlLexerTest {

    @Test
    @DisplayName("Should tokenize simple SELECT query")
    void shouldTokenizeSimpleSelectQuery() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize("SELECT e FROM Employee e");

        assertThat(tokens).hasSize(6); // SELECT, e, FROM, Employee, e, EOF
        assertThat(tokens.get(0).type()).isEqualTo(JpqlLexer.TokenType.SELECT);
        assertThat(tokens.get(1).type()).isEqualTo(JpqlLexer.TokenType.IDENT);
        assertThat(tokens.get(1).text()).isEqualTo("e");
        assertThat(tokens.get(2).type()).isEqualTo(JpqlLexer.TokenType.FROM);
        assertThat(tokens.get(3).type()).isEqualTo(JpqlLexer.TokenType.IDENT);
        assertThat(tokens.get(3).text()).isEqualTo("Employee");
        assertThat(tokens.get(4).type()).isEqualTo(JpqlLexer.TokenType.IDENT);
        assertThat(tokens.get(4).text()).isEqualTo("e");
        assertThat(tokens.get(5).type()).isEqualTo(JpqlLexer.TokenType.EOF);
    }

    @Test
    @DisplayName("Should tokenize SELECT with WHERE clause")
    void shouldTokenizeSelectWithWhereClause() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(
                "SELECT e FROM Employee e WHERE e.age > 25");

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).containsExactly(
                JpqlLexer.TokenType.SELECT,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.FROM,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.WHERE,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.DOT,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.GT,
                JpqlLexer.TokenType.NUMBER,
                JpqlLexer.TokenType.EOF
        );

        // Check the number token has correct value
        JpqlLexer.Token numberToken = tokens.get(tokens.size() - 2);
        assertThat(numberToken.literal()).isEqualTo(25L);
    }

    @Test
    @DisplayName("Should tokenize all comparison operators")
    void shouldTokenizeAllComparisonOperators() {
        String query = "WHERE a = b AND c != d AND e <> f AND g < h AND i <= j AND k > l AND m >= n";
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens).extracting(JpqlLexer.Token::type).contains(
                JpqlLexer.TokenType.EQ,
                JpqlLexer.TokenType.NE,
                JpqlLexer.TokenType.NE,  // <> is also NE
                JpqlLexer.TokenType.LT,
                JpqlLexer.TokenType.LTE,
                JpqlLexer.TokenType.GT,
                JpqlLexer.TokenType.GTE
        );
    }

    @Test
    @DisplayName("Should tokenize string literals")
    void shouldTokenizeStringLiterals() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(
                "SELECT e FROM Employee e WHERE e.name = 'John Doe'");

        JpqlLexer.Token stringToken = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.STRING)
                .findFirst()
                .orElseThrow();

        assertThat(stringToken.text()).isEqualTo("John Doe");
        assertThat(stringToken.literal()).isEqualTo("John Doe");
    }

    @Test
    @DisplayName("Should tokenize string with escaped quotes")
    void shouldTokenizeStringWithEscapedQuotes() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(
                "WHERE e.name = 'O''Brien'");

        JpqlLexer.Token stringToken = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.STRING)
                .findFirst()
                .orElseThrow();

        assertThat(stringToken.literal()).isEqualTo("O'Brien");
    }

    @Test
    @DisplayName("Should tokenize integer numbers")
    void shouldTokenizeIntegerNumbers() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize("WHERE e.age = 42");

        JpqlLexer.Token numberToken = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.NUMBER)
                .findFirst()
                .orElseThrow();

        assertThat(numberToken.literal()).isEqualTo(42L);
    }

    @Test
    @DisplayName("Should tokenize decimal numbers")
    void shouldTokenizeDecimalNumbers() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize("WHERE e.salary = 50000.50");

        JpqlLexer.Token numberToken = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.NUMBER)
                .findFirst()
                .orElseThrow();

        assertThat(numberToken.literal()).isInstanceOf(BigDecimal.class);
        assertThat(numberToken.literal()).isEqualTo(new BigDecimal("50000.50"));
    }

    @Test
    @DisplayName("Should tokenize named parameters")
    void shouldTokenizeNamedParameters() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(
                "SELECT e FROM Employee e WHERE e.name = :name AND e.age > :minAge");

        List<JpqlLexer.Token> paramTokens = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.PARAM_NAMED)
                .toList();

        assertThat(paramTokens).hasSize(2);
        assertThat(paramTokens.get(0).text()).isEqualTo("name");
        assertThat(paramTokens.get(0).literal()).isEqualTo("name");
        assertThat(paramTokens.get(1).text()).isEqualTo("minAge");
    }

    @Test
    @DisplayName("Should tokenize positional parameters")
    void shouldTokenizePositionalParameters() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(
                "WHERE e.name = ?1 AND e.age > ?2");

        List<JpqlLexer.Token> paramTokens = tokens.stream()
                .filter(t -> t.type() == JpqlLexer.TokenType.PARAM_POSITIONAL)
                .toList();

        assertThat(paramTokens).hasSize(2);
        assertThat(paramTokens.get(0).text()).isEqualTo("?1");
        assertThat(paramTokens.get(0).literal()).isEqualTo(1);
        assertThat(paramTokens.get(1).literal()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should tokenize all keywords case-insensitively")
    void shouldTokenizeKeywordsCaseInsensitively() {
        String query = "select e from Employee e where e.active = true " +
                      "and e.name in ('A', 'B') " +
                      "order by e.name asc";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.SELECT,
                JpqlLexer.TokenType.FROM,
                JpqlLexer.TokenType.WHERE,
                JpqlLexer.TokenType.AND,
                JpqlLexer.TokenType.IN,
                JpqlLexer.TokenType.ORDER,
                JpqlLexer.TokenType.BY,
                JpqlLexer.TokenType.ASC
        );
    }

    @Test
    @DisplayName("Should tokenize JOIN clauses")
    void shouldTokenizeJoinClauses() {
        String query = "SELECT e FROM Employee e " +
                      "JOIN e.department d " +
                      "LEFT JOIN e.manager m";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.JOIN,
                JpqlLexer.TokenType.LEFT,
                JpqlLexer.TokenType.JOIN
        );
    }

    @Test
    @DisplayName("Should tokenize GROUP BY and HAVING")
    void shouldTokenizeGroupByAndHaving() {
        String query = "SELECT COUNT(e) FROM Employee e GROUP BY e.department HAVING COUNT(e) > 1";
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.GROUP,
                JpqlLexer.TokenType.BY,
                JpqlLexer.TokenType.HAVING,
                JpqlLexer.TokenType.COUNT
        );
    }

    @Test
    @DisplayName("Should tokenize COUNT and DISTINCT")
    void shouldTokenizeCountAndDistinct() {
        String query = "SELECT COUNT(DISTINCT e.department) FROM Employee e";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.COUNT,
                JpqlLexer.TokenType.DISTINCT
        );
    }

    @Test
    @DisplayName("Should tokenize BETWEEN predicate")
    void shouldTokenizeBetweenPredicate() {
        String query = "WHERE e.age BETWEEN 18 AND 65";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.BETWEEN,
                JpqlLexer.TokenType.AND
        );
    }

    @Test
    @DisplayName("Should tokenize IS NULL and IS NOT NULL")
    void shouldTokenizeIsNull() {
        String query1 = "WHERE e.name IS NULL";
        String query2 = "WHERE e.name IS NOT NULL";

        List<JpqlLexer.Token> tokens1 = JpqlLexer.tokenize(query1);
        List<JpqlLexer.Token> tokens2 = JpqlLexer.tokenize(query2);

        assertThat(tokens1.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.IS,
                JpqlLexer.TokenType.NULL
        );

        assertThat(tokens2.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.IS,
                JpqlLexer.TokenType.NOT,
                JpqlLexer.TokenType.NULL
        );
    }

    @Test
    @DisplayName("Should tokenize LIKE and ILIKE operators")
    void shouldTokenizeLikeOperators() {
        String query = "WHERE e.name LIKE 'John%' AND e.code ILIKE 'ABC%'";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.LIKE,
                JpqlLexer.TokenType.ILIKE
        );
    }

    @Test
    @DisplayName("Should tokenize UPDATE statement")
    void shouldTokenizeUpdateStatement() {
        String query = "UPDATE Employee e SET e.salary = 50000 WHERE e.id = 1";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.get(0).type()).isEqualTo(JpqlLexer.TokenType.UPDATE);
        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.SET,
                JpqlLexer.TokenType.WHERE
        );
    }

    @Test
    @DisplayName("Should tokenize DELETE statement")
    void shouldTokenizeDeleteStatement() {
        String query1 = "DELETE FROM Employee e WHERE e.active = false";
        String query2 = "DELETE Employee e WHERE e.active = false";

        List<JpqlLexer.Token> tokens1 = JpqlLexer.tokenize(query1);
        List<JpqlLexer.Token> tokens2 = JpqlLexer.tokenize(query2);

        assertThat(tokens1.get(0).type()).isEqualTo(JpqlLexer.TokenType.DELETE);
        assertThat(tokens2.get(0).type()).isEqualTo(JpqlLexer.TokenType.DELETE);
    }

    @Test
    @DisplayName("Should tokenize AS alias")
    void shouldTokenizeAsAlias() {
        String query = "SELECT e.name AS employeeName FROM Employee AS e";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.AS
        );
    }

    @Test
    @DisplayName("Should tokenize ORDER BY with multiple columns")
    void shouldTokenizeOrderByWithMultipleColumns() {
        String query = "ORDER BY e.lastName ASC, e.firstName DESC";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).containsExactly(
                JpqlLexer.TokenType.ORDER,
                JpqlLexer.TokenType.BY,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.DOT,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.ASC,
                JpqlLexer.TokenType.COMMA,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.DOT,
                JpqlLexer.TokenType.IDENT,
                JpqlLexer.TokenType.DESC,
                JpqlLexer.TokenType.EOF
        );
    }

    @Test
    @DisplayName("Should tokenize FETCH JOIN")
    void shouldTokenizeFetchJoin() {
        String query = "SELECT e FROM Employee e JOIN FETCH e.department";

        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.stream().map(JpqlLexer.Token::type)).contains(
                JpqlLexer.TokenType.JOIN,
                JpqlLexer.TokenType.FETCH
        );
    }

    @Test
    @DisplayName("Should reject empty named parameter")
    void shouldRejectEmptyNamedParameter() {
        assertThatThrownBy(() -> JpqlLexer.tokenize("WHERE e.name = :"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected parameter name after ':'");
    }

    @Test
    @DisplayName("Should reject empty positional parameter")
    void shouldRejectEmptyPositionalParameter() {
        assertThatThrownBy(() -> JpqlLexer.tokenize("WHERE e.name = ?"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected positional parameter after '?'");
    }

    @Test
    @DisplayName("Should reject unexpected exclamation mark")
    void shouldRejectUnexpectedExclamationMark() {
        assertThatThrownBy(() -> JpqlLexer.tokenize("WHERE e.active !"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected '!'");
    }

    @Test
    @DisplayName("Should reject unexpected character")
    void shouldRejectUnexpectedCharacter() {
        assertThatThrownBy(() -> JpqlLexer.tokenize("WHERE e.name = @"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected character '@'");
    }

    @Test
    @DisplayName("Should handle identifiers with underscores")
    void shouldHandleIdentifiersWithUnderscores() {
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize("SELECT first_name FROM user_table");

        assertThat(tokens.get(1).type()).isEqualTo(JpqlLexer.TokenType.IDENT);
        assertThat(tokens.get(1).text()).isEqualTo("first_name");
        assertThat(tokens.get(3).type()).isEqualTo(JpqlLexer.TokenType.IDENT);
        assertThat(tokens.get(3).text()).isEqualTo("user_table");
    }

    @Test
    @DisplayName("Should track token positions")
    void shouldTrackTokenPositions() {
        String query = "SELECT x FROM y";
        List<JpqlLexer.Token> tokens = JpqlLexer.tokenize(query);

        assertThat(tokens.get(0).position()).isEqualTo(0);   // SELECT
        assertThat(tokens.get(1).position()).isEqualTo(7);   // x
        assertThat(tokens.get(2).position()).isEqualTo(9);   // FROM
        assertThat(tokens.get(3).position()).isEqualTo(14);  // y
    }
}
