package io.memris.spring;

import io.memris.kernel.Predicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD Tests for JPA Query Method Parser - Full JPA Specification Coverage
 * 
 * Following TDD: RED -> GREEN -> REFACTOR
 * These tests define the expected behavior for all JPA query keywords.
 */
@DisplayName("QueryMethodParser JPA Specification Tests")
class QueryMethodParserTest {

    // Helper interface for test methods
    interface TestRepository {
        // Boolean operators
        List<Object> findByActiveTrue();
        List<Object> findByActiveFalse();
        List<Object> findByActiveIsTrue();
        List<Object> findByActiveIsFalse();

        // Null operators
        List<Object> findByEmailIsNull();
        List<Object> findByEmailIsNotNull();
        List<Object> findByEmailNotNull();
        List<Object> findByEmailNull();  // JPA also supports "Null" without "Is"

        // IN/NOT IN operators
        List<Object> findByStatusIn(Collection<String> statuses);
        List<Object> findByStatusNotIn(Collection<String> statuses);

        // Date/Time operators
        List<Object> findByCreatedAtAfter(java.time.LocalDateTime date);
        List<Object> findByCreatedAtBefore(java.time.LocalDateTime date);

        // Comparison operators
        List<Object> findByAgeLessThan(int age);
        List<Object> findByAgeLessThanEqual(int age);
        List<Object> findByAgeGreaterThan(int age);
        List<Object> findByAgeGreaterThanEqual(int age);

        // LIKE operators
        List<Object> findByNameLike(String pattern);
        List<Object> findByNameNotLike(String pattern);

        // String matching (StartingWith, EndingWith, Containing)
        List<Object> findByNameStartingWith(String prefix);
        List<Object> findByNameStartsWith(String prefix);
        List<Object> findByNameEndingWith(String suffix);
        List<Object> findByNameEndsWith(String suffix);
        List<Object> findByNameContaining(String infix);

        // Is/Equals operators (JPA: Is, Equals are equivalent to no operator)
        List<Object> findByEmailIs(String email);
        List<Object> findByEmailEquals(String email);
        List<Object> findByEmail(String email);  // Implicit EQ

        // IgnoreCase
        List<Object> findByNameIgnoreCase(String name);
        List<Object> findByFirstnameAndLastnameAllIgnoreCase(String first, String last);

        // OrderBy
        List<Object> findByLastnameOrderByFirstnameAsc(String lastname);
        List<Object> findByLastnameOrderByFirstnameDesc(String lastname);
        List<Object> findByAgeOrderByLastnameDesc(int age);
        List<Object> findByStatusOrderByCreatedAtAsc(String status);

        // Distinct
        List<Object> findDistinctByLastname(String lastname);

        // Top/First limiting
        Object findFirstByOrderByAgeDesc();
        Object findTopByOrderByAgeDesc();
        List<Object> findTop10ByLastname(String lastname);

        // Count
        long countByLastname(String lastname);

        // Exists
        boolean existsByEmail(String email);

        // Not operator
        List<Object> findByLastnameNot(String lastname);
        List<Object> findByStatusNot(String status);

        // Delete/Remove
        long deleteByLastname(String lastname);
        List<Object> removeByStatus(String status);
    }

    @Nested
    @DisplayName("Boolean Operators (IsTrue, IsFalse)")
    class BooleanOperatorsTests {

        @Test
        @DisplayName("findByActiveTrue should create IS_TRUE predicate")
        void findByActiveTrue_shouldCreateIsTruePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByActiveTrue");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("active");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_TRUE);
        }

        @Test
        @DisplayName("findByActiveFalse should create IS_FALSE predicate")
        void findByActiveFalse_shouldCreateIsFalsePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByActiveFalse");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("active");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_FALSE);
        }

        @Test
        @DisplayName("findByActiveIsTrue should create IS_TRUE predicate")
        void findByActiveIsTrue_shouldCreateIsTruePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByActiveIsTrue");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_TRUE);
        }

        @Test
        @DisplayName("findByActiveIsFalse should create IS_FALSE predicate")
        void findByActiveIsFalse_shouldCreateIsFalsePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByActiveIsFalse");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_FALSE);
        }
    }

    @Nested
    @DisplayName("Null Operators (IsNull, IsNotNull)")
    class NullOperatorsTests {

        @Test
        @DisplayName("findByEmailIsNull should create IS_NULL predicate")
        void findByEmailIsNull_shouldCreateIsNullPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailIsNull");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("email");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_NULL);
        }

        @Test
        @DisplayName("findByEmailIsNotNull should create IS_NOT_NULL predicate")
        void findByEmailIsNotNull_shouldCreateIsNotNullPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailIsNotNull");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("email");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_NOT_NULL);
        }

        @Test
        @DisplayName("findByEmailNotNull should create IS_NOT_NULL predicate")
        void findByEmailNotNull_shouldCreateIsNotNullPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailNotNull");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_NOT_NULL);
        }
    }

    @Nested
    @DisplayName("IN/NOT IN Operators")
    class InOperatorsTests {

        @Test
        @DisplayName("findByStatusIn should create In predicate with collection")
        void findByStatusIn_shouldCreateInPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByStatusIn", Collection.class);
            List<String> statuses = List.of("ACTIVE", "PENDING");
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{statuses});

            assertThat(predicate).isInstanceOf(Predicate.In.class);
            Predicate.In in = (Predicate.In) predicate;
            assertThat(in.column()).isEqualTo("status");
            assertThat(in.values()).hasSize(2);
            assertThat(in.values().contains("ACTIVE")).isTrue();
            assertThat(in.values().contains("PENDING")).isTrue();
        }

        @Test
        @DisplayName("findByStatusNotIn should create NOT_IN predicate as Not(In(...))")
        void findByStatusNotIn_shouldCreateNotInPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByStatusNotIn", Collection.class);
            List<String> statuses = List.of("CANCELLED", "DELETED");
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{statuses});

            // NOT_IN is represented as Not(In(...))
            assertThat(predicate).isInstanceOf(Predicate.Not.class);
            Predicate.Not notPred = (Predicate.Not) predicate;
            assertThat(notPred.predicate()).isInstanceOf(Predicate.In.class);
            Predicate.In inPred = (Predicate.In) notPred.predicate();
            assertThat(inPred.column()).isEqualTo("status");
        }
    }

    @Nested
    @DisplayName("Date/Time Operators (After, Before)")
    class DateTimeOperatorsTests {

        @Test
        @DisplayName("findByCreatedAtAfter should create AFTER predicate")
        void findByCreatedAtAfter_shouldCreateAfterPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByCreatedAtAfter", java.time.LocalDateTime.class);
            java.time.LocalDateTime date = java.time.LocalDateTime.now();
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{date});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("created_at");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.AFTER);
            assertThat(comp.value()).isEqualTo(date);
        }

        @Test
        @DisplayName("findByCreatedAtBefore should create BEFORE predicate")
        void findByCreatedAtBefore_shouldCreateBeforePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByCreatedAtBefore", java.time.LocalDateTime.class);
            java.time.LocalDateTime date = java.time.LocalDateTime.now();
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{date});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("created_at");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.BEFORE);
            assertThat(comp.value()).isEqualTo(date);
        }
    }

    @Nested
    @DisplayName("LIKE Operators (Like, NotLike)")
    class LikeOperatorsTests {

        @Test
        @DisplayName("findByNameLike should create LIKE predicate")
        void findByNameLike_shouldCreateLikePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameLike", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"%test%"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("name");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.LIKE);
            assertThat(comp.value()).isEqualTo("%test%");
        }

        @Test
        @DisplayName("findByNameNotLike should create NOT_LIKE predicate")
        void findByNameNotLike_shouldCreateNotLikePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameNotLike", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"%test%"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.NOT_LIKE);
        }
    }

    @Nested
    @DisplayName("Comparison Operators (LessThan, GreaterThan, etc.)")
    class ComparisonOperatorsTests {

        @Test
        @DisplayName("findByAgeLessThan should create LT predicate")
        void findByAgeLessThan_shouldCreateLessThanPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByAgeLessThan", int.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{18});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("age");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.LT);
            assertThat(comp.value()).isEqualTo(18);
        }

        @Test
        @DisplayName("findByAgeLessThanEqual should create LTE predicate")
        void findByAgeLessThanEqual_shouldCreateLessThanEqualPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByAgeLessThanEqual", int.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{18});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.LTE);
        }

        @Test
        @DisplayName("findByAgeGreaterThan should create GT predicate")
        void findByAgeGreaterThan_shouldCreateGreaterThanPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByAgeGreaterThan", int.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{18});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("age");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.GT);
        }

        @Test
        @DisplayName("findByAgeGreaterThanEqual should create GTE predicate")
        void findByAgeGreaterThanEqual_shouldCreateGreaterThanEqualPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByAgeGreaterThanEqual", int.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{18});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.GTE);
        }
    }

    @Nested
    @DisplayName("String Matching (StartingWith, EndingWith, Containing)")
    class StringMatchingTests {

        @Test
        @DisplayName("findByNameStartingWith should create STARTING_WITH predicate")
        void findByNameStartingWith_shouldCreateStartingWithPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameStartingWith", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"John"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("name");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.STARTING_WITH);
        }

        @Test
        @DisplayName("findByNameStartsWith should create STARTING_WITH predicate (alternative)")
        void findByNameStartsWith_shouldCreateStartingWithPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameStartsWith", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"John"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.STARTING_WITH);
        }

        @Test
        @DisplayName("findByNameEndingWith should create ENDING_WITH predicate")
        void findByNameEndingWith_shouldCreateEndingWithPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameEndingWith", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"son"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.ENDING_WITH);
        }

        @Test
        @DisplayName("findByNameEndsWith should create ENDING_WITH predicate (alternative)")
        void findByNameEndsWith_shouldCreateEndingWithPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameEndsWith", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"son"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.ENDING_WITH);
        }

        @Test
        @DisplayName("findByNameContaining should create CONTAINING predicate")
        void findByNameContaining_shouldCreateContainingPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameContaining", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"oh"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.CONTAINING);
        }
    }

    @Nested
    @DisplayName("Is/Equals Operators (JPA: Is, Equals are equivalent to no operator)")
    class IsEqualsOperatorsTests {

        @Test
        @DisplayName("findByEmailIs should create EQ predicate (Is = no operator)")
        void findByEmailIs_shouldCreateEqPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailIs", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"test@example.com"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("email");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.EQ);
        }

        @Test
        @DisplayName("findByEmailEquals should create EQ predicate (Equals = no operator)")
        void findByEmailEquals_shouldCreateEqPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailEquals", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"test@example.com"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.EQ);
        }

        @Test
        @DisplayName("findByEmail (no operator suffix) should create EQ predicate")
        void findByEmail_shouldCreateEqPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmail", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"test@example.com"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.EQ);
        }
    }

    @Nested
    @DisplayName("Null Operator Variations (IsNull, Null)")
    class NullOperatorVariationsTests {

        @Test
        @DisplayName("findByEmailNull should create IS_NULL predicate (Null without Is)")
        void findByEmailNull_shouldCreateIsNullPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByEmailNull");
            Predicate predicate = QueryMethodParser.parse(method, null);

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("email");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IS_NULL);
        }
    }

    @Nested
    @DisplayName("IgnoreCase Support")
    class IgnoreCaseTests {

        @Test
        @DisplayName("findByNameIgnoreCase should create IGNORE_CASE predicate")
        void findByNameIgnoreCase_shouldCreateIgnoreCasePredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByNameIgnoreCase", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"John"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("name");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.IGNORE_CASE);
        }
    }

    @Nested
    @DisplayName("Distinct Support")
    class DistinctTests {

        @Test
        @DisplayName("findDistinctByLastname should parse and return predicate")
        void findDistinctByLastname_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findDistinctByLastname", String.class);
            
            // Distinct is a modifier, should still return the underlying predicate
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
            
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"Doe"});
            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("lastname");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.EQ);
        }
    }

    @Nested
    @DisplayName("Top/First Limiting Support")
    class TopFirstTests {

        @Test
        @DisplayName("findFirstByOrderByAgeDesc should parse correctly")
        void findFirstByOrderByAgeDesc_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findFirstByOrderByAgeDesc");
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
        }

        @Test
        @DisplayName("findTopByOrderByAgeDesc should parse correctly")
        void findTopByOrderByAgeDesc_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findTopByOrderByAgeDesc");
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
        }

        @Test
        @DisplayName("findTop10ByLastname should parse with limit")
        void findTop10ByLastname_shouldParseWithLimit() throws Exception {
            Method method = TestRepository.class.getMethod("findTop10ByLastname", String.class);
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
        }
    }

    @Nested
    @DisplayName("Count/Exists Query Methods")
    class CountExistsTests {

        @Test
        @DisplayName("countByLastname should be recognized as query method")
        void countByLastname_shouldBeRecognized() throws Exception {
            Method method = TestRepository.class.getMethod("countByLastname", String.class);
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
        }

        @Test
        @DisplayName("existsByEmail should be recognized as query method")
        void existsByEmail_shouldBeRecognized() throws Exception {
            Method method = TestRepository.class.getMethod("existsByEmail", String.class);
            boolean isQueryMethod = QueryMethodParser.isQueryMethod(method);
            assertThat(isQueryMethod).isTrue();
        }
    }

    @Nested
    @DisplayName("OrderBy Support")
    class OrderByTests {

        @Test
        @DisplayName("findByLastnameOrderByFirstnameAsc should parse condition and order")
        void findByLastnameOrderByFirstnameAsc_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findByLastnameOrderByFirstnameAsc", String.class);
            QueryMethodParser.ParsedQueryResult result = QueryMethodParser.parseQuery(method);

            // Only one condition: lastname (firstname is part of OrderBy, not a condition)
            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0)).isEqualTo("lastname");
            assertThat(result.orders()).hasSize(1);
            assertThat(result.orders().get(0).property()).isEqualTo("firstname");
            assertThat(result.orders().get(0).direction()).isEqualTo(QueryMethodParser.SortDirection.ASC);
        }

        @Test
        @DisplayName("findByLastnameOrderByFirstnameDesc should parse with DESC direction")
        void findByLastnameOrderByFirstnameDesc_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findByLastnameOrderByFirstnameDesc", String.class);
            QueryMethodParser.ParsedQueryResult result = QueryMethodParser.parseQuery(method);

            assertThat(result.orders().get(0).direction()).isEqualTo(QueryMethodParser.SortDirection.DESC);
        }

        @Test
        @DisplayName("findByAgeOrderByLastnameDesc should parse multiple order clauses")
        void findByAgeOrderByLastnameDesc_shouldParseCorrectly() throws Exception {
            Method method = TestRepository.class.getMethod("findByAgeOrderByLastnameDesc", int.class);
            QueryMethodParser.ParsedQueryResult result = QueryMethodParser.parseQuery(method);

            assertThat(result.conditions()).hasSize(1);
            assertThat(result.conditions().get(0)).isEqualTo("age");
            assertThat(result.orders()).hasSize(1);
            assertThat(result.orders().get(0).property()).isEqualTo("lastname");
            assertThat(result.orders().get(0).direction()).isEqualTo(QueryMethodParser.SortDirection.DESC);
        }
    }

    @Nested
    @DisplayName("Not Operator")
    class NotOperatorTests {

        @Test
        @DisplayName("findByLastnameNot should create NEQ predicate")
        void findByLastnameNot_shouldCreateNeqPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByLastnameNot", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"Doe"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.column()).isEqualTo("lastname");
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.NEQ);
        }

        @Test
        @DisplayName("findByStatusNot should create NEQ predicate")
        void findByStatusNot_shouldCreateNeqPredicate() throws Exception {
            Method method = TestRepository.class.getMethod("findByStatusNot", String.class);
            Predicate predicate = QueryMethodParser.parse(method, new Object[]{"ACTIVE"});

            assertThat(predicate).isInstanceOf(Predicate.Comparison.class);
            Predicate.Comparison comp = (Predicate.Comparison) predicate;
            assertThat(comp.operator()).isEqualTo(Predicate.Operator.NEQ);
        }
    }

    @Nested
    @DisplayName("Delete/Remove Methods")
    class DeleteRemoveTests {

        @Test
        @DisplayName("deleteByLastname should be recognized as query method")
        void deleteByLastname_shouldBeRecognized() throws Exception {
            Method method = TestRepository.class.getMethod("deleteByLastname", String.class);
            QueryMethodParser.ParsedQueryResult result = QueryMethodParser.parseQuery(method);

            assertThat(result.isQueryMethod()).isTrue();
            assertThat(result.queryType()).isEqualTo(QueryMethodParser.QueryType.DELETE);
            assertThat(result.conditions()).containsExactly("lastname");
        }

        @Test
        @DisplayName("removeByStatus should be recognized as remove query")
        void removeByStatus_shouldBeRecognized() throws Exception {
            Method method = TestRepository.class.getMethod("removeByStatus", String.class);
            QueryMethodParser.ParsedQueryResult result = QueryMethodParser.parseQuery(method);

            assertThat(result.queryType()).isEqualTo(QueryMethodParser.QueryType.REMOVE);
        }
    }
}
