package io.memris.kernel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PredicateTest {

    @Test
    @DisplayName("Comparison should store column, operator and value")
    void comparison_shouldStoreColumnOperatorAndValue() {
        Predicate.Comparison comparison = new Predicate.Comparison("name", Predicate.Operator.EQ, "John");

        assertThat(comparison.column()).isEqualTo("name");
        assertThat(comparison.operator()).isEqualTo(Predicate.Operator.EQ);
        assertThat(comparison.value()).isEqualTo("John");
    }

    @Test
    @DisplayName("Comparison should reject null column")
    void comparison_shouldRejectNullColumn() {
        assertThatThrownBy(() -> new Predicate.Comparison(null, Predicate.Operator.EQ, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("Comparison should reject blank column")
    void comparison_shouldRejectBlankColumn() {
        assertThatThrownBy(() -> new Predicate.Comparison("   ", Predicate.Operator.EQ, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("Comparison should reject null operator")
    void comparison_shouldRejectNullOperator() {
        assertThatThrownBy(() -> new Predicate.Comparison("name", null, "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator required");
    }

    @Test
    @DisplayName("Comparison should accept all operators")
    void comparison_shouldAcceptAllOperators() {
        for (Predicate.Operator operator : Predicate.Operator.values()) {
            Predicate.Comparison comparison = new Predicate.Comparison("field", operator, "value");
            assertThat(comparison.operator()).isEqualTo(operator);
        }
    }

    @Test
    @DisplayName("In should store column and values")
    void in_shouldStoreColumnAndValues() {
        List<String> values = Arrays.asList("A", "B", "C");
        Predicate.In in = new Predicate.In("status", values);

        assertThat(in.column()).isEqualTo("status");
        @SuppressWarnings("unchecked")
        Collection<Object> inValues = (Collection<Object>) in.values();
        assertThat(inValues).containsExactly("A", "B", "C");
    }

    @Test
    @DisplayName("In should reject null column")
    void in_shouldRejectNullColumn() {
        assertThatThrownBy(() -> new Predicate.In(null, Arrays.asList("A", "B")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("In should reject blank column")
    void in_shouldRejectBlankColumn() {
        assertThatThrownBy(() -> new Predicate.In("   ", Arrays.asList("A", "B")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("In should reject null values")
    void in_shouldRejectNullValues() {
        assertThatThrownBy(() -> new Predicate.In("status", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("values required");
    }

    @Test
    @DisplayName("In should accept empty collection")
    void in_shouldAcceptEmptyCollection() {
        Predicate.In in = new Predicate.In("status", Collections.emptyList());
        assertThat(in.values()).isEmpty();
    }

    @Test
    @DisplayName("Between should store column and bounds")
    void between_shouldStoreColumnAndBounds() {
        Predicate.Between between = new Predicate.Between("age", 18, 65);

        assertThat(between.column()).isEqualTo("age");
        assertThat(between.lower()).isEqualTo(18);
        assertThat(between.upper()).isEqualTo(65);
    }

    @Test
    @DisplayName("Between should reject null column")
    void between_shouldRejectNullColumn() {
        assertThatThrownBy(() -> new Predicate.Between(null, 1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("Between should reject blank column")
    void between_shouldRejectBlankColumn() {
        assertThatThrownBy(() -> new Predicate.Between("   ", 1, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column required");
    }

    @Test
    @DisplayName("Between should accept null bounds")
    void between_shouldAcceptNullBounds() {
        Predicate.Between between = new Predicate.Between("age", null, null);
        assertThat(between.lower()).isNull();
        assertThat(between.upper()).isNull();
    }

    @Test
    @DisplayName("And should store predicates")
    void and_shouldStorePredicates() {
        Predicate.Comparison p1 = new Predicate.Comparison("name", Predicate.Operator.EQ, "John");
        Predicate.Comparison p2 = new Predicate.Comparison("age", Predicate.Operator.GT, 18);
        Predicate.And and = new Predicate.And(Arrays.asList(p1, p2));

        assertThat(and.predicates()).hasSize(2);
        assertThat(and.predicates().get(0)).isEqualTo(p1);
        assertThat(and.predicates().get(1)).isEqualTo(p2);
    }

    @Test
    @DisplayName("And should reject null predicates")
    void and_shouldRejectNullPredicates() {
        assertThatThrownBy(() -> new Predicate.And(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicates required");
    }

    @Test
    @DisplayName("And should reject empty predicates")
    void and_shouldRejectEmptyPredicates() {
        assertThatThrownBy(() -> new Predicate.And(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicates required");
    }

    @Test
    @DisplayName("And should accept single predicate")
    void and_shouldAcceptSinglePredicate() {
        Predicate.Comparison p1 = new Predicate.Comparison("name", Predicate.Operator.EQ, "John");
        Predicate.And and = new Predicate.And(Collections.singletonList(p1));

        assertThat(and.predicates()).hasSize(1);
    }

    @Test
    @DisplayName("Or should store predicates")
    void or_shouldStorePredicates() {
        Predicate.Comparison p1 = new Predicate.Comparison("status", Predicate.Operator.EQ, "ACTIVE");
        Predicate.Comparison p2 = new Predicate.Comparison("status", Predicate.Operator.EQ, "PENDING");
        Predicate.Or or = new Predicate.Or(Arrays.asList(p1, p2));

        assertThat(or.predicates()).hasSize(2);
    }

    @Test
    @DisplayName("Or should reject null predicates")
    void or_shouldRejectNullPredicates() {
        assertThatThrownBy(() -> new Predicate.Or(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicates required");
    }

    @Test
    @DisplayName("Or should reject empty predicates")
    void or_shouldRejectEmptyPredicates() {
        assertThatThrownBy(() -> new Predicate.Or(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicates required");
    }

    @Test
    @DisplayName("Not should store predicate")
    void not_shouldStorePredicate() {
        Predicate.Comparison inner = new Predicate.Comparison("active", Predicate.Operator.EQ, true);
        Predicate.Not not = new Predicate.Not(inner);

        assertThat(not.predicate()).isEqualTo(inner);
    }

    @Test
    @DisplayName("Not should reject null predicate")
    void not_shouldRejectNullPredicate() {
        assertThatThrownBy(() -> new Predicate.Not(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicate required");
    }

    @Test
    @DisplayName("Should create complex nested predicates")
    void shouldCreateComplexNestedPredicates() {
        Predicate.Comparison nameEq = new Predicate.Comparison("name", Predicate.Operator.EQ, "John");
        Predicate.Comparison ageGt = new Predicate.Comparison("age", Predicate.Operator.GT, 18);
        Predicate.Comparison statusIn = new Predicate.Comparison("status", Predicate.Operator.EQ, "ACTIVE");

        Predicate.And andPredicate = new Predicate.And(Arrays.asList(nameEq, ageGt));
        Predicate.Or orPredicate = new Predicate.Or(Arrays.asList(andPredicate, statusIn));
        Predicate.Not notPredicate = new Predicate.Not(orPredicate);

        assertThat(notPredicate.predicate()).isInstanceOf(Predicate.Or.class);
    }

    @Test
    @DisplayName("Operator enum should have all expected values")
    void operatorEnum_shouldHaveAllExpectedValues() {
        assertThat(Predicate.Operator.values()).containsExactlyInAnyOrder(
                Predicate.Operator.EQ,
                Predicate.Operator.NEQ,
                Predicate.Operator.GT,
                Predicate.Operator.GTE,
                Predicate.Operator.LT,
                Predicate.Operator.LTE,
                Predicate.Operator.BETWEEN,
                Predicate.Operator.IN,
                Predicate.Operator.NOT_IN,
                Predicate.Operator.CONTAINING,
                Predicate.Operator.NOT_CONTAINING,
                Predicate.Operator.STARTING_WITH,
                Predicate.Operator.NOT_STARTING_WITH,
                Predicate.Operator.ENDING_WITH,
                Predicate.Operator.NOT_ENDING_WITH,
                Predicate.Operator.LIKE,
                Predicate.Operator.NOT_LIKE,
                Predicate.Operator.IS_TRUE,
                Predicate.Operator.IS_FALSE,
                Predicate.Operator.IS_NULL,
                Predicate.Operator.IS_NOT_NULL,
                Predicate.Operator.AFTER,
                Predicate.Operator.BEFORE,
                Predicate.Operator.IGNORE_CASE
        );
    }

    @Test
    @DisplayName("Comparison should work with different value types")
    void comparison_shouldWorkWithDifferentValueTypes() {
        Predicate.Comparison stringComp = new Predicate.Comparison("name", Predicate.Operator.EQ, "text");
        Predicate.Comparison intComp = new Predicate.Comparison("count", Predicate.Operator.GT, 42);
        Predicate.Comparison boolComp = new Predicate.Comparison("active", Predicate.Operator.EQ, true);
        Predicate.Comparison nullComp = new Predicate.Comparison("optional", Predicate.Operator.IS_NULL, null);

        assertThat(stringComp.value()).isInstanceOf(String.class);
        assertThat(intComp.value()).isInstanceOf(Integer.class);
        assertThat(boolComp.value()).isInstanceOf(Boolean.class);
        assertThat(nullComp.value()).isNull();
    }
}
