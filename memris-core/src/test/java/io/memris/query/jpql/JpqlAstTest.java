package io.memris.query.jpql;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for JPQL AST node classes.
 */
class JpqlAstTest {

    @Test
    @DisplayName("Query record should store all fields")
    void queryRecordShouldStoreAllFields() {
        JpqlAst.SelectItem item1 = new JpqlAst.SelectItem("e.name", "employeeName");
        JpqlAst.SelectItem item2 = new JpqlAst.SelectItem("e.age", null);
        JpqlAst.Join join = new JpqlAst.Join(JpqlAst.JoinType.LEFT, "e.department", "d");
        JpqlAst.Expression where = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.active", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal(true))
        );
        JpqlAst.OrderBy orderBy = new JpqlAst.OrderBy("e.name", true);

        JpqlAst.Query query = new JpqlAst.Query(
                false,  // count
                true,   // distinct
                List.of(item1, item2),
                "Employee",
                "e",
                List.of(join),
                where,
                List.of(orderBy)
        );

        assertThat(query.count()).isFalse();
        assertThat(query.distinct()).isTrue();
        assertThat(query.selectItems()).hasSize(2);
        assertThat(query.entityName()).isEqualTo("Employee");
        assertThat(query.rootAlias()).isEqualTo("e");
        assertThat(query.joins()).hasSize(1);
        assertThat(query.where()).isEqualTo(where);
        assertThat(query.orderBy()).hasSize(1);
    }

    @Test
    @DisplayName("Update record should store all fields")
    void updateRecordShouldStoreAllFields() {
        JpqlAst.Assignment assignment = new JpqlAst.Assignment("e.salary", new JpqlAst.Literal(50000));
        JpqlAst.Expression where = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.id", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal(1))
        );

        JpqlAst.Update update = new JpqlAst.Update(
                "Employee",
                "e",
                List.of(assignment),
                where
        );

        assertThat(update.entityName()).isEqualTo("Employee");
        assertThat(update.rootAlias()).isEqualTo("e");
        assertThat(update.assignments()).hasSize(1);
        assertThat(update.where()).isEqualTo(where);
    }

    @Test
    @DisplayName("Delete record should store all fields")
    void deleteRecordShouldStoreAllFields() {
        JpqlAst.Expression where = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.active", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal(false))
        );

        JpqlAst.Delete delete = new JpqlAst.Delete("Employee", "e", where);

        assertThat(delete.entityName()).isEqualTo("Employee");
        assertThat(delete.rootAlias()).isEqualTo("e");
        assertThat(delete.where()).isEqualTo(where);
    }

    @Test
    @DisplayName("Join record should store type, path and alias")
    void joinRecordShouldStoreTypePathAndAlias() {
        JpqlAst.Join innerJoin = new JpqlAst.Join(JpqlAst.JoinType.INNER, "e.department", "d");
        JpqlAst.Join leftJoin = new JpqlAst.Join(JpqlAst.JoinType.LEFT, "e.manager", "m");

        assertThat(innerJoin.type()).isEqualTo(JpqlAst.JoinType.INNER);
        assertThat(innerJoin.path()).isEqualTo("e.department");
        assertThat(innerJoin.alias()).isEqualTo("d");

        assertThat(leftJoin.type()).isEqualTo(JpqlAst.JoinType.LEFT);
    }

    @Test
    @DisplayName("SelectItem record should store path and alias")
    void selectItemRecordShouldStorePathAndAlias() {
        JpqlAst.SelectItem withAlias = new JpqlAst.SelectItem("e.name", "n");
        JpqlAst.SelectItem withoutAlias = new JpqlAst.SelectItem("e.age", null);

        assertThat(withAlias.path()).isEqualTo("e.name");
        assertThat(withAlias.alias()).isEqualTo("n");

        assertThat(withoutAlias.path()).isEqualTo("e.age");
        assertThat(withoutAlias.alias()).isNull();
    }

    @Test
    @DisplayName("OrderBy record should store path and ascending flag")
    void orderByRecordShouldStorePathAndAscendingFlag() {
        JpqlAst.OrderBy asc = new JpqlAst.OrderBy("e.name", true);
        JpqlAst.OrderBy desc = new JpqlAst.OrderBy("e.age", false);

        assertThat(asc.path()).isEqualTo("e.name");
        assertThat(asc.ascending()).isTrue();

        assertThat(desc.path()).isEqualTo("e.age");
        assertThat(desc.ascending()).isFalse();
    }

    @Test
    @DisplayName("Assignment record should store path and value")
    void assignmentRecordShouldStorePathAndValue() {
        JpqlAst.Literal value = new JpqlAst.Literal(100);
        JpqlAst.Assignment assignment = new JpqlAst.Assignment("e.salary", value);

        assertThat(assignment.path()).isEqualTo("e.salary");
        assertThat(assignment.value()).isEqualTo(value);
    }

    @Test
    @DisplayName("And expression should store left and right operands")
    void andExpressionShouldStoreLeftAndRightOperands() {
        JpqlAst.Expression left = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.age", JpqlAst.ComparisonOp.GT, new JpqlAst.Literal(18))
        );
        JpqlAst.Expression right = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.active", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal(true))
        );

        JpqlAst.And and = new JpqlAst.And(left, right);

        assertThat(and.left()).isEqualTo(left);
        assertThat(and.right()).isEqualTo(right);
    }

    @Test
    @DisplayName("Or expression should store left and right operands")
    void orExpressionShouldStoreLeftAndRightOperands() {
        JpqlAst.Expression left = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.status", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal("A"))
        );
        JpqlAst.Expression right = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.status", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal("B"))
        );

        JpqlAst.Or or = new JpqlAst.Or(left, right);

        assertThat(or.left()).isEqualTo(left);
        assertThat(or.right()).isEqualTo(right);
    }

    @Test
    @DisplayName("Not expression should store inner expression")
    void notExpressionShouldStoreInnerExpression() {
        JpqlAst.Expression inner = new JpqlAst.PredicateExpr(
                new JpqlAst.Comparison("e.deleted", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal(true))
        );

        JpqlAst.Not not = new JpqlAst.Not(inner);

        assertThat(not.expression()).isEqualTo(inner);
    }

    @Test
    @DisplayName("PredicateExpr should wrap a predicate")
    void predicateExprShouldWrapAPredicate() {
        JpqlAst.Comparison comparison = new JpqlAst.Comparison(
                "e.name", JpqlAst.ComparisonOp.EQ, new JpqlAst.Literal("John")
        );

        JpqlAst.PredicateExpr expr = new JpqlAst.PredicateExpr(comparison);

        assertThat(expr.predicate()).isEqualTo(comparison);
    }

    @Test
    @DisplayName("Comparison predicate should store path, operator and value")
    void comparisonPredicateShouldStorePathOperatorAndValue() {
        JpqlAst.Literal value = new JpqlAst.Literal(25);

        JpqlAst.Comparison comparison = new JpqlAst.Comparison("e.age", JpqlAst.ComparisonOp.GT, value);

        assertThat(comparison.path()).isEqualTo("e.age");
        assertThat(comparison.op()).isEqualTo(JpqlAst.ComparisonOp.GT);
        assertThat(comparison.value()).isEqualTo(value);
    }

    @Test
    @DisplayName("Between predicate should store path and bounds")
    void betweenPredicateShouldStorePathAndBounds() {
        JpqlAst.Literal lower = new JpqlAst.Literal(18);
        JpqlAst.Literal upper = new JpqlAst.Literal(65);

        JpqlAst.Between between = new JpqlAst.Between("e.age", lower, upper);

        assertThat(between.path()).isEqualTo("e.age");
        assertThat(between.lower()).isEqualTo(lower);
        assertThat(between.upper()).isEqualTo(upper);
    }

    @Test
    @DisplayName("In predicate should store path, values and negation flag")
    void inPredicateShouldStorePathValuesAndNegationFlag() {
        List<JpqlAst.Value> values = List.of(
                new JpqlAst.Literal("A"),
                new JpqlAst.Literal("B"),
                new JpqlAst.Literal("C")
        );

        JpqlAst.In in = new JpqlAst.In("e.status", values, false);
        JpqlAst.In notIn = new JpqlAst.In("e.status", values, true);

        assertThat(in.path()).isEqualTo("e.status");
        assertThat(in.values()).isEqualTo(values);
        assertThat(in.negated()).isFalse();

        assertThat(notIn.negated()).isTrue();
    }

    @Test
    @DisplayName("IsNull predicate should store path and negation flag")
    void isNullPredicateShouldStorePathAndNegationFlag() {
        JpqlAst.IsNull isNull = new JpqlAst.IsNull("e.deletedAt", false);
        JpqlAst.IsNull isNotNull = new JpqlAst.IsNull("e.deletedAt", true);

        assertThat(isNull.path()).isEqualTo("e.deletedAt");
        assertThat(isNull.negated()).isFalse();

        assertThat(isNotNull.negated()).isTrue();
    }

    @Test
    @DisplayName("Parameter value should store name and position")
    void parameterValueShouldStoreNameAndPosition() {
        JpqlAst.Parameter namedParam = new JpqlAst.Parameter("name", null);
        JpqlAst.Parameter positionalParam = new JpqlAst.Parameter(null, 1);

        assertThat(namedParam.name()).isEqualTo("name");
        assertThat(namedParam.position()).isNull();

        assertThat(positionalParam.name()).isNull();
        assertThat(positionalParam.position()).isEqualTo(1);
    }

    @Test
    @DisplayName("Literal value should store the value")
    void literalValueShouldStoreTheValue() {
        JpqlAst.Literal stringLiteral = new JpqlAst.Literal("John");
        JpqlAst.Literal intLiteral = new JpqlAst.Literal(42);
        JpqlAst.Literal boolLiteral = new JpqlAst.Literal(true);
        JpqlAst.Literal nullLiteral = new JpqlAst.Literal(null);

        assertThat(stringLiteral.value()).isEqualTo("John");
        assertThat(intLiteral.value()).isEqualTo(42);
        assertThat(boolLiteral.value()).isEqualTo(true);
        assertThat(nullLiteral.value()).isNull();
    }

    @Test
    @DisplayName("All comparison operators should be defined")
    void allComparisonOperatorsShouldBeDefined() {
        assertThat(JpqlAst.ComparisonOp.values()).containsExactlyInAnyOrder(
                JpqlAst.ComparisonOp.EQ,
                JpqlAst.ComparisonOp.NE,
                JpqlAst.ComparisonOp.GT,
                JpqlAst.ComparisonOp.GTE,
                JpqlAst.ComparisonOp.LT,
                JpqlAst.ComparisonOp.LTE,
                JpqlAst.ComparisonOp.LIKE,
                JpqlAst.ComparisonOp.ILIKE,
                JpqlAst.ComparisonOp.NOT_LIKE,
                JpqlAst.ComparisonOp.NOT_ILIKE
        );
    }

    @Test
    @DisplayName("All join types should be defined")
    void allJoinTypesShouldBeDefined() {
        assertThat(JpqlAst.JoinType.values()).containsExactlyInAnyOrder(
                JpqlAst.JoinType.INNER,
                JpqlAst.JoinType.LEFT
        );
    }

    @Test
    @DisplayName("Statement should be a sealed interface")
    void statementShouldBeASealedInterface() {
        assertThat(JpqlAst.Statement.class).isInterface();
        // Query, Update, Delete all implement Statement
        assertThat(JpqlAst.Query.class.getInterfaces()).contains(JpqlAst.Statement.class);
        assertThat(JpqlAst.Update.class.getInterfaces()).contains(JpqlAst.Statement.class);
        assertThat(JpqlAst.Delete.class.getInterfaces()).contains(JpqlAst.Statement.class);
    }

    @Test
    @DisplayName("Expression should be a sealed interface")
    void expressionShouldBeASealedInterface() {
        assertThat(JpqlAst.Expression.class).isInterface();
        // And, Or, Not, PredicateExpr all implement Expression
        assertThat(JpqlAst.And.class.getInterfaces()).contains(JpqlAst.Expression.class);
        assertThat(JpqlAst.Or.class.getInterfaces()).contains(JpqlAst.Expression.class);
        assertThat(JpqlAst.Not.class.getInterfaces()).contains(JpqlAst.Expression.class);
        assertThat(JpqlAst.PredicateExpr.class.getInterfaces()).contains(JpqlAst.Expression.class);
    }

    @Test
    @DisplayName("Predicate should be a sealed interface")
    void predicateShouldBeASealedInterface() {
        assertThat(JpqlAst.Predicate.class).isInterface();
        // Comparison, Between, In, IsNull all implement Predicate
        assertThat(JpqlAst.Comparison.class.getInterfaces()).contains(JpqlAst.Predicate.class);
        assertThat(JpqlAst.Between.class.getInterfaces()).contains(JpqlAst.Predicate.class);
        assertThat(JpqlAst.In.class.getInterfaces()).contains(JpqlAst.Predicate.class);
        assertThat(JpqlAst.IsNull.class.getInterfaces()).contains(JpqlAst.Predicate.class);
    }

    @Test
    @DisplayName("Value should be a sealed interface")
    void valueShouldBeASealedInterface() {
        assertThat(JpqlAst.Value.class).isInterface();
        // Parameter and Literal implement Value
        assertThat(JpqlAst.Parameter.class.getInterfaces()).contains(JpqlAst.Value.class);
        assertThat(JpqlAst.Literal.class.getInterfaces()).contains(JpqlAst.Value.class);
    }
}
