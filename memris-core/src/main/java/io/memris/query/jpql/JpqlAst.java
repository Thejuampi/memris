package io.memris.query.jpql;

import java.util.List;

public final class JpqlAst {
    private JpqlAst() {
    }

    public sealed interface Statement permits Query, Update, Delete {
    }

    public record Query(
            boolean count,
            boolean distinct,
            List<SelectItem> selectItems,
            String entityName,
            String rootAlias,
            List<Join> joins,
            Expression where,
            List<String> groupBy,
            Expression having,
            List<OrderBy> orderBy
    ) implements Statement {
    }

    public record Update(
            String entityName,
            String rootAlias,
            List<Assignment> assignments,
            Expression where
    ) implements Statement {
    }

    public record Delete(
            String entityName,
            String rootAlias,
            Expression where
    ) implements Statement {
    }

    public record Join(
            JoinType type,
            String path,
            String alias
    ) {
    }

    public enum JoinType {
        INNER,
        LEFT
    }

    public record OrderBy(String path, boolean ascending) {
    }

    public record SelectItem(String path, String alias) {
    }

    public record Assignment(String path, Value value) {
    }

    public sealed interface Expression permits And, Or, Not, PredicateExpr {
    }

    public record And(Expression left, Expression right) implements Expression {
    }

    public record Or(Expression left, Expression right) implements Expression {
    }

    public record Not(Expression expression) implements Expression {
    }

    public record PredicateExpr(Predicate predicate) implements Expression {
    }

    public sealed interface Predicate permits Comparison, Between, In, IsNull {
    }

    public record Comparison(String path, ComparisonOp op, Value value) implements Predicate {
    }

    public record Between(String path, Value lower, Value upper) implements Predicate {
    }

    public record In(String path, List<Value> values, boolean negated) implements Predicate {
    }

    public record IsNull(String path, boolean negated) implements Predicate {
    }

    public enum ComparisonOp {
        EQ,
        NE,
        GT,
        GTE,
        LT,
        LTE,
        LIKE,
        ILIKE,
        NOT_LIKE,
        NOT_ILIKE
    }

    public sealed interface Value permits Parameter, Literal {
    }

    public record Parameter(String name, Integer position) implements Value {
    }

    public record Literal(Object value) implements Value {
    }
}
