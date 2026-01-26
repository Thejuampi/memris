package io.memris.kernel;

import java.util.Collection;
import java.util.List;

public sealed interface Predicate permits Predicate.Comparison, Predicate.In, Predicate.Between,
        Predicate.And, Predicate.Or, Predicate.Not {

    enum Operator {
        EQ,
        NEQ,
        GT,
        GTE,
        LT,
        LTE,
        BETWEEN,
        IN,
        NOT_IN,
        CONTAINING,
        NOT_CONTAINING,
        STARTING_WITH,
        NOT_STARTING_WITH,
        ENDING_WITH,
        NOT_ENDING_WITH,
        LIKE,
        NOT_LIKE,
        IS_TRUE,
        IS_FALSE,
        IS_NULL,
        IS_NOT_NULL,
        AFTER,
        BEFORE,
        IGNORE_CASE
    }

    record Comparison(String column, Operator operator, Object value) implements Predicate {
        public Comparison {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("column required");
            }
            if (operator == null) {
                throw new IllegalArgumentException("operator required");
            }
        }
    }

    record In(String column, Collection<?> values) implements Predicate {
        public In {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("column required");
            }
            if (values == null) {
                throw new IllegalArgumentException("values required");
            }
        }
    }

    record Between(String column, Object lower, Object upper) implements Predicate {
        public Between {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("column required");
            }
        }
    }

    record And(List<Predicate> predicates) implements Predicate {
        public And {
            if (predicates == null || predicates.isEmpty()) {
                throw new IllegalArgumentException("predicates required");
            }
        }
    }

    record Or(List<Predicate> predicates) implements Predicate {
        public Or {
            if (predicates == null || predicates.isEmpty()) {
                throw new IllegalArgumentException("predicates required");
            }
        }
    }

    record Not(Predicate predicate) implements Predicate {
        public Not {
            if (predicate == null) {
                throw new IllegalArgumentException("predicate required");
            }
        }
    }
}
