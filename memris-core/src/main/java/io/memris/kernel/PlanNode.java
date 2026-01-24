package io.memris.kernel;

import java.util.List;

public sealed interface PlanNode permits PlanNode.Scan, PlanNode.Filter, PlanNode.Join,
        PlanNode.Project, PlanNode.Sort, PlanNode.Limit {

    record Scan(Table table) implements PlanNode {
        public Scan {
            if (table == null) {
                throw new IllegalArgumentException("table required");
            }
        }
    }

    record Filter(PlanNode child, Predicate predicate) implements PlanNode {
        public Filter {
            if (child == null) {
                throw new IllegalArgumentException("child required");
            }
            if (predicate == null) {
                throw new IllegalArgumentException("predicate required");
            }
        }
    }

    record Join(PlanNode left, PlanNode right, JoinType type, JoinCondition condition) implements PlanNode {
        public Join {
            if (left == null || right == null) {
                throw new IllegalArgumentException("left and right required");
            }
            if (type == null) {
                throw new IllegalArgumentException("type required");
            }
            if (condition == null) {
                throw new IllegalArgumentException("condition required");
            }
        }
    }

    record Project(PlanNode child, List<String> columns) implements PlanNode {
        public Project {
            if (child == null) {
                throw new IllegalArgumentException("child required");
            }
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("columns required");
            }
        }
    }

    record Sort(PlanNode child, List<Ordering> orderings) implements PlanNode {
        public Sort {
            if (child == null) {
                throw new IllegalArgumentException("child required");
            }
            if (orderings == null || orderings.isEmpty()) {
                throw new IllegalArgumentException("orderings required");
            }
        }
    }

    record Limit(PlanNode child, long offset, long limit) implements PlanNode {
        public Limit {
            if (child == null) {
                throw new IllegalArgumentException("child required");
            }
            if (offset < 0 || limit < 0) {
                throw new IllegalArgumentException("offset and limit must be non-negative");
            }
        }
    }

    enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL
    }

    record Ordering(String column, Direction direction) {
        public Ordering {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("column required");
            }
            if (direction == null) {
                throw new IllegalArgumentException("direction required");
            }
        }
    }

    enum Direction {
        ASC,
        DESC
    }
}
