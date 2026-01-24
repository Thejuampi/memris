package io.memris.kernel;

public record JoinCondition(String leftColumn, String rightColumn) {
    public JoinCondition {
        if (leftColumn == null || leftColumn.isBlank()) {
            throw new IllegalArgumentException("leftColumn required");
        }
        if (rightColumn == null || rightColumn.isBlank()) {
            throw new IllegalArgumentException("rightColumn required");
        }
    }
}
