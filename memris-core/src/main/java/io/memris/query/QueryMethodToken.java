package io.memris.query;

public record QueryMethodToken(
    QueryMethodTokenType type,
    String value,
    int start,
    int end,
    boolean ignoreCase
) { }
