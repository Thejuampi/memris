package io.memris.spring.plan;

public record QueryMethodToken(
    QueryMethodTokenType type,
    String value,
    int start,
    int end,
    boolean ignoreCase
) {}
