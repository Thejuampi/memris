package io.memris.spring.plan.jpql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class JpqlLexer {
    private JpqlLexer() {
    }

    public enum TokenType {
        IDENT,
        STRING,
        NUMBER,
        PARAM_NAMED,
        PARAM_POSITIONAL,
        SELECT,
        FROM,
        WHERE,
        AND,
        OR,
        NOT,
        JOIN,
        LEFT,
        FETCH,
        AS,
        ORDER,
        BY,
        ASC,
        DESC,
        IN,
        BETWEEN,
        IS,
        NULL,
        LIKE,
        ILIKE,
        COUNT,
        DISTINCT,
        TRUE,
        FALSE,
        COMMA,
        DOT,
        LPAREN,
        RPAREN,
        EQ,
        NE,
        LT,
        LTE,
        GT,
        GTE,
        EOF
    }

    public record Token(TokenType type, String text, Object literal, int position) {
    }

    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

    static {
        KEYWORDS.put("SELECT", TokenType.SELECT);
        KEYWORDS.put("FROM", TokenType.FROM);
        KEYWORDS.put("WHERE", TokenType.WHERE);
        KEYWORDS.put("AND", TokenType.AND);
        KEYWORDS.put("OR", TokenType.OR);
        KEYWORDS.put("NOT", TokenType.NOT);
        KEYWORDS.put("JOIN", TokenType.JOIN);
        KEYWORDS.put("LEFT", TokenType.LEFT);
        KEYWORDS.put("FETCH", TokenType.FETCH);
        KEYWORDS.put("AS", TokenType.AS);
        KEYWORDS.put("ORDER", TokenType.ORDER);
        KEYWORDS.put("BY", TokenType.BY);
        KEYWORDS.put("ASC", TokenType.ASC);
        KEYWORDS.put("DESC", TokenType.DESC);
        KEYWORDS.put("IN", TokenType.IN);
        KEYWORDS.put("BETWEEN", TokenType.BETWEEN);
        KEYWORDS.put("IS", TokenType.IS);
        KEYWORDS.put("NULL", TokenType.NULL);
        KEYWORDS.put("LIKE", TokenType.LIKE);
        KEYWORDS.put("ILIKE", TokenType.ILIKE);
        KEYWORDS.put("COUNT", TokenType.COUNT);
        KEYWORDS.put("DISTINCT", TokenType.DISTINCT);
        KEYWORDS.put("TRUE", TokenType.TRUE);
        KEYWORDS.put("FALSE", TokenType.FALSE);
    }

    public static List<Token> tokenize(String input) {
        List<Token> tokens = new ArrayList<>();
        int length = input.length();
        int i = 0;
        while (i < length) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }

            int pos = i;
            switch (c) {
                case '(' -> {
                    tokens.add(new Token(TokenType.LPAREN, "(", null, pos));
                    i++;
                }
                case ')' -> {
                    tokens.add(new Token(TokenType.RPAREN, ")", null, pos));
                    i++;
                }
                case ',' -> {
                    tokens.add(new Token(TokenType.COMMA, ",", null, pos));
                    i++;
                }
                case '.' -> {
                    tokens.add(new Token(TokenType.DOT, ".", null, pos));
                    i++;
                }
                case ':' -> {
                    i++;
                    int start = i;
                    while (i < length && isIdentifierPart(input.charAt(i))) {
                        i++;
                    }
                    if (start == i) {
                        throw new IllegalArgumentException("Expected parameter name after ':' at position " + pos);
                    }
                    String name = input.substring(start, i);
                    tokens.add(new Token(TokenType.PARAM_NAMED, name, name, pos));
                }
                case '?' -> {
                    i++;
                    int start = i;
                    while (i < length && Character.isDigit(input.charAt(i))) {
                        i++;
                    }
                    if (start == i) {
                        throw new IllegalArgumentException("Expected positional parameter after '?' at position " + pos);
                    }
                    int index = Integer.parseInt(input.substring(start, i));
                    tokens.add(new Token(TokenType.PARAM_POSITIONAL, "?" + index, index, pos));
                }
                case '=' -> {
                    tokens.add(new Token(TokenType.EQ, "=", null, pos));
                    i++;
                }
                case '!' -> {
                    if (i + 1 < length && input.charAt(i + 1) == '=') {
                        tokens.add(new Token(TokenType.NE, "!=", null, pos));
                        i += 2;
                    } else {
                        throw new IllegalArgumentException("Unexpected '!' at position " + pos);
                    }
                }
                case '<' -> {
                    if (i + 1 < length && input.charAt(i + 1) == '=') {
                        tokens.add(new Token(TokenType.LTE, "<=", null, pos));
                        i += 2;
                    } else if (i + 1 < length && input.charAt(i + 1) == '>') {
                        tokens.add(new Token(TokenType.NE, "<>", null, pos));
                        i += 2;
                    } else {
                        tokens.add(new Token(TokenType.LT, "<", null, pos));
                        i++;
                    }
                }
                case '>' -> {
                    if (i + 1 < length && input.charAt(i + 1) == '=') {
                        tokens.add(new Token(TokenType.GTE, ">=", null, pos));
                        i += 2;
                    } else {
                        tokens.add(new Token(TokenType.GT, ">", null, pos));
                        i++;
                    }
                }
                case '\'' -> {
                    i++;
                    StringBuilder sb = new StringBuilder();
                    while (i < length) {
                        char ch = input.charAt(i);
                        if (ch == '\'') {
                            if (i + 1 < length && input.charAt(i + 1) == '\'') {
                                sb.append('\'');
                                i += 2;
                                continue;
                            }
                            i++;
                            break;
                        }
                        sb.append(ch);
                        i++;
                    }
                    tokens.add(new Token(TokenType.STRING, sb.toString(), sb.toString(), pos));
                }
                default -> {
                    if (Character.isDigit(c)) {
                        int start = i;
                        boolean hasDot = false;
                        while (i < length) {
                            char ch = input.charAt(i);
                            if (Character.isDigit(ch)) {
                                i++;
                                continue;
                            }
                            if (ch == '.') {
                                if (hasDot) {
                                    break;
                                }
                                hasDot = true;
                                i++;
                                continue;
                            }
                            break;
                        }
                        String text = input.substring(start, i);
                        Object value = hasDot ? new BigDecimal(text) : Long.valueOf(text);
                        tokens.add(new Token(TokenType.NUMBER, text, value, pos));
                        continue;
                    }
                    if (isIdentifierStart(c)) {
                        int start = i;
                        i++;
                        while (i < length && isIdentifierPart(input.charAt(i))) {
                            i++;
                        }
                        String ident = input.substring(start, i);
                        TokenType type = KEYWORDS.get(ident.toUpperCase(Locale.ROOT));
                        if (type != null) {
                            tokens.add(new Token(type, ident, ident, pos));
                        } else {
                            tokens.add(new Token(TokenType.IDENT, ident, ident, pos));
                        }
                        continue;
                    }
                    throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + pos);
                }
            }
        }
        tokens.add(new Token(TokenType.EOF, "", null, input.length()));
        return tokens;
    }

    private static boolean isIdentifierStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
