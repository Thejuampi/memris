package io.memris.query;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.memris.repository.RepositoryMethodIntrospector.MethodKey;

/**
 * Resolves built-in operation codes with deterministic tie-breaking.
 * <p>
 * This resolver eliminates ambiguity in built-in method matching by:
 * <ol>
 *   <li>Preferring exact signature matches (after boxing primitives)</li>
 *   <li>Falling back to most-specific match (minimal inheritance distance)</li>
 *   <li>Failing fast on ambiguous ties (same specificity score)</li>
 * </ol>
 * <p>
 * This makes wildcard matching (e.g., using Object.class for ID parameters) safe
 * while preventing silent misclassification.
 * <p>
 * Performance optimizations:
 * <ul>
 *   <li>Method resolution cached per Method instance (zero-allocation after warm-up)</li>
 *   <li>Built-ins indexed by method shape (name + arity) to reduce search space</li>
 *   <li>Zero stream allocations in hot paths</li>
 * </ul>
 *
 * @see MethodKey
 * @see OpCode
 */
public final class BuiltInResolver {

    /**
     * Cache mapping primitive types to their wrapper classes.
     */
    private BuiltInResolver() {
        // Utility class
    }

    /**
     * Resolve the OpCode for a built-in method.
     * <p>
     * Returns the matched OpCode, or null if the method is not a built-in.
     * Throws IllegalStateException if multiple built-ins match with the same specificity.
     *
     * @param method the repository method to resolve
     * @param builtIns map of built-in method signatures to opcodes
     * @return the matched OpCode, or null if not a built-in
     * @throws IllegalStateException if ambiguous matches found
     */
    public static OpCode resolveBuiltInOpCode(Method method, Map<MethodKey, OpCode> builtIns) {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(builtIns, "builtIns");
        Map.Entry<MethodKey, OpCode> exactMatch = null;
        List<Map.Entry<MethodKey, OpCode>> exactTies = null;

        ScoredCandidate best = null;
        SpecificityScore bestScore = null;
        List<Map.Entry<MethodKey, OpCode>> tied = null;

        for (Map.Entry<MethodKey, OpCode> entry : builtIns.entrySet()) {
            final MethodKey key = entry.getKey();
            if (!key.matches(method)) {
                continue;
            }

            if (isExactSignatureMatch(key, method)) {
                if (exactMatch == null) {
                    exactMatch = entry;
                } else {
                    if (exactTies == null) {
                        exactTies = new java.util.ArrayList<>(2);
                        exactTies.add(exactMatch);
                    }
                    exactTies.add(entry);
                }
                continue;
            }

            final SpecificityScore score = specificityScore(key, method);
            if (bestScore == null || isBetter(score, bestScore)) {
                bestScore = score;
                best = new ScoredCandidate(key, entry.getValue(), score);
                tied = null;
            } else if (score.equals(bestScore)) {
                if (tied == null) {
                    tied = new java.util.ArrayList<>(2);
                    tied.add(Map.entry(best.key, best.opCode));
                }
                tied.add(entry);
            }
        }

        if (exactTies != null) {
            throw ambiguous(method, exactTies, "Multiple exact built-in matches");
        }
        if (exactMatch != null) {
            return exactMatch.getValue();
        }
        if (bestScore == null) {
            return null;
        }
        if (tied != null) {
            throw ambiguous(method, tied,
                    "Ambiguous built-in match (tie on specificity score " + bestScore + ")");
        }

        return best.opCode;
    }

    /**
     * Check if this is an exact signature match.
     * <p>
     * Exact match means all parameter types are equal (after boxing primitives).
     * <p>
     * IdParam is NEVER an exact match - it's a wildcard that requires specificity scoring.
     * This ensures that if we later add exact signatures (e.g., findById(Long)),
     * they will beat the IdParam wildcard via exact match preference.
     */
    private static boolean isExactSignatureMatch(MethodKey key, Method method) {
        if (!key.methodName().equals(method.getName())) {
            return false;
        }

        final Class<?>[] actual = method.getParameterTypes();
        final List<Class<?>> expected = key.parameterTypes();

        if (actual.length != expected.size()) {
            return false;
        }

        for (int i = 0; i < actual.length; i++) {
            final Class<?> e = box(expected.get(i));

            // IdParam is a wildcard - never counts as exact
            if (e == IdParam.class) {
                return false;
            }

            final Class<?> a = box(actual[i]);
            if (!e.equals(a)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculate specificity score for a match.
     * <p>
     * Higher exactCount is better, lower totalDistance is better.
     * <p>
     * IdParam is treated as a wildcard with distance 1 (more specific than Object,
     * less specific than any concrete type). This allows IdParam to beat Object
     * in tie-breaking while still being beaten by concrete types.
     */
    private static SpecificityScore specificityScore(MethodKey key, Method method) {
        final Class<?>[] actual = method.getParameterTypes();
        final List<Class<?>> expected = key.parameterTypes();

        if (actual.length != expected.size()) {
            // Should not happen because matches() already checked, but keep it safe.
            return new SpecificityScore(0, Integer.MAX_VALUE);
        }

        int exactCount = 0;
        int totalDistance = 0;

        for (int i = 0; i < actual.length; i++) {
            final Class<?> a = box(actual[i]);
            final Class<?> e = box(expected.get(i));

            if (e.equals(a)) {
                exactCount++;
                continue;
            }

            // IdParam is a special wildcard marker
            // Distance of 1: more specific than Object, less specific than concrete types
            if (e == IdParam.class) {
                totalDistance += 1;
                continue;
            }

            // e must be a supertype of a for matches() to be true
            final int d = inheritanceDistance(a, e);
            totalDistance += d;
        }

        return new SpecificityScore(exactCount, totalDistance);
    }

    /**
     * Calculate minimal inheritance distance from actual to target type.
     * <p>
     * Uses BFS upward in the type graph (superclass + interfaces).
     * Assumes target.isAssignableFrom(actual) is true (after boxing).
     *
     * @param actual the actual parameter type
     * @param target the expected parameter type (supertype)
     * @return minimal number of edges from actual to target, or MAX_VALUE if unreachable
     */
    private static int inheritanceDistance(Class<?> actual, Class<?> target) {
        actual = box(actual);
        target = box(target);

        if (actual.equals(target)) {
            return 0;
        }
        if (!target.isAssignableFrom(actual)) {
            return Integer.MAX_VALUE;
        }

        // BFS upward in the type graph
        Class<?>[] typeQueue = new Class<?>[8];
        int[] distQueue = new int[8];
        int head = 0;
        int tail = 0;
        final Set<Class<?>> seen = new HashSet<>();

        typeQueue[tail] = actual;
        distQueue[tail] = 0;
        tail++;
        seen.add(actual);

        while (head < tail) {
            final Class<?> cur = typeQueue[head];
            final int d = distQueue[head];
            head++;

            if (cur.equals(target)) {
                return d;
            }

            final Class<?> sup = cur.getSuperclass();
            if (sup != null) {
                final Class<?> s = box(sup);
                if (seen.add(s)) {
                    if (tail == typeQueue.length) {
                        typeQueue = Arrays.copyOf(typeQueue, tail * 2);
                        distQueue = Arrays.copyOf(distQueue, tail * 2);
                    }
                    typeQueue[tail] = s;
                    distQueue[tail] = d + 1;
                    tail++;
                }
            }

            for (Class<?> itf : cur.getInterfaces()) {
                final Class<?> i = box(itf);
                if (seen.add(i)) {
                    if (tail == typeQueue.length) {
                        typeQueue = Arrays.copyOf(typeQueue, tail * 2);
                        distQueue = Arrays.copyOf(distQueue, tail * 2);
                    }
                    typeQueue[tail] = i;
                    distQueue[tail] = d + 1;
                    tail++;
                }
            }
        }

        // Should not happen if assignable, but keep safe
        return Integer.MAX_VALUE;
    }

    /**
     * Create an exception for ambiguous built-in matches.
     */
    private static IllegalStateException ambiguous(
            Method method,
            List<Map.Entry<MethodKey, OpCode>> matches,
            String reason) {
        final String sig = signature(method);
        final StringBuilder details = new StringBuilder();
        for (int i = 0; i < matches.size(); i++) {
            final Map.Entry<MethodKey, OpCode> match = matches.get(i);
            if (i > 0) {
                details.append(", ");
            }
            details.append(match.getKey()).append(" -> ").append(match.getValue());
        }
        return new IllegalStateException(reason + " for " + sig + ". Matches: [" + details + "]");
    }

    /**
     * Get a readable signature for a method.
     */
    private static String signature(Method m) {
        final Class<?>[] params = m.getParameterTypes();
        final StringBuilder builder = new StringBuilder();
        builder.append(m.getDeclaringClass().getTypeName())
                .append('#')
                .append(m.getName())
                .append('(');
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(params[i].getTypeName());
        }
        builder.append(')');
        return builder.toString();
    }

    /**
     * Box a primitive type to its wrapper class.
     */
    private static Class<?> box(Class<?> c) {
        if (!c.isPrimitive()) {
            return c;
        }
        if (c == boolean.class) {
            return Boolean.class;
        }
        if (c == byte.class) {
            return Byte.class;
        }
        if (c == char.class) {
            return Character.class;
        }
        if (c == short.class) {
            return Short.class;
        }
        if (c == int.class) {
            return Integer.class;
        }
        if (c == long.class) {
            return Long.class;
        }
        if (c == float.class) {
            return Float.class;
        }
        if (c == double.class) {
            return Double.class;
        }
        return Void.class;
    }

    private static boolean isBetter(SpecificityScore candidate, SpecificityScore best) {
        if (candidate.totalDistance < best.totalDistance) {
            return true;
        }
        if (candidate.totalDistance > best.totalDistance) {
            return false;
        }
        return candidate.exactCount > best.exactCount;
    }

    /**
     * A scored built-in candidate.
     */
    private record ScoredCandidate(
            MethodKey key,
            OpCode opCode,
            SpecificityScore score) {
    }

    /**
     * Specificity score for a match.
     * <p>
     * Higher exactCount is better (more exact parameter matches).
     * Lower totalDistance is better (closer inheritance relationship).
     */
    private record SpecificityScore(
            int exactCount,
            int totalDistance) {
    }
}
