package io.memris.query;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
 *
 * @see MethodKey
 * @see OpCode
 */
public final class BuiltInResolver {

    /**
     * Cache mapping primitive types to their wrapper classes.
     */
    private static final Map<Class<?>, Class<?>> WRAPPER_TYPES = Map.of(
            boolean.class, Boolean.class,
            byte.class, Byte.class,
            char.class, Character.class,
            short.class, Short.class,
            int.class, Integer.class,
            long.class, Long.class,
            float.class, Float.class,
            double.class, Double.class,
            void.class, Void.class
    );

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

        // Find all matching built-ins
        final List<Map.Entry<MethodKey, OpCode>> candidates = builtIns.entrySet()
                .stream()
                .filter(e -> e.getKey().matches(method))
                .toList();

        if (candidates.isEmpty()) {
            return null;
        }

        // 1) Prefer exact matches (after boxing primitives)
        final List<Map.Entry<MethodKey, OpCode>> exact = candidates.stream()
                .filter(e -> isExactSignatureMatch(e.getKey(), method))
                .toList();

        if (exact.size() == 1) {
            return exact.get(0).getValue();
        }
        if (exact.size() > 1) {
            throw ambiguous(method, exact, "Multiple exact built-in matches");
        }

        // 2) Fall back to most-specific: minimal summed distance across parameters
        final List<ScoredCandidate> scored = candidates.stream()
                .map(e -> new ScoredCandidate(e.getKey(), e.getValue(), specificityScore(e.getKey(), method)))
                .sorted(Comparator.comparingInt((ScoredCandidate sc) -> sc.score.totalDistance)
                        .thenComparingInt(sc -> -sc.score.exactCount)) // tie-break: more exact params preferred
                .toList();

        final ScoredCandidate best = scored.get(0);

        // 3) Fail fast on ambiguity (same score)
        final List<ScoredCandidate> tied = scored.stream()
                .filter(sc -> sc.score.equals(best.score))
                .toList();

        if (tied.size() > 1) {
            throw ambiguous(method,
                    tied.stream().map(sc -> Map.entry(sc.key, sc.opCode)).toList(),
                    "Ambiguous built-in match (tie on specificity score " + best.score + ")");
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
        final ArrayDeque<Class<?>> q = new ArrayDeque<>();
        final ArrayDeque<Integer> dist = new ArrayDeque<>();
        final Set<Class<?>> seen = new HashSet<>();

        q.add(actual);
        dist.add(0);
        seen.add(actual);

        while (!q.isEmpty()) {
            final Class<?> cur = q.removeFirst();
            final int d = dist.removeFirst();

            if (cur.equals(target)) {
                return d;
            }

            final Class<?> sup = cur.getSuperclass();
            if (sup != null) {
                final Class<?> s = box(sup);
                if (seen.add(s)) {
                    q.addLast(s);
                    dist.addLast(d + 1);
                }
            }

            for (Class<?> itf : cur.getInterfaces()) {
                final Class<?> i = box(itf);
                if (seen.add(i)) {
                    q.addLast(i);
                    dist.addLast(d + 1);
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
        final String details = matches.stream()
                .map(e -> e.getKey() + " -> " + e.getValue())
                .collect(Collectors.joining(", "));
        return new IllegalStateException(reason + " for " + sig + ". Matches: [" + details + "]");
    }

    /**
     * Get a readable signature for a method.
     */
    private static String signature(Method m) {
        final String params = Arrays.stream(m.getParameterTypes())
                .map(Class::getTypeName)
                .collect(Collectors.joining(", "));
        return m.getDeclaringClass().getTypeName() + "#" + m.getName() + "(" + params + ")";
    }

    /**
     * Box a primitive type to its wrapper class.
     */
    private static Class<?> box(Class<?> c) {
        if (!c.isPrimitive()) {
            return c;
        }
        return WRAPPER_TYPES.get(c);
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
