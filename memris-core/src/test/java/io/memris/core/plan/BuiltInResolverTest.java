package io.memris.core.plan;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.memris.repository.RepositoryMethodIntrospector.MethodKey;
import io.memris.query.BuiltInResolver;
import io.memris.query.OpCode;

/**
 * Tests for built-in operation resolution with tie-breaking.
 * <p>
 * These tests verify that the BuiltInResolver correctly handles:
 * <ul>
 * <li>Exact signature matches beating wildcard matches</li>
 * <li>Most-specific match selection when multiple wildcards match</li>
 * <li>Fail-fast on ambiguous ties</li>
 * </ul>
 */
class BuiltInResolverTest {

    // ==================== Test 1: Exact beats wildcard ====================

    @Test
    void exactBeatsWildcard_whenBothPresent() {
        // Setup: BUILT_INS contains both wildcard and specific variants
        // Note: In real code, we wouldn't have both, but this tests the resolver logic
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("findById", List.of(Object.class)), OpCode.FIND_BY_ID,
                new MethodKey("findById", List.of(Long.class)), OpCode.FIND_ALL // Different opcode for testing
        );

        // Method: findById(Long)
        Method method = getMethod(TestRepo.class, "findById", Long.class);

        // Should resolve to exact match, not wildcard
        OpCode result = BuiltInResolver.resolveBuiltInOpCode(method, builtIns);

        assertThat(result).isEqualTo(OpCode.FIND_ALL); // The exact match won
    }

    @Test
    void wildcardOnly_whenNoExactMatch() {
        // Setup: Only wildcard present
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("findById", List.of(Object.class)), OpCode.FIND_BY_ID);

        // Method: findById(Long)
        Method method = getMethod(TestRepo.class, "findById", Long.class);

        // Should resolve to wildcard (Long is assignable to Object)
        OpCode result = BuiltInResolver.resolveBuiltInOpCode(method, builtIns);

        assertThat(result).isEqualTo(OpCode.FIND_BY_ID);
    }

    // ==================== Test 2: Tie explodes ====================

    @Test
    void throwsOnAmbiguousTies() {
        // String implements both Comparable and Serializable, creating equal
        // specificity matches.
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("process", List.of(Comparable.class)), OpCode.FIND_ALL,
                new MethodKey("process", List.of(java.io.Serializable.class)), OpCode.FIND_BY_ID);

        Method method = getMethod(TestRepo.class, "process", String.class);

        assertThatThrownBy(() -> BuiltInResolver.resolveBuiltInOpCode(method, builtIns))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Ambiguous built-in match");
    }

    // ==================== Test 3: Most-specific wins ====================

    @Test
    void mostSpecificWins_whenMultipleWildcardsMatch() {
        // Setup: Two wildcard keys with different specificity
        // Number is more specific than Object for Long (Long extends Number)
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("save", List.of(Object.class)), OpCode.SAVE_ONE,
                new MethodKey("save", List.of(Number.class)), OpCode.SAVE_ALL // Different opcode for testing
        );

        // Method: save(Integer) - Integer extends Number, both Number and Object match
        Method method = getMethod(TestRepo.class, "save", Integer.class);

        // Should resolve to Number (more specific than Object)
        OpCode result = BuiltInResolver.resolveBuiltInOpCode(method, builtIns);

        assertThat(result).isEqualTo(OpCode.SAVE_ALL); // The Number-specific match won
    }

    // ==================== Test 4: Not a built-in ====================

    @Test
    void returnsNull_whenNotABuiltIn() {
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("findById", List.of(Object.class)), OpCode.FIND_BY_ID);

        // Method: findByName - not a built-in
        Method method = getMethod(TestRepo.class, "findByName", String.class);

        OpCode result = BuiltInResolver.resolveBuiltInOpCode(method, builtIns);

        assertThat(result).isNull();
    }

    // ==================== Test 5: Zero-arg exact match ====================

    @Test
    void exactMatchForZeroArgMethods() {
        // Setup: findAll() with no parameters
        Map<MethodKey, OpCode> builtIns = Map.of(
                new MethodKey("findAll", List.of()), OpCode.FIND_ALL);

        Method method = getMethod(TestRepo.class, "findAll");

        OpCode result = BuiltInResolver.resolveBuiltInOpCode(method, builtIns);

        assertThat(result).isEqualTo(OpCode.FIND_ALL);
    }

    // ==================== Helper methods ====================

    private Method getMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
        try {
            return clazz.getMethod(name, paramTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    // Test interface
    interface TestRepo {
        Object findById(Long id);

        Object findById(String id); // Different overload

        void deleteById(Long id);

        void deleteById(String id); // Different overload

        void save(Integer entity);

        void save(Number entity);

        Object findByName(String name);

        List<?> findAll();

        void process(String value); // String implements Comparable and Serializable
    }
}
