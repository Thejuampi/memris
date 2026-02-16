package io.memris.repository;

import io.memris.query.MethodKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryMethodIntrospectorTest {

    @Test
    void extractQueryMethodsUsesDeterministicOrderingAndFiltersDefaults() {
        Method[] methods = RepositoryMethodIntrospector.extractQueryMethods(SampleRepo.class);

        assertThat(Arrays.stream(methods).map(Method::getName))
                .containsExactly("a", "b", "b");
        assertThat(methods[1].getParameterTypes()).containsExactly(int.class);
        assertThat(methods[2].getParameterTypes()).containsExactly(String.class);
    }

    @Test
    void methodKeyOfDelegatesToMethodKeyFactory() throws Exception {
        Method method = SampleRepo.class.getMethod("b", String.class);
        MethodKey key = RepositoryMethodIntrospector.methodKeyOf(method);

        assertThat(key).isEqualTo(MethodKey.of(method));
    }

    @Test
    void privateIsQueryMethodBranches() throws Exception {
        Method isQueryMethod = RepositoryMethodIntrospector.class.getDeclaredMethod("isQueryMethod", Method.class);
        isQueryMethod.setAccessible(true);

        Method abstractMethod = SampleRepo.class.getMethod("a");
        Method defaultMethod = SampleRepo.class.getMethod("defaultMethod");
        Method objectMethod = Object.class.getMethod("toString");
        Method bridgeMethod = Arrays.stream(GenericChild.class.getDeclaredMethods())
                .filter(Method::isBridge)
                .findFirst()
                .orElseThrow();
        Method syntheticMethod = Arrays.stream(SyntheticMethodSource.class.getDeclaredMethods())
                .filter(Method::isSynthetic)
                .findFirst()
                .orElseThrow();

        assertThat((boolean) isQueryMethod.invoke(null, abstractMethod)).isTrue();
        assertThat((boolean) isQueryMethod.invoke(null, defaultMethod)).isFalse();
        assertThat((boolean) isQueryMethod.invoke(null, objectMethod)).isFalse();
        assertThat((boolean) isQueryMethod.invoke(null, bridgeMethod)).isFalse();
        assertThat((boolean) isQueryMethod.invoke(null, syntheticMethod)).isFalse();
    }

    interface SampleRepo {
        void a();
        void b(int value);
        void b(String value);

        default void defaultMethod() {
        }
    }

    static class GenericBase<T> {
        T value() {
            return null;
        }
    }

    static class GenericChild extends GenericBase<String> {
        @Override
        String value() {
            return "x";
        }
    }

    static class SyntheticMethodSource {
        Runnable build() {
            return () -> {
            };
        }
    }
}
