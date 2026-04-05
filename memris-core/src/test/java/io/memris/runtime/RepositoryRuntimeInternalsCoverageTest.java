package io.memris.runtime;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeInternalsCoverageTest {

    @Test
    void intAccumulatorIntersectionUnionAndMaterialization() throws Exception {
        Class<?> accumulatorClass = Class.forName("io.memris.runtime.RepositoryRuntime$IntAccumulator");
        Constructor<?> ctor = accumulatorClass.getDeclaredConstructor(int[].class);
        ctor.setAccessible(true);
        Method intersect = accumulatorClass.getDeclaredMethod("intersect", int[].class);
        Method union = accumulatorClass.getDeclaredMethod("union", accumulatorClass);
        Method toIntArray = accumulatorClass.getDeclaredMethod("toIntArray");
        Method size = accumulatorClass.getDeclaredMethod("size");
        Method isEmpty = accumulatorClass.getDeclaredMethod("isEmpty");
        intersect.setAccessible(true);
        union.setAccessible(true);
        toIntArray.setAccessible(true);
        size.setAccessible(true);
        isEmpty.setAccessible(true);

        Object small = ctor.newInstance((Object) new int[] { 5, 1, 3 });
        Object large = ctor.newInstance((Object) IntStream.range(0, 100).toArray());
        Object empty = ctor.newInstance((Object) new int[] { 1 });
        Object smallForUnion = ctor.newInstance((Object) new int[] { 3 });

        intersect.invoke(small, (Object) new int[] { 3, 7 });
        assertThat((int[]) toIntArray.invoke(small)).containsExactly(3);

        // force small->bitset conversion path
        int[] largeOther = IntStream.range(0, 80).toArray();
        intersect.invoke(small, (Object) largeOther);
        assertThat((int[]) toIntArray.invoke(small)).containsExactly(3);

        // bitset intersection path
        intersect.invoke(large, (Object) new int[] { 1, 50, 200 });
        assertThat((int[]) toIntArray.invoke(large)).containsExactly(1, 50);

        // create empty via intersection
        intersect.invoke(empty, (Object) new int[] { 99 });
        assertThat((boolean) isEmpty.invoke(empty)).isTrue();

        // union into empty accumulator
        union.invoke(empty, smallForUnion);
        assertThat((int[]) toIntArray.invoke(empty)).containsExactly(3);

        // union where other is empty should be no-op
        int before = (int) size.invoke(large);
        union.invoke(large, ctor.newInstance((Object) new int[0]));
        assertThat((int) size.invoke(large)).isEqualTo(before);
    }

    @Test
    void distinctKeyOfHandlesVariousRowIndices() throws Exception {
        Class<?> distinctKeyClass = Class.forName("io.memris.runtime.RepositoryRuntime$DistinctKey");
        Method of = distinctKeyClass.getDeclaredMethod("of", int.class);
        Method rowIndex = distinctKeyClass.getDeclaredMethod("rowIndex");
        of.setAccessible(true);
        rowIndex.setAccessible(true);

        Object key42 = of.invoke(null, 42);
        Object key99 = of.invoke(null, 99);

        assertThat(rowIndex.invoke(key42)).isEqualTo(42);
        assertThat(rowIndex.invoke(key99)).isEqualTo(99);
        assertThat(key42).isNotEqualTo(key99);
        assertThat(key42).isEqualTo(of.invoke(null, 42));
    }
}
