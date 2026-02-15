package io.memris.runtime.codegen;

import io.memris.core.FloatEncoding;
import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeExecutorGeneratorCoverageTest {

    @AfterEach
    void tearDown() {
        System.clearProperty(RuntimeExecutorGenerator.CODEGEN_ENABLED_PROPERTY);
        RuntimeExecutorGenerator.setConfiguration(null);
        RuntimeExecutorGenerator.clearCache();
    }

    @Test
    void shouldHonorConfigurationAndSystemPropertyForEnablement() {
        RuntimeExecutorGenerator.setConfiguration(null);
        System.setProperty(RuntimeExecutorGenerator.CODEGEN_ENABLED_PROPERTY, "false");
        assertThat(RuntimeExecutorGenerator.isEnabled()).isFalse();

        System.setProperty(RuntimeExecutorGenerator.CODEGEN_ENABLED_PROPERTY, "true");
        assertThat(RuntimeExecutorGenerator.isEnabled()).isTrue();

        RuntimeExecutorGenerator.setConfiguration(MemrisConfiguration.builder().codegenEnabled(false).build());
        assertThat(RuntimeExecutorGenerator.isEnabled()).isFalse();
    }

    @Test
    void shouldExecuteFieldValueAndStorageReadersInFallbackMode() {
        RuntimeExecutorGenerator.setConfiguration(MemrisConfiguration.builder().codegenEnabled(false).build());
        RuntimeExecutorGenerator.clearCache();

        var table = new RecordingTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.stringValue(0, "abc");
        table.longValue(1, 99L);
        table.intValue(2, 7);

        TypeConverter<String, String> converter = new PrefixConverter();

        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(0, TypeCodes.TYPE_STRING, converter)
                .read(table, 0)).isEqualTo("converted:abc");
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(1, TypeCodes.TYPE_LONG, null)
                .read(table, 0)).isEqualTo(99L);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(1, TypeCodes.TYPE_INSTANT, null)
                .read(table, 0)).isEqualTo(99L);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_INT, null)
                .read(table, 0)).isEqualTo(7);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_BOOLEAN, null)
                .read(table, 0)).isEqualTo(true);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_BYTE, null)
                .read(table, 0)).isEqualTo((byte) 7);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_SHORT, null)
                .read(table, 0)).isEqualTo((short) 7);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_CHAR, null)
                .read(table, 0)).isEqualTo((char) 7);

        table.intValue(2, FloatEncoding.floatToSortableInt(1.5f));
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, TypeCodes.TYPE_FLOAT, null)
                .read(table, 0)).isEqualTo(1.5f);

        table.longValue(1, FloatEncoding.doubleToSortableLong(2.5d));
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(1, TypeCodes.TYPE_DOUBLE, null)
                .read(table, 0)).isEqualTo(2.5d);

        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(2, (byte) 127, null).read(table, 0))
                .isEqualTo(table.readInt(2, 0));

        table.present(0, false);
        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(0, TypeCodes.TYPE_STRING, null).read(table, 0))
                .isNull();

        table.stringValue(0, "raw");
        table.longValue(1, 123L);
        table.intValue(2, 42);
        assertThat(RuntimeExecutorGenerator.generateStorageValueReader(0, TypeCodes.TYPE_STRING).read(table, 0))
                .isEqualTo("raw");
        assertThat(RuntimeExecutorGenerator.generateStorageValueReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(123L);
        assertThat(RuntimeExecutorGenerator.generateStorageValueReader(2, TypeCodes.TYPE_INT).read(table, 0))
                .isEqualTo(42);
        assertThat(RuntimeExecutorGenerator.generateStorageValueReader(2, (byte) 99).read(table, 0))
                .isEqualTo(42);
    }

    @Test
    void shouldExecuteFkReadersAndTargetResolversInFallbackMode() {
        RuntimeExecutorGenerator.setConfiguration(MemrisConfiguration.builder().codegenEnabled(false).build());
        RuntimeExecutorGenerator.clearCache();

        var table = new RecordingTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.stringValue(0, "id-1");
        table.longValue(1, 77L);
        table.longValue(2, 33L);
        table.intValue(2, 33);

        assertThat(RuntimeExecutorGenerator.generateFkReader(0, TypeCodes.TYPE_STRING).read(table, 0))
                .isEqualTo("id-1");
        assertThat(RuntimeExecutorGenerator.generateFkReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(77L);
        assertThat(RuntimeExecutorGenerator.generateFkReader(2, TypeCodes.TYPE_INT).read(table, 0))
                .isEqualTo(33);
        assertThat(RuntimeExecutorGenerator.generateFkReader(2, (byte) 127).read(table, 0))
                .isEqualTo(33L);

        table.lookupByIdStringResult = Selection.pack(15, 1);
        var idStringResolver = RuntimeExecutorGenerator.generateTargetRowResolver(true, TypeCodes.TYPE_STRING, 1);
        assertThat(idStringResolver.resolve(table, "id-1")).isEqualTo(15);
        assertThat(idStringResolver.resolve(table, null)).isEqualTo(-1);

        table.lookupByIdResult = Selection.pack(19, 1);
        var idLongResolver = RuntimeExecutorGenerator.generateTargetRowResolver(true, TypeCodes.TYPE_LONG, 1);
        var idIntResolver = RuntimeExecutorGenerator.generateTargetRowResolver(true, TypeCodes.TYPE_INT, 1);
        var idDefaultResolver = RuntimeExecutorGenerator.generateTargetRowResolver(true, (byte) 90, 1);
        assertThat(idLongResolver.resolve(table, 99L)).isEqualTo(19);
        assertThat(idIntResolver.resolve(table, 99)).isEqualTo(19);
        assertThat(idDefaultResolver.resolve(table, 99)).isEqualTo(19);

        table.scanEqualsStringResult = new int[] { 8, 9 };
        table.scanEqualsLongResult = new int[] { 6 };
        table.scanEqualsIntResult = new int[] { 5 };
        var stringResolver = RuntimeExecutorGenerator.generateTargetRowResolver(false, TypeCodes.TYPE_STRING, 0);
        var longResolver = RuntimeExecutorGenerator.generateTargetRowResolver(false, TypeCodes.TYPE_LONG, 1);
        var intResolver = RuntimeExecutorGenerator.generateTargetRowResolver(false, TypeCodes.TYPE_INT, 2);
        var defaultResolver = RuntimeExecutorGenerator.generateTargetRowResolver(false, (byte) 90, 1);
        assertThat(stringResolver.resolve(table, "id-1")).isEqualTo(8);
        assertThat(longResolver.resolve(table, 77L)).isEqualTo(6);
        assertThat(intResolver.resolve(table, 33)).isEqualTo(5);
        assertThat(defaultResolver.resolve(table, 77L)).isEqualTo(6);
        assertThat(stringResolver.resolve(table, null)).isEqualTo(-1);
    }

    @Test
    void shouldExecuteGroupingBetweenAndInExecutorsInFallbackMode() {
        RuntimeExecutorGenerator.setConfiguration(MemrisConfiguration.builder().codegenEnabled(false).build());
        RuntimeExecutorGenerator.clearCache();

        var table = new RecordingTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.stringValue(0, "x");
        table.longValue(1, 123L);
        table.intValue(2, 65);

        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(0, TypeCodes.TYPE_STRING).read(table, 0))
                .isEqualTo("x");
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(123L);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_INT).read(table, 0))
                .isEqualTo(65);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_BOOLEAN).read(table, 0))
                .isEqualTo(true);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_BYTE).read(table, 0))
                .isEqualTo((byte) 65);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_SHORT).read(table, 0))
                .isEqualTo((short) 65);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_CHAR).read(table, 0))
                .isEqualTo('A');

        table.intValue(2, FloatEncoding.floatToSortableInt(1.25f));
        table.longValue(1, FloatEncoding.doubleToSortableLong(3.5d));
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_FLOAT).read(table, 0))
                .isEqualTo(1.25f);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(1, TypeCodes.TYPE_DOUBLE).read(table, 0))
                .isEqualTo(3.5d);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, (byte) 100).read(table, 0))
                .isEqualTo(table.readInt(2, 0));

        table.scanBetweenLongResult = new int[] { 2, 4 };
        table.scanBetweenIntResult = new int[] { 1, 3 };
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_LONG)
                .execute(table, 0, new Object[] { 10L, 30L }))).containsExactly(2, 4);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, 0, new Object[] { 1, 2 }))).containsExactly(1, 3);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_CHAR)
                .execute(table, 0, new Object[] { 'A', "C" }))).containsExactly(1, 3);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_FLOAT)
                .execute(table, 0, new Object[] { 1.0f, 2.0f }))).containsExactly(1, 3);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_DOUBLE)
                .execute(table, 0, new Object[] { 1.0d, 2.0d }))).containsExactly(2, 4);

        var temporalArgs = new Object[] {
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 12, 31)
        };
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_LOCAL_DATE)
                .execute(table, 0, temporalArgs))).containsExactly(2, 4);

        assertThatThrownBy(() -> RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, 0, new Object[] { 1 }))
                        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_STRING)
                .execute(table, 0, new Object[] { "a", "b" }))
                        .isInstanceOf(UnsupportedOperationException.class);

        table.scanInStringResult = new int[] { 7 };
        table.scanInLongResult = new int[] { 6 };
        table.scanInIntResult = new int[] { 5 };

        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(0, TypeCodes.TYPE_STRING)
                .execute(table, List.of("x", "y")))).containsExactly(7);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(1, TypeCodes.TYPE_LONG)
                .execute(table, new Object[] { 1L, 2L }))).containsExactly(6);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(1, TypeCodes.TYPE_DATE)
                .execute(table, new Date[] { new Date(10), new Date(20) }))).containsExactly(6);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, new int[] { 1, 2 }))).containsExactly(5);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_CHAR)
                .execute(table, new char[] { 'A', 'B' }))).containsExactly(5);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_BOOLEAN)
                .execute(table, new boolean[] { true, false }))).containsExactly(5);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_FLOAT)
                .execute(table, new float[] { 1.0f, 2.0f }))).containsExactly(5);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, null))).isEmpty();
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(0, TypeCodes.TYPE_STRING)
                .execute(table, "x"))).containsExactly(7);
        assertThatThrownBy(() -> RuntimeExecutorGenerator.generateInListExecutor(2, (byte) 111)
                .execute(table, List.of(1, 2)))
                        .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldExecuteGeneratedExecutorsWhenCodegenEnabled() {
        RuntimeExecutorGenerator.setConfiguration(MemrisConfiguration.builder().codegenEnabled(true).build());
        RuntimeExecutorGenerator.clearCache();

        var table = new RecordingTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.stringValue(0, "codegen");
        table.longValue(1, 777L);
        table.intValue(2, 42);
        table.lookupByIdResult = Selection.pack(3, 1);
        table.scanEqualsLongResult = new int[] { 3 };
        table.scanEqualsStringResult = new int[] { 4 };
        table.scanEqualsIntResult = new int[] { 5 };
        table.scanBetweenIntResult = new int[] { 9 };
        table.scanBetweenLongResult = new int[] { 10 };
        table.scanInIntResult = new int[] { 8 };
        table.scanInLongResult = new int[] { 7 };
        table.scanInStringResult = new int[] { 6 };

        assertThat(RuntimeExecutorGenerator.generateFieldValueReader(0, TypeCodes.TYPE_STRING, null)
                .read(table, 0)).isEqualTo("codegen");
        assertThat(RuntimeExecutorGenerator.generateFkReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(777L);
        assertThat(RuntimeExecutorGenerator.generateTargetRowResolver(true, TypeCodes.TYPE_LONG, 0)
                .resolve(table, 777L)).isEqualTo(3);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_INT).read(table, 0))
                .isEqualTo(42);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, 0, new Object[] { 1, 2 }))).containsExactly(9);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_LONG)
                .execute(table, 0, new Object[] { 1L, 2L }))).containsExactly(10);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_CHAR)
                .execute(table, 0, new Object[] { 'A', "C" }))).containsExactly(9);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(2, TypeCodes.TYPE_FLOAT)
                .execute(table, 0, new Object[] { 1.0f, 2.0f }))).containsExactly(9);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_DOUBLE)
                .execute(table, 0, new Object[] { 1.0d, 2.0d }))).containsExactly(10);
        assertThat(rows(RuntimeExecutorGenerator.generateBetweenExecutor(1, TypeCodes.TYPE_LOCAL_DATE)
                .execute(table, 0, new Object[] { LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 31) })))
                        .containsExactly(10);
        assertThatThrownBy(() -> RuntimeExecutorGenerator.generateBetweenExecutor(0, TypeCodes.TYPE_STRING)
                .execute(table, 0, new Object[] { "a", "b" }))
                        .isInstanceOf(UnsupportedOperationException.class);

        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(0, TypeCodes.TYPE_STRING).read(table, 0))
                .isEqualTo("codegen");
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(777L);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_BOOLEAN).read(table, 0))
                .isEqualTo(true);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_BYTE).read(table, 0))
                .isEqualTo((byte) 42);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_SHORT).read(table, 0))
                .isEqualTo((short) 42);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_CHAR).read(table, 0))
                .isEqualTo('*');
        table.intValue(2, FloatEncoding.floatToSortableInt(1.75f));
        table.longValue(1, FloatEncoding.doubleToSortableLong(2.25d));
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, TypeCodes.TYPE_FLOAT).read(table, 0))
                .isEqualTo(1.75f);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(1, TypeCodes.TYPE_DOUBLE).read(table, 0))
                .isEqualTo(2.25d);
        assertThat(RuntimeExecutorGenerator.generateGroupingValueReader(2, (byte) 101).read(table, 0))
                .isEqualTo(table.readInt(2, 0));

        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(2, TypeCodes.TYPE_INT)
                .execute(table, new int[] { 1, 2 }))).containsExactly(8);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(1, TypeCodes.TYPE_LONG)
                .execute(table, new long[] { 1L, 2L }))).containsExactly(7);
        assertThat(rows(RuntimeExecutorGenerator.generateInListExecutor(0, TypeCodes.TYPE_STRING)
                .execute(table, new String[] { "codegen" }))).containsExactly(6);
        assertThatThrownBy(() -> RuntimeExecutorGenerator.generateInListExecutor(0, (byte) 111)
                .execute(table, List.of(1)))
                        .isInstanceOf(UnsupportedOperationException.class);

        assertThat(RuntimeExecutorGenerator.generateStorageValueReader(1, TypeCodes.TYPE_LONG).read(table, 0))
                .isEqualTo(table.readLong(1, 0));
    }

    @Test
    void shouldCoverPrivateHelperConversionMethodsViaReflection() {
        var table = new RecordingTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.stringValue(0, "v");
        table.longValue(1, 88L);
        table.intValue(2, 22);
        table.lookupByIdResult = Selection.pack(12, 1);
        table.lookupByIdStringResult = Selection.pack(13, 1);
        table.scanEqualsIntResult = new int[] { 2 };
        table.scanEqualsLongResult = new int[] { 3 };
        table.scanEqualsStringResult = new int[] { 4 };

        var fallbackGrouping = (RuntimeExecutorGenerator.GroupingValueReader) invokePrivate(
                "createFallbackGroupingValueReader",
                new Class<?>[] { int.class, byte.class },
                new Object[] { 2, TypeCodes.TYPE_INT });
        assertThat(fallbackGrouping.read(table, 0)).isEqualTo(22);

        assertThat((long[]) invokePrivate("toLongArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_LONG, new int[] { 1, 2 } }))
                        .containsExactly(1L, 2L);
        assertThat((long[]) invokePrivate("toLongArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_LOCAL_DATE, List.of(LocalDate.of(2024, 1, 1)) }))
                        .containsExactly(LocalDate.of(2024, 1, 1).toEpochDay());
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_BOOLEAN, new boolean[] { true, false } }))
                        .containsExactly(1, 0);
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INT, new byte[] { 1, 2 } }))
                        .containsExactly(1, 2);
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INT, new short[] { 3, 4 } }))
                        .containsExactly(3, 4);
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_CHAR, new char[] { 'A' } }))
                        .containsExactly((int) 'A');
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_FLOAT, new float[] { 1.5f } }))
                        .containsExactly(FloatEncoding.floatToSortableInt(1.5f));
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INT, new Object[] { 9, 10L } }))
                        .containsExactly(9, 10);
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INT, List.of(11, 12) }))
                        .containsExactly(11, 12);
        assertThat((int[]) invokePrivate("toIntArray",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INT, 13L }))
                        .containsExactly(13);
        assertThat((String[]) invokePrivate("toStringArray",
                new Class<?>[] { Object.class },
                new Object[] { List.of("a", "b") }))
                        .containsExactly("a", "b");
        assertThat((String[]) invokePrivate("toStringArray",
                new Class<?>[] { Object.class },
                new Object[] { new String[] { "x", "y" } }))
                        .containsExactly("x", "y");
        assertThat((String[]) invokePrivate("toStringArray",
                new Class<?>[] { Object.class },
                new Object[] { new Object[] { "x", 1 } }))
                        .containsExactly("x", "1");
        assertThat((String[]) invokePrivate("toStringArray",
                new Class<?>[] { Object.class },
                new Object[] { 99 }))
                        .containsExactly("99");

        assertThat((Long) invokePrivate("convertToLong",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_DOUBLE, 3.5d }))
                        .isEqualTo(FloatEncoding.doubleToSortableLong(3.5d));
        assertThat((Integer) invokePrivate("convertToInt",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_CHAR, 'K' }))
                        .isEqualTo((int) 'K');
        assertThat((Long) invokePrivate("convertToEpochLong",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_INSTANT, Instant.ofEpochMilli(1234L) }))
                        .isEqualTo(1234L);
        assertThat((Long) invokePrivate("convertToEpochLong",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_LOCAL_DATE, LocalDate.ofEpochDay(2) }))
                        .isEqualTo(2L);
        assertThat((Long) invokePrivate("convertToEpochLong",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_LOCAL_DATE_TIME, LocalDateTime.ofEpochSecond(1, 0, java.time.ZoneOffset.UTC) }))
                        .isEqualTo(1_000L);
        assertThat((Long) invokePrivate("convertToEpochLong",
                new Class<?>[] { byte.class, Object.class },
                new Object[] { TypeCodes.TYPE_DATE, new Date(777L) }))
                        .isEqualTo(777L);
    }

    private static Object invokePrivate(String name, Class<?>[] parameterTypes, Object[] args) {
        try {
            Method method = RuntimeExecutorGenerator.class.getDeclaredMethod(name, parameterTypes);
            method.setAccessible(true);
            return method.invoke(null, args);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static int[] rows(Selection selection) {
        return selection.toIntArray();
    }

    private static final class PrefixConverter implements TypeConverter<String, String> {
        @Override
        public Class<String> javaType() {
            return String.class;
        }

        @Override
        public Class<String> storageType() {
            return String.class;
        }

        @Override
        public String toStorage(String javaValue) {
            return javaValue;
        }

        @Override
        public String fromStorage(String storageValue) {
            return storageValue == null ? null : "converted:" + storageValue;
        }
    }

    private static final class RecordingTable implements GeneratedTable {
        private final Map<Integer, Boolean> present = new HashMap<>();
        private final Map<Integer, Integer> ints = new HashMap<>();
        private final Map<Integer, Long> longs = new HashMap<>();
        private final Map<Integer, String> strings = new HashMap<>();

        private long lookupByIdResult = -1;
        private long lookupByIdStringResult = -1;
        private int[] scanEqualsIntResult = new int[0];
        private int[] scanEqualsLongResult = new int[0];
        private int[] scanEqualsStringResult = new int[0];
        private int[] scanBetweenIntResult = new int[0];
        private int[] scanBetweenLongResult = new int[0];
        private int[] scanInIntResult = new int[0];
        private int[] scanInLongResult = new int[0];
        private int[] scanInStringResult = new int[0];

        void present(int column, boolean value) {
            present.put(column, value);
        }

        void intValue(int column, int value) {
            ints.put(column, value);
        }

        void longValue(int column, long value) {
            longs.put(column, value);
        }

        void stringValue(int column, String value) {
            strings.put(column, value);
        }

        @Override
        public int columnCount() {
            return 0;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return 0;
        }

        @Override
        public long allocatedCount() {
            return 0;
        }

        @Override
        public long liveCount() {
            return 0;
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
            return reader.get();
        }

        @Override
        public long lookupById(long id) {
            return lookupByIdResult;
        }

        @Override
        public long lookupByIdString(String id) {
            return lookupByIdStringResult;
        }

        @Override
        public void removeById(long id) {
        }

        @Override
        public long insertFrom(Object[] values) {
            return 0;
        }

        @Override
        public void tombstone(long ref) {
        }

        @Override
        public boolean isLive(long ref) {
            return true;
        }

        @Override
        public long currentGeneration() {
            return 0;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return 1L + rowIndex;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return scanEqualsLongResult;
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return scanEqualsIntResult;
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return scanEqualsStringResult;
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            return scanBetweenIntResult;
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return scanBetweenLongResult;
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return scanInLongResult;
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            return scanInIntResult;
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return scanInStringResult;
        }

        @Override
        public int[] scanAll() {
            return new int[0];
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return longs.getOrDefault(columnIndex, 0L);
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return ints.getOrDefault(columnIndex, 0);
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return strings.get(columnIndex);
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return present.getOrDefault(columnIndex, false);
        }
    }
}
