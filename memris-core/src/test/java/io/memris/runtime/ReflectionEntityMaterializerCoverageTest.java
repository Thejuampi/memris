package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionEntityMaterializerCoverageTest {

    @Test
    void materializeAssignsPrimitiveValuesAndSkipsMissingReference() {
        var metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        var materializer = new ReflectionEntityMaterializer<>(metadata);

        var table = new SimpleTable();
        TestEntity entity = materializer.materialize(table, 0);

        assertThat(entity.id).isEqualTo(11L);
        assertThat(entity.count).isEqualTo(7);
        assertThat(entity.name).isNull();
    }

    @Test
    void readValueCoversConversionsAndUnknownType() throws Exception {
        Method readValue = ReflectionEntityMaterializer.class.getDeclaredMethod(
                "readValue",
                GeneratedTable.class,
                int.class,
                int.class,
                byte.class);
        readValue.setAccessible(true);
        var table = new SimpleTable();

        assertThat(readValue.invoke(null, table, 0, 0, TypeCodes.TYPE_BOOLEAN)).isEqualTo(true);
        assertThat(readValue.invoke(null, table, 0, 0, TypeCodes.TYPE_BYTE)).isEqualTo((byte) 1);
        assertThat(readValue.invoke(null, table, 0, 0, TypeCodes.TYPE_SHORT)).isEqualTo((short) 1);
        assertThat(readValue.invoke(null, table, 3, 0, TypeCodes.TYPE_FLOAT)).isEqualTo(1.0f);
        assertThat(readValue.invoke(null, table, 3, 0, TypeCodes.TYPE_DOUBLE)).isEqualTo(2.0d);
        assertThat(readValue.invoke(null, table, 2, 0, TypeCodes.TYPE_CHAR)).isEqualTo('A');

        assertThatThrownBy(() -> readValue.invoke(null, table, 0, 0, (byte) 127))
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Entity
    static class TestEntity {
        @Id
        Long id;
        int count;
        String name;
    }

    static class SimpleTable implements GeneratedTable {
        @Override public int columnCount() { return 4; }
        @Override public byte typeCodeAt(int columnIndex) { return switch (columnIndex) {
            case 0 -> TypeCodes.TYPE_LONG;
            case 1 -> TypeCodes.TYPE_INT;
            default -> TypeCodes.TYPE_STRING;
        }; }
        @Override public long allocatedCount() { return 1; }
        @Override public long liveCount() { return 1; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) { }
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[] { 0 }; }
        @Override public long readLong(int columnIndex, int rowIndex) { return switch (columnIndex) {
            case 0 -> 11L;
            case 3 -> io.memris.core.FloatEncoding.doubleToSortableLong(2.0d);
            default -> 0L;
        }; }
        @Override public int readInt(int columnIndex, int rowIndex) { return switch (columnIndex) {
            case 0 -> 1;
            case 1 -> 7;
            case 2 -> 'A';
            case 3 -> io.memris.core.FloatEncoding.floatToSortableInt(1.0f);
            default -> 0;
        }; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return columnIndex != 2; }
    }
}
