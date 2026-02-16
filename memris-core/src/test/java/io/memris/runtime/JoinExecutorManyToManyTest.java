package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.storage.SimpleTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JoinExecutorManyToManyTest {

    @Test
    void leftJoinWithoutSourceSelectionShouldSelectAllRows() {
        GeneratedTable source = table(new Object[][] {
                { 10L },
                { 20L }
        });

        JoinExecutorManyToMany executor = new JoinExecutorManyToMany(
                null,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.LEFT);

        Selection selected = executor.filterJoin(source, source, null, null);
        assertThat(selected.toIntArray()).containsExactly(0, 1);
    }

    @Test
    void shouldThrowForUnsupportedJoinType() {
        GeneratedTable source = table(new Object[][] {
                { 1L }
        });

        JoinExecutorManyToMany executor = new JoinExecutorManyToMany(
                joinTable(Long.class, Long.class),
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.RIGHT);

        assertThatThrownBy(() -> executor.filterJoin(source, source, null, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Join type not supported");
    }

    @Test
    void shouldReturnEmptyForNullJoinTableAndMissingJoinColumns() {
        GeneratedTable source = table(new Object[][] {
                { 1L }
        });

        JoinExecutorManyToMany nullJoinTable = new JoinExecutorManyToMany(
                null,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        assertThat(nullJoinTable.filterJoin(source, source, null, null).toIntArray()).isEmpty();

        SimpleTable wrongColumns = new SimpleTable(
                "bad_join",
                List.of(
                        new SimpleTable.ColumnSpec<>("a", Long.class),
                        new SimpleTable.ColumnSpec<>("b", Long.class)));
        wrongColumns.insert(1L, 2L);

        JoinExecutorManyToMany missingColumns = new JoinExecutorManyToMany(
                wrongColumns,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        assertThat(missingColumns.filterJoin(source, source, null, null).toIntArray()).isEmpty();
    }

    @Test
    void shouldReturnEmptyWhenAllowedTargetsOrAllowedSourcesAreEmpty() {
        GeneratedTable source = table(new Object[][] {
                { 1L }
        });
        GeneratedTable targetWithNullId = table(new Object[][] {
                { null }
        });
        Selection onlyNullTarget = SelectionImpl.fromScanIndices(targetWithNullId, new int[] { 0 });

        SimpleTable join = joinTable(Long.class, String.class);
        join.insert(1L, "A");

        JoinExecutorManyToMany allowedTargetsEmpty = new JoinExecutorManyToMany(
                join,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Join.JoinType.INNER);

        assertThat(allowedTargetsEmpty.filterJoin(source, targetWithNullId, null, onlyNullTarget).toIntArray()).isEmpty();

        SimpleTable nullJoinValues = joinTable(Long.class, Long.class);
        nullJoinValues.insert(null, 10L);
        nullJoinValues.insert(1L, null);

        JoinExecutorManyToMany noAllowedSources = new JoinExecutorManyToMany(
                nullJoinValues,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        assertThat(noAllowedSources.filterJoin(source, source, null, null).toIntArray()).isEmpty();
    }

    @Test
    void shouldFilterUsingScanAllWhenSourceSelectionIsNull() {
        GeneratedTable source = table(new Object[][] {
                { 11L },
                { 22L },
                { 33L },
                { null }
        });
        SimpleTable join = joinTable(Long.class, Long.class);
        join.insert(11L, 100L);
        join.insert(33L, 101L);

        JoinExecutorManyToMany executor = new JoinExecutorManyToMany(
                join,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        Selection result = executor.filterJoin(source, source, null, null);
        assertThat(result.toIntArray()).containsExactly(0, 2);
    }

    @Test
    void shouldFilterProvidedSourceSelectionAndSkipInvalidRows() {
        GeneratedTable source = table(new Object[][] {
                { 11L },
                { 22L },
                { null }
        });
        SimpleTable join = joinTable(Long.class, Long.class);
        join.insert(22L, 200L);

        JoinExecutorManyToMany executor = new JoinExecutorManyToMany(
                join,
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        Selection sourceSelection = new SelectionImpl(new long[] {
                io.memris.storage.Selection.pack(-1, 1),
                io.memris.storage.Selection.pack(2, source.rowGeneration(2)),
                io.memris.storage.Selection.pack(1, source.rowGeneration(1))
        });

        Selection result = executor.filterJoin(source, source, sourceSelection, null);
        assertThat(result.toIntArray()).containsExactly(1);
    }

    @Test
    void shouldReadAllIdTypes() throws Exception {
        GeneratedTable table = table(new Object[][] {
                { 999L, 258, "SKU-9" }
        });

        JoinExecutorManyToMany executor = new JoinExecutorManyToMany(
                joinTable(Long.class, Long.class),
                "source_id",
                "target_id",
                0,
                TypeCodes.TYPE_LONG,
                0,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER);

        Method readId = JoinExecutorManyToMany.class.getDeclaredMethod(
                "readId", GeneratedTable.class, int.class, byte.class, int.class);
        readId.setAccessible(true);

        assertThat(readId.invoke(executor, table, 0, TypeCodes.TYPE_LONG, 0)).isEqualTo(999L);
        assertThat(readId.invoke(executor, table, 1, TypeCodes.TYPE_INT, 0)).isEqualTo(258);
        assertThat(readId.invoke(executor, table, 1, TypeCodes.TYPE_SHORT, 0)).isEqualTo((short) 258);
        assertThat(readId.invoke(executor, table, 1, TypeCodes.TYPE_BYTE, 0)).isEqualTo((byte) 2);
        assertThat(readId.invoke(executor, table, 2, TypeCodes.TYPE_STRING, 0)).isEqualTo("SKU-9");
        assertThat(readId.invoke(executor, table, 0, (byte) 99, 0)).isEqualTo(999L);
    }

    private static SimpleTable joinTable(Class<?> sourceType, Class<?> targetType) {
        return new SimpleTable(
                "join_table",
                List.of(
                        new SimpleTable.ColumnSpec<>("source_id", sourceType),
                        new SimpleTable.ColumnSpec<>("target_id", targetType)));
    }

    private static GeneratedTable table(Object[][] rows) {
        return new ArrayTable(rows);
    }

    private static final class ArrayTable implements GeneratedTable {
        private final Object[][] rows;

        private ArrayTable(Object[][] rows) {
            this.rows = rows;
        }

        @Override
        public int columnCount() {
            return rows.length == 0 ? 0 : rows[0].length;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return 0;
        }

        @Override
        public long allocatedCount() {
            return rows.length;
        }

        @Override
        public long liveCount() {
            return rows.length;
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
            return reader.get();
        }

        @Override
        public long lookupById(long id) {
            return -1;
        }

        @Override
        public long lookupByIdString(String id) {
            return -1;
        }

        @Override
        public void removeById(long id) {
        }

        @Override
        public long insertFrom(Object[] values) {
            throw new UnsupportedOperationException();
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
            return 1;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return 1;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return new int[0];
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return new int[0];
        }

        @Override
        public int[] scanAll() {
            int[] all = new int[rows.length];
            for (int i = 0; i < rows.length; i++) {
                all[i] = i;
            }
            return all;
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            Object value = rows[rowIndex][columnIndex];
            return value == null ? 0L : ((Number) value).longValue();
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            Object value = rows[rowIndex][columnIndex];
            return value == null ? 0 : ((Number) value).intValue();
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            Object value = rows[rowIndex][columnIndex];
            return value == null ? null : value.toString();
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return rows[rowIndex][columnIndex] != null;
        }
    }
}
