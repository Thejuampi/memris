package io.memris.runtime;

import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbstractTypeHandlerTest {

    @Test
    void shouldExecuteAllCoreOperatorsIncludingNegationsAndNullChecks() {
        TestTable table = new TestTable();
        DummyIntHandler handler = new DummyIntHandler();

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, 2, false).toIntArray())
                .containsExactly(1);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.NE, 2, false).toIntArray())
                .containsExactlyInAnyOrder(0, 2, 3);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 1, false).toIntArray())
                .containsExactlyInAnyOrder(1, 2);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 2, false).toIntArray())
                .containsExactlyInAnyOrder(1, 2);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 3, false).toIntArray())
                .containsExactlyInAnyOrder(0, 1);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 2, false).toIntArray())
                .containsExactlyInAnyOrder(0, 1);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.BETWEEN, 2, false).toIntArray())
                .containsExactly(1);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 3, false).toIntArray())
                .containsExactly(2);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.NOT_IN, 3, false).toIntArray())
                .containsExactlyInAnyOrder(0, 1, 3);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IS_NULL, null, false).toIntArray())
                .containsExactlyInAnyOrder(1, 3);

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.NOT_NULL, null, false).toIntArray())
                .containsExactlyInAnyOrder(0, 2);
    }

    @Test
    void shouldThrowForUnsupportedOperator() {
        DummyIntHandler handler = new DummyIntHandler();

        assertThatThrownBy(() -> handler.executeCondition(new TestTable(), 0, LogicalQuery.Operator.CONTAINING, 1, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("not supported");
    }

    private static final class DummyIntHandler extends AbstractTypeHandler<Integer> {
        @Override
        public byte getTypeCode() {
            return 0;
        }

        @Override
        public Class<Integer> getJavaType() {
            return Integer.class;
        }

        @Override
        public Integer convertValue(Object value) {
            return (Integer) value;
        }

        @Override
        protected Selection executeEquals(GeneratedTable table, int columnIndex, Integer value, boolean ignoreCase) {
            return createSelection(table, table.scanEqualsInt(columnIndex, value));
        }

        @Override
        protected Selection executeGreaterThan(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanBetweenInt(columnIndex, value + 1, Integer.MAX_VALUE));
        }

        @Override
        protected Selection executeGreaterThanOrEqual(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanBetweenInt(columnIndex, value, Integer.MAX_VALUE));
        }

        @Override
        protected Selection executeLessThan(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, value - 1));
        }

        @Override
        protected Selection executeLessThanOrEqual(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanBetweenInt(columnIndex, Integer.MIN_VALUE, value));
        }

        @Override
        protected Selection executeBetween(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanBetweenInt(columnIndex, value, value));
        }

        @Override
        protected Selection executeIn(GeneratedTable table, int columnIndex, Integer value) {
            return createSelection(table, table.scanInInt(columnIndex, new int[] { value }));
        }
    }

    private static final class TestTable implements GeneratedTable {
        private final int[] all = new int[] { 0, 1, 2, 3 };

        @Override
        public int columnCount() {
            return 1;
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
            return 1;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return value == 2 ? new int[] { 1 } : new int[0];
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
            if (min == 2 && max == Integer.MAX_VALUE) {
                return new int[] { 1, 2 };
            }
            if (min == 2 && max == 2) {
                return new int[] { 1 };
            }
            if (min == Integer.MIN_VALUE && max == 2) {
                return new int[] { 0, 1 };
            }
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
            if (values.length == 1 && values[0] == 3) {
                return new int[] { 2 };
            }
            return new int[0];
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return new int[0];
        }

        @Override
        public int[] scanAll() {
            return all;
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return null;
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return rowIndex == 0 || rowIndex == 2;
        }
    }
}
