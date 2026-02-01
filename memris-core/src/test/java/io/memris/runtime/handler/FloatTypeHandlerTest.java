package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FloatTypeHandlerTest {

    @Test
    void executeBetweenRangeUsesSortableEncoding() {
        FloatTypeHandler handler = new FloatTypeHandler();
        GeneratedTable table = new FakeFloatTable(new float[] { -5.5f, -1.0f, -0.25f, 2.5f, 7.5f });

        Selection selection = handler.executeBetweenRange(table, 0, -2.0f, -0.1f);

        assertThat(selection.toIntArray())
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void executeBetweenRangeAcrossZeroUsesSortableEncoding() {
        FloatTypeHandler handler = new FloatTypeHandler();
        GeneratedTable table = new FakeFloatTable(new float[] { -5.5f, -1.0f, 0.0f, 2.5f, 7.5f });

        Selection selection = handler.executeBetweenRange(table, 0, -2.0f, 3.0f);

        assertThat(selection.toIntArray())
                .containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void executeBetweenDelegatedToKernel() {
        FloatTypeHandler handler = new FloatTypeHandler();

        assertThatThrownBy(() -> handler.executeCondition(new FakeFloatTable(new float[] { 1.0f }), 0,
                LogicalQuery.Operator.BETWEEN, 1.0f, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("HeapRuntimeKernel");
    }

    private static final class FakeFloatTable implements GeneratedTable {
        private final int[] values;

        private FakeFloatTable(float[] floats) {
            this.values = new int[floats.length];
            for (int i = 0; i < floats.length; i++) {
                values[i] = FloatEncoding.floatToSortableInt(floats[i]);
            }
        }

        @Override
        public int columnCount() {
            return 1;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return TypeCodes.TYPE_FLOAT;
        }

        @Override
        public long allocatedCount() {
            return values.length;
        }

        @Override
        public long liveCount() {
            return values.length;
        }

        @Override
        public long lookupById(long id) {
            return -1L;
        }

        @Override
        public long lookupByIdString(String id) {
            return -1L;
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
            return 0L;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return 0L;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] == value) {
                    matches[count++] = i;
                }
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                int value = values[i];
                if (value >= min && value <= max) {
                    matches[count++] = i;
                }
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            int[] matches = new int[this.values.length];
            int count = 0;
            for (int i = 0; i < this.values.length; i++) {
                int candidate = this.values[i];
                for (int value : values) {
                    if (candidate == value) {
                        matches[count++] = i;
                        break;
                    }
                }
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                rows[i] = i;
            }
            return rows;
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return values[rowIndex];
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return true;
        }
    }
}
