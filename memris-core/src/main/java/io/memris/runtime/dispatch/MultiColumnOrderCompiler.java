package io.memris.runtime.dispatch;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.storage.GeneratedTable;

public final class MultiColumnOrderCompiler {

    private MultiColumnOrderCompiler() {
    }

    public interface OrderKeyBuilder {
        OrderKey build(GeneratedTable table, int[] rows);
    }

    public static OrderKeyBuilder[] compileBuilders(CompiledQuery.CompiledOrderBy[] orderBy,
            GeneratedTable table,
            boolean[] primitiveNonNullColumns) {
        var builders = new OrderKeyBuilder[orderBy.length];
        for (var i = 0; i < orderBy.length; i++) {
            var columnIndex = orderBy[i].columnIndex();
            var ascending = orderBy[i].ascending();
            var typeCode = table.typeCodeAt(columnIndex);
            builders[i] = buildOrderKeyBuilder(columnIndex,
                    ascending,
                    typeCode,
                    isPrimitiveNonNullColumn(primitiveNonNullColumns, columnIndex));
        }
        return builders;
    }

    public static int[] sortByCompiledBuilders(GeneratedTable table, int[] rows, OrderKeyBuilder[] builders) {
        var result = rows.clone();
        var keys = buildOrderKeys(table, result, builders);
        quickSortMulti(result, keys, 0, result.length - 1);
        return result;
    }

    private static boolean isPrimitiveNonNullColumn(boolean[] primitiveNonNullColumns, int columnIndex) {
        return primitiveNonNullColumns != null
                && columnIndex >= 0
                && columnIndex < primitiveNonNullColumns.length
                && primitiveNonNullColumns[columnIndex];
    }

    private static OrderKeyBuilder buildOrderKeyBuilder(int columnIndex,
            boolean ascending,
            byte typeCode,
            boolean primitiveNonNull) {
        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_BOOLEAN,
                    TypeCodes.TYPE_BYTE,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR ->
                primitiveNonNull
                        ? new IntOrderKeyBuilderNonNull(columnIndex, ascending)
                        : new IntOrderKeyBuilder(columnIndex, ascending);
            case TypeCodes.TYPE_FLOAT -> primitiveNonNull
                    ? new FloatOrderKeyBuilderNonNull(columnIndex, ascending)
                    : new FloatOrderKeyBuilder(columnIndex, ascending);
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                primitiveNonNull
                        ? new LongOrderKeyBuilderNonNull(columnIndex, ascending)
                        : new LongOrderKeyBuilder(columnIndex, ascending);
            case TypeCodes.TYPE_DOUBLE -> primitiveNonNull
                    ? new DoubleOrderKeyBuilderNonNull(columnIndex, ascending)
                    : new DoubleOrderKeyBuilder(columnIndex, ascending);
            case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER ->
                new StringOrderKeyBuilder(columnIndex, ascending);
            default -> throw new IllegalArgumentException("Unsupported order type code: " + typeCode);
        };
    }

    private static OrderKey[] buildOrderKeys(GeneratedTable table, int[] rows, OrderKeyBuilder[] builders) {
        var keys = new OrderKey[builders.length];
        for (var i = 0; i < builders.length; i++) {
            keys[i] = builders[i].build(table, rows);
        }
        return keys;
    }

    private static void quickSortMulti(int[] rows, OrderKey[] keys, int low, int high) {
        var i = low;
        var j = high;
        var pivotIndex = low + ((high - low) >>> 1);

        while (i <= j) {
            while (compareMulti(i, pivotIndex, rows, keys) < 0) {
                i++;
            }
            while (compareMulti(j, pivotIndex, rows, keys) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                for (var key : keys) {
                    key.swap(i, j);
                }
                i++;
                j--;
            }
        }

        if (low < j) {
            quickSortMulti(rows, keys, low, j);
        }
        if (i < high) {
            quickSortMulti(rows, keys, i, high);
        }
    }

    private static int compareMulti(int left, int right, int[] rows, OrderKey[] keys) {
        if (left == right) {
            return 0;
        }
        for (var key : keys) {
            var cmp = key.compare(left, right);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(rows[left], rows[right]);
    }

    private static int compareNullable(boolean presentA, boolean presentB) {
        if (presentA == presentB) {
            return 0;
        }
        return presentA ? -1 : 1;
    }

    private static int compareStringValueOrder(String left, String right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return left.compareTo(right);
    }

    private static void swap(int[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(long[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(double[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(float[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(boolean[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(String[] arr, int i, int j) {
        var tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private interface OrderKey {
        int compare(int left, int right);

        void swap(int i, int j);
    }

    private static final class IntOrderKeyBuilder implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        IntOrderKeyBuilder(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var present = new boolean[rows.length];
            var values = new int[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int idx = i;
                final var row = rows[i];
                table.readWithSeqLock(row, () -> {
                    present[idx] = table.isPresent(columnIndex, row);
                    values[idx] = table.readInt(columnIndex, row);
                    return null;
                });
            }
            return new IntOrderKey(ascending, present, values);
        }
    }

    private static final class IntOrderKeyBuilderNonNull implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        IntOrderKeyBuilderNonNull(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var values = new int[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int ri = rows[i];
                values[i] = table.readWithSeqLock(ri, () -> table.readInt(columnIndex, ri));
            }
            return new IntOrderKeyNonNull(ascending, values);
        }
    }

    private static final class FloatOrderKeyBuilder implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        FloatOrderKeyBuilder(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var present = new boolean[rows.length];
            var values = new float[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int idx = i;
                final var row = rows[i];
                table.readWithSeqLock(row, () -> {
                    present[idx] = table.isPresent(columnIndex, row);
                    values[idx] = FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, row));
                    return null;
                });
            }
            return new FloatOrderKey(ascending, present, values);
        }
    }

    private static final class FloatOrderKeyBuilderNonNull implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        FloatOrderKeyBuilderNonNull(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var values = new float[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int ri = rows[i];
                values[i] = table.readWithSeqLock(ri,
                        () -> FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, ri)));
            }
            return new FloatOrderKeyNonNull(ascending, values);
        }
    }

    private static final class LongOrderKeyBuilder implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        LongOrderKeyBuilder(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var present = new boolean[rows.length];
            var values = new long[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int idx = i;
                final var row = rows[i];
                table.readWithSeqLock(row, () -> {
                    present[idx] = table.isPresent(columnIndex, row);
                    values[idx] = table.readLong(columnIndex, row);
                    return null;
                });
            }
            return new LongOrderKey(ascending, present, values);
        }
    }

    private static final class LongOrderKeyBuilderNonNull implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        LongOrderKeyBuilderNonNull(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var values = new long[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int ri = rows[i];
                values[i] = table.readWithSeqLock(ri, () -> table.readLong(columnIndex, ri));
            }
            return new LongOrderKeyNonNull(ascending, values);
        }
    }

    private static final class DoubleOrderKeyBuilder implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        DoubleOrderKeyBuilder(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var present = new boolean[rows.length];
            var values = new double[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int idx = i;
                final var row = rows[i];
                table.readWithSeqLock(row, () -> {
                    present[idx] = table.isPresent(columnIndex, row);
                    values[idx] = Double.longBitsToDouble(table.readLong(columnIndex, row));
                    return null;
                });
            }
            return new DoubleOrderKey(ascending, present, values);
        }
    }

    private static final class DoubleOrderKeyBuilderNonNull implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        DoubleOrderKeyBuilderNonNull(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var values = new double[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int ri = rows[i];
                values[i] = table.readWithSeqLock(ri, () -> Double.longBitsToDouble(table.readLong(columnIndex, ri)));
            }
            return new DoubleOrderKeyNonNull(ascending, values);
        }
    }

    private static final class StringOrderKeyBuilder implements OrderKeyBuilder {
        private final int columnIndex;
        private final boolean ascending;

        StringOrderKeyBuilder(int columnIndex, boolean ascending) {
            this.columnIndex = columnIndex;
            this.ascending = ascending;
        }

        @Override
        public OrderKey build(GeneratedTable table, int[] rows) {
            var present = new boolean[rows.length];
            var values = new String[rows.length];
            for (var i = 0; i < rows.length; i++) {
                final int idx = i;
                final var row = rows[i];
                table.readWithSeqLock(row, () -> {
                    values[idx] = table.readString(columnIndex, row);
                    present[idx] = values[idx] != null;
                    return null;
                });
            }
            return new StringOrderKey(ascending, present, values);
        }
    }

    private static final class IntOrderKey implements OrderKey {
        private final boolean ascending;
        private final boolean[] present;
        private final int[] keys;

        IntOrderKey(boolean ascending, boolean[] present, int[] keys) {
            this.ascending = ascending;
            this.present = present;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = Integer.compare(keys[left], keys[right]);
            }
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(present, i, j);
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class IntOrderKeyNonNull implements OrderKey {
        private final boolean ascending;
        private final int[] keys;

        IntOrderKeyNonNull(boolean ascending, int[] keys) {
            this.ascending = ascending;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = Integer.compare(keys[left], keys[right]);
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class LongOrderKey implements OrderKey {
        private final boolean ascending;
        private final boolean[] present;
        private final long[] keys;

        LongOrderKey(boolean ascending, boolean[] present, long[] keys) {
            this.ascending = ascending;
            this.present = present;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = Long.compare(keys[left], keys[right]);
            }
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(present, i, j);
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class LongOrderKeyNonNull implements OrderKey {
        private final boolean ascending;
        private final long[] keys;

        LongOrderKeyNonNull(boolean ascending, long[] keys) {
            this.ascending = ascending;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = Long.compare(keys[left], keys[right]);
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class FloatOrderKey implements OrderKey {
        private final boolean ascending;
        private final boolean[] present;
        private final float[] keys;

        FloatOrderKey(boolean ascending, boolean[] present, float[] keys) {
            this.ascending = ascending;
            this.present = present;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = Float.compare(keys[left], keys[right]);
            }
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(present, i, j);
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class FloatOrderKeyNonNull implements OrderKey {
        private final boolean ascending;
        private final float[] keys;

        FloatOrderKeyNonNull(boolean ascending, float[] keys) {
            this.ascending = ascending;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = Float.compare(keys[left], keys[right]);
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class DoubleOrderKey implements OrderKey {
        private final boolean ascending;
        private final boolean[] present;
        private final double[] keys;

        DoubleOrderKey(boolean ascending, boolean[] present, double[] keys) {
            this.ascending = ascending;
            this.present = present;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = Double.compare(keys[left], keys[right]);
            }
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(present, i, j);
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class DoubleOrderKeyNonNull implements OrderKey {
        private final boolean ascending;
        private final double[] keys;

        DoubleOrderKeyNonNull(boolean ascending, double[] keys) {
            this.ascending = ascending;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = Double.compare(keys[left], keys[right]);
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }

    private static final class StringOrderKey implements OrderKey {
        private final boolean ascending;
        private final boolean[] present;
        private final String[] keys;

        StringOrderKey(boolean ascending, boolean[] present, String[] keys) {
            this.ascending = ascending;
            this.present = present;
            this.keys = keys;
        }

        @Override
        public int compare(int left, int right) {
            var cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = compareStringValueOrder(keys[left], keys[right]);
            }
            return ascending ? cmp : -cmp;
        }

        @Override
        public void swap(int i, int j) {
            MultiColumnOrderCompiler.swap(present, i, j);
            MultiColumnOrderCompiler.swap(keys, i, j);
        }
    }
}
