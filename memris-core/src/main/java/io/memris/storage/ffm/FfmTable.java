package io.memris.storage.ffm;

import io.memris.kernel.Column;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.Table;
import io.memris.kernel.selection.IntEnumerator;
import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FfmTable implements Table {
    private static final int OFFSET_BITS = 16;
    private static final int DEFAULT_CAPACITY = 1024;

    private final String name;
    private final Arena arena;
    private final List<Column<?>> columns;
    private final Map<String, FfmColumn> columnsByName;
    private int size;
    private int capacity;

    public FfmTable(String name, Arena arena, List<ColumnSpec> specs) {
        this(name, arena, specs, DEFAULT_CAPACITY);
    }

    public FfmTable(String name, Arena arena, List<ColumnSpec> specs, int initialCapacity) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (arena == null) {
            throw new IllegalArgumentException("arena required");
        }
        if (specs == null || specs.isEmpty()) {
            throw new IllegalArgumentException("specs required");
        }
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        this.name = name;
        this.arena = arena;
        this.capacity = initialCapacity;
        this.columns = new ArrayList<>(specs.size());
        this.columnsByName = new HashMap<>(specs.size());
        for (ColumnSpec spec : specs) {
            Class<?> type = spec.type();
            // Note: switch on type.getName() - pattern matching on Class doesn't work well with generic capture
            FfmColumn<?> column = switch (type.getName()) {
                case "int", "java.lang.Integer" -> new FfmIntColumn(spec.name(), arena, capacity);
                case "long", "java.lang.Long" -> new FfmLongColumn(spec.name(), arena, capacity);
                case "boolean", "java.lang.Boolean" -> new FfmBooleanColumn(spec.name(), arena, capacity);
                case "byte", "java.lang.Byte" -> new FfmByteColumn(spec.name(), arena, capacity);
                case "short", "java.lang.Short" -> new FfmShortColumn(spec.name(), arena, capacity);
                case "float", "java.lang.Float" -> new FfmFloatColumn(spec.name(), arena, capacity);
                case "double", "java.lang.Double" -> new FfmDoubleColumn(spec.name(), arena, capacity);
                case "char", "java.lang.Character" -> new FfmCharColumn(spec.name(), arena, capacity);
                case "java.lang.String" -> new FfmStringColumnImpl(spec.name(), arena, capacity);
                default -> throw new IllegalArgumentException("Unsupported column type: " + type);
            };
            columns.add(column);
            columnsByName.put(spec.name(), column);
        }
    }

    public RowId insert(Object... values) {
        if (values == null || values.length != columns.size()) {
            throw new IllegalArgumentException("values length must match column count");
        }
        ensureCapacity(size + 1);
        for (int i = 0; i < columns.size(); i++) {
            FfmColumn<?> column = columnsByName.get(columns.get(i).name());
            column.set(size, values[i]);
        }
        long page = size >>> OFFSET_BITS;
        int offset = size & ((1 << OFFSET_BITS) - 1);
        size++;
        return new RowId(page, offset);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long rowCount() {
        return size;
    }

    @Override
    public Column<?> column(String name) {
        return columnsByName.get(name);
    }

    @Override
    public Collection<Column<?>> columns() {
        return Collections.unmodifiableList(columns);
    }

    public int getInt(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == int.class || col.type() == Integer.class) {
            return ((FfmIntColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not int: " + column);
    }

    public long getLong(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == long.class || col.type() == Long.class) {
            return ((FfmLongColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not long: " + column);
    }

    public boolean getBoolean(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == boolean.class || col.type() == Boolean.class) {
            return ((FfmBooleanColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not boolean: " + column);
    }

    public byte getByte(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == byte.class || col.type() == Byte.class) {
            return ((FfmByteColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not byte: " + column);
    }

    public short getShort(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == short.class || col.type() == Short.class) {
            return ((FfmShortColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not short: " + column);
    }

    public float getFloat(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == float.class || col.type() == Float.class) {
            return ((FfmFloatColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not float: " + column);
    }

    public double getDouble(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == double.class || col.type() == Double.class) {
            return ((FfmDoubleColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not double: " + column);
    }

    public char getChar(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == char.class || col.type() == Character.class) {
            return ((FfmCharColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not char: " + column);
    }

    public String getString(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.type() == String.class) {
            return ((FfmStringColumnImpl) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not String: " + column);
    }

    public SelectionVector scan(Predicate predicate, SelectionVectorFactory factory) {
        if (predicate == null) {
            return scanAll(factory);
        }
        return switch (predicate) {
            case Predicate.Comparison comp -> scanComparison(comp, factory);
            case Predicate.Between bet -> scanBetween(bet, factory);
            case Predicate.In in -> scanIn(in, factory);
            default -> scanAll(factory).filter(predicate, factory);
        };
    }

    public SelectionVector scanAll(SelectionVectorFactory factory) {
        MutableSelectionVector selection = factory.create(size);
        for (int i = 0; i < size; i++) {
            selection.add(i);
        }
        return selection;
    }

    private SelectionVector scanComparison(Predicate.Comparison comp, SelectionVectorFactory factory) {
        FfmColumn<?> col = columnsByName.get(comp.column());
        if (col == null) {
            return SelectionVectorFactory.defaultFactory().create(0);
        }
        Class<?> colType = col.type();
        Predicate.Operator op = comp.operator();

        if (colType == int.class || colType == Integer.class) {
            FfmIntColumn intCol = (FfmIntColumn) col;
            int value = ((Number) comp.value()).intValue();
            return switch (op) {
                case EQ -> intCol.scanEquals(value, size, factory);
                case GT -> intCol.scanGreaterThan(value, size, factory);
                case GTE -> intCol.scanGreaterThanOrEqual(value, size, factory);
                case LT -> intCol.scanLessThan(value, size, factory);
                case LTE -> intCol.scanLessThanOrEqual(value, size, factory);
                case NEQ -> {
                    SelectionVector equals = intCol.scanEquals(value, size, factory);
                    MutableSelectionVector result = factory.create(size);
                    for (int i = 0; i < size; i++) {
                        if (!equals.contains(i)) {
                            result.add(i);
                        }
                    }
                    yield result;
                }
                default -> SelectionVectorFactory.defaultFactory().create(0);
            };
        } else if (colType == long.class || colType == Long.class) {
            FfmLongColumn longCol = (FfmLongColumn) col;
            long value = ((Number) comp.value()).longValue();
            return switch (op) {
                case EQ -> longCol.scanEquals(value, size, factory);
                case GT -> longCol.scanGreaterThan(value, size, factory);
                case GTE -> longCol.scanGreaterThanOrEqual(value, size, factory);
                case LT -> longCol.scanLessThan(value, size, factory);
                case LTE -> longCol.scanLessThanOrEqual(value, size, factory);
                case NEQ -> {
                    SelectionVector equals = longCol.scanEquals(value, size, factory);
                    MutableSelectionVector result = factory.create(size);
                    for (int i = 0; i < size; i++) {
                        if (!equals.contains(i)) {
                            result.add(i);
                        }
                    }
                    yield result;
                }
                default -> SelectionVectorFactory.defaultFactory().create(0);
            };
        } else if (colType == String.class) {
            FfmStringColumnImpl strCol = (FfmStringColumnImpl) col;
            String value = (String) comp.value();
            return switch (op) {
                case EQ -> strCol.scanEquals(value, size, factory);
                case NEQ -> {
                    SelectionVector equals = strCol.scanEquals(value, size, factory);
                    MutableSelectionVector result = factory.create((int) (size * 0.9));
                    for (int i = 0; i < size; i++) {
                        if (!equals.contains(i)) {
                            result.add(i);
                        }
                    }
                    yield result;
                }
                case CONTAINING -> strCol.scanLike("%" + value + "%", size, factory);
                case STARTING_WITH -> strCol.scanStartingWith(value, size, factory);
                case ENDING_WITH -> strCol.scanEndingWith(value, size, factory);
                case LIKE -> strCol.scanLike(value, size, factory);
                case NOT_LIKE -> {
                    SelectionVector matches = strCol.scanLike(value, size, factory);
                    MutableSelectionVector result = factory.create((int) (size * 0.9));
                    for (int i = 0; i < size; i++) {
                        if (!matches.contains(i)) {
                            result.add(i);
                        }
                    }
                    yield result;
                }
                case IGNORE_CASE -> strCol.scanEqualsIgnoreCase(value, size, factory);
                default -> SelectionVectorFactory.defaultFactory().create(0);
            };
        }
        return SelectionVectorFactory.defaultFactory().create(0);
    }

    private SelectionVector scanBetween(Predicate.Between bet, SelectionVectorFactory factory) {
        FfmColumn<?> col = columnsByName.get(bet.column());
        if (col == null) {
            return SelectionVectorFactory.defaultFactory().create(0);
        }
        Class<?> colType = col.type();
        if (colType == int.class || colType == Integer.class) {
            FfmIntColumn intCol = (FfmIntColumn) col;
            int lower = ((Number) bet.lower()).intValue();
            int upper = ((Number) bet.upper()).intValue();
            return intCol.scanBetween(lower, upper, size, factory);
        } else if (colType == long.class || colType == Long.class) {
            FfmLongColumn longCol = (FfmLongColumn) col;
            long lower = ((Number) bet.lower()).longValue();
            long upper = ((Number) bet.upper()).longValue();
            return longCol.scanBetween(lower, upper, size, factory);
        }
        return SelectionVectorFactory.defaultFactory().create(0);
    }

    private SelectionVector scanIn(Predicate.In in, SelectionVectorFactory factory) {
        FfmColumn<?> col = columnsByName.get(in.column());
        if (col == null) {
            return SelectionVectorFactory.defaultFactory().create(0);
        }
        MutableSelectionVector result = factory.create((int) (size * 0.1));
        for (Object value : in.values()) {
            SelectionVector matches = switch (col.type().getName()) {
                case "int", "java.lang.Integer" -> ((FfmIntColumn) col).scanEquals(((Number) value).intValue(), size, factory);
                case "long", "java.lang.Long" -> ((FfmLongColumn) col).scanEquals(((Number) value).longValue(), size, factory);
                case "java.lang.String" -> ((FfmStringColumnImpl) col).scanEquals((String) value, size, factory);
                default -> SelectionVectorFactory.defaultFactory().create(0);
            };
            IntEnumerator e = matches.enumerator();
            while (e.hasNext()) {
                result.add(e.nextInt());
            }
        }
        return result;
    }

    private void ensureCapacity(int desired) {
        if (desired <= capacity) {
            return;
        }
        int newCapacity = Math.max(capacity * 2, desired);
        for (FfmColumn<?> column : columnsByName.values()) {
            column.resize(newCapacity);
        }
        capacity = newCapacity;
    }

    public record ColumnSpec(String name, Class<?> type) {
        public ColumnSpec {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("name required");
            }
            if (type == null) {
                throw new IllegalArgumentException("type required");
            }
        }
    }

    private interface FfmColumn<T> extends Column<T> {
        void set(int index, Object value);

        void resize(int newCapacity);
    }

    private static final class FfmIntColumn implements FfmColumn<Integer> {
        private final String name;
        private final io.memris.storage.ffm.FfmIntColumn column;

        FfmIntColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmIntColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Integer> type() {
            return Integer.class;
        }

        @Override
        public Integer get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        // Direct int access for performance
        public int get(int rowIndex) {
            return column.get(rowIndex);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).intValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(int value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public SelectionVector scanGreaterThan(int value, int rowCount, SelectionVectorFactory factory) {
            return column.scanGreaterThan(value, rowCount, factory);
        }

        public SelectionVector scanGreaterThanOrEqual(int value, int rowCount, SelectionVectorFactory factory) {
            return column.scanGreaterThanOrEqual(value, rowCount, factory);
        }

        public SelectionVector scanLessThan(int value, int rowCount, SelectionVectorFactory factory) {
            return column.scanLessThan(value, rowCount, factory);
        }

        public SelectionVector scanLessThanOrEqual(int value, int rowCount, SelectionVectorFactory factory) {
            return column.scanLessThanOrEqual(value, rowCount, factory);
        }

        public SelectionVector scanBetween(int lower, int upper, int rowCount, SelectionVectorFactory factory) {
            return column.scanBetween(lower, upper, rowCount, factory);
        }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmLongColumn implements FfmColumn<Long> {
        private final String name;
        private final io.memris.storage.ffm.FfmLongColumn column;

        FfmLongColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmLongColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Long> type() {
            return Long.class;
        }

        @Override
        public Long get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        // Direct int access for performance
        public long get(int rowIndex) {
            return column.get(rowIndex);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).longValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(long value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public SelectionVector scanGreaterThan(long value, int rowCount, SelectionVectorFactory factory) {
            return column.scanGreaterThan(value, rowCount, factory);
        }

        public SelectionVector scanGreaterThanOrEqual(long value, int rowCount, SelectionVectorFactory factory) {
            return column.scanGreaterThanOrEqual(value, rowCount, factory);
        }

        public SelectionVector scanLessThan(long value, int rowCount, SelectionVectorFactory factory) {
            return column.scanLessThan(value, rowCount, factory);
        }

        public SelectionVector scanLessThanOrEqual(long value, int rowCount, SelectionVectorFactory factory) {
            return column.scanLessThanOrEqual(value, rowCount, factory);
        }

        public SelectionVector scanBetween(long lower, long upper, int rowCount, SelectionVectorFactory factory) {
            return column.scanBetween(lower, upper, rowCount, factory);
        }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmStringColumnImpl implements FfmColumn<String> {
        private final String name;
        private final FfmStringColumn column;

        FfmStringColumnImpl(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new FfmStringColumn(arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<String> type() {
            return String.class;
        }

        @Override
        public String get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, (String) value);
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(String value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public SelectionVector scanStartingWith(String prefix, int rowCount, SelectionVectorFactory factory) {
            return column.scanStartingWith(prefix, rowCount, factory);
        }

        public SelectionVector scanEndingWith(String suffix, int rowCount, SelectionVectorFactory factory) {
            return column.scanEndingWith(suffix, rowCount, factory);
        }

        public SelectionVector scanLike(String pattern, int rowCount, SelectionVectorFactory factory) {
            return column.scanLike(pattern, rowCount, factory);
        }

        public SelectionVector scanEqualsIgnoreCase(String value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEqualsIgnoreCase(value, rowCount, factory);
        }

        public String get(int rowIndex) {
            return column.get(rowIndex);
        }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmBooleanColumn implements FfmColumn<Boolean> {
        private final String name;
        private final io.memris.storage.ffm.FfmBooleanColumn column;

        FfmBooleanColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmBooleanColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Boolean> type() { return Boolean.class; }

        @Override
        public Boolean get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, (Boolean) value);
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(boolean value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public boolean get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmByteColumn implements FfmColumn<Byte> {
        private final String name;
        private final io.memris.storage.ffm.FfmByteColumn column;

        FfmByteColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmByteColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Byte> type() { return Byte.class; }

        @Override
        public Byte get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).byteValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(byte value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public byte get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmShortColumn implements FfmColumn<Short> {
        private final String name;
        private final io.memris.storage.ffm.FfmShortColumn column;

        FfmShortColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmShortColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Short> type() { return Short.class; }

        @Override
        public Short get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).shortValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(short value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public short get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmFloatColumn implements FfmColumn<Float> {
        private final String name;
        private final io.memris.storage.ffm.FfmFloatColumn column;

        FfmFloatColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmFloatColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Float> type() { return Float.class; }

        @Override
        public Float get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).floatValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(float value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public float get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmDoubleColumn implements FfmColumn<Double> {
        private final String name;
        private final io.memris.storage.ffm.FfmDoubleColumn column;

        FfmDoubleColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmDoubleColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Double> type() { return Double.class; }

        @Override
        public Double get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, ((Number) value).doubleValue());
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(double value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public double get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }

    private static final class FfmCharColumn implements FfmColumn<Character> {
        private final String name;
        private final io.memris.storage.ffm.FfmCharColumn column;

        FfmCharColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmCharColumn(name, arena, capacity);
        }

        @Override
        public String name() { return name; }

        @Override
        public Class<Character> type() { return Character.class; }

        @Override
        public Character get(RowId rowId) {
            int index = rowIndex(rowId);
            return column.get(index);
        }

        @Override
        public void set(int index, Object value) {
            column.set(index, (Character) value);
        }

        @Override
        public void resize(int newCapacity) {
            throw new UnsupportedOperationException("Resize not yet implemented for FFM columns");
        }

        public SelectionVector scanEquals(char value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public char get(int rowIndex) { return column.get(rowIndex); }

        private int rowIndex(RowId rowId) {
            long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
            if (index > Integer.MAX_VALUE) {
                throw new IllegalStateException("rowId exceeds supported range");
            }
            return (int) index;
        }
    }
}
