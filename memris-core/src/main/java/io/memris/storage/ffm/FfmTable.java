package io.memris.storage.ffm;

import io.memris.kernel.Column;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.Table;
import io.memris.kernel.selection.IntEnumerator;
import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;

import io.memris.spring.TypeCodes;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * FFM MemorySegment-backed table with type-specific columns.
 * <p>
 * <b>Type Compatibility Note:</b> This class checks both primitive types (int.class) and
 * wrapper types (Integer.class) for API compatibility. Entity definitions may use either,
 * but <b>primitive types are strongly recommended for optimal performance</b> as they avoid
 * boxing/unboxing overhead in hot paths.
 * </p>
 */
public final class FfmTable implements Table {
    private static final int OFFSET_BITS = 16;
    private static final int DEFAULT_CAPACITY = 1024;

    private final String name;
    private final Arena arena;
    private final List<Column<?>> columns;
    private final Map<String, FfmColumn> columnsByName;
    private int size;
    private int capacity;

    // Column factory map for creating type-specific columns
    private static final Map<Class<?>, BiFunction<String, ColumnContext, FfmColumn<?>>> COLUMN_FACTORIES = Map.ofEntries(
            Map.entry(int.class, (name, ctx) -> new FfmIntColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Integer.class, (name, ctx) -> new FfmIntColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(long.class, (name, ctx) -> new FfmLongColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Long.class, (name, ctx) -> new FfmLongColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(boolean.class, (name, ctx) -> new FfmBooleanColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Boolean.class, (name, ctx) -> new FfmBooleanColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(byte.class, (name, ctx) -> new FfmByteColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Byte.class, (name, ctx) -> new FfmByteColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(short.class, (name, ctx) -> new FfmShortColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Short.class, (name, ctx) -> new FfmShortColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(float.class, (name, ctx) -> new FfmFloatColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Float.class, (name, ctx) -> new FfmFloatColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(double.class, (name, ctx) -> new FfmDoubleColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Double.class, (name, ctx) -> new FfmDoubleColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(char.class, (name, ctx) -> new FfmCharColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(Character.class, (name, ctx) -> new FfmCharColumn(name, ctx.arena, ctx.capacity)),
            Map.entry(String.class, (name, ctx) -> new FfmStringColumnImpl(name, ctx.arena, ctx.capacity))
    );

    private record ColumnContext(Arena arena, int capacity) {
    }

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

        ColumnContext context = new ColumnContext(arena, initialCapacity);
        for (ColumnSpec spec : specs) {
            BiFunction<String, ColumnContext, FfmColumn<?>> factory = COLUMN_FACTORIES.get(spec.type());
            if (factory == null) {
                throw new IllegalArgumentException(STR."Unsupported column type: \{spec.type()}");
            }
            FfmColumn<?> column = factory.apply(spec.name(), context);
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
        if (col.typeCode() == TypeCodes.TYPE_INT) {
            return ((FfmIntColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not int: " + column);
    }

    public long getLong(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_LONG) {
            return ((FfmLongColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not long: " + column);
    }

    public boolean getBoolean(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_BOOLEAN) {
            return ((FfmBooleanColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not boolean: " + column);
    }

    public byte getByte(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_BYTE) {
            return ((FfmByteColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not byte: " + column);
    }

    public short getShort(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_SHORT) {
            return ((FfmShortColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not short: " + column);
    }

    public float getFloat(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_FLOAT) {
            return ((FfmFloatColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not float: " + column);
    }

    public double getDouble(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_DOUBLE) {
            return ((FfmDoubleColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not double: " + column);
    }

    public char getChar(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_CHAR) {
            return ((FfmCharColumn) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not char: " + column);
    }

    public String getString(String column, int rowIndex) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_STRING) {
            return ((FfmStringColumnImpl) col).get(rowIndex);
        }
        throw new IllegalArgumentException("Column is not String: " + column);
    }

    public void setInt(String column, int rowIndex, int value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_INT) {
            ((FfmIntColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not int: " + column);
    }

    public void setLong(String column, int rowIndex, long value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_LONG) {
            ((FfmLongColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not long: " + column);
    }

    public void setBoolean(String column, int rowIndex, boolean value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_BOOLEAN) {
            ((FfmBooleanColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not boolean: " + column);
    }

    public void setByte(String column, int rowIndex, byte value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_BYTE) {
            ((FfmByteColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not byte: " + column);
    }

    public void setShort(String column, int rowIndex, short value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_SHORT) {
            ((FfmShortColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not short: " + column);
    }

    public void setFloat(String column, int rowIndex, float value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_FLOAT) {
            ((FfmFloatColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not float: " + column);
    }

    public void setDouble(String column, int rowIndex, double value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column statement not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_DOUBLE) {
            ((FfmDoubleColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not double: " + column);
    }

    public void setChar(String column, int rowIndex, char value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_CHAR) {
            ((FfmCharColumn) col).column.set(rowIndex, value);
            return;
        }
        throw new IllegalArgumentException("Column is not char: " + column);
    }

    public void setString(String column, int rowIndex, String value) {
        FfmColumn<?> col = columnsByName.get(column);
        if (col == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (col.typeCode() == TypeCodes.TYPE_STRING) {
            ((FfmStringColumnImpl) col).column.set(rowIndex, value);
            return;
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
        byte colType = col.typeCode();
        Predicate.Operator op = comp.operator();

        if (colType == TypeCodes.TYPE_INT) {
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
        } else if (colType == TypeCodes.TYPE_LONG) {
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
        } else if (colType == TypeCodes.TYPE_STRING) {
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
        byte colType = col.typeCode();
        if (colType == TypeCodes.TYPE_INT) {
            FfmIntColumn intCol = (FfmIntColumn) col;
            int lower = ((Number) bet.lower()).intValue();
            int upper = ((Number) bet.upper()).intValue();
            return intCol.scanBetween(lower, upper, size, factory);
        } else if (colType == TypeCodes.TYPE_LONG) {
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
            SelectionVector matches = switch (col.typeCode()) {
                case TypeCodes.TYPE_INT ->
                        ((FfmIntColumn) col).scanEquals(((Number) value).intValue(), size, factory);
                case TypeCodes.TYPE_LONG ->
                        ((FfmLongColumn) col).scanEquals(((Number) value).longValue(), size, factory);
                case TypeCodes.TYPE_STRING -> ((FfmStringColumnImpl) col).scanEquals((String) value, size, factory);
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

        @Override
        byte typeCode();
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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_INT;
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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_LONG;
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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_STRING;
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
        public String name() {
            return name;
        }

        @Override
        public Class<Boolean> type() {
            return Boolean.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_BOOLEAN;
        }

        public SelectionVector scanEquals(boolean value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public boolean get(int rowIndex) {
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

    private static final class FfmByteColumn implements FfmColumn<Byte> {
        private final String name;
        private final io.memris.storage.ffm.FfmByteColumn column;

        FfmByteColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmByteColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Byte> type() {
            return Byte.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_BYTE;
        }

        public SelectionVector scanEquals(byte value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public byte get(int rowIndex) {
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

    private static final class FfmShortColumn implements FfmColumn<Short> {
        private final String name;
        private final io.memris.storage.ffm.FfmShortColumn column;

        FfmShortColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmShortColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Short> type() {
            return Short.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_SHORT;
        }

        public SelectionVector scanEquals(short value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public short get(int rowIndex) {
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

    private static final class FfmFloatColumn implements FfmColumn<Float> {
        private final String name;
        private final io.memris.storage.ffm.FfmFloatColumn column;

        FfmFloatColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmFloatColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Float> type() {
            return Float.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_FLOAT;
        }

        public SelectionVector scanEquals(float value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public float get(int rowIndex) {
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

    private static final class FfmDoubleColumn implements FfmColumn<Double> {
        private final String name;
        private final io.memris.storage.ffm.FfmDoubleColumn column;

        FfmDoubleColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmDoubleColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Double> type() {
            return Double.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_DOUBLE;
        }

        public SelectionVector scanEquals(double value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public double get(int rowIndex) {
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

    private static final class FfmCharColumn implements FfmColumn<Character> {
        private final String name;
        private final io.memris.storage.ffm.FfmCharColumn column;

        FfmCharColumn(String name, Arena arena, int capacity) {
            this.name = name;
            this.column = new io.memris.storage.ffm.FfmCharColumn(name, arena, capacity);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<Character> type() {
            return Character.class;
        }

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

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_CHAR;
        }

        public SelectionVector scanEquals(char value, int rowCount, SelectionVectorFactory factory) {
            return column.scanEquals(value, rowCount, factory);
        }

        public char get(int rowIndex) {
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
}