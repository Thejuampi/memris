package io.memris.storage;

import io.memris.kernel.Column;
import io.memris.kernel.RowId;
import io.memris.kernel.Table;
import io.memris.spring.TypeCodes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SimpleTable implements Table {
    private static final int OFFSET_BITS = 16;
    private static final int DEFAULT_CAPACITY = 1024;

    private final String name;
    private final List<Column<?>> columns;
    private final Map<String, SimpleColumn<?>> columnsByName;
    private int size;
    private int capacity;

    public SimpleTable(String name, List<ColumnSpec<?>> specs) {
        this(name, specs, DEFAULT_CAPACITY);
    }

    public SimpleTable(String name, List<ColumnSpec<?>> specs, int initialCapacity) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (specs == null || specs.isEmpty()) {
            throw new IllegalArgumentException("specs required");
        }
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        this.name = name;
        this.capacity = initialCapacity;
        this.columns = new ArrayList<>(specs.size());
        this.columnsByName = new HashMap<>(specs.size());
        for (ColumnSpec<?> spec : specs) {
            SimpleColumn<?> column = new SimpleColumn<>(spec.name(), spec.type(), capacity);
            columns.add(column);
            columnsByName.put(spec.name(), column);
        }
    }

    public RowId insert(Object... values) {
        if (values == null || values.length != columns.size()) {
            throw new IllegalArgumentException("values length must match column count");
        }
        ensureCapacity(size + 1);
        int rowIndex = size++;
        for (int i = 0; i < columns.size(); i++) {
            SimpleColumn<?> column = columnsByName.get(columns.get(i).name());
            column.set(rowIndex, values[i]);
        }
        long page = rowIndex >>> OFFSET_BITS;
        int offset = rowIndex & ((1 << OFFSET_BITS) - 1);
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

    private void ensureCapacity(int desired) {
        if (desired <= capacity) {
            return;
        }
        int newCapacity = Math.max(capacity * 2, desired);
        for (SimpleColumn<?> column : columnsByName.values()) {
            column.resize(newCapacity);
        }
        capacity = newCapacity;
    }

    private int rowIndex(RowId rowId) {
        long index = (rowId.page() << OFFSET_BITS) | (rowId.offset() & 0xFFFFL);
        if (index >= size) {
            throw new IndexOutOfBoundsException("rowId out of range: " + rowId);
        }
        if (index > Integer.MAX_VALUE) {
            throw new IllegalStateException("rowId exceeds supported range: " + rowId);
        }
        return (int) index;
    }

    public record ColumnSpec<T>(String name, Class<T> type) {
        public ColumnSpec {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("name required");
            }
            if (type == null) {
                throw new IllegalArgumentException("type required");
            }
        }
    }

    private final class SimpleColumn<T> implements Column<T> {
        private final String name;
        private final Class<T> type;
        private final byte typeCode;
        private Object[] data;

        private SimpleColumn(String name, Class<T> type, int capacity) {
            this.name = name;
            this.type = type;
            this.typeCode = computeTypeCode(type);
            this.data = new Object[capacity];
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<T> type() {
            return type;
        }

        @Override
        public T get(RowId rowId) {
            int index = rowIndex(rowId);
            return type.cast(data[index]);
        }

        @Override
        public byte typeCode() {
            return typeCode;
        }

        private void set(int index, Object value) {
            data[index] = value;
        }

        private void resize(int newCapacity) {
            data = Arrays.copyOf(data, newCapacity);
        }

        private byte computeTypeCode(Class<?> type) {
            if (type == int.class || type == Integer.class) {
                return TypeCodes.TYPE_INT;
            } else if (type == long.class || type == Long.class) {
                return TypeCodes.TYPE_LONG;
            } else if (type == boolean.class || type == Boolean.class) {
                return TypeCodes.TYPE_BOOLEAN;
            } else if (type == byte.class || type == Byte.class) {
                return TypeCodes.TYPE_BYTE;
            } else if (type == short.class || type == Short.class) {
                return TypeCodes.TYPE_SHORT;
            } else if (type == float.class || type == Float.class) {
                return TypeCodes.TYPE_FLOAT;
            } else if (type == double.class || type == Double.class) {
                return TypeCodes.TYPE_DOUBLE;
            } else if (type == char.class || type == Character.class) {
                return TypeCodes.TYPE_CHAR;
            } else if (type == String.class) {
                return TypeCodes.TYPE_STRING;
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
    }
}
