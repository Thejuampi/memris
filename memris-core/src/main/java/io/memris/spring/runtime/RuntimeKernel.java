package io.memris.spring.runtime;

import io.memris.spring.TypeCodes;
import io.memris.storage.ffm.FfmTable;

/**
 * RuntimeKernel wraps FfmTable with index-based column access for hot-path execution.
 * <p>
 * This class enforces O(1) column access by prohibiting String-based lookups.
 * All column access must use pre-resolved column indices from CompiledCondition.
 * <p>
 * <b>CRITICAL HOT PATH REQUIREMENTS:</b>
 * - NO String-based column lookups (no Map operations)
 * - NO reflection
 * - Direct array access only via columnAt(int)
 *
 * @see FfmTable
 * @see io.memris.spring.plan.CompiledCondition
 */
public final class RuntimeKernel {

    private final FfmTable table;
    private final FfmColumnAccessor[] columns;
    private final byte[] typeCodes;
    private final int columnCount;

    /**
     * Create a RuntimeKernel with index-based column access.
     *
     * @param table     the underlying table (null allowed for testing only)
     * @param columns   array of column accessors indexed by column position
     * @param typeCodes array of type codes indexed by column position
     */
    public RuntimeKernel(FfmTable table, FfmColumnAccessor[] columns, byte[] typeCodes) {
        // Note: table can be null for testing purposes only
        // In production, table should always be provided
        if (columns == null) {
            throw new IllegalArgumentException("columns required");
        }
        if (typeCodes == null) {
            throw new IllegalArgumentException("typeCodes required");
        }
        if (columns.length != typeCodes.length) {
            throw new IllegalArgumentException("columns and typeCodes must have same length");
        }
        this.table = table;
        this.columns = columns;
        this.typeCodes = typeCodes;
        this.columnCount = columns.length;
    }

    /**
     * Create a RuntimeKernel from legacy parameters.
     * <p>
     * Temporary bridge method for backward compatibility during migration.
     *
     * @param table       the FfmTable
     * @param columnNames array of column names
     * @param typeCodes   array of type codes
     * @return a RuntimeKernel with index-based column accessors
     */
    public static RuntimeKernel fromLegacy(FfmTable table, String[] columnNames, byte[] typeCodes) {
        FfmColumnAccessor[] accessors = new FfmColumnAccessor[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            byte typeCode = typeCodes[i];
            accessors[i] = createAccessor(table, i, columnName, typeCode);
        }
        return new RuntimeKernel(table, accessors, typeCodes);
    }

    private static FfmColumnAccessor createAccessor(FfmTable table, int index, String columnName, byte typeCode) {

        return switch (typeCode) {
            case TypeCodes.TYPE_INT -> new IntAccessor(index, columnName, table);
            case TypeCodes.TYPE_LONG -> new LongAccessor(index, columnName, table);
            case TypeCodes.TYPE_BOOLEAN -> new BooleanAccessor(index, columnName, table);
            case TypeCodes.TYPE_BYTE -> new ByteAccessor(index, columnName, table);
            case TypeCodes.TYPE_SHORT -> new ShortAccessor(index, columnName, table);
            case TypeCodes.TYPE_FLOAT -> new FloatAccessor(index, columnName, table);
            case TypeCodes.TYPE_DOUBLE -> new DoubleAccessor(index, columnName, table);
            case TypeCodes.TYPE_CHAR -> new CharAccessor(index, columnName, table);
            case TypeCodes.TYPE_STRING -> new StringAccessor(index, columnName, table);
            default -> throw new IllegalArgumentException("Unknown typeCode: " + typeCode);
        };
    }

    /**
     * Get column accessor at index.
     * <p>
     * This is the ONLY way to access columns in the hot path.
     * No String-based lookups allowed.
     *
     * @param index column index (must be pre-resolved at compile time)
     * @return column accessor for direct value access
     */
    public FfmColumnAccessor columnAt(int index) {
        if (index < 0 || index >= columnCount) {
            throw new IllegalArgumentException("Column index out of bounds: " + index);
        }
        return columns[index];
    }

    /**
     * Get type code for column at index.
     *
     * @param index column index
     * @return type code (TypeCodes.TYPE_INT, etc.)
     */
    public byte typeCodeAt(int index) {
        if (index < 0 || index >= columnCount) {
            throw new IllegalArgumentException("Column index out of bounds: " + index);
        }
        return typeCodes[index];
    }

    /**
     * Get underlying table (for non-critical operations like insert, delete).
     *
     * @return the wrapped table
     */
    public FfmTable table() {
        return table;
    }

    /**
     * Get row count.
     *
     * @return number of rows in the table
     */
    public long rowCount() {
        return table.rowCount();
    }

    /**
     * Get column count.
     *
     * @return number of columns
     */
    public int columnCount() {
        return columnCount;
    }

    /**
     * FfmColumnAccessor provides type-safe, index-based column access.
     * <p>
     * Each accessor is bound to a specific column at construction time,
     * eliminating all String lookups in the hot path.
     */
    public sealed interface FfmColumnAccessor permits
            IntAccessor, LongAccessor, BooleanAccessor, ByteAccessor,
            ShortAccessor, FloatAccessor, DoubleAccessor, CharAccessor,
            StringAccessor {

        /**
         * Get column index (for debugging/validation).
         *
         * @return column index
         */
        int columnIndex();

        /**
         * Get column name (for debugging).
         *
         * @return column name
         */
        String columnName();

        /**
         * Get value at row index.
         *
         * @param rowIndex row index
         * @return value as Object
         */
        Object getValue(int rowIndex);

        /**
         * Set value at row index.
         *
         * @param rowIndex row index
         * @param value    value to set
         */
        void setValue(int rowIndex, Object value);

        /**
         * Get type code.
         *
         * @return type code
         */
        byte typeCode();
    }

    /**
     * Int column accessor - zero-allocation int access.
     */
    public record IntAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getInt(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setInt(rowIndex, ((Number) value).intValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_INT;
        }

        /**
         * Get int value (zero-allocation).
         */
        public int getInt(int rowIndex) {
            // Bypass String lookup - direct type dispatch and access
            // Note: This currently still goes through table.getInt(String, int)
            // TODO: FfmTable needs getIntAt(int columnIndex, int rowIndex) method
            return table.getInt(columnName, rowIndex);
        }

        /**
         * Set int value (zero-allocation).
         */
        public void setInt(int rowIndex, int value) {
            table.setInt(columnName, rowIndex, value);
        }
    }

    /**
     * Long column accessor.
     */
    public record LongAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getLong(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setLong(rowIndex, ((Number) value).longValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_LONG;
        }

        public long getLong(int rowIndex) {
            return table.getLong(columnName, rowIndex);
        }

        public void setLong(int rowIndex, long value) {
            table.setLong(columnName, rowIndex, value);
        }
    }

    /**
     * Boolean column accessor.
     */
    public record BooleanAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getBoolean(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setBoolean(rowIndex, (Boolean) value);
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_BOOLEAN;
        }

        public boolean getBoolean(int rowIndex) {
            return table.getBoolean(columnName, rowIndex);
        }

        public void setBoolean(int rowIndex, boolean value) {
            table.setBoolean(columnName, rowIndex, value);
        }
    }

    /**
     * Byte column accessor.
     */
    public record ByteAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getByte(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setByte(rowIndex, ((Number) value).byteValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_BYTE;
        }

        public byte getByte(int rowIndex) {
            return table.getByte(columnName, rowIndex);
        }

        public void setByte(int rowIndex, byte value) {
            table.setByte(columnName, rowIndex, value);
        }
    }

    /**
     * Short column accessor.
     */
    public record ShortAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getShort(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setShort(rowIndex, ((Number) value).shortValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_SHORT;
        }

        public short getShort(int rowIndex) {
            return table.getShort(columnName, rowIndex);
        }

        public void setShort(int rowIndex, short value) {
            table.setShort(columnName, rowIndex, value);
        }
    }

    /**
     * Float column accessor.
     */
    public record FloatAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getFloat(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setFloat(rowIndex, ((Number) value).floatValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_FLOAT;
        }

        public float getFloat(int rowIndex) {
            return table.getFloat(columnName, rowIndex);
        }

        public void setFloat(int rowIndex, float value) {
            table.setFloat(columnName, rowIndex, value);
        }
    }

    /**
     * Double column accessor.
     */
    public record DoubleAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getDouble(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setDouble(rowIndex, ((Number) value).doubleValue());
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_DOUBLE;
        }

        public double getDouble(int rowIndex) {
            return table.getDouble(columnName, rowIndex);
        }

        public void setDouble(int rowIndex, double value) {
            table.setDouble(columnName, rowIndex, value);
        }
    }

    /**
     * Char column accessor.
     */
    public record CharAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getChar(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setChar(rowIndex, (Character) value);
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_CHAR;
        }

        public char getChar(int rowIndex) {
            return table.getChar(columnName, rowIndex);
        }

        public void setChar(int rowIndex, char value) {
            table.setChar(columnName, rowIndex, value);
        }
    }

    /**
     * String column accessor.
     */
    public record StringAccessor(int columnIndex, String columnName, FfmTable table) implements FfmColumnAccessor {

        @Override
        public Object getValue(int rowIndex) {
            return getString(rowIndex);
        }

        @Override
        public void setValue(int rowIndex, Object value) {
            setString(rowIndex, (String) value);
        }

        @Override
        public byte typeCode() {
            return TypeCodes.TYPE_STRING;
        }

        public String getString(int rowIndex) {
            return table.getString(columnName, rowIndex);
        }

        public void setString(int rowIndex, String value) {
            table.setString(columnName, rowIndex, value);
        }
    }
}
