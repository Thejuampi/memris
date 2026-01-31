package io.memris.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Hot-path query execution kernel using type handlers for extensibility.
 * 
 * <p>This kernel uses the {@link TypeHandlerRegistry} to dispatch operations
 * to type-specific handlers. This design allows new types to be added without
 * modifying the kernel - simply register a new {@link TypeHandler}.
 * 
 * <p>The kernel supports all standard comparison operators:
 * <ul>
 *   <li>Equality: EQ, NE</li>
 *   <li>Comparison: GT, GTE, LT, LTE</li>
 *   <li>Range: BETWEEN</li>
 *   <li>Set: IN, NOT_IN</li>
 *   <li>Null checks: IS_NULL, IS_NOT_NULL</li>
 * </ul>
 */
public final class HeapRuntimeKernel {
    
    private final GeneratedTable table;
    private final TypeHandlerRegistry handlerRegistry;
    
    /**
     * Create a kernel with the default handler registry.
     */
    public HeapRuntimeKernel(GeneratedTable table) {
        this(table, TypeHandlerRegistry.getDefault());
    }
    
    /**
     * Create a kernel with a custom handler registry.
     * Use this for testing or when you need custom type handlers.
     */
    public HeapRuntimeKernel(GeneratedTable table, TypeHandlerRegistry handlerRegistry) {
        this.table = table;
        this.handlerRegistry = handlerRegistry;
    }
    
    public long rowCount() {
        return table.liveCount();
    }
    
    public long allocatedCount() {
        return table.allocatedCount();
    }
    
    public int columnCount() {
        return table.columnCount();
    }
    
    public byte typeCodeAt(int columnIndex) {
        return table.typeCodeAt(columnIndex);
    }
    
    public GeneratedTable table() {
        return table;
    }
    
    /**
     * Get the handler registry for this kernel.
     * Can be used to register additional type handlers.
     */
    public TypeHandlerRegistry getHandlerRegistry() {
        return handlerRegistry;
    }
    
    /**
     * Execute a compiled condition against the table.
     * 
     * @param cc the compiled condition
     * @param args the argument values from the method call
     * @return selection of matching rows
     * @throws IllegalArgumentException if no handler is registered for the column type
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Selection executeCondition(CompiledQuery.CompiledCondition cc, Object[] args) {
        int columnIndex = cc.columnIndex();
        LogicalQuery.Operator operator = cc.operator();
        Object value = null;
        if (operator != LogicalQuery.Operator.IS_NULL && operator != LogicalQuery.Operator.NOT_NULL) {
            value = args[cc.argumentIndex()];
        }

        if (operator == LogicalQuery.Operator.IN || operator == LogicalQuery.Operator.NOT_IN) {
            Selection inSelection = executeInList(columnIndex, value);
            if (operator == LogicalQuery.Operator.NOT_IN) {
                int[] all = table.scanAll();
                return subtractSelections(all, inSelection);
            }
            return inSelection;
        }

        if (operator == LogicalQuery.Operator.BETWEEN) {
            return executeBetween(columnIndex, cc.argumentIndex(), args);
        }
        
        byte typeCode = table.typeCodeAt(columnIndex);
        TypeHandler handler = handlerRegistry.getHandler(typeCode);
        
        if (handler == null) {
            throw new IllegalArgumentException(
                "No handler registered for type code: " + typeCode + 
                " (column index: " + columnIndex + ")");
        }
        
        // Convert the value to the handler's type if needed
        Object convertedValue = value != null ? handler.convertValue(value) : null;
        
        return handler.executeCondition(table, columnIndex, operator, convertedValue, cc.ignoreCase());
    }

    private Selection executeBetween(int columnIndex, int argIndex, Object[] args) {
        if (argIndex + 1 >= args.length) {
            throw new IllegalArgumentException("BETWEEN requires two arguments");
        }
        Object minObj = args[argIndex];
        Object maxObj = args[argIndex + 1];
        byte typeCode = table.typeCodeAt(columnIndex);
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_LONG -> {
                long min = ((Number) minObj).longValue();
                long max = ((Number) maxObj).longValue();
                yield createSelection(table, table.scanBetweenLong(columnIndex, min, max));
            }
            case io.memris.core.TypeCodes.TYPE_INT -> {
                int min = ((Number) minObj).intValue();
                int max = ((Number) maxObj).intValue();
                yield createSelection(table, table.scanBetweenInt(columnIndex, min, max));
            }
            case io.memris.core.TypeCodes.TYPE_INSTANT,
                io.memris.core.TypeCodes.TYPE_LOCAL_DATE,
                io.memris.core.TypeCodes.TYPE_LOCAL_DATE_TIME,
                io.memris.core.TypeCodes.TYPE_DATE -> {
                long min = convertToEpochLong(typeCode, minObj);
                long max = convertToEpochLong(typeCode, maxObj);
                yield createSelection(table, table.scanBetweenLong(columnIndex, min, max));
            }
            default -> throw new UnsupportedOperationException("BETWEEN not supported for type code: " + typeCode);
        };
    }

    private Selection executeInList(int columnIndex, Object value) {
        if (value == null) {
            return createSelection(table, new int[0]);
        }
        byte typeCode = table.typeCodeAt(columnIndex);
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_STRING,
                io.memris.core.TypeCodes.TYPE_BIG_DECIMAL,
                io.memris.core.TypeCodes.TYPE_BIG_INTEGER -> createSelection(table, table.scanInString(columnIndex, toStringArray(value)));
            case io.memris.core.TypeCodes.TYPE_LONG,
                io.memris.core.TypeCodes.TYPE_INSTANT,
                io.memris.core.TypeCodes.TYPE_LOCAL_DATE,
                io.memris.core.TypeCodes.TYPE_LOCAL_DATE_TIME,
                io.memris.core.TypeCodes.TYPE_DATE,
                io.memris.core.TypeCodes.TYPE_DOUBLE -> createSelection(table, table.scanInLong(columnIndex, toLongArray(typeCode, value)));
            case io.memris.core.TypeCodes.TYPE_INT,
                io.memris.core.TypeCodes.TYPE_BOOLEAN,
                io.memris.core.TypeCodes.TYPE_BYTE,
                io.memris.core.TypeCodes.TYPE_SHORT,
                io.memris.core.TypeCodes.TYPE_CHAR,
                io.memris.core.TypeCodes.TYPE_FLOAT -> createSelection(table, table.scanInInt(columnIndex, toIntArray(typeCode, value)));
            default -> throw new UnsupportedOperationException("IN not supported for type code: " + typeCode);
        };
    }

    private long[] toLongArray(byte typeCode, Object value) {
        if (value instanceof long[] longs) {
            return longs;
        }
        if (value instanceof int[] ints) {
            long[] result = new long[ints.length];
            for (int i = 0; i < ints.length; i++) {
                result[i] = ints[i];
            }
            return result;
        }
        if (value instanceof Object[] objects) {
            long[] result = new long[objects.length];
            for (int i = 0; i < objects.length; i++) {
                result[i] = convertToLong(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            // Optimized: two-pass to avoid ArrayList allocation
            int size = 0;
            for (Object ignored : iterable) {
                size++;
            }
            long[] result = new long[size];
            int i = 0;
            for (Object item : iterable) {
                result[i++] = convertToLong(typeCode, item);
            }
            return result;
        }
        return new long[]{convertToLong(typeCode, value)};
    }

    private int[] toIntArray(byte typeCode, Object value) {
        if (value instanceof int[] ints) {
            return ints;
        }
        if (value instanceof Object[] objects) {
            int[] result = new int[objects.length];
            for (int i = 0; i < objects.length; i++) {
                result[i] = convertToInt(typeCode, objects[i]);
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            // Optimized: two-pass to avoid ArrayList allocation
            int size = 0;
            for (Object ignored : iterable) {
                size++;
            }
            int[] result = new int[size];
            int i = 0;
            for (Object item : iterable) {
                result[i++] = convertToInt(typeCode, item);
            }
            return result;
        }
        return new int[]{convertToInt(typeCode, value)};
    }

    private String[] toStringArray(Object value) {
        if (value instanceof String[] strings) {
            return strings;
        }
        if (value instanceof Object[] objects) {
            String[] result = new String[objects.length];
            for (int i = 0; i < objects.length; i++) {
                result[i] = objects[i] != null ? objects[i].toString() : null;
            }
            return result;
        }
        if (value instanceof Iterable<?> iterable) {
            // Optimized: two-pass to avoid ArrayList allocation
            int size = 0;
            for (Object ignored : iterable) {
                size++;
            }
            String[] result = new String[size];
            int i = 0;
            for (Object item : iterable) {
                result[i++] = item != null ? item.toString() : null;
            }
            return result;
        }
        return new String[]{value.toString()};
    }

    private long convertToLong(byte typeCode, Object value) {
        if (value == null) {
            return 0L;
        }
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_INSTANT -> ((java.time.Instant) value).toEpochMilli();
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE -> ((java.time.LocalDate) value).toEpochDay();
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE_TIME -> ((java.time.LocalDateTime) value).toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            case io.memris.core.TypeCodes.TYPE_DATE -> ((java.util.Date) value).getTime();
            case io.memris.core.TypeCodes.TYPE_DOUBLE -> Double.doubleToLongBits(((Number) value).doubleValue());
            default -> ((Number) value).longValue();
        };
    }

    private int convertToInt(byte typeCode, Object value) {
        if (value == null) {
            return 0;
        }
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_BOOLEAN -> (value instanceof Boolean b && b) ? 1 : 0;
            case io.memris.core.TypeCodes.TYPE_CHAR -> (value instanceof Character c) ? c : (int) value.toString().charAt(0);
            case io.memris.core.TypeCodes.TYPE_FLOAT -> Float.floatToIntBits(((Number) value).floatValue());
            default -> ((Number) value).intValue();
        };
    }

    private Selection subtractSelections(int[] allRows, Selection toRemove) {
        long[] packed = new long[allRows.length];
        for (int i = 0; i < allRows.length; i++) {
            int rowIndex = allRows[i];
            packed[i] = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
        }
        return new io.memris.storage.SelectionImpl(packed).subtract(toRemove);
    }

    private static long convertToEpochLong(byte typeCode, Object value) {
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_INSTANT -> ((Instant) value).toEpochMilli();
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE -> ((LocalDate) value).toEpochDay();
            case io.memris.core.TypeCodes.TYPE_LOCAL_DATE_TIME -> ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
            case io.memris.core.TypeCodes.TYPE_DATE -> ((Date) value).getTime();
            default -> ((Number) value).longValue();
        };
    }

    private Selection createSelection(GeneratedTable table, int[] indices) {
        long[] packed = new long[indices.length];
        for (int i = 0; i < indices.length; i++) {
            int rowIndex = indices[i];
            packed[i] = io.memris.storage.Selection.pack(rowIndex, table.rowGeneration(rowIndex));
        }
        return new io.memris.storage.SelectionImpl(packed);
    }
}
