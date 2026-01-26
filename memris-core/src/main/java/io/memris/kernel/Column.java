package io.memris.kernel;

public interface Column<T> {
    String name();

    Class<T> type();

    T get(RowId rowId);

    /**
     * Returns the type code for this column.
     * Used for zero-overhead type switching in hot paths.
     * @see io.memris.spring.TypeCodes
     */
    byte typeCode();
}
