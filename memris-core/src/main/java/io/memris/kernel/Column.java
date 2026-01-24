package io.memris.kernel;

public interface Column<T> {
    String name();

    Class<T> type();

    T get(RowId rowId);
}
