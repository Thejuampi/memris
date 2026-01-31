package io.memris.kernel;

public interface MutableRowIdSet extends RowIdSet {
    void add(RowId rowId);

    void remove(RowId rowId);
}
