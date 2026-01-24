package io.memris.kernel;

public interface RowIdSet {
    int size();

    boolean contains(RowId rowId);

    long[] toLongArray();

    LongEnumerator enumerator();
}
