package io.memris.kernel;

import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;

public final class MemrisStore implements AutoCloseable {
    private final Arena arena;
    private final FfmTable table;

    public MemrisStore(Class<?> entityClass, List<ColumnDef> columns) {
        this.arena = Arena.ofConfined();
        this.table = new FfmTable(
                entityClass.getSimpleName(),
                arena,
                columns.stream().map(c -> new FfmTable.ColumnSpec(c.name(), c.type())).toList());
    }

    public Object insert(Object... values) {
        return table.insert(values);
    }

    public SelectionVector scan(Predicate predicate) {
        return table.scan(predicate, SelectionVectorFactory.defaultFactory());
    }

    public SelectionVector scanAll() {
        return table.scanAll(SelectionVectorFactory.defaultFactory());
    }

    public long rowCount() {
        return table.rowCount();
    }

    @Override
    public void close() {
        arena.close();
    }

    public record ColumnDef(String name, Class<?> type) {}
}
