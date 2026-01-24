package io.memris.spring;

import io.memris.kernel.Predicate;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Optional;

public class MemrisTemplate {
    private final Arena arena;
    private final FfmTable table;

    public MemrisTemplate(Class<?> entityClass, Arena arena, List<ColumnDef> columns) {
        this.arena = arena;
        this.table = new FfmTable(entityClass.getSimpleName(), arena, mapColumns(columns));
    }

    private static List<FfmTable.ColumnSpec> mapColumns(List<ColumnDef> columns) {
        return columns.stream().map(c -> new FfmTable.ColumnSpec(c.name(), c.type())).toList();
    }

    public <T> T save(T entity) {
        return entity;
    }

    public <T> Optional<T> findById(Object id) {
        return Optional.empty();
    }

    public List<?> findBy(String column, Object value) {
        SelectionVector rows = table.scan(
                new Predicate.Comparison(column, Predicate.Operator.EQ, value),
                SelectionVectorFactory.defaultFactory());
        return List.of();
    }

    public long count() {
        return table.rowCount();
    }

    public void deleteAll() {
    }

    public record ColumnDef(String name, Class<?> type) {}
}
