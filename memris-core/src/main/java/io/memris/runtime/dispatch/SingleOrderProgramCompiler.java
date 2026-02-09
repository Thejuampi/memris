package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.runtime.OrderExecutor;

public final class SingleOrderProgramCompiler {

    private SingleOrderProgramCompiler() {
    }

    public static OrderExecutor compile(CompiledQuery.CompiledOrderBy orderBy,
            int limit,
            byte typeCode,
            boolean primitiveNonNull) {
        var columnIndex = orderBy.columnIndex();
        var ascending = orderBy.ascending();

        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_BOOLEAN,
                    TypeCodes.TYPE_BYTE,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR ->
                (runtime, rows) -> runtime.applyIntOrderCompiled(rows, columnIndex, ascending, limit, primitiveNonNull);
            case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE ->
                (runtime, rows) -> runtime.applyLongOrderCompiled(rows, columnIndex, ascending, limit, primitiveNonNull);
            case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER ->
                (runtime, rows) -> runtime.applyStringOrderCompiled(rows, columnIndex, ascending, limit);
            case TypeCodes.TYPE_FLOAT ->
                (runtime, rows) -> runtime.applyFloatOrderCompiled(rows, columnIndex, ascending, limit, primitiveNonNull);
            case TypeCodes.TYPE_DOUBLE ->
                (runtime, rows) -> runtime.applyDoubleOrderCompiled(rows, columnIndex, ascending, limit, primitiveNonNull);
            default -> (runtime, rows) -> runtime.applySingleOrderFallback(rows, orderBy, limit);
        };
    }
}
