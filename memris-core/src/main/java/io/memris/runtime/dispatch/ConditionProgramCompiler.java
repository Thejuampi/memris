package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;

public final class ConditionProgramCompiler {

        private ConditionProgramCompiler() {
        }

        public static DirectConditionExecutor compile(CompiledQuery.CompiledCondition condition,
                        boolean primitiveNonNull) {
                var operator = condition.operator();
                var typeCode = condition.typeCode();
                var columnIndex = condition.columnIndex();
                var argumentIndex = condition.argumentIndex();
                var ignoreCase = condition.ignoreCase();

                if (primitiveNonNull) {
                        if (operator == LogicalQuery.Operator.IS_NULL) {
                                return (table, kernel, args) -> selectionFromRows(table, new int[0]);
                        }
                        if (operator == LogicalQuery.Operator.NOT_NULL) {
                                return (table, kernel, args) -> selectionFromRows(table, table.scanAll());
                        }
                }

                return switch (operator) {
                        case EQ -> compileEq(columnIndex, argumentIndex, ignoreCase, typeCode);
                        case NE -> compileNe(columnIndex, argumentIndex, ignoreCase, typeCode);
                        case GT -> compileGt(columnIndex, argumentIndex, typeCode);
                        case GTE -> compileGte(columnIndex, argumentIndex, typeCode);
                        case LT -> compileLt(columnIndex, argumentIndex, typeCode);
                        case LTE -> compileLte(columnIndex, argumentIndex, typeCode);
                        case BETWEEN -> compileBetween(columnIndex, argumentIndex, typeCode);
                        case IN -> compileIn(columnIndex, argumentIndex, typeCode);
                        case NOT_IN -> compileNotIn(columnIndex, argumentIndex, typeCode);
                        default -> compileKernelFallback(condition);
                };
        }

        private static DirectConditionExecutor compileEq(int columnIndex, int argumentIndex, boolean ignoreCase,
                        byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_STRING,
                                        TypeCodes.TYPE_BIG_DECIMAL,
                                        TypeCodes.TYPE_BIG_INTEGER ->
                                ignoreCase
                                                ? (table, kernel, args) -> selectionFromRows(table,
                                                                table.scanEqualsStringIgnoreCase(
                                                                                columnIndex,
                                                                                ConditionArgDecoders.toStringValue(
                                                                                                ConditionArgDecoders
                                                                                                                .argAt(args, argumentIndex))))
                                                : (table, kernel, args) -> selectionFromRows(table,
                                                                table.scanEqualsString(columnIndex,
                                                                                ConditionArgDecoders.toStringValue(
                                                                                                ConditionArgDecoders
                                                                                                                .argAt(args,
                                                                                                                                argumentIndex))));
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanEqualsLong(columnIndex,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex))));
                        default -> (table, kernel, args) -> selectionFromRows(table, table.scanEqualsInt(columnIndex,
                                        ConditionArgDecoders.toInt(typeCode,
                                                        ConditionArgDecoders.argAt(args, argumentIndex))));
                };
        }

        private static DirectConditionExecutor compileNe(int columnIndex, int argumentIndex, boolean ignoreCase,
                        byte typeCode) {
                var eq = compileEq(columnIndex, argumentIndex, ignoreCase, typeCode);
                return (table, kernel, args) -> selectionFromRows(table, table.scanAll())
                                .subtract(eq.execute(table, kernel, args));
        }

        private static DirectConditionExecutor compileGt(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanBetweenLong(columnIndex,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex))
                                                                                + 1L,
                                                                Long.MAX_VALUE));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanBetweenInt(columnIndex,
                                                        ConditionArgDecoders.toInt(typeCode,
                                                                        ConditionArgDecoders.argAt(args, argumentIndex))
                                                                        + 1,
                                                        Integer.MAX_VALUE));
                };
        }

        private static DirectConditionExecutor compileGte(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanBetweenLong(columnIndex,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex)),
                                                                Long.MAX_VALUE));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanBetweenInt(columnIndex,
                                                        ConditionArgDecoders.toInt(typeCode,
                                                                        ConditionArgDecoders.argAt(args,
                                                                                        argumentIndex)),
                                                        Integer.MAX_VALUE));
                };
        }

        private static DirectConditionExecutor compileLt(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanBetweenLong(columnIndex,
                                                                Long.MIN_VALUE,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex))
                                                                                - 1L));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanBetweenInt(columnIndex,
                                                        Integer.MIN_VALUE,
                                                        ConditionArgDecoders.toInt(typeCode,
                                                                        ConditionArgDecoders.argAt(args, argumentIndex))
                                                                        - 1));
                };
        }

        private static DirectConditionExecutor compileLte(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanBetweenLong(columnIndex,
                                                                Long.MIN_VALUE,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex))));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanBetweenInt(columnIndex,
                                                        Integer.MIN_VALUE,
                                                        ConditionArgDecoders.toInt(typeCode, ConditionArgDecoders
                                                                        .argAt(args, argumentIndex))));
                };
        }

        private static DirectConditionExecutor compileBetween(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanBetweenLong(columnIndex,
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex)),
                                                                ConditionArgDecoders.toLong(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex + 1))));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanBetweenInt(columnIndex,
                                                        ConditionArgDecoders.toInt(typeCode,
                                                                        ConditionArgDecoders.argAt(args,
                                                                                        argumentIndex)),
                                                        ConditionArgDecoders.toInt(typeCode, ConditionArgDecoders
                                                                        .argAt(args, argumentIndex + 1))));
                };
        }

        private static DirectConditionExecutor compileIn(int columnIndex, int argumentIndex, byte typeCode) {
                return switch (typeCode) {
                        case TypeCodes.TYPE_STRING,
                                        TypeCodes.TYPE_BIG_DECIMAL,
                                        TypeCodes.TYPE_BIG_INTEGER ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanInString(columnIndex,
                                                                ConditionArgDecoders.toStringArray(typeCode,
                                                                                ConditionArgDecoders
                                                                                .argAt(args, argumentIndex))));
                        case TypeCodes.TYPE_LONG,
                                        TypeCodes.TYPE_INSTANT,
                                        TypeCodes.TYPE_LOCAL_DATE,
                                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                                        TypeCodes.TYPE_DATE,
                                        TypeCodes.TYPE_DOUBLE ->
                                (table, kernel, args) -> selectionFromRows(table,
                                                table.scanInLong(columnIndex,
                                                                ConditionArgDecoders.toLongArray(typeCode,
                                                                                ConditionArgDecoders.argAt(args,
                                                                                                argumentIndex))));
                        default -> (table, kernel, args) -> selectionFromRows(table,
                                        table.scanInInt(columnIndex,
                                                        ConditionArgDecoders.toIntArray(typeCode, ConditionArgDecoders
                                                                        .argAt(args, argumentIndex))));
                };
        }

        private static DirectConditionExecutor compileNotIn(int columnIndex, int argumentIndex, byte typeCode) {
                var in = compileIn(columnIndex, argumentIndex, typeCode);
                return (table, kernel, args) -> selectionFromRows(table, table.scanAll())
                                .subtract(in.execute(table, kernel, args));
        }

        private static DirectConditionExecutor compileKernelFallback(CompiledQuery.CompiledCondition condition) {
                return (table, kernel, args) -> kernel.executeCondition(condition, args);
        }

        private static Selection selectionFromRows(GeneratedTable table, int[] rows) {
                var packed = new long[rows.length];
                var count = 0;
                for (var rowIndex : rows) {
                        var generation = table.rowGeneration(rowIndex);
                        var ref = Selection.pack(rowIndex, generation);
                        if (table.isLive(ref)) {
                                packed[count++] = ref;
                        }
                }
                if (count == packed.length) {
                        return new SelectionImpl(packed);
                }
                var trimmed = new long[count];
                System.arraycopy(packed, 0, trimmed, 0, count);
                return new SelectionImpl(trimmed);
        }
}
