package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;

@FunctionalInterface
public interface IdLookup {

    long lookup(GeneratedTable table, Object id);

    static IdLookup forTypeCode(byte idTypeCode) {
        return switch (idTypeCode) {
            case TypeCodes.TYPE_STRING -> StringIdLookup.INSTANCE;
            default -> LongIdLookup.INSTANCE;
        };
    }

    final class LongIdLookup implements IdLookup {
        static final LongIdLookup INSTANCE = new LongIdLookup();

        private LongIdLookup() {
        }

        @Override
        public long lookup(GeneratedTable table, Object id) {
            return table.lookupById(((Number) id).longValue());
        }
    }

    final class StringIdLookup implements IdLookup {
        static final StringIdLookup INSTANCE = new StringIdLookup();

        private StringIdLookup() {
        }

        @Override
        public long lookup(GeneratedTable table, Object id) {
            return table.lookupByIdString((String) id);
        }
    }
}
