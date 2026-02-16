package io.memris.storage.heap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TableImplementationStrategyTest {

    @Test
    void columnFieldInfo_recordAccessors() {
        var info = new TableImplementationStrategy.ColumnFieldInfo("col", String.class, (byte)42, 3, true);

        assertThat(info.fieldName()).isEqualTo("col");
        assertThat(info.columnType()).isEqualTo(String.class);
        assertThat(info.typeCode()).isEqualTo((byte)42);
        assertThat(info.index()).isEqualTo(3);
        assertThat(info.primitiveNonNull()).isTrue();
    }

    @Test
    void columnFieldInfo_fourArgConstructorDefaultsPrimitiveFlag() {
        var info = new TableImplementationStrategy.ColumnFieldInfo("id", long.class, (byte) 1, 0);

        assertThat(info.primitiveNonNull()).isFalse();
    }
}
