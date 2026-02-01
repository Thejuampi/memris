package io.memris.storage.heap;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicTableGrowthTest {

    private static final TableMetadata METADATA = new TableMetadata(
            "Numbers",
            "io.memris.test.Numbers",
            List.of(
                    new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                    new FieldMetadata("value", TypeCodes.TYPE_INT, false, false)
            )
    );

    @Test
    void growsBeyondInitialPages() throws Exception {
        GeneratedTable table = newTable(2, 4, 1);

        table.insertFrom(new Object[]{1L, 10});
        table.insertFrom(new Object[]{2L, 11});
        table.insertFrom(new Object[]{3L, 12});

        assertThat(table.readLong(0, 2)).isEqualTo(3L);
    }

    @Test
    void scansAcrossPages() throws Exception {
        GeneratedTable table = newTable(2, 4, 1);

        table.insertFrom(new Object[]{1L, 7});
        table.insertFrom(new Object[]{2L, 8});
        table.insertFrom(new Object[]{3L, 7});
        table.insertFrom(new Object[]{4L, 9});

        assertThat(table.scanEqualsInt(1, 7)).containsExactly(0, 2);
    }

    private static GeneratedTable newTable(int pageSize, int maxPages, int initialPages) throws Exception {
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(METADATA);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class)
                .newInstance(pageSize, maxPages, initialPages);
    }
}
