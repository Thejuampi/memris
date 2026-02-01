package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JoinExecutorImplTest {

    @Test
    void filterJoinLeftSelectsAllWhenSourceSelectionNull() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadataIdOnly());

        long first = sourceTable.insertFrom(new Object[]{1L, 10L});
        long second = sourceTable.insertFrom(new Object[]{2L, 20L});

        JoinExecutorImpl executor = new JoinExecutorImpl(1, 0, true, TypeCodes.TYPE_LONG, LogicalQuery.Join.JoinType.LEFT);
        Selection selection = executor.filterJoin(sourceTable, targetTable, null, null);

        assertThat(selection.toIntArray())
                .containsExactlyInAnyOrder(Selection.index(first), Selection.index(second));
    }

    @Test
    void filterJoinInnerMatchesForeignKeysAgainstIds() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadataIdOnly());

        targetTable.insertFrom(new Object[]{10L});
        targetTable.insertFrom(new Object[]{20L});

        long matchFirst = sourceTable.insertFrom(new Object[]{1L, 10L});
        sourceTable.insertFrom(new Object[]{2L, 999L});
        long matchSecond = sourceTable.insertFrom(new Object[]{3L, 20L});

        JoinExecutorImpl executor = new JoinExecutorImpl(1, 0, true, TypeCodes.TYPE_LONG, LogicalQuery.Join.JoinType.INNER);
        Selection selection = executor.filterJoin(sourceTable, targetTable, null, null);

        assertThat(selection.toIntArray())
                .containsExactlyInAnyOrder(Selection.index(matchFirst), Selection.index(matchSecond));
    }

    @Test
    void filterJoinInnerUsesTargetSelectionForNonIdJoins() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadataWithGroup());

        targetTable.insertFrom(new Object[]{10L, 100L});
        long allowed = targetTable.insertFrom(new Object[]{11L, 200L});
        targetTable.insertFrom(new Object[]{12L, 100L});

        sourceTable.insertFrom(new Object[]{1L, 100L});
        long matched = sourceTable.insertFrom(new Object[]{2L, 200L});
        sourceTable.insertFrom(new Object[]{3L, 999L});

        Selection targetSelection = new SelectionImpl(new long[]{allowed});

        JoinExecutorImpl executor = new JoinExecutorImpl(1, 1, false, TypeCodes.TYPE_LONG, LogicalQuery.Join.JoinType.INNER);
        Selection selection = executor.filterJoin(sourceTable, targetTable, null, targetSelection);

        assertThat(selection.toIntArray())
                .containsExactlyInAnyOrder(Selection.index(matched));
    }

    private GeneratedTable newTable(TableMetadata metadata) throws Exception {
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class).newInstance(32, 4);
    }

    private TableMetadata sourceMetadata() {
        return new TableMetadata(
                "Source",
                "io.memris.test.Source",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("fk", TypeCodes.TYPE_LONG, false, false)
                )
        );
    }

    private TableMetadata targetMetadataIdOnly() {
        return new TableMetadata(
                "Target",
                "io.memris.test.Target",
                List.of(new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true))
        );
    }

    private TableMetadata targetMetadataWithGroup() {
        return new TableMetadata(
                "TargetWithGroup",
                "io.memris.test.TargetWithGroup",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("groupId", TypeCodes.TYPE_LONG, false, false)
                )
        );
    }
}
