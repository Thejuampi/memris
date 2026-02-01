package io.memris.storage;

import io.memris.kernel.Column;
import io.memris.kernel.RowId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for SimpleTable - basic table implementation.
 */
class SimpleTableTest {

    @Test
    @DisplayName("Should create table with column specs")
    void shouldCreateTableWithColumnSpecs() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class),
                new SimpleTable.ColumnSpec<>("age", Integer.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);

        assertThat(table.name()).isEqualTo("TestTable");
        assertThat(table.rowCount()).isEqualTo(0);
        assertThat(table.columns()).hasSize(3);
    }

    @Test
    @DisplayName("Should create table with custom capacity")
    void shouldCreateTableWithCustomCapacity() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs, 100);

        assertThat(table.name()).isEqualTo("TestTable");
        assertThat(table.rowCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should reject null name")
    void shouldRejectNullName() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        assertThatThrownBy(() -> new SimpleTable(null, specs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name required");
    }

    @Test
    @DisplayName("Should reject blank name")
    void shouldRejectBlankName() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        assertThatThrownBy(() -> new SimpleTable("   ", specs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name required");
    }

    @Test
    @DisplayName("Should reject null specs")
    void shouldRejectNullSpecs() {
        assertThatThrownBy(() -> new SimpleTable("TestTable", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specs required");
    }

    @Test
    @DisplayName("Should reject empty specs")
    void shouldRejectEmptySpecs() {
        assertThatThrownBy(() -> new SimpleTable("TestTable", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("specs required");
    }

    @Test
    @DisplayName("Should reject non-positive capacity")
    void shouldRejectNonPositiveCapacity() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        assertThatThrownBy(() -> new SimpleTable("TestTable", specs, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialCapacity must be positive");

        assertThatThrownBy(() -> new SimpleTable("TestTable", specs, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialCapacity must be positive");
    }

    @Test
    @DisplayName("Should insert row and return RowId")
    void shouldInsertRowAndReturnRowId() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        RowId rowId = table.insert(1L, "John");

        assertThat(rowId).isNotNull();
        assertThat(table.rowCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should insert multiple rows")
    void shouldInsertMultipleRows() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        table.insert(1L, "John");
        table.insert(2L, "Jane");
        table.insert(3L, "Bob");

        assertThat(table.rowCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("Should reject insert with null values")
    void shouldRejectInsertWithNullValues() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);

        assertThatThrownBy(() -> table.insert((Object[]) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("values length must match column count");
    }

    @Test
    @DisplayName("Should reject insert with wrong number of values")
    void shouldRejectInsertWithWrongNumberOfValues() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);

        assertThatThrownBy(() -> table.insert(1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("values length must match column count");

        assertThatThrownBy(() -> table.insert(1L, "John", "Extra"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("values length must match column count");
    }

    @Test
    @DisplayName("Should get column by name")
    void shouldGetColumnByName() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        Column<?> idColumn = table.column("id");
        Column<?> nameColumn = table.column("name");

        assertThat(idColumn).isNotNull();
        assertThat(idColumn.name()).isEqualTo("id");
        assertThat(nameColumn).isNotNull();
        assertThat(nameColumn.name()).isEqualTo("name");
    }

    @Test
    @DisplayName("Should return null for non-existent column")
    void shouldReturnNullForNonExistentColumn() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        Column<?> column = table.column("nonexistent");

        assertThat(column).isNull();
    }

    @Test
    @DisplayName("Should retrieve inserted values via column")
    void shouldRetrieveInsertedValuesViaColumn() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        RowId rowId = table.insert(42L, "Test Name");

        Column<Long> idColumn = (Column<Long>) table.column("id");
        Column<String> nameColumn = (Column<String>) table.column("name");

        assertThat(idColumn.get(rowId)).isEqualTo(42L);
        assertThat(nameColumn.get(rowId)).isEqualTo("Test Name");
    }

    @Test
    @DisplayName("Should handle null values in columns")
    void shouldHandleNullValuesInColumns() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class),
                new SimpleTable.ColumnSpec<>("name", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        RowId rowId = table.insert(1L, null);

        Column<String> nameColumn = (Column<String>) table.column("name");
        assertThat(nameColumn.get(rowId)).isNull();
    }

    @Test
    @DisplayName("Should auto-grow capacity when inserting many rows")
    void shouldAutoGrowCapacityWhenInsertingManyRows() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        // Start with small capacity
        SimpleTable table = new SimpleTable("TestTable", specs, 2);

        // Insert more rows than initial capacity
        for (int i = 0; i < 100; i++) {
            table.insert((long) i);
        }

        assertThat(table.rowCount()).isEqualTo(100);

        // Verify all values are accessible
        Column<Long> idColumn = (Column<Long>) table.column("id");
        for (int i = 0; i < 100; i++) {
            RowId rowId = new RowId(0, i);
            assertThat(idColumn.get(rowId)).isEqualTo((long) i);
        }
    }

    @Test
    @DisplayName("Should support all primitive types")
    void shouldSupportAllPrimitiveTypes() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("longCol", Long.class),
                new SimpleTable.ColumnSpec<>("intCol", Integer.class),
                new SimpleTable.ColumnSpec<>("shortCol", Short.class),
                new SimpleTable.ColumnSpec<>("byteCol", Byte.class),
                new SimpleTable.ColumnSpec<>("booleanCol", Boolean.class),
                new SimpleTable.ColumnSpec<>("floatCol", Float.class),
                new SimpleTable.ColumnSpec<>("doubleCol", Double.class),
                new SimpleTable.ColumnSpec<>("charCol", Character.class),
                new SimpleTable.ColumnSpec<>("stringCol", String.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        RowId rowId = table.insert(
                1L, 2, (short) 3, (byte) 4, true, 1.5f, 2.5, 'A', "test"
        );

        assertThat(table.rowCount()).isEqualTo(1);

        assertThat(((Column<Long>) table.column("longCol")).get(rowId)).isEqualTo(1L);
        assertThat(((Column<Integer>) table.column("intCol")).get(rowId)).isEqualTo(2);
        assertThat(((Column<Short>) table.column("shortCol")).get(rowId)).isEqualTo((short) 3);
        assertThat(((Column<Byte>) table.column("byteCol")).get(rowId)).isEqualTo((byte) 4);
        assertThat(((Column<Boolean>) table.column("booleanCol")).get(rowId)).isTrue();
        assertThat(((Column<Float>) table.column("floatCol")).get(rowId)).isEqualTo(1.5f);
        assertThat(((Column<Double>) table.column("doubleCol")).get(rowId)).isEqualTo(2.5);
        assertThat(((Column<Character>) table.column("charCol")).get(rowId)).isEqualTo('A');
        assertThat(((Column<String>) table.column("stringCol")).get(rowId)).isEqualTo("test");
    }

    @Test
    @DisplayName("Should reject unsupported column type")
    void shouldRejectUnsupportedColumnType() {
        class CustomType {}

        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("custom", CustomType.class)
        );

        assertThatThrownBy(() -> new SimpleTable("TestTable", specs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported type");
    }

    @Test
    @DisplayName("Should reject null column name in spec")
    void shouldRejectNullColumnNameInSpec() {
        assertThatThrownBy(() -> new SimpleTable.ColumnSpec<>(null, Long.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name required");
    }

    @Test
    @DisplayName("Should reject blank column name in spec")
    void shouldRejectBlankColumnNameInSpec() {
        assertThatThrownBy(() -> new SimpleTable.ColumnSpec<>("   ", Long.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name required");
    }

    @Test
    @DisplayName("Should reject null column type in spec")
    void shouldRejectNullColumnTypeInSpec() {
        assertThatThrownBy(() -> new SimpleTable.ColumnSpec<>("id", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type required");
    }

    @Test
    @DisplayName("Should throw when accessing rowId out of range")
    void shouldThrowWhenAccessingRowIdOutOfRange() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);
        table.insert(1L);

        Column<Long> idColumn = (Column<Long>) table.column("id");
        RowId invalidRowId = new RowId(0, 999);

        assertThatThrownBy(() -> idColumn.get(invalidRowId))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessageContaining("rowId out of range");
    }

    @Test
    @DisplayName("Should return unmodifiable column collection")
    void shouldReturnUnmodifiableColumnCollection() {
        List<SimpleTable.ColumnSpec<?>> specs = List.of(
                new SimpleTable.ColumnSpec<>("id", Long.class)
        );

        SimpleTable table = new SimpleTable("TestTable", specs);

        assertThatThrownBy(() -> table.columns().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
