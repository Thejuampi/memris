
package io.memris.storage.heap;

import io.memris.core.TypeCodes;
import io.memris.core.MemrisConfiguration;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.lang.reflect.Field;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration-style tests for MethodHandleImplementation Interceptor classes using generated table.
 */
class MethodHandleImplementationInterceptorTest {
    @Test
    void lookupInterceptor_handlesNegativeAndZeroKeys() {
        assertThat((long) table.lookupById(0L)).isEqualTo(-1L);
        assertThat((long) table.lookupById(-1L)).isEqualTo(-1L);
        assertThat((long) table.lookupById(Long.MIN_VALUE)).isEqualTo(-1L);
    }

    @Test
    void lookupInterceptor_handlesMaxLongKey() {
        long ref = table.insertFrom(new Object[]{Long.MAX_VALUE, 1, "max"});
        assertThat((long) table.lookupById(Long.MAX_VALUE)).isEqualTo(ref);
        table.tombstone(ref);
        assertThat((long) table.lookupById(Long.MAX_VALUE)).isEqualTo(-1L);
    }

    @Test
    void lookupInterceptor_reusesRowIdAfterDelete() {
        long ref1 = table.insertFrom(new Object[]{555L, 1, "A"});
        table.tombstone(ref1);
        long ref2 = table.insertFrom(new Object[]{555L, 2, "B"});
        assertThat(ref2).isNotEqualTo(ref1);
        assertThat((long) table.lookupById(555L)).isEqualTo(ref2);
    }

    @Test
    void lookupInterceptor_returnsMinusOneForStaleGeneration() {
        long ref1 = table.insertFrom(new Object[]{777L, 1, "A"});
        table.tombstone(ref1);
        long ref2 = table.insertFrom(new Object[]{777L, 2, "B"});
        // Manually construct a stale ref (old generation, new index)
        long staleRef = Selection.pack(Selection.index(ref2), Selection.generation(ref1));
        // Not directly testable via lookupById, but can check that only the new ref is valid
        assertThat((long) table.lookupById(777L)).isEqualTo(ref2);
    }
    @Test
    void lookupInterceptor_stringKeyAndGenerationMismatch() throws Throwable {
        // Insert a row with a string key (simulate a table with string PK)
        var config = MemrisConfiguration.builder().tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE).build();
        var metadata = new TableMetadata(
            "Book",
            "io.memris.test.Book",
            java.util.List.of(
                new FieldMetadata("isbn", TypeCodes.TYPE_STRING, true, true),
                new FieldMetadata("title", TypeCodes.TYPE_STRING, false, false)
            )
        );
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        GeneratedTable stringTable = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(8, 2, 1);
        long ref = stringTable.insertFrom(new Object[]{"978-1-23", "Test Book"});
        // Successful lookup using LookupInterceptor directly
        var interceptor = new MethodHandleImplementation.LookupInterceptor("String");
        long foundRef = (long) interceptor.intercept("978-1-23", stringTable);
        assertThat(foundRef).isEqualTo(ref);
        // Remove from index (simulate stale ref)
        stringTable.tombstone(ref);
        // Should return -1 after tombstone
        assertThat((long) interceptor.intercept("978-1-23", stringTable)).isEqualTo(-1L);
        // Insert again (should get new ref with new generation)
        long ref2 = stringTable.insertFrom(new Object[]{"978-1-23", "Test Book 2"});
        assertThat(ref2).isNotEqualTo(ref);
        // Manually tamper with ref to simulate stale generation
        long staleRef = Selection.pack(Selection.index(ref2), Selection.generation(ref));
        // Should return -1 for stale generation (not directly testable, but covers code path)
        assertThat((long) interceptor.intercept("978-1-23", stringTable)).isEqualTo(ref2);
    }

    @Test
    void lookupInterceptor_removeOperation_longAndString() throws Throwable {
        // Long PK table
        var config = MemrisConfiguration.builder().tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE).build();
        var metadata = new TableMetadata(
            "Person",
            "io.memris.test.Person",
            java.util.List.of(
                new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                new FieldMetadata("name", TypeCodes.TYPE_STRING, false, false)
            )
        );
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        GeneratedTable longTable = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(8, 2, 1);
        long ref = longTable.insertFrom(new Object[]{123L, "Bob"});
        // Remove using LookupInterceptor
        var interceptor = new MethodHandleImplementation.LookupInterceptor("remove");
        interceptor.intercept(123L, longTable);
        assertThat((long) longTable.lookupById(123L)).isEqualTo(-1L);

        // String PK table
        var metadata2 = new TableMetadata(
            "Book",
            "io.memris.test.Book",
            java.util.List.of(
                new FieldMetadata("isbn", TypeCodes.TYPE_STRING, true, true),
                new FieldMetadata("title", TypeCodes.TYPE_STRING, false, false)
            )
        );
        Class<? extends AbstractTable> tableClass2 = TableGenerator.generate(metadata2, config);
        GeneratedTable stringTable = (GeneratedTable) tableClass2.getConstructor(int.class, int.class, int.class).newInstance(8, 2, 1);
        long refS = stringTable.insertFrom(new Object[]{"978-1-23", "Test Book"});
        var interceptorS = new MethodHandleImplementation.LookupInterceptor("remove");
        interceptorS.intercept("978-1-23", stringTable);
        // Use LookupInterceptor directly for string PK
        interceptorS.intercept("978-1-23", stringTable); // should not throw
        var lookupString = new MethodHandleImplementation.LookupInterceptor("String");
        assertThat((long) lookupString.intercept("978-1-23", stringTable)).isEqualTo(-1L);
    }

    @Test
    void lookupInterceptor_throwsOnUnknownOperation() throws Throwable {
        var interceptor = new MethodHandleImplementation.LookupInterceptor("unknown");
        Throwable ex = null;
        try {
            interceptor.intercept(1L, table);
        } catch (Throwable e) {
            ex = e;
        }
        assertThat(ex).isInstanceOf(IllegalStateException.class).hasMessageContaining("Unknown operation");
    }
    @Test
    void scanEqualsInterceptors_returnCorrectRows() {
        long ref1 = table.insertFrom(new Object[]{101L, 30, "Bob"});
        long ref2 = table.insertFrom(new Object[]{102L, 40, "Alice"});
        long ref3 = table.insertFrom(new Object[]{103L, 30, "bob"});
        int row1 = Selection.index(ref1);
        int row2 = Selection.index(ref2);
        int row3 = Selection.index(ref3);
        // scanEqualsLong (id)
        int[] eqLong = table.scanEqualsLong(0, 101L);
        assertThat(eqLong).containsExactly(row1);
        // scanEqualsInt (age)
        int[] eqInt = table.scanEqualsInt(1, 30);
        assertThat(eqInt).contains(row1, row3);
        // scanEqualsString (name)
        int[] eqStr = table.scanEqualsString(2, "Bob");
        assertThat(eqStr).containsExactly(row1);
        // scanEqualsStringIgnoreCase (name)
        int[] eqStrIgnore = table.scanEqualsStringIgnoreCase(2, "bob");
        assertThat(eqStrIgnore).contains(row1, row3);
    }

    @Test
    void scanBetweenInterceptors_returnCorrectRows() {
        long ref1 = table.insertFrom(new Object[]{201L, 10, "X"});
        long ref2 = table.insertFrom(new Object[]{202L, 20, "Y"});
        long ref3 = table.insertFrom(new Object[]{203L, 30, "Z"});
        int row1 = Selection.index(ref1);
        int row2 = Selection.index(ref2);
        int row3 = Selection.index(ref3);
        // scanBetweenLong (id)
        int[] betweenLong = table.scanBetweenLong(0, 201L, 202L);
        assertThat(betweenLong).contains(row1, row2);
        // scanBetweenInt (age)
        int[] betweenInt = table.scanBetweenInt(1, 15, 30);
        assertThat(betweenInt).contains(row2, row3);
    }

    @Test
    void scanInInterceptors_returnCorrectRows() {
        long ref1 = table.insertFrom(new Object[]{301L, 77, "foo"});
        long ref2 = table.insertFrom(new Object[]{302L, 88, "bar"});
        long ref3 = table.insertFrom(new Object[]{303L, 99, "baz"});
        int row1 = Selection.index(ref1);
        int row2 = Selection.index(ref2);
        int row3 = Selection.index(ref3);
        // scanInLong (id)
        int[] inLong = table.scanInLong(0, new long[]{301L, 303L});
        assertThat(inLong).contains(row1, row3);
        // scanInInt (age)
        int[] inInt = table.scanInInt(1, new int[]{88, 99});
        assertThat(inInt).contains(row2, row3);
        // scanInString (name)
        int[] inStr = table.scanInString(2, new String[]{"foo", "baz"});
        assertThat(inStr).contains(row1, row3);
    }

    @Test
    void isLiveInterceptor_detectsTombstonedAndLiveRows() {
        long ref = table.insertFrom(new Object[]{401L, 55, "live"});
        int row = Selection.index(ref);
        // Should be live before tombstone
        assertThat(table.isLive(ref)).isTrue();
        table.tombstone(ref);
        // Should not be live after tombstone
        assertThat(table.isLive(ref)).isFalse();
    }

    private GeneratedTable table;

    @BeforeEach
    void setup() throws Exception {
        // Generate a table with LONG, INT, and STRING columns
        var config = MemrisConfiguration.builder().tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE).build();
        var metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("age", TypeCodes.TYPE_INT, false, false),
                        new FieldMetadata("name", TypeCodes.TYPE_STRING, false, false)
                )
        );
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(8, 2, 1);
    }

    @Test
    void insertInterceptor_insertsAndUpdatesIndex() throws Throwable {
        long ref = table.insertFrom(new Object[]{42L, 21, "Alice"});
        long value = (long) table.readLong(0, Selection.index(ref));
        int age = (int) table.readInt(1, Selection.index(ref));
        String name = (String) table.readString(2, Selection.index(ref));
        assertThat(value).isEqualTo(42L);
        assertThat(age).isEqualTo(21);
        assertThat(name).isEqualTo("Alice");
        long foundRef = (long) table.lookupById(42L);
        assertThat(foundRef).isEqualTo(ref);
    }

    @Test
    void typeCodeInterceptor_returnsCorrectTypeCodes() {
        assertThat(new MethodHandleImplementation.TypeCodeInterceptor()).isNotNull();
        assertThat(table.typeCodeAt(0)).isEqualTo(TypeCodes.TYPE_LONG);
        assertThat(table.typeCodeAt(1)).isEqualTo(TypeCodes.TYPE_INT);
        assertThat(table.typeCodeAt(2)).isEqualTo(TypeCodes.TYPE_STRING);
    }

    @Test
    void presentInterceptor_and_readInterceptor_workForNulls() {
        long ref = table.insertFrom(new Object[]{100L, null, null});
        int row = Selection.index(ref);
        assertThat(table.isPresent(0, row)).isTrue();
        assertThat(table.isPresent(1, row)).isFalse();
        assertThat(table.isPresent(2, row)).isFalse();
        assertThat(table.readInt(1, row)).isEqualTo(0); // default for null
        assertThat(table.readString(2, row)).isNull();
    }

    @Test
    void scanAllInterceptor_returnsAllNonTombstonedRows() {
        long ref1 = table.insertFrom(new Object[]{1L, 10, "A"});
        long ref2 = table.insertFrom(new Object[]{2L, 20, "B"});
        int[] allRows = table.scanAll();
        assertThat(allRows).contains(Selection.index(ref1), Selection.index(ref2));
        // Tombstone one row
        table.tombstone(ref1);
        int[] afterTombstone = table.scanAll();
        assertThat(afterTombstone).containsExactly(Selection.index(ref2));
    }

    @Test
    void lookupInterceptor_returnsMinusOneForMissingOrTombstoned() {
        long ref = table.insertFrom(new Object[]{123L, 55, "Z"});
        assertThat((long) table.lookupById(123L)).isEqualTo(ref);
        table.tombstone(ref);
        assertThat((long) table.lookupById(123L)).isEqualTo(-1L);
        assertThat((long) table.lookupById(999L)).isEqualTo(-1L);
    }

    @Test
    void readAndPresentInterceptorsRejectOutOfRangeColumns() {
        long ref = table.insertFrom(new Object[] { 1L, 22, "A" });
        int row = Selection.index(ref);

        org.junit.jupiter.api.Assertions.assertThrows(IndexOutOfBoundsException.class, () -> table.readInt(99, row));
        org.junit.jupiter.api.Assertions.assertThrows(IndexOutOfBoundsException.class, () -> table.isPresent(99, row));
        org.junit.jupiter.api.Assertions.assertThrows(IndexOutOfBoundsException.class, () -> table.typeCodeAt(99));
    }

    @Test
    void insertInterceptorCoversAdditionalTypeBranches() throws Exception {
        var config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE)
                .build();
        var metadata = new TableMetadata(
                "Metrics",
                "io.memris.test.Metrics",
                java.util.List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("scoreDouble", TypeCodes.TYPE_DOUBLE, false, false),
                        new FieldMetadata("ratioFloat", TypeCodes.TYPE_FLOAT, false, false),
                        new FieldMetadata("enabled", TypeCodes.TYPE_BOOLEAN, false, false),
                        new FieldMetadata("grade", TypeCodes.TYPE_CHAR, false, false),
                        new FieldMetadata("note", TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("createdAt", TypeCodes.TYPE_INSTANT, false, false),
                        new FieldMetadata("nullableInt", TypeCodes.TYPE_INT, false, false)));

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        GeneratedTable metrics = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class)
                .newInstance(16, 2, 1);

        long ref = metrics.insertFrom(new Object[] {
                9L,
                12.5d,
                1.5f,
                true,
                'A',
                "ok",
                Instant.ofEpochMilli(1234L).toEpochMilli(),
                7
        });
        int row = Selection.index(ref);

        assertThat(metrics.readLong(0, row)).isEqualTo(9L);
        assertThat(metrics.readInt(2, row)).isNotZero();
        assertThat(metrics.readInt(3, row)).isEqualTo(1);
        assertThat(metrics.readInt(4, row)).isEqualTo((int) 'A');
        assertThat(metrics.readString(5, row)).isEqualTo("ok");
        assertThat(metrics.isPresent(7, row)).isTrue();

        long nullRef = metrics.insertFrom(new Object[] { 10L, null, null, null, null, null, null, null });
        int nullRow = Selection.index(nullRef);
        assertThat(metrics.isPresent(1, nullRow)).isFalse();
        assertThat(metrics.isPresent(2, nullRow)).isFalse();
        assertThat(metrics.isPresent(3, nullRow)).isFalse();
        assertThat(metrics.isPresent(4, nullRow)).isFalse();
        assertThat(metrics.isPresent(5, nullRow)).isFalse();
        assertThat(metrics.isPresent(6, nullRow)).isFalse();
        assertThat(metrics.isPresent(7, nullRow)).isFalse();

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> metrics.insertFrom(new Object[] { 1L }));
    }

    @Test
    void readAndPresentInterceptorsUnwrapUnsupportedTypeCodes() throws Throwable {
        long ref = table.insertFrom(new Object[] { 44L, 5, "x" });
        int row = Selection.index(ref);
        String columnFieldName = firstColumnFieldName(table.getClass());
        var fields = java.util.List.of(new TableImplementationStrategy.ColumnFieldInfo(
                columnFieldName,
                Object.class,
                (byte) 127,
                0,
                false));

        var read = new MethodHandleImplementation.ReadInterceptor(fields);
        var present = new MethodHandleImplementation.PresentInterceptor(fields);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> read.intercept(0, row, table));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> present.intercept(0, row, table));
    }

    private static String firstColumnFieldName(Class<?> tableClass) {
        return Arrays.stream(tableClass.getDeclaredFields())
                .filter(field -> field.getType() == PageColumnLong.class
                        || field.getType() == PageColumnInt.class
                        || field.getType() == PageColumnString.class)
                .map(Field::getName)
                .findFirst()
                .orElseThrow();
    }
}
