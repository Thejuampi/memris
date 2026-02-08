package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * TDD Tests for BytecodeTableGenerator - true bytecode table implementation
 * without MethodHandle.
 *
 * These tests verify that generated table classes:
 * 1. Have direct field access (no MethodHandle.invoke)
 * 2. Support all scan operations (equals, between, in)
 * 3. Handle tombstone filtering inline
 * 4. Use O(1) array access for type codes
 */
class BytecodeTableGeneratorTest {

        @Test
        void generatedTableShouldSupportScanEqualsLong() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                long ref1 = table.insertFrom(new Object[] { 1L, "Alice", 100, 1000L });
                long ref2 = table.insertFrom(new Object[] { 2L, "Bob", 101, 1000L });
                long ref3 = table.insertFrom(new Object[] { 3L, "Charlie", 102, 1000L });

                int[] matches = table.scanEqualsLong(0, 2L);

                assertThat(matches).containsExactly(1);
        }

        @Test
        void generatedTableShouldSupportScanEqualsString() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                table.insertFrom(new Object[] { 1L, "Alice", 100, 1000L });
                table.insertFrom(new Object[] { 2L, "Bob", 101, 1000L });
                table.insertFrom(new Object[] { 3L, "Charlie", 102, 1000L });

                int[] matches = table.scanEqualsString(1, "Bob");

                assertThat(new StringMatchSnapshot(matches.length, table.readString(1, matches[0])))
                                .usingRecursiveComparison()
                                .isEqualTo(new StringMatchSnapshot(1, "Bob"));
        }

        @Test
        void generatedTableShouldSupportScanBetweenLong() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                table.insertFrom(new Object[] { 1L, "Alice", 100, 1000L });
                table.insertFrom(new Object[] { 2L, "Bob", 200, 2000L });
                table.insertFrom(new Object[] { 3L, "Charlie", 300, 3000L });

                // Scan salary (index 3) - LONG column
                int[] matches = table.scanBetweenLong(3, 1500L, 2500L);

                assertThat(matches).hasSize(1);
                assertThat(table.readLong(3, matches[0])).isEqualTo(2000L);

                // Scan age (index 2) - INT column with scanBetweenLong (contract: supports
                // int/long)
                int[] intMatches = table.scanBetweenLong(2, 150L, 250L);
                assertThat(intMatches).hasSize(1);
                assertThat(table.readInt(2, intMatches[0])).isEqualTo(200);
        }

        @Test
        void generatedTableShouldSupportScanInLong() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                table.insertFrom(new Object[] { 1L, "Alice", 100, 1000L });
                table.insertFrom(new Object[] { 2L, "Bob", 200, 2000L });
                table.insertFrom(new Object[] { 3L, "Charlie", 300, 3000L });

                int[] matches = table.scanInLong(0, new long[] { 1L, 3L });

                assertThat(matches).hasSize(2);
        }

        @Test
        void generatedTableShouldFilterTombstonedRows() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                long ref1 = table.insertFrom(new Object[] { 1L, "Alice", 100, 1000L });
                table.insertFrom(new Object[] { 2L, "Bob", 101, 1000L });
                table.insertFrom(new Object[] { 3L, "Charlie", 102, 1000L });

                // Tombstone the first row
                table.tombstone(ref1);

                int[] allMatches = table.scanEqualsLong(0, 1L);
                assertThat(allMatches).isEmpty();

                int[] remaining = table.scanAll();
                assertThat(remaining).hasSize(2);
        }

        @Test
        void generatedTableShouldHaveO1TypeCodeAccess() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                // Type code access should use direct array lookup
                assertThat(new byte[] { table.typeCodeAt(0), table.typeCodeAt(1), table.typeCodeAt(2),
                                table.typeCodeAt(3) })
                                .containsExactly(io.memris.core.TypeCodes.TYPE_LONG,
                                                io.memris.core.TypeCodes.TYPE_STRING,
                                                io.memris.core.TypeCodes.TYPE_INT,
                                                io.memris.core.TypeCodes.TYPE_LONG);
        }

        @Test
        void generatedTableShouldSupportLookupById() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);
                GeneratedTable table = (GeneratedTable) abstractTable;

                long ref = table.insertFrom(new Object[] { 42L, "Test", 999, 5000L });

                long found = table.lookupById(42L);

                assertThat(found).isEqualTo(ref);
        }

        @Test
        void generatedTableShouldNotUseReflection() throws Exception {
                // This test verifies the generated class doesn't have MethodHandle fields
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);

                // Check for no MethodHandle fields
                java.lang.reflect.Field[] fields = tableClass.getDeclaredFields();
                for (java.lang.reflect.Field field : fields) {
                        if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                                fail("Generated table should not have MethodHandle fields: " + field.getName());
                        }
                }
        }

        /**
         * Test that exercises the seqlock retry path under contention.
         * This will fail with linkage errors if the backoff method is incorrectly
         * invoked
         * (e.g., INVOKEVIRTUAL instead of INVOKESTATIC, or if backoff is not
         * accessible).
         */
        @Test
        void generatedTableShouldHandleSeqlockContentionWithoutLinkageErrors() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(256, 8, 1); // Larger table for contention test
                GeneratedTable table = (GeneratedTable) abstractTable;

                // Insert initial data
                table.insertFrom(new Object[] { 1L, "Alice", 30, 50000L });

                java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
                java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
                java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(2);
                java.util.concurrent.CountDownLatch writerHoldingRowLock = new java.util.concurrent.CountDownLatch(1);

                // Writer thread: repeatedly toggle seqlock on row 0 to force retry path
                Thread writer = new Thread(() -> {
                        try {
                                startLatch.await();

                                // Hold row 0 in write state once so reader is guaranteed to observe
                                // contention.
                                abstractTable.beginSeqLock(0);
                                try {
                                        writerHoldingRowLock.countDown();
                                        Thread.sleep(5);
                                } finally {
                                        abstractTable.endSeqLock(0);
                                }

                                for (int i = 0; i < 5000 && !Thread.currentThread().isInterrupted(); i++) {
                                        abstractTable.beginSeqLock(0);
                                        try {
                                                Thread.onSpinWait();
                                        } finally {
                                                abstractTable.endSeqLock(0);
                                        }
                                }
                        } catch (Throwable t) {
                                error.compareAndSet(null, t);
                        } finally {
                                doneLatch.countDown();
                        }
                });

                // Reader thread: continuously read to trigger seqlock retries
                Thread reader = new Thread(() -> {
                        try {
                                startLatch.await();

                                // Wait until writer has entered write phase for row 0.
                                writerHoldingRowLock.await();

                                for (int i = 0; i < 5000 && !Thread.currentThread().isInterrupted(); i++) {
                                        table.readLong(0, 0);
                                        table.readString(1, 0);
                                        table.readInt(2, 0);
                                }
                        } catch (Throwable t) {
                                error.compareAndSet(null, t);
                        } finally {
                                doneLatch.countDown();
                        }
                });

                writer.start();
                reader.start();
                startLatch.countDown();

                try {
                        boolean completed = doneLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
                        if (!completed) {
                                fail("Contention test timed out - threads did not complete in 10 seconds");
                        }

                        // Check for linkage errors (the bug we're preventing)
                        Throwable caught = error.get();
                        if (caught != null) {
                                if (caught instanceof LinkageError) {
                                        fail("Seqlock retry path has linkage error (likely backoff method issue): "
                                                        + caught.getMessage());
                                }
                                // Re-throw other unexpected errors
                                throw new RuntimeException("Unexpected error during contention test", caught);
                        }
                } finally {
                        // Always cleanup threads
                        writer.interrupt();
                        reader.interrupt();
                        writer.join(1000);
                        reader.join(1000);
                }
        }

        private TableMetadata createPersonMetadata() {
                return new TableMetadata(
                                "Person",
                                "io.memris.test.Person",
                                java.util.List.of(
                                                new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                                                new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false,
                                                                false),
                                                new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false,
                                                                false),
                                                new FieldMetadata("salary", io.memris.core.TypeCodes.TYPE_LONG, false,
                                                                false)));
        }

        private record StringMatchSnapshot(int size, String value) {
        }

        @Test
        void generatedTableShouldHavePerColumnDirectAccessors() throws Exception {
                TableMetadata metadata = createPersonMetadata();
                Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
                AbstractTable abstractTable = tableClass
                                .getConstructor(int.class, int.class, int.class)
                                .newInstance(32, 4, 1);

                // Insert test data
                GeneratedTable table = (GeneratedTable) abstractTable;
                table.insertFrom(new Object[] { 1L, "Alice", 30, 50000L });
                table.insertFrom(new Object[] { 2L, "Bob", 25, 60000L });

                // Verify per-column read methods exist and work
                // read{Col}Long for id column (col0)
                java.lang.reflect.Method readCol0Long = tableClass.getMethod("readCol0Long", int.class);
                long idValue = (long) readCol0Long.invoke(abstractTable, 0);
                assertThat(idValue).isEqualTo(1L);

                // read{Col}String for name column (col1)
                java.lang.reflect.Method readCol1String = tableClass.getMethod("readCol1String", int.class);
                String nameValue = (String) readCol1String.invoke(abstractTable, 0);
                assertThat(nameValue).isEqualTo("Alice");

                // read{Col}Int for age column (col2)
                java.lang.reflect.Method readCol2Int = tableClass.getMethod("readCol2Int", int.class);
                int ageValue = (int) readCol2Int.invoke(abstractTable, 0);
                assertThat(ageValue).isEqualTo(30);

                // read{Col}Long for salary column (col3)
                java.lang.reflect.Method readCol3Long = tableClass.getMethod("readCol3Long", int.class);
                long salaryValue = (long) readCol3Long.invoke(abstractTable, 0);
                assertThat(salaryValue).isEqualTo(50000L);

                // Verify per-column scan methods exist and work
                java.lang.reflect.Method scanEqualsCol0Long = tableClass.getMethod("scanEqualsCol0Long", long.class);
                int[] matches = (int[]) scanEqualsCol0Long.invoke(abstractTable, 1L);
                assertThat(matches).containsExactly(0);

                java.lang.reflect.Method scanEqualsCol1String = tableClass.getMethod("scanEqualsCol1String",
                                String.class);
                int[] stringMatches = (int[]) scanEqualsCol1String.invoke(abstractTable, "Bob");
                assertThat(stringMatches).containsExactly(1);
        }

}
