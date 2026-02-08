package io.memris.core.scaffold;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.repository.EntitySaverGenerator;
import io.memris.core.MetadataExtractor;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import io.memris.runtime.EntitySaver;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import io.memris.storage.heap.FieldMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * TDD Tests for EntitySaverGenerator - zero-reflection entity save operations.
 */
class EntitySaverGeneratorTest {

    @Test
    void generatedSaverShouldSkipNoOpConvertersForDefaultTypes() throws Exception {
        var metadata = MetadataExtractor.extractEntityMetadata(Customer.class);
        var saver = EntitySaverGenerator.generate(Customer.class, metadata);

        var converterFieldCount = Arrays.stream(saver.getClass().getDeclaredFields())
                .filter(field -> TypeConverter.class.isAssignableFrom(field.getType()))
                .count();

        assertThat(converterFieldCount).isZero();
    }

    @Test
    void generatedSaverShouldApplyCustomSameTypeConverter() throws Exception {
        TypeConverterRegistry.getInstance().registerFieldConverter(InventoryItem.class, "stock",
                new OffsetStockConverter());

        var metadata = MetadataExtractor.extractEntityMetadata(InventoryItem.class);
        var saver = EntitySaverGenerator.generate(InventoryItem.class, metadata);

        var tableMetadata = new TableMetadata(
                "InventoryItem",
                "io.memris.test.InventoryItem",
                List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("stock", io.memris.core.TypeCodes.TYPE_INT, false, false)));
        var tableClass = TableGenerator.generate(tableMetadata);
        var table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);

        var item = new InventoryItem();
        item.stock = 10;

        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = saver;
        rawSaver.save(item, table, 7L);

        var rowIndex = Selection.index(table.lookupById(7L));
        var storedStock = table.readInt(1, rowIndex);

        assertThat(storedStock).isEqualTo(15);
    }

    @Test
    void generatedSaverShouldSaveEntityWithDirectFieldAccess() throws Exception {
        // Create test entity class
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);

        // Generate the saver
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        // Verify no MethodHandle fields
        assertNoMethodHandleFields(saver.getClass());

        // Create table
        TableMetadata tableMetadata = new TableMetadata(
                "Customer",
                "io.memris.test.Customer",
                List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("email", io.memris.core.TypeCodes.TYPE_STRING, false, false)));
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);

        // Create and save entity
        Customer customer = new Customer();
        customer.name = "Alice";
        customer.email = "alice@example.com";

        Long id = 1L;
        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = saver;
        @SuppressWarnings("unchecked")
        Customer saved = (Customer) rawSaver.save(customer, table, id);

        // Verify entity was saved with ID
        assertThat(saved).usingRecursiveComparison().isEqualTo(new Customer(id, "Alice", "alice@example.com"));

        // Verify in table
        long ref = table.lookupById(id);
        assertThat(ref).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void generatedSaverShouldExtractId() throws Exception {
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Customer customer = new Customer();
        customer.id = 42L;

        Object extracted = saver.extractId(customer);

        assertThat(extracted).isEqualTo(42L);
    }

    @Test
    void generatedSaverShouldSetId() throws Exception {
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Customer customer = new Customer();
        assertThat(customer.id).isNull();

        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = saver;
        rawSaver.setId(customer, 99L);

        assertThat(customer.id).isEqualTo(99L);
    }

    @Test
    void generatedSaverShouldHandleRelationships() throws Exception {
        // Test with Order that has Customer relationship
        Class<Order> entityClass = Order.class;
        EntityMetadata<Order> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Order, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Customer customer = new Customer();
        customer.id = 5L;
        customer.name = "Bob";

        Order order = new Order();
        order.id = 10L;
        order.amount = 1000L;
        order.customer = customer;

        // Resolve relationship ID
        Object customerId = saver.resolveRelationshipId("customer", customer);
        assertThat(customerId).isEqualTo(5L);
    }

    @Test
    void generatedSaverShouldNotUseReflection() throws Exception {
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Class<?> saverClass = saver.getClass();

        // Check for no MethodHandle fields
        java.lang.reflect.Field[] fields = saverClass.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("EntitySaver should not have MethodHandle fields: " + field.getName());
            }
        }
    }

    private void assertNoMethodHandleFields(Class<?> clazz) {
        java.lang.reflect.Field[] fields = clazz.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("Generated saver should not have MethodHandle fields: " + field.getName());
            }
        }
    }

    // Test entity classes
    public static class Customer {
        public Customer() {
        }

        public Customer(Long id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        @Id
        public Long id;
        public String name;
        public String email;
    }

    public static class Order {
        @Id
        public Long id;
        public Long amount;
        public Customer customer;
    }

    public static class InventoryItem {
        @Id
        public Long id;
        public Integer stock;
    }

    private static final class OffsetStockConverter implements TypeConverter<Integer, Integer> {
        @Override
        public Class<Integer> javaType() {
            return Integer.class;
        }

        @Override
        public Class<Integer> storageType() {
            return Integer.class;
        }

        @Override
        public Integer toStorage(Integer javaValue) {
            return javaValue == null ? null : javaValue + 5;
        }

        @Override
        public Integer fromStorage(Integer storageValue) {
            return storageValue == null ? null : storageValue - 5;
        }
    }
}
