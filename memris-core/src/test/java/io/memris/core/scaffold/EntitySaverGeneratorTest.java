package io.memris.core.scaffold;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.repository.EntitySaverGenerator;
import io.memris.core.MetadataExtractor;
import io.memris.runtime.EntitySaver;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import io.memris.storage.heap.FieldMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for EntitySaverGenerator - zero-reflection entity save operations.
 */
class EntitySaverGeneratorTest {

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
        assertEquals(id, saved.id);
        assertEquals("Alice", saved.name);
        assertEquals("alice@example.com", saved.email);

        // Verify in table
        long ref = table.lookupById(id);
        assertTrue(ref >= 0);
    }

    @Test
    void generatedSaverShouldExtractId() throws Exception {
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Customer customer = new Customer();
        customer.id = 42L;

        Object extracted = saver.extractId(customer);

        assertEquals(42L, extracted);
    }

    @Test
    void generatedSaverShouldSetId() throws Exception {
        Class<Customer> entityClass = Customer.class;
        EntityMetadata<Customer> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<Customer, ?> saver = EntitySaverGenerator.generate(entityClass, metadata);

        Customer customer = new Customer();
        assertNull(customer.id);

        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = saver;
        rawSaver.setId(customer, 99L);

        assertEquals(99L, customer.id);
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
        assertEquals(5L, customerId);
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
}
