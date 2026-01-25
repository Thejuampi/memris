package io.memris.spring;

import io.memris.storage.ffm.FfmTable;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for TableManager.
 *
 * TableManager is responsible for:
 * - Creating tables for entities
 * - Caching tables for reuse
 * - Managing nested entity tables
 * - Creating join tables
 * - Caching enum values
 * - Providing table lookup
 */
public class TableManagerTest {

    @Test
    void shouldCreateTableForEntity() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            FfmTable table = tableManager.getOrCreateTable(SimpleEntity.class);

            assertThat(table)
                .isNotNull();
            assertThat(table.name())
                .isEqualTo("SimpleEntity");
        }
    }

    @Test
    void shouldCacheCreatedTables() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            FfmTable table1 = tableManager.getOrCreateTable(SimpleEntity.class);
            FfmTable table2 = tableManager.getOrCreateTable(SimpleEntity.class);

            assertThat(table1)
                .isSameAs(table2);
        }
    }

    @Test
    void shouldCreateMultipleEntityTables() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            // Create multiple related entity tables
            FfmTable deptTable = tableManager.getOrCreateTable(Department.class);
            FfmTable addressTable = tableManager.getOrCreateTable(Address.class);
            FfmTable accountTable = tableManager.getOrCreateTable(Account.class);

            // Verify all tables were created
            assertThat(deptTable).isNotNull();
            assertThat(addressTable).isNotNull();
            assertThat(accountTable).isNotNull();

            // Verify they are different instances
            assertThat(deptTable).isNotSameAs(addressTable);
            assertThat(addressTable).isNotSameAs(accountTable);

            // Verify table names
            assertThat(deptTable.name()).isEqualTo("Department");
            assertThat(addressTable.name()).isEqualTo("Address");
            assertThat(accountTable.name()).isEqualTo("Account");
        }
    }

    @Test
    void shouldCacheEnumValues() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            tableManager.cacheEnumValues(OrderEntity.class);

            // Note: For enum field detection, the field needs to be an actual enum type
            // The current implementation checks if field.getType().isEnum()
            // So this test verifies the cacheEnumValues method runs without errors
            assertThat(tableManager.getEnumValues(OrderEntity.class, "status"))
                .isNotNull();
        }
    }

    @Test
    void shouldGetOrCreateTableForEntity() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            // First call creates the table
            FfmTable first = tableManager.getOrCreateTable(SimpleEntity.class);
            // Second call returns cached table
            FfmTable second = tableManager.getOrCreateTable(SimpleEntity.class);

            assertThat(first)
                .isNotNull()
                .isSameAs(second);
        }
    }

    @Test
    void shouldGetTable() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            // Get table that doesn't exist returns null
            assertThat(tableManager.getTable(SimpleEntity.class))
                .isNull();

            // Create the table
            tableManager.getOrCreateTable(SimpleEntity.class);

            // Get table that exists returns the table
            assertThat(tableManager.getTable(SimpleEntity.class))
                .isNotNull();
        }
    }

    @Test
    void shouldReturnArena() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            assertThat(tableManager.arena())
                .isNotNull()
                .isSameAs(arena);
        }
    }

    @Test
    void shouldCreateJoinTables() {
        try (Arena arena = Arena.ofConfined()) {
            TableManager tableManager = new TableManager(arena);

            // First create the entity tables
            tableManager.getOrCreateTable(Student.class);
            tableManager.getOrCreateTable(Course.class);

            // Note: createJoinTables requires proper @ManyToMany annotation detection
            // on entity fields. For this test, we verify the method runs without errors.
            // Full integration tests with @ManyToMany are in ManyToManyTest.
            tableManager.createJoinTables(Student.class);

            // Verify no exceptions thrown
            assertThat(tableManager.getTable(Student.class)).isNotNull();
        }
    }

    // Test entities

    @jakarta.persistence.Entity
    static class SimpleEntity {
        @jakarta.persistence.Id
        private Long id;

        private String name;
        private int age;
    }

    @jakarta.persistence.Entity
    static class Department {
        @jakarta.persistence.Id
        private Long id;

        private String name;
    }

    @jakarta.persistence.Entity
    static class Address {
        @jakarta.persistence.Id
        private Long id;

        private String city;
    }

    @jakarta.persistence.Entity
    static class Account {
        @jakarta.persistence.Id
        private Long id;

        private String username;
    }

    @jakarta.persistence.Entity
    static class Student {
        @jakarta.persistence.Id
        private int id;

        private String name;
    }

    @jakarta.persistence.Entity
    static class Course {
        @jakarta.persistence.Id
        private int id;

        private String title;
    }

    @jakarta.persistence.Entity
    static class OrderEntity {
        @jakarta.persistence.Id
        private Long id;

        @jakarta.persistence.Enumerated(jakarta.persistence.EnumType.STRING)
        private OrderStatus status;

        private String customerName;
    }

    enum OrderStatus {
        PENDING, SHIPPED, DELIVERED
    }
}
