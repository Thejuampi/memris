package io.memris.realworld;

import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import io.memris.spring.boot.autoconfigure.MemrisAutoConfiguration;
import io.memris.spring.data.repository.MemrisSpringRepository;
import io.memris.spring.data.repository.config.EnableMemrisRepositories;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.repository.NoRepositoryBean;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringInventoryRealWorldTest {
    private ApplicationContextRunner inventoryContext() {
        return arenaContext().withUserConfiguration(InventoryConfiguration.class);
    }

    private ApplicationContextRunner arenaContext() {
        String table = System.getProperty("memris.test.table", "BYTECODE");
        String codegen = System.getProperty("memris.test.codegen", "true");
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MemrisAutoConfiguration.class))
                .withPropertyValues("memris.arenas.default.initial-pages=1", "memris.arenas.default.page-size=64",
                        "memris.arenas.default.table-implementation=" + table,
                        "memris.arenas.default.codegen-enabled=" + codegen,
                        "memris.arenas.reporting.initial-pages=1", "memris.arenas.reporting.page-size=64",
                        "memris.arenas.reporting.table-implementation=" + table,
                        "memris.arenas.reporting.codegen-enabled=" + codegen);
    }

    @Test
    void shouldRoundTripConvertedAndEmbeddedStockMappingsThroughSpring() {
        inventoryContext().run(context -> {
            var inventory = context.getBean(InventoryRepository.class);
            var batch = UUID.fromString("13d7322b-194f-4ce4-85de-e66d7bad87c3");
            var restock = LocalDate.of(2026, 11, 2);
            var laptop = new Inventory(new Sku("MAPPED"), "Mapped laptop", 4);
            laptop.cost = new Money(125_000L);
            laptop.details = new StockDetails("north", 2, restock, batch);
            laptop = inventory.save(laptop);
            var draft = inventory.save(new Inventory(null, "Draft mapping", 0));

            var loaded = inventory.findById(laptop.id).orElseThrow();
            assertThat(loaded.cost).isEqualTo(new Money(125_000L));
            assertThat(loaded.details).usingRecursiveComparison().isEqualTo(laptop.details);
            assertThat(inventory.findByDetailsWarehouse("north"))
                    .extracting(row -> row.id).containsExactly(laptop.id);
            assertThat(inventory.findByCostIsNull()).extracting(row -> row.id).containsExactly(draft.id);
            assertThat(inventory.findById(draft.id)).get().satisfies(row -> {
                assertThat(row.cost).isNull();
                assertThat(row.details).isNull();
            });

            loaded.cost = new Money(119_000L);
            loaded.details.warehouse = "south";
            loaded.details.minimumStock = null;
            loaded.details.restockDate = null;
            loaded.details.batch = null;
            inventory.save(loaded);
            assertThat(inventory.findByDetailsWarehouse("north")).isEmpty();
            assertThat(inventory.findByDetailsWarehouse("south"))
                    .singleElement().satisfies(row -> {
                        assertThat(row.id).isEqualTo(loaded.id);
                        assertThat(row.cost).isEqualTo(new Money(119_000L));
                        assertThat(row.details.minimumStock).isNull();
                        assertThat(row.details.restockDate).isNull();
                        assertThat(row.details.batch).isNull();
                    });
            loaded.details = null;
            loaded.cost = null;
            inventory.save(loaded);
            assertThat(inventory.findById(loaded.id)).get().satisfies(row -> {
                assertThat(row.details).isNull();
                assertThat(row.cost).isNull();
            });
        });
    }

    @Test
    void shouldPreserveAssignedLongIdsAndGenerateForNullAndZeroIds() {
        inventoryContext().run(context -> {
            var inventory = context.getBean(InventoryRepository.class);
            var imported = new Inventory(new Sku("IMPORTED"), "Imported stock", 7);
            imported.id = 1L;
            assertThat(inventory.save(imported).id).isEqualTo(1L);
            var generated = inventory.save(new Inventory(new Sku("AUTO"), "New stock", 3));
            var zero = new Inventory(new Sku("ZERO"), "Unassigned stock", 1);
            zero.id = 0L;
            zero = inventory.save(zero);
            assertThat(generated.id).isPositive().isNotEqualTo(imported.id);
            assertThat(zero.id).isPositive().isNotIn(generated.id, imported.id);
            imported.stock = 9;
            inventory.save(imported);
            assertThat(inventory.count()).isEqualTo(3);
            assertThat(inventory.findById(1L)).get().extracting(row -> row.stock).isEqualTo(9);
            inventory.deleteById(generated.id);
            assertThat(inventory.findById(generated.id)).isEmpty();
            assertThat(inventory.findById(zero.id)).isPresent();
            var lastImport = new Inventory(new Sku("LAST"), "Last imported ID", 1);
            lastImport.id = Long.MAX_VALUE;
            inventory.save(lastImport);
            assertThatThrownBy(() -> inventory.save(new Inventory(new Sku("EXHAUSTED"), "No ID left", 1)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("ID range exhausted");
            assertThat(inventory.findById(Long.MAX_VALUE)).get().extracting(row -> row.name)
                    .isEqualTo("Last imported ID");
            assertThat(inventory.count()).isEqualTo(3);
        });
    }

    @Test
    void shouldGenerateIntegerReceiptIdsAndHydrateTheRelatedInventory() {
        inventoryContext().run(context -> {
            var inventory = context.getBean(InventoryRepository.class);
            var receipts = context.getBean(ReceiptRepository.class);
            var laptop = inventory.save(new Inventory(new Sku("RECEIVED"), "Received laptop", 8));
            var imported = receipts.save(new Receipt(1, laptop, "imported receipt"));
            var first = receipts.save(new Receipt(null, laptop, "first receipt"));
            var second = receipts.save(new Receipt(0, laptop, "second receipt"));
            assertThat(first.receiptNumber).isPositive().isNotEqualTo(imported.receiptNumber);
            assertThat(second.receiptNumber).isPositive().isNotIn(first.receiptNumber, imported.receiptNumber);
            assertThat(imported.receiptNumber).isEqualTo(1);
            assertThat(receipts.findById(first.receiptNumber)).get().extracting(receipt -> receipt.reference)
                    .isEqualTo("first receipt");
            var joined = receipts.findByInventoryId(laptop.id);
            assertThat(joined).allSatisfy(receipt -> {
                assertThat(receipt.inventory.id).isEqualTo(laptop.id);
                assertThat(receipt.inventory.sku).isEqualTo(new Sku("RECEIVED"));
            });
            assertThat(joined).extracting(row -> row.receiptNumber)
                    .containsExactlyInAnyOrder(first.receiptNumber, second.receiptNumber, 1);
            imported.reference = "corrected import";
            receipts.save(imported);
            assertThat(receipts.count()).isEqualTo(3);
            assertThat(receipts.findById(1)).get().extracting(row -> row.reference).isEqualTo("corrected import");
            receipts.deleteById(first.receiptNumber);
            assertThat(receipts.findById(first.receiptNumber)).isEmpty();
            assertThat(receipts.findById(second.receiptNumber)).isPresent();
            receipts.save(new Receipt(Integer.MAX_VALUE, laptop, "last imported receipt"));
            assertThatThrownBy(() -> receipts.save(new Receipt(null, laptop, "no number left")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("ID range exhausted");
            assertThat(receipts.findById(Integer.MAX_VALUE)).get().extracting(row -> row.reference)
                    .isEqualTo("last imported receipt");
            assertThat(receipts.count()).isEqualTo(3);
        });
    }

    @Test
    void shouldKeepApplicationAssignedWarehouseKeysStableAcrossUpdatesAndArenas() {
        inventoryContext().run(context -> {
            var warehouses = context.getBean(WarehouseRepository.class);
            warehouses.save(new Warehouse("north-001", "North warehouse"));
            warehouses.save(new Warehouse("south-002", "South warehouse"));
            var north = warehouses.findById("north-001").orElseThrow();
            north.name = "North distribution center";
            warehouses.save(north);
            assertThat(warehouses.count()).isEqualTo(2);
            assertThat(warehouses.findById("north-001")).get().extracting(row -> row.name)
                    .isEqualTo("North distribution center");
            var reporting = context.getBean(MemrisArenaProvider.class).getArena("reporting")
                    .createRepository(WarehouseRepository.class);
            reporting.save(new Warehouse("north-001", "Independent report"));
            assertThat(reporting.findById("north-001")).get().extracting(row -> row.name)
                    .isEqualTo("Independent report");
            warehouses.deleteById("north-001");
            assertThat(warehouses.findById("north-001")).isEmpty();
            assertThat(warehouses.findById("south-002")).isPresent();
            assertThat(reporting.findById("north-001")).isPresent();
        });
    }

    @Test
    void shouldRejectJpaOnlyIdsWhenStartingARepositoryContext() {
        arenaContext().withUserConfiguration(JpaOnlyConfiguration.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("Missing explicit ID field")
                    .hasStackTraceContaining("@io.memris.core.Id");
        });
    }

    @Test
    void shouldRejectUnimplementedIdStrategiesBeforeAcceptingReceipts() {
        inventoryContext().run(context -> {
            var arena = context.getBean(MemrisArena.class);
            assertThatThrownBy(() -> arena.createRepository(CustomReceiptRepository.class))
                    .isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining("ID generation strategy CUSTOM is not supported");
            assertThatThrownBy(() -> arena.createRepository(UuidReceiptRepository.class))
                    .isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining("ID generation strategy UUID is not supported");
            assertThatThrownBy(() -> arena.createRepository(NamedReceiptRepository.class))
                    .isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining("Named ID generators are not supported");
            assertThatThrownBy(() -> arena.createRepository(GeneratedWarehouseRepository.class))
                    .isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining("Generated IDs require Long or Integer fields");
            var inventory = context.getBean(InventoryRepository.class);
            var item = inventory.save(new Inventory(new Sku("VALID"), "Valid after rejected mapping", 1));
            assertThat(inventory.findById(item.id)).isPresent();
        });
    }

    @Test
    void shouldManageInventoryThroughAnInjectedSpringRepositoryAndJpaConverter() {
        var closedArena = new AtomicReference<MemrisArena>();
        inventoryContext().run(context -> {
            assertThat(context).hasSingleBean(MemrisRepositoryFactory.class)
                    .hasSingleBean(MemrisArenaProvider.class).hasSingleBean(MemrisArena.class)
                    .hasSingleBean(InventoryRepository.class);
            var inventory = context.getBean(InventoryRepository.class);
            var laptop = inventory.save(new Inventory(new Sku("LAPTOP"), "Laptop", 2));
            inventory.save(new Inventory(null, "Draft", 0));

            assertThat(laptop.id).isNotNull();
            assertThat(inventory.findById(laptop.id)).get().extracting(row -> row.sku).isEqualTo(new Sku("LAPTOP"));
            assertThat(inventory.findByStockGreaterThan(0)).extracting(row -> row.name).containsExactly("Laptop");
            assertThat(inventory.countByStockGreaterThan(0)).isEqualTo(1);

            laptop.stock = 0;
            inventory.save(laptop);
            assertThat(inventory.findByStockGreaterThan(0)).isEmpty();
            assertThat(inventory.countByStockGreaterThan(0)).isZero();
            inventory.deleteById(laptop.id);
            assertThat(inventory.findById(laptop.id)).isEmpty();
            assertThat(inventory.findByName("Draft")).singleElement().extracting(row -> row.sku).isNull();
            closedArena.set(context.getBean(MemrisArena.class));
        });
        assertThatThrownBy(() -> closedArena.get().createRepository(InventoryRepository.class))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test
    void shouldIsolateReportingDataAndStartFreshInTheNextApplicationContext() {
        var firstId = new AtomicReference<Long>();
        inventoryContext().run(context -> {
            var inventory = context.getBean(InventoryRepository.class);
            var item = inventory.save(new Inventory(new Sku("MAIN"), "Main inventory", 2));
            firstId.set(item.id);
            var reporting = context.getBean(MemrisArenaProvider.class).getArena("reporting")
                    .createRepository(InventoryRepository.class);
            assertThat(reporting.findById(item.id)).isEmpty();
            reporting.save(new Inventory(new Sku("REPORT"), "Reporting inventory", 1));
            assertThat(inventory.findByName("Reporting inventory")).isEmpty();
            assertThat(reporting.findByName("Main inventory")).isEmpty();
        });
        inventoryContext().run(context -> {
            var inventory = context.getBean(InventoryRepository.class);
            assertThat(inventory.findById(firstId.get())).isEmpty();
            assertThat(inventory.findByName("Main inventory")).isEmpty();
        });
    }

    @Test
    void shouldUseAnApplicationSuppliedConfigurationForRealRepositoryOperations() {
        var custom = MemrisConfiguration.builder().pageSize(32).maxPages(8).initialPages(1)
                .tableImplementation(MemrisConfiguration.TableImplementation.valueOf(
                        System.getProperty("memris.test.table", "BYTECODE")))
                .codegenEnabled(Boolean.parseBoolean(System.getProperty("memris.test.codegen", "true"))).build();
        inventoryContext().withBean(MemrisConfiguration.class, () -> custom).run(context -> {
            assertThat(context.getBean(MemrisConfiguration.class)).isSameAs(custom);
            var inventory = context.getBean(InventoryRepository.class);
            var item = inventory.save(new Inventory(new Sku("CUSTOM"), "Configured inventory", 7));
            assertThat(inventory.findById(item.id)).get().extracting(row -> row.stock).isEqualTo(7);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableMemrisRepositories(basePackageClasses = InventoryRepository.class, considerNestedRepositories = true,
            excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = JpaOnlyRepository.class))
    static class InventoryConfiguration {
        @Bean
        SkuConverter skuConverter() {
            return new SkuConverter();
        }

        @Bean
        MoneyConverter moneyConverter() {
            return new MoneyConverter();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableMemrisRepositories(basePackageClasses = JpaOnlyRepository.class, considerNestedRepositories = true,
            excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE,
                    classes = { InventoryRepository.class, ReceiptRepository.class, WarehouseRepository.class }))
    static class JpaOnlyConfiguration {
    }

    public record Money(long cents) {
    }

    public abstract static class LongValueConverter<T> implements AttributeConverter<T, Long> {
    }

    @Converter(autoApply = true)
    public static class MoneyConverter extends LongValueConverter<Money> {
        @Override
        public Long convertToDatabaseColumn(Money value) {
            return value == null ? null : value.cents();
        }

        @Override
        public Money convertToEntityAttribute(Long value) {
            return value == null ? null : new Money(value);
        }
    }

    public record Sku(String value) {
    }

    @Converter(autoApply = true)
    public static class SkuConverter implements AttributeConverter<Sku, String> {
        @Override
        public String convertToDatabaseColumn(Sku attribute) {
            return attribute == null ? null : attribute.value();
        }

        @Override
        public Sku convertToEntityAttribute(String value) {
            return value == null ? null : new Sku(value);
        }
    }

    public static class Inventory {
        @io.memris.core.Id
        @io.memris.core.GeneratedValue(strategy = io.memris.core.GenerationType.AUTO)
        public Long id;
        public Sku sku;
        public String name;
        public int stock;
        public StockDetails details;
        public Money cost;

        public Inventory() {
        }

        public Inventory(Sku sku, String name, int stock) {
            this.sku = sku;
            this.name = name;
            this.stock = stock;
        }
    }

    public static class StockDetails {
        public String warehouse;
        public Integer minimumStock;
        public LocalDate restockDate;
        public UUID batch;

        public StockDetails() {
        }

        public StockDetails(String warehouse, Integer minimumStock, LocalDate restockDate, UUID batch) {
            this.warehouse = warehouse;
            this.minimumStock = minimumStock;
            this.restockDate = restockDate;
            this.batch = batch;
        }
    }

    public static class Receipt {
        public String reference;
        @io.memris.core.Id
        @io.memris.core.GeneratedValue(strategy = io.memris.core.GenerationType.IDENTITY)
        public Integer receiptNumber;
        @io.memris.core.ManyToOne
        public Inventory inventory;

        public Receipt() {
        }

        public Receipt(Integer receiptNumber, Inventory inventory, String reference) {
            this.receiptNumber = receiptNumber;
            this.inventory = inventory;
            this.reference = reference;
        }
    }

    public static class Warehouse {
        @io.memris.core.Id
        public String code;
        public String name;

        public Warehouse() {
        }

        public Warehouse(String code, String name) {
            this.code = code;
            this.name = name;
        }
    }

    public static class JpaOnlyInventory {
        @javax.persistence.Id
        @javax.persistence.GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
        public Long id;
        public String name;

        public JpaOnlyInventory() {
        }
    }

    public interface JpaOnlyRepository extends MemrisSpringRepository<JpaOnlyInventory, Long> {
        JpaOnlyInventory save(JpaOnlyInventory inventory);
    }

    public static class CustomReceipt {
        @io.memris.core.Id
        @io.memris.core.GeneratedValue(strategy = io.memris.core.GenerationType.CUSTOM, generator = "receiptNumbers")
        public Long id;

        public CustomReceipt() {
        }
    }

    public static class UuidReceipt {
        @io.memris.core.Id
        @io.memris.core.GeneratedValue(strategy = io.memris.core.GenerationType.UUID)
        public UUID id;

        public UuidReceipt() {
        }
    }

    // Invalid mappings are requested explicitly through the context's arena, never auto-scanned.
    @NoRepositoryBean
    public interface CustomReceiptRepository extends MemrisSpringRepository<CustomReceipt, Long> {
        CustomReceipt save(CustomReceipt receipt);
    }

    @NoRepositoryBean
    public interface UuidReceiptRepository extends MemrisSpringRepository<UuidReceipt, UUID> {
        UuidReceipt save(UuidReceipt receipt);
    }

    public static class NamedReceipt {
        @io.memris.core.Id
        @io.memris.core.GeneratedValue(generator = "receiptNumbers")
        public Long id;

        public NamedReceipt() {
        }
    }

    public static class GeneratedWarehouse {
        @io.memris.core.Id
        @io.memris.core.GeneratedValue
        public String code;

        public GeneratedWarehouse() {
        }
    }

    @NoRepositoryBean
    public interface NamedReceiptRepository extends MemrisSpringRepository<NamedReceipt, Long> {
        NamedReceipt save(NamedReceipt receipt);
    }

    @NoRepositoryBean
    public interface GeneratedWarehouseRepository extends MemrisSpringRepository<GeneratedWarehouse, String> {
        GeneratedWarehouse save(GeneratedWarehouse warehouse);
    }

    public interface ReceiptRepository extends MemrisSpringRepository<Receipt, Integer> {
        Receipt save(Receipt receipt);
        Optional<Receipt> findById(Integer id);
        List<Receipt> findByInventoryId(Long id);
        long count();
        void deleteById(Integer id);
    }

    public interface WarehouseRepository extends MemrisSpringRepository<Warehouse, String> {
        Warehouse save(Warehouse warehouse);
        Optional<Warehouse> findById(String id);
        long count();
        void deleteById(String id);
    }

    public interface InventoryRepository extends MemrisSpringRepository<Inventory, Long> {
        Inventory save(Inventory inventory);
        Optional<Inventory> findById(Long id);
        List<Inventory> findByName(String name);
        List<Inventory> findByDetailsWarehouse(String warehouse);
        List<Inventory> findByCostIsNull();
        List<Inventory> findByStockGreaterThan(int stock);
        long countByStockGreaterThan(int stock);
        long count();
        void deleteById(Long id);
    }
}
