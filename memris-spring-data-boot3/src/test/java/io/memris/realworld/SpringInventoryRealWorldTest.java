package io.memris.realworld;

import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import io.memris.spring.boot.autoconfigure.MemrisAutoConfiguration;
import io.memris.spring.data.repository.MemrisSpringRepository;
import io.memris.spring.data.repository.config.EnableMemrisRepositories;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpringInventoryRealWorldTest {
    private ApplicationContextRunner inventoryContext() {
        String table = System.getProperty("memris.test.table", "BYTECODE");
        String codegen = System.getProperty("memris.test.codegen", "true");
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MemrisAutoConfiguration.class))
                .withUserConfiguration(InventoryConfiguration.class)
                .withPropertyValues("memris.arenas.default.initial-pages=1", "memris.arenas.default.page-size=64",
                        "memris.arenas.default.table-implementation=" + table,
                        "memris.arenas.default.codegen-enabled=" + codegen,
                        "memris.arenas.reporting.initial-pages=1", "memris.arenas.reporting.page-size=64",
                        "memris.arenas.reporting.table-implementation=" + table,
                        "memris.arenas.reporting.codegen-enabled=" + codegen);
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
    @EnableMemrisRepositories(basePackageClasses = InventoryRepository.class, considerNestedRepositories = true)
    static class InventoryConfiguration {
        @Bean
        SkuConverter skuConverter() {
            return new SkuConverter();
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
        public Long id;
        public Sku sku;
        public String name;
        public int stock;

        public Inventory() {
        }

        public Inventory(Sku sku, String name, int stock) {
            this.sku = sku;
            this.name = name;
            this.stock = stock;
        }
    }

    public interface InventoryRepository extends MemrisSpringRepository<Inventory, Long> {
        Inventory save(Inventory inventory);
        Optional<Inventory> findById(Long id);
        List<Inventory> findByName(String name);
        List<Inventory> findByStockGreaterThan(int stock);
        long countByStockGreaterThan(int stock);
        void deleteById(Long id);
    }
}
