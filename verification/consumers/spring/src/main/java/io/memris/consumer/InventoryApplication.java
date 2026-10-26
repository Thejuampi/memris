package io.memris.consumer;

import io.memris.repository.MemrisArena;
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import io.memris.spring.data.repository.MemrisSpringRepository;
import io.memris.spring.data.repository.config.EnableMemrisRepositories;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@SpringBootApplication
@EnableMemrisRepositories(basePackageClasses = InventoryApplication.class, considerNestedRepositories = true)
public class InventoryApplication {
    public static void main(String[] args) throws Exception {
        Class.forName("org.springframework.transaction.TransactionException");
        ConverterConfiguration.verifyPersistenceGeneration();
        var application = new SpringApplication(InventoryApplication.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setDefaultProperties(Map.of(
                "spring.main.banner-mode", "off",
                "memris.arenas.default.initial-pages", "1",
                "memris.arenas.default.page-size", "64",
                "memris.arenas.reporting.initial-pages", "1",
                "memris.arenas.reporting.page-size", "64"));
        MemrisArena closedArena;
        try (var context = application.run(args)) {
            var inventory = context.getBean(InventoryRepository.class);
            var laptop = inventory.save(new Inventory(new Sku("LAPTOP"), "Laptop", 3));
            require(inventory.findById(laptop.id).orElseThrow().sku.equals(new Sku("LAPTOP")), "converter round trip");
            var draft = inventory.save(new Inventory(null, "Draft", 0));
            require(inventory.findById(draft.id).orElseThrow().sku == null, "null converter value");
            require(inventory.findByNameEqualsIgnoreCase("laptop").size() == 1, "injected query");
            var reporting = context.getBean(MemrisArenaProvider.class).getArena("reporting")
                    .createRepository(InventoryRepository.class);
            require(reporting.findById(laptop.id).isEmpty(), "named arena isolation");
            laptop.stock = 0;
            inventory.save(laptop);
            require(inventory.countByStockGreaterThan(0) == 0, "stock update");
            inventory.deleteById(laptop.id);
            require(inventory.findById(laptop.id).isEmpty(), "delete");
            closedArena = context.getBean(MemrisArena.class);
        }
        try {
            closedArena.createRepository(InventoryRepository.class);
            throw new IllegalStateException("Arena remained usable after context close");
        } catch (IllegalStateException expected) {
            require(expected.getMessage().contains("closed"), "terminal arena close");
        }
        System.out.println("Spring packaged consumer passed");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public record Sku(String value) { }

    public static class Inventory {
        @io.memris.core.Id public Long id;
        public Sku sku;
        public String name;
        public int stock;

        public Inventory() { }

        public Inventory(Sku sku, String name, int stock) {
            this.sku = sku;
            this.name = name;
            this.stock = stock;
        }
    }

    public interface InventoryRepository extends MemrisSpringRepository<Inventory, Long> {
        Inventory save(Inventory inventory);
        Optional<Inventory> findById(Long id);
        List<Inventory> findByNameEqualsIgnoreCase(String name);
        long countByStockGreaterThan(int stock);
        void deleteById(Long id);
    }
}
