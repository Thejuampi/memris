package io.memris.consumer;

import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;

import java.util.List;
import java.util.Optional;

public class InventoryApplication {
    public static void main(String[] args) {
        var configuration = MemrisConfiguration.builder().pageSize(64).maxPages(8).initialPages(1).build();
        try (var factory = new MemrisRepositoryFactory(configuration); var arena = factory.createArena()) {
            var inventory = arena.createRepository(InventoryRepository.class);
            var laptop = inventory.save(new Inventory("Laptop", 3));
            require(inventory.findById(laptop.id).orElseThrow().stock == 3, "saved stock");
            require(inventory.findByNameEqualsIgnoreCase("laptop").size() == 1, "derived query alias");
            laptop.stock = 0;
            inventory.save(laptop);
            require(inventory.countByStockGreaterThan(0) == 0, "stock update");
            require(inventory.removeByName("Laptop") == 1, "delete alias");
            require(inventory.findById(laptop.id).isEmpty(), "deleted row");
        }
        System.out.println("Core packaged consumer passed");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public static class Inventory {
        @Id public Long id;
        @Index(type = Index.IndexType.HASH) public String name;
        public int stock;

        public Inventory() { }

        public Inventory(String name, int stock) {
            this.name = name;
            this.stock = stock;
        }
    }

    public interface InventoryRepository extends MemrisRepository<Inventory> {
        Inventory save(Inventory inventory);
        Optional<Inventory> findById(Long id);
        List<Inventory> findByNameEqualsIgnoreCase(String name);
        long countByStockGreaterThan(int stock);
        long removeByName(String name);
    }
}
