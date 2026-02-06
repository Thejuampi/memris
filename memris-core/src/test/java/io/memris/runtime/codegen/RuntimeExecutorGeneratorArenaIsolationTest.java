package io.memris.runtime.codegen;

import io.memris.core.MemrisArena;
import io.memris.core.ManyToOne;
import io.memris.core.JoinColumn;
import io.memris.core.Id;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RuntimeExecutorGenerator focusing on arena isolation.
 * 
 * Verifies that the global executor cache doesn't cause data leakage
 * between arenas when using projections and foreign key reads.
 */
class RuntimeExecutorGeneratorArenaIsolationTest {

    private MemrisRepositoryFactory factory;

    @BeforeEach
    void setUp() {
        RuntimeExecutorGenerator.clearCache(); // Start with clean cache
        factory = new MemrisRepositoryFactory();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldIsolateFieldValueReadingBetweenArenas() {
        // Given - Two arenas with the same entity type
        MemrisArena arena1 = factory.createArena();
        MemrisArena arena2 = factory.createArena();

        ProductRepository repo1 = arena1.createRepository(ProductRepository.class);
        ProductRepository repo2 = arena2.createRepository(ProductRepository.class);

        // When - Save different entities in each arena
        Product product1 = new Product();
        product1.name = "Product in Arena 1";
        product1.price = 100;
        Product saved1 = repo1.save(product1);

        Product product2 = new Product();
        product2.name = "Product in Arena 2";
        product2.price = 200;
        Product saved2 = repo2.save(product2);

        // Then - Each arena should only see its own data
        Product found1 = repo1.findById(saved1.id).orElseThrow();
        Product found2 = repo2.findById(saved2.id).orElseThrow();

        Optional<Product> cross1 = repo1.findById(saved2.id);
        Optional<Product> cross2 = repo2.findById(saved1.id);
        boolean arena1MissingArena2 = cross1.filter(product -> "Product in Arena 2".equals(product.name)).isEmpty();
        boolean arena2MissingArena1 = cross2.filter(product -> "Product in Arena 1".equals(product.name)).isEmpty();

        ArenaIsolationResult actual = new ArenaIsolationResult(
                found1.name,
                found1.price,
                found2.name,
                found2.price,
                arena1MissingArena2,
                arena2MissingArena1);

        ArenaIsolationResult expected = new ArenaIsolationResult(
                "Product in Arena 1",
                100,
                "Product in Arena 2",
                200,
                true,
                true);

        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldSharedCacheNotAffectDifferentColumnIndices() {
        // Given - Entities with different column layouts (same type codes)
        MemrisArena arena = factory.createArena();

        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        CategoryRepository categoryRepo = arena.createRepository(CategoryRepository.class);

        // When - Save entities
        Category category = new Category();
        category.name = "Electronics";
        category.description = "Electronic devices";
        Category savedCategory = categoryRepo.save(category);

        Product product = new Product();
        product.name = "Laptop";
        product.price = 999;
        Product savedProduct = productRepo.save(product);

        // Then - Each entity type should read its own column correctly
        Product foundProduct = productRepo.findById(savedProduct.id).orElseThrow();
        Category foundCategory = categoryRepo.findById(savedCategory.id).orElseThrow();

        CacheIsolationResult actual = new CacheIsolationResult(
                foundProduct.name,
                foundCategory.name,
                foundCategory.description);

        CacheIsolationResult expected = new CacheIsolationResult(
                "Laptop",
                "Electronics",
                "Electronic devices");

        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldIsolateForeignKeyReadsBetweenArenas() {
        // Given - Two arenas with entities having FK relationships
        MemrisArena arena1 = factory.createArena();
        MemrisArena arena2 = factory.createArena();

        CategoryRepository catRepo1 = arena1.createRepository(CategoryRepository.class);
        ProductWithCategoryRepository prodRepo1 = arena1.createRepository(ProductWithCategoryRepository.class);

        CategoryRepository catRepo2 = arena2.createRepository(CategoryRepository.class);
        ProductWithCategoryRepository prodRepo2 = arena2.createRepository(ProductWithCategoryRepository.class);

        // When - Create categories and products in both arenas
        Category cat1 = new Category();
        cat1.name = "Arena1 Category";
        cat1.description = "Cat in A1";
        Category savedCat1 = catRepo1.save(cat1);

        ProductWithCategory prod1 = new ProductWithCategory();
        prod1.name = "Arena1 Product";
        prod1.category = savedCat1;
        ProductWithCategory savedProd1 = prodRepo1.save(prod1);

        Category cat2 = new Category();
        cat2.name = "Arena2 Category";
        cat2.description = "Cat in A2";
        Category savedCat2 = catRepo2.save(cat2);

        ProductWithCategory prod2 = new ProductWithCategory();
        prod2.name = "Arena2 Product";
        prod2.category = savedCat2;
        ProductWithCategory savedProd2 = prodRepo2.save(prod2);

        // Then - FK reads should be isolated per arena
        ProductWithCategory found1 = prodRepo1.findById(savedProd1.id).orElseThrow();
        ProductWithCategory found2 = prodRepo2.findById(savedProd2.id).orElseThrow();

        Optional<ProductWithCategory> cross1 = prodRepo1.findById(savedProd2.id);
        Optional<ProductWithCategory> cross2 = prodRepo2.findById(savedProd1.id);
        boolean arena1MissingArena2 = cross1.filter(product -> "Arena2 Product".equals(product.name)).isEmpty();
        boolean arena2MissingArena1 = cross2.filter(product -> "Arena1 Product".equals(product.name)).isEmpty();

        ForeignKeyIsolationResult actual = new ForeignKeyIsolationResult(
                found1.name,
                found2.name,
                arena1MissingArena2,
                arena2MissingArena1);

        ForeignKeyIsolationResult expected = new ForeignKeyIsolationResult(
                "Arena1 Product",
                "Arena2 Product",
                true,
                true);

        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    // Test entities and repositories

    public static class Product {
        @Id
        public Long id;
        public String name;
        public int price;
    }

    public interface ProductRepository extends MemrisRepository<Product> {
        Product save(Product entity);

        Optional<Product> findById(Long id);
    }

    public static class Category {
        @Id
        public Long id;
        public String name;
        public String description;
    }

    public interface CategoryRepository extends MemrisRepository<Category> {
        Category save(Category entity);

        Optional<Category> findById(Long id);
    }

    public static class ProductWithCategory {
        @Id
        public Long id;
        public String name;

        @ManyToOne
        @JoinColumn(name = "category_id")
        public Category category;
    }

    public interface ProductWithCategoryRepository extends MemrisRepository<ProductWithCategory> {
        ProductWithCategory save(ProductWithCategory entity);

        Optional<ProductWithCategory> findById(Long id);
    }

    private record ArenaIsolationResult(
            String arena1Name,
            int arena1Price,
            String arena2Name,
            int arena2Price,
            boolean arena1MissingArena2,
            boolean arena2MissingArena1) {
    }

    private record CacheIsolationResult(
            String productName,
            String categoryName,
            String categoryDescription) {
    }

    private record ForeignKeyIsolationResult(
            String arena1ProductName,
            String arena2ProductName,
            boolean arena1MissingArena2,
            boolean arena2MissingArena1) {
    }
}
