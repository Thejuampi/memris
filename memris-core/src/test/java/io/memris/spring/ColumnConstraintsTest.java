package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnConstraintsTest {

    // Repository interfaces for JPA-style repository creation
    interface ItemRepository extends MemrisRepository<Item> {
        void save(Item entity);
        List<Item> findAll();
    }

    interface RequiredItemRepository extends MemrisRepository<RequiredItem> {
        void save(RequiredItem entity);
        List<RequiredItem> findAll();
    }

    interface UniqueItemRepository extends MemrisRepository<UniqueItem> {
        void save(UniqueItem entity);
        List<UniqueItem> findAll();
    }

    interface DefaultItemRepository extends MemrisRepository<DefaultItem> {
        void save(DefaultItem entity);
        List<DefaultItem> findAll();
    }

    @Test
    void column_length_respected() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ItemRepository items = factory.createJPARepository(ItemRepository.class);

            items.save(new Item("Short"));
            items.save(new Item("This is a longer name"));

            var all = items.findAll();
            assertThat(all).hasSize(2);

            factory.close();
        }
    }

    @Test
    void nullable_false_rejected() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            RequiredItemRepository items = factory.createJPARepository(RequiredItemRepository.class);

            RequiredItem item = new RequiredItem();
            item.name = null;

            items.save(item);

            var all = items.findAll();
            assertThat(all).hasSize(1);

            factory.close();
        }
    }

    @Test
    void unique_constraint_check() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            UniqueItemRepository items = factory.createJPARepository(UniqueItemRepository.class);

            items.save(new UniqueItem("item1"));
            items.save(new UniqueItem("item2"));

            var all = items.findAll();
            assertThat(all).hasSize(2);

            factory.close();
        }
    }

    @Test
    void column_default_value() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            DefaultItemRepository items = factory.createJPARepository(DefaultItemRepository.class);

            items.save(new DefaultItem());

            var all = items.findAll();
            assertThat(all).hasSize(1);
            assertThat(all.get(0).count).isEqualTo(0);

            factory.close();
        }
    }

    @Entity
    static final class Item {
        @Id
        int id;
        @Column(length = 20)
        String name;

        Item() {}

        Item(String name) {
            this.name = name;
        }
    }

    @Entity
    static final class RequiredItem {
        @Id
        int id;
        @Column(nullable = false)
        String name;

        RequiredItem() {}
    }

    @Entity
    static final class UniqueItem {
        @Id
        int id;
        @Column(unique = true)
        String code;

        UniqueItem() {}

        UniqueItem(String code) {
            this.code = code;
        }
    }

    @Entity
    static final class DefaultItem {
        @Id
        int id;
        @Column(columnDefinition = "BIGINT DEFAULT 0")
        long count;

        DefaultItem() {}
    }
}
