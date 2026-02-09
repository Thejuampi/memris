package io.memris.runtime.dispatch;

import io.memris.core.MemrisArena;
import io.memris.core.MetadataExtractor;
import io.memris.query.CompiledQuery;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class MultiColumnOrderCompilerTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    @DisplayName("should sort with compiled multi-column builders")
    void shouldSortWithCompiledMultiColumnBuilders() {
        var repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Bob", 25));
        repo.save(new TestEntity(null, "Alice", 20));
        repo.save(new TestEntity(null, "Bob", 35));
        repo.save(new TestEntity(null, "Alice", 30));

        var metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        var nameColumn = metadata.resolvePropertyPosition("name");
        var ageColumn = metadata.resolvePropertyPosition("age");
        var table = arena.getTable(TestEntity.class);
        var primitiveNonNull = new boolean[table.columnCount()];
        primitiveNonNull[ageColumn] = true;

        var orderBy = new CompiledQuery.CompiledOrderBy[] {
                new CompiledQuery.CompiledOrderBy(nameColumn, true),
                new CompiledQuery.CompiledOrderBy(ageColumn, false)
        };
        var builders = MultiColumnOrderCompiler.compileBuilders(orderBy, table, primitiveNonNull);
        var sortedRows = MultiColumnOrderCompiler.sortByCompiledBuilders(table, table.scanAll(), builders);
        var actual = Arrays.stream(sortedRows)
                .mapToObj(row -> table.readString(nameColumn, row) + ":" + table.readInt(ageColumn, row))
                .toList();

        assertThat(actual).containsExactly("Alice:30", "Alice:20", "Bob:35", "Bob:25");
    }
}
