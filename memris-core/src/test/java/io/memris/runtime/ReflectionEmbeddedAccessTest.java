package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.ManyToOne;
import io.memris.core.MemrisArena;
import io.memris.core.MetadataExtractor;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionEmbeddedAccessTest {

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
    void shouldReadAndWriteNestedPaths() {
        var accessor = PropertyPathAccessor.compile(Owner.class, "embedded.inner.city");
        var owner = new Owner();
        owner.embedded = new Embedded();
        owner.embedded.inner = new Inner();
        owner.embedded.inner.city = "Madrid";

        assertThat(accessor.get(owner)).isEqualTo("Madrid");

        var target = new Owner();
        accessor.set(target, "Bogota");
        assertThat(target.embedded).isNotNull();
        assertThat(target.embedded.inner).isNotNull();
        assertThat(target.embedded.inner.city).isEqualTo("Bogota");
    }

    @Test
    void shouldFailForUnknownPathSegment() {
        assertThatThrownBy(() -> PropertyPathAccessor.compile(Owner.class, "embedded.unknown.city"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field path not found");
    }

    @Test
    void shouldFailWhenIntermediateHasNoNoArgConstructor() {
        assertThatThrownBy(() -> PropertyPathAccessor.compile(NoDefaultRoot.class, "intermediate.value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires no-arg constructor");
    }

    @Test
    void shouldSaveEmbeddedAndRelationshipValues() {
        var metadata = MetadataExtractor.extractEntityMetadata(Owner.class);
        GeneratedTable table = arena.getOrCreateTable(Owner.class);
        var saver = new ReflectionEntitySaver<>(metadata);

        var owner = new Owner();
        owner.name = "juan";
        owner.score = 7;
        owner.birthday = LocalDate.of(1990, 1, 1);
        owner.active = true;
        owner.ratio = 1.5d;
        owner.level = 'A';
        owner.embedded = new Embedded("A1", new Inner("Madrid"));
        owner.related = new Related(99L, "rel");

        saver.save(owner, table, 10L);

        int row = io.memris.storage.Selection.index(table.lookupById(10L));
        assertThat(owner.id).isEqualTo(10L);
        assertThat(table.readString(metadata.resolvePropertyPosition("embedded.code"), row)).isEqualTo("A1");
        assertThat(table.readString(metadata.resolvePropertyPosition("embedded.inner.city"), row)).isEqualTo("Madrid");
        assertThat(table.readLong(metadata.resolvePropertyPosition("related"), row)).isEqualTo(99L);
        assertThat(table.readLong(metadata.resolvePropertyPosition("birthday"), row)).isNotZero();
        assertThat(saver.extractId(owner)).isEqualTo(10L);
    }

    @Test
    void shouldResolveRelationshipIdsByFieldName() {
        var metadata = MetadataExtractor.extractEntityMetadata(Owner.class);
        var saver = new ReflectionEntitySaver<>(metadata);

        assertThat(saver.resolveRelationshipId("related", new Related(42L, "x"))).isEqualTo(42L);
        assertThat(saver.resolveRelationshipId("related", null)).isNull();
        assertThat(saver.resolveRelationshipId("missing", new Related(1L, "x"))).isNull();
    }

    @Test
    void shouldMaterializeEmbeddedValues() {
        var metadata = MetadataExtractor.extractEntityMetadata(Owner.class);
        GeneratedTable table = arena.getOrCreateTable(Owner.class);
        var saver = new ReflectionEntitySaver<>(metadata);
        var materializer = new ReflectionEntityMaterializer<>(metadata);

        var owner = new Owner();
        owner.name = "ana";
        owner.score = 20;
        owner.birthday = LocalDate.of(1995, 5, 20);
        owner.active = true;
        owner.ratio = 2.75d;
        owner.level = 'Z';
        owner.embedded = new Embedded("B2", new Inner("Quito"));
        saver.save(owner, table, 11L);

        int row = io.memris.storage.Selection.index(table.lookupById(11L));
        Owner reloaded = materializer.materialize(table, row);

        assertThat(reloaded.id).isEqualTo(11L);
        assertThat(reloaded.name).isEqualTo("ana");
        assertThat(reloaded.score).isEqualTo(20);
        assertThat(reloaded.birthday).isEqualTo(LocalDate.of(1995, 5, 20));
        assertThat(reloaded.active).isTrue();
        assertThat(reloaded.ratio).isEqualTo(2.75d);
        assertThat(reloaded.level).isEqualTo('Z');
        assertThat(reloaded.embedded).isNotNull();
        assertThat(reloaded.embedded.code).isEqualTo("B2");
        assertThat(reloaded.embedded.inner).isNotNull();
        assertThat(reloaded.embedded.inner.city).isEqualTo("Quito");
        assertThat(reloaded.related).isNull();
    }

    @Test
    void shouldLeaveEmbeddedRootNullWhenColumnsAreAbsent() {
        var metadata = MetadataExtractor.extractEntityMetadata(Owner.class);
        GeneratedTable table = arena.getOrCreateTable(Owner.class);
        var materializer = new ReflectionEntityMaterializer<>(metadata);

        Object[] values = new Object[table.columnCount()];
        values[metadata.resolvePropertyPosition("id")] = 12L;
        values[metadata.resolvePropertyPosition("name")] = "empty";
        values[metadata.resolvePropertyPosition("score")] = 3;
        values[metadata.resolvePropertyPosition("active")] = false;
        values[metadata.resolvePropertyPosition("ratio")] = 0.25d;
        values[metadata.resolvePropertyPosition("level")] = 'Q';
        table.insertFrom(values);

        int row = io.memris.storage.Selection.index(table.lookupById(12L));
        Owner reloaded = materializer.materialize(table, row);

        assertThat(reloaded.id).isEqualTo(12L);
        assertThat(reloaded.name).isEqualTo("empty");
        assertThat(reloaded.score).isEqualTo(3);
        assertThat(reloaded.active).isFalse();
        assertThat(reloaded.ratio).isEqualTo(0.25d);
        assertThat(reloaded.level).isEqualTo('Q');
        assertThat(reloaded.embedded).isNull();
    }

    @Entity
    public static class Owner {
        @Id
        public Long id;
        public String name;
        public Embedded embedded;
        public int score;
        public LocalDate birthday;
        public boolean active;
        public double ratio;
        public char level;
        @ManyToOne
        public Related related;

        public Owner() {
        }
    }

    public static class Embedded {
        public String code;
        public Inner inner;

        public Embedded() {
        }

        public Embedded(String code, Inner inner) {
            this.code = code;
            this.inner = inner;
        }
    }

    public static class Inner {
        public String city;

        public Inner() {
        }

        public Inner(String city) {
            this.city = city;
        }
    }

    @Entity
    public static class Related {
        @Id
        public Long id;
        public String label;

        public Related() {
        }

        public Related(Long id, String label) {
            this.id = id;
            this.label = label;
        }
    }

    static class NoDefaultRoot {
        NoDefaultIntermediate intermediate;
    }

    static class NoDefaultIntermediate {
        final String value;

        NoDefaultIntermediate(String value) {
            this.value = value;
        }
    }
}
