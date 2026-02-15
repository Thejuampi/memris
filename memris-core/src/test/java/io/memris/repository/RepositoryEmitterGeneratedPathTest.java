package io.memris.repository;

import io.memris.core.Entity;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadataProvider;
import io.memris.core.Id;
import io.memris.core.MemrisArena;
import io.memris.core.MetadataExtractor;
import io.memris.runtime.EntityMaterializer;
import io.memris.runtime.ReflectionEntityMaterializer;
import io.memris.runtime.ReflectionEntitySaver;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryEmitterGeneratedPathTest {

    @Test
    void createEntitySaverShouldUseGeneratedPath() throws Exception {
        var metadata = MetadataExtractor.extractEntityMetadata(EmbeddedOwner.class);
        Method method = RepositoryEmitter.class.getDeclaredMethod(
                "createEntitySaver",
                Class.class,
                EntityMetadata.class);
        method.setAccessible(true);

        Object saver = method.invoke(null, EmbeddedOwner.class, metadata);

        assertThat(saver).isNotInstanceOf(ReflectionEntitySaver.class);
    }

    @Test
    void buildJoinMaterializersShouldUseGeneratedPath() throws Exception {
        try (var factory = new MemrisRepositoryFactory()) {
            MemrisArena arena = factory.createArena();
            GeneratedTable table = arena.getOrCreateTable(EmbeddedOwner.class);

            Method method = RepositoryEmitter.class.getDeclaredMethod(
                    "buildJoinMaterializers",
                    Map.class,
                    EntityMetadataProvider.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            Map<Class<?>, EntityMaterializer<?>> materializers = (Map<Class<?>, EntityMaterializer<?>>) method.invoke(
                    null,
                    Map.of(EmbeddedOwner.class, table),
                    (EntityMetadataProvider) MetadataExtractor::extractEntityMetadata);

            assertThat(materializers.get(EmbeddedOwner.class)).isNotInstanceOf(ReflectionEntityMaterializer.class);
        }
    }

    @Entity
    public static class EmbeddedOwner {
        @Id
        public Long id;
        public Profile profile;

        public EmbeddedOwner() {
        }
    }

    public static class Profile {
        public String city;

        public Profile() {
        }
    }
}
