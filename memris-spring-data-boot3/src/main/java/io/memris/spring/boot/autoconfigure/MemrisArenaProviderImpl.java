package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MemrisArenaProviderImpl implements MemrisArenaProvider {
    private final MemrisRepositoryFactory defaultFactory;
    private final String defaultArenaName;
    private final Map<String, MemrisArena> arenas = new ConcurrentHashMap<>();
    private final Map<String, MemrisConfigurationProperties> arenaConfigs;

    public MemrisArenaProviderImpl(MemrisRepositoryFactory defaultFactory,
            MemrisArenaProperties arenaProperties) {
        this.defaultFactory = defaultFactory;
        this.defaultArenaName = arenaProperties.getDefaultArena();
        this.arenaConfigs = arenaProperties.getArenas();
    }

    @Override
    public MemrisArena getDefaultArena() {
        return getArena(defaultArenaName);
    }

    @Override
    public MemrisArena getArena(String name) {
        var arenaName = name == null || name.isBlank() ? defaultArenaName : name;
        return arenas.computeIfAbsent(arenaName, this::createArena);
    }

    private MemrisArena createArena(String name) {
        var arenaName = name == null || name.isBlank() ? defaultArenaName : name;
        var configProps = arenaConfigs.get(arenaName);
        if (configProps == null || defaultArenaName.equals(arenaName)) {
            return defaultFactory.createArena();
        }
        MemrisConfiguration config = configProps.toConfiguration();
        var factory = new MemrisRepositoryFactory(config);
        return factory.createArena();
    }
}
