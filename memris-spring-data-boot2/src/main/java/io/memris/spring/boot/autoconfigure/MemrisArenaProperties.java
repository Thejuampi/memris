package io.memris.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Binds top-level Memris arena configuration properties.
 */
@ConfigurationProperties("memris")
public class MemrisArenaProperties {
    private String defaultArena = "default";
    private Map<String, MemrisConfigurationProperties> arenas = new HashMap<>();

    /**
     * Creates properties with default arena name and empty arena map.
     */
    public MemrisArenaProperties() {
    }

    /**
     * Returns the name of the default arena.
     *
     * @return default arena name
     */
    public String getDefaultArena() {
        return defaultArena;
    }

    /**
     * Sets the name of the default arena.
     *
     * @param defaultArena default arena name
     */
    public void setDefaultArena(String defaultArena) {
        this.defaultArena = defaultArena;
    }

    /**
     * Returns configured arenas keyed by name.
     *
     * @return arena configuration map
     */
    public Map<String, MemrisConfigurationProperties> getArenas() {
        return arenas;
    }

    /**
     * Sets configured arenas keyed by name.
     *
     * @param arenas arena configuration map
     */
    public void setArenas(Map<String, MemrisConfigurationProperties> arenas) {
        this.arenas = arenas;
    }
}
