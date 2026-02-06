package io.memris.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("memris")
public class MemrisArenaProperties {
    private String defaultArena = "default";
    private Map<String, MemrisConfigurationProperties> arenas = new HashMap<>();

    public String getDefaultArena() {
        return defaultArena;
    }

    public void setDefaultArena(String defaultArena) {
        this.defaultArena = defaultArena;
    }

    public Map<String, MemrisConfigurationProperties> getArenas() {
        return arenas;
    }

    public void setArenas(Map<String, MemrisConfigurationProperties> arenas) {
        this.arenas = arenas;
    }
}
