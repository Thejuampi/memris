package io.memris.spring.boot.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties("memris")
public class MemrisArenaProperties {
    private String defaultArena = "default";
    private Map<String, MemrisConfigurationProperties> arenas = new HashMap<>();
}
