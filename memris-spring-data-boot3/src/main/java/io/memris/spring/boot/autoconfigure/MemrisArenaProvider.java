package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisArena;

/**
 * Provides access to Memris arenas configured in the Spring context.
 */
public interface MemrisArenaProvider {
    /**
     * Returns the default arena.
     *
     * @return default Memris arena
     */
    MemrisArena getDefaultArena();

    /**
     * Returns an arena by name.
     *
     * @param name arena name
     * @return resolved Memris arena
     */
    MemrisArena getArena(String name);
}
