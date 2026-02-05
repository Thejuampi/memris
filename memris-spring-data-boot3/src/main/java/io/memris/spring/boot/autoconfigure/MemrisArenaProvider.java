package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisArena;

public interface MemrisArenaProvider {
    MemrisArena getDefaultArena();

    MemrisArena getArena(String name);
}
