package io.memris.realworld;

import io.memris.core.MemrisConfiguration;
import io.memris.core.MemrisConfiguration.TableImplementation;

/** Shared, small arena configuration for repository scenarios and their execution matrix. */
public final class RealWorldSupport {
    private RealWorldSupport() {
    }

    public static MemrisConfiguration.Builder configuration() {
        return MemrisConfiguration.builder()
                .pageSize(128).maxPages(1024).initialPages(1)
                .codegenEnabled(Boolean.parseBoolean(System.getProperty("memris.test.codegen", "true")))
                .tableImplementation(TableImplementation.valueOf(
                        System.getProperty("memris.test.table", "BYTECODE")));
    }
}
